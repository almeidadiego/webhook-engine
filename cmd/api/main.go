package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/almeidadiego/webhook-engine/internal/domain"
	"github.com/almeidadiego/webhook-engine/internal/infra/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// config holds the API's runtime configuration. It mirrors the worker's pattern
// (env vars with typed defaults) so both entrypoints behave consistently during
// deployment and local development.
type config struct {
	PostgresURL  string     // DATABASE_URL — required; same pool as the worker uses.
	Addr         string     // API_ADDR — default ":8080" so load testing tools can reach us.
	LogLevel     slog.Level // LOG_LEVEL — default info, keep the ingestion path quiet.
	MaxBodyBytes int64      // API_MAX_BODY_BYTES — default 1MB; prevents memory exhaustion from large payloads.
}

// createJobRequest is the JSON envelope accepted by POST /jobs.
// We keep it close to the domain model but decouple wire format from storage.
type createJobRequest struct {
	TenantID       string            `json:"tenant_id"`
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers"`
	Body           json.RawMessage   `json:"body"`
	ScheduleAt     string            `json:"schedule_at"` // RFC3339
	IdempotencyKey string            `json:"idempotency_key"`
	MaxAttempts    int               `json:"max_attempts"`
}

// api groups the HTTP handler dependencies. It is intentionally thin: the API
// is an ingestion layer, so all business logic stays in the domain/repository.
type api struct {
	repo         domain.JobRepository
	pool         *pgxpool.Pool // for pool stats endpoint — exposes DB connection saturation for load testing.
	log          *slog.Logger
	maxBodyBytes int64 // per-request body limit carried from config into handlers.
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		panic(fmt.Errorf("load config: %w", err))
	}

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	// signal.NotifyContext gives us a context that is cancelled on SIGINT/SIGTERM.
	// We use this as the lifecycle signal for the server and the database pool.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pgPool, err := newPostgresPool(ctx, cfg.PostgresURL)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	// We close the pool after the server has drained, not via defer, because
	// Shutdown must finish before closing connections.

	repo := repository.NewPostgresJobRepository(pgPool)

	api := &api{repo: repo, pool: pgPool, log: logger, maxBodyBytes: cfg.MaxBodyBytes}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", api.handleCreateJob)
	mux.HandleFunc("/metrics/pool", api.handlePoolStats)

	// pprof handlers are registered explicitly so they live on the same mux
	// instead of the default serve mux. This keeps the API self-contained and
	// easy to reason about during load testing.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}

	go func() {
		logger.Info("api server listening", "addr", cfg.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown requested, draining server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	pgPool.Close()
	logger.Info("api finished")
}

// handleCreateJob receives webhook job submissions from load testing tools,
// validates them, and persists them via PostgresJobRepository.Insert.
// The handler is deliberately minimal to stay fast under high concurrency.
func (a *api) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var jobID string

	rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		// Request logging captures the critical path for bottleneck investigation:
		// method, path, status, duration, and the created job ID when available.
		a.log.Info("request handled",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration", time.Since(start),
			"job_id", jobID,
		)
	}()

	// MaxBytesReader prevents memory exhaustion from large payloads during k6 runs.
	r.Body = http.MaxBytesReader(rec, r.Body, a.maxBodyBytes)
	defer r.Body.Close()

	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeJSON(rec, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
			return
		}
		writeJSON(rec, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	// Basic validation: we reject malformed input early so the worker never has to.
	if err := validateCreateJobRequest(&req); err != nil {
		writeJSON(rec, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	job, err := a.buildJob(&req)
	if err != nil {
		writeJSON(rec, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := a.repo.Insert(r.Context(), job)
	if err != nil {
		if errors.Is(err, domain.ErrIdempotencyKeyExists) {
			writeJSON(rec, http.StatusConflict, map[string]string{
				"error":       "idempotency key already exists",
				"existing_id": result.ID.String(),
			})
			return
		}
		a.log.Error("failed to insert job", "error", err)
		writeJSON(rec, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	jobID = result.ID.String()
	slog.Info("job created",
		"id", jobID,
		"status", result.Status,
		"idempotency_key", result.IdempotencyKey,
	)

	writeJSON(rec, http.StatusCreated, map[string]interface{}{
		"id":              jobID,
		"status":          result.Status,
		"idempotency_key": result.IdempotencyKey,
	})
}

// validateCreateJobRequest enforces the minimal contract for a webhook job.
// URL and HTTP method correctness are checked here so the worker pool does not
// waste cycles on undeliverable requests.
func validateCreateJobRequest(req *createJobRequest) error {
	if req.IdempotencyKey == "" {
		return errors.New("idempotency_key is required")
	}
	if req.URL == "" {
		return errors.New("url is required")
	}
	if !strings.HasPrefix(req.URL, "http://") && !strings.HasPrefix(req.URL, "https://") {
		return errors.New("url must start with http:// or https://")
	}
	if req.Method != "" {
		switch strings.ToUpper(req.Method) {
		case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			return errors.New("method must be one of GET, POST, PUT, PATCH, DELETE")
		}
	}
	return nil
}

// buildJob maps the wire request into a domain.ScheduledJob, applying sensible
// defaults for load testing: POST method, immediate scheduling, and 5 retries.
func (a *api) buildJob(req *createJobRequest) (*domain.ScheduledJob, error) {
	var tenantID uuid.UUID
	if req.TenantID != "" {
		parsed, err := uuid.Parse(req.TenantID)
		if err != nil {
			return nil, errors.New("invalid tenant_id")
		}
		tenantID = parsed
	}

	method := strings.ToUpper(req.Method)
	if method == "" {
		method = http.MethodPost
	}

	scheduleAt := time.Now()
	if req.ScheduleAt != "" {
		parsed, err := time.Parse(time.RFC3339, req.ScheduleAt)
		if err != nil {
			return nil, errors.New("invalid schedule_at: expected RFC3339")
		}
		scheduleAt = parsed
	}

	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}

	headers := req.Headers
	if headers == nil {
		headers = map[string]string{}
	}

	var body []byte
	if len(req.Body) > 0 {
		body = []byte(req.Body)
	}

	return &domain.ScheduledJob{
		TenantID:       tenantID,
		IdempotencyKey: req.IdempotencyKey,
		URL:            req.URL,
		HTTPMethod:     method,
		RequestHeaders: headers,
		RequestBody:    body,
		ScheduleAt:     scheduleAt,
		Status:         domain.StatusPending,
		AttemptCount:   0,
		MaxAttempts:    maxAttempts,
	}, nil
}

// handlePoolStats exposes live pgxpool statistics. During load testing this
// reveals connection saturation (acquired vs idle vs max) so we can correlate
// API latency with Postgres pool exhaustion.
func (a *api) handlePoolStats(w http.ResponseWriter, r *http.Request) {
	stats := a.pool.Stat()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"acquire_count":          stats.AcquireCount(),
		"acquired_conns":         stats.AcquiredConns(),
		"idle_conns":             stats.IdleConns(),
		"max_conns":              stats.MaxConns(),
		"total_conns":            stats.TotalConns(),
		"max_idle_destroyed":     stats.MaxIdleDestroyCount(),
		"max_lifetime_destroyed": stats.MaxLifetimeDestroyCount(),
	})
}

// writeJSON is a small helper that sets Content-Type, status, and encodes JSON.
// Encoding errors are ignored because the headers are already committed.
func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

// responseRecorder wraps http.ResponseWriter to capture the status code for
// request logging without losing the behavior of the underlying writer.
type responseRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

func (rr *responseRecorder) WriteHeader(status int) {
	rr.status = status
	rr.written = true
	rr.ResponseWriter.WriteHeader(status)
}

func (rr *responseRecorder) Write(p []byte) (int, error) {
	if !rr.written {
		rr.status = http.StatusOK
		rr.written = true
	}
	return rr.ResponseWriter.Write(p)
}

// loadConfig reads environment variables with the same helper style as the worker.
func loadConfig() (config, error) {
	addr := getEnv("API_ADDR", ":8080")
	logLevel, err := parseLogLevel(getEnv("LOG_LEVEL", "info"))
	if err != nil {
		return config{}, err
	}
	maxBodyBytes, err := getEnvInt64("API_MAX_BODY_BYTES", 1<<20)
	if err != nil {
		return config{}, err
	}
	postgresURL := os.Getenv("DATABASE_URL")
	if postgresURL == "" {
		return config{}, errors.New("DATABASE_URL is required")
	}
	return config{
		PostgresURL:  postgresURL,
		Addr:         addr,
		LogLevel:     logLevel,
		MaxBodyBytes: maxBodyBytes,
	}, nil
}

// newLogger creates a text slog handler with the configured level. Same as the
// worker so both binaries emit the same log format.
func newLogger(level slog.Level) *slog.Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	return slog.New(handler)
}

// newPostgresPool parses DATABASE_URL, creates a pgxpool, and verifies connectivity
// with a ping. Copied from the worker to keep the entrypoint self-contained.
func newPostgresPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// getEnv returns the environment variable value or a fallback.
func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// getEnvInt64 parses an int64 environment variable, returning the fallback if
// the variable is not set.
func getEnvInt64(key string, fallback int64) (int64, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

// parseLogLevel converts a string level into a slog.Level.
func parseLogLevel(value string) (slog.Level, error) {
	switch value {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid LOG_LEVEL: %s", value)
	}
}
