package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Dummy HTTP server that counts POST requests per job key (last path segment)
// and atomically writes the counts to a JSON file after each delivery.
//
// This is the load-test counterpart of the webhook worker: it simulates a
// downstream webhook target (Stripe, Slack, etc.) with optional realistic
// latency.
//
// WHY GO INSTEAD OF PYTHON:
// The previous Python implementation used http.server.HTTPServer, which is
// single-threaded: concurrent delayed requests were serialized, so the dummy
// server itself became the bottleneck of chaos/load experiments instead of the
// webhook engine being measured. Go's net/http serves each request in its own
// goroutine — the simulated latency is paid concurrently, exactly like a real
// HTTP target — so the only thing left to saturate is the engine's semaphore.
//
// Usage:
//
//	DUMMY_PORT=9999 go run ./cmd/dummy-server/                            # instant (baseline)
//	DUMMY_MIN_DELAY_MS=100 DUMMY_MAX_DELAY_MS=300 go run ./cmd/dummy-server/   # realistic
//	DUMMY_MIN_DELAY_MS=500 DUMMY_MAX_DELAY_MS=1500 go run ./cmd/dummy-server/  # stressed

type config struct {
	Port           string
	MinDelayMs     int
	MaxDelayMs     int
	DeliveriesFile string
}

type server struct {
	// mu guards BOTH the counts map and the file write. net/http handles each
	// request in its own goroutine, so without this lock two requests could:
	//   (a) race on the map (concurrent map writes = runtime panic), and
	//   (b) interleave the tmp-file write + rename — one goroutine could rename
	//       a partially-written temp file created by the other (readers would
	//       see garbage or, worse, an absent file).
	// One mutex keeps the "read counts → marshal → write tmp → rename" cycle
	// indivisible, which is what makes the file safe for external readers
	// (jq/python in the validation scripts) at any instant.
	mu         sync.Mutex
	counts     map[string]int
	deliveries string
	minDelayMs int
	maxDelayMs int
}

func (s *server) handlePost(w http.ResponseWriter, r *http.Request) {
	// Simulate realistic downstream latency BEFORE processing. The worker holds
	// its semaphore slot for the full request duration — the realistic external
	// target behavior. Because each HTTP request here runs in its own
	// goroutine, N concurrent delayed deliveries sleep in parallel (the Python
	// HTTPServer serialized them, capping N at 1 regardless of the worker's
	// concurrency setting).
	if s.maxDelayMs > 0 {
		delay := s.minDelayMs + rand.IntN(s.maxDelayMs-s.minDelayMs+1)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	// Job key = last path segment, mirroring the Python version's
	// self.path.split('/')[-1]: "/webhook/abc" → "abc", "/webhook" → "webhook".
	// path.Base("") == "." and path.Base("/") == "." or "/" edge cases do not
	// occur here: the engine always POSTs to a non-empty path prefix + job id.
	key := path.Base(r.URL.Path)

	s.mu.Lock()
	s.counts[key]++
	s.flushLocked()
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// flushLocked writes the counts atomically. Caller must hold s.mu.
func (s *server) flushLocked() {
	data, err := json.MarshalIndent(s.counts, "", "  ")
	if err != nil {
		slog.Error("marshal counts", "error", err)
		return
	}
	tmp := s.deliveries + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		slog.Error("write deliveries temp", "error", err)
		return
	}
	if err := os.Rename(tmp, s.deliveries); err != nil {
		slog.Error("rename deliveries", "error", err)
	}
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}
	// Parse once in loadConfig so the delay arithmetic in the hot path stays
	// error-free and the config is validated (e.g. negative values) up front.
	srv := &server{
		counts:     make(map[string]int),
		deliveries: cfg.DeliveriesFile,
		minDelayMs: cfg.MinDelayMs,
		maxDelayMs: cfg.MaxDelayMs,
	}

	// Start from a clean slate so per-run counters never mix with a previous
	// run's deliveries file (stale file would inflate the duplicates verdict).
	if err := os.Remove(cfg.DeliveriesFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Error("remove stale deliveries file", "error", err)
	}

	mux := http.NewServeMux()
	// Register for "/" so ANY path is accepted and counted by its last
	// segment; the method check inside keeps /health (or other methods) out.
	mux.HandleFunc("/", srv.handlePost)
	mux.HandleFunc("/health", srv.handleHealth)

	httpServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info(
		"dummy server listening",
		"port", cfg.Port,
		"delay_min_ms", cfg.MinDelayMs,
		"delay_max_ms", cfg.MaxDelayMs,
		"deliveries_file", cfg.DeliveriesFile,
	)

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown requested, draining in-flight requests")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Warn("graceful shutdown incomplete", "error", err)
	}
	slog.Info("dummy server stopped")
}

func loadConfig() (config, error) {
	port := getEnv("DUMMY_PORT", "9999")

	minDelayMs, err := getEnvInt("DUMMY_MIN_DELAY_MS", 0)
	if err != nil {
		return config{}, err
	}

	maxDelayMs, err := getEnvInt("DUMMY_MAX_DELAY_MS", 0)
	if err != nil {
		return config{}, err
	}

	if minDelayMs < 0 {
		return config{}, fmt.Errorf("DUMMY_MIN_DELAY_MS must be >= 0, got %d", minDelayMs)
	}
	// maxDelayMs == 0 means "no artificial delay"; a negative max is a config
	// mistake and would make rand.IntN's argument negative (panics at runtime
	// on the first request — better to fail at startup).
	if maxDelayMs < 0 {
		return config{}, fmt.Errorf("DUMMY_MAX_DELAY_MS must be >= 0, got %d", maxDelayMs)
	}

	deliveriesFile := getEnv("DUMMY_DELIVERIES_FILE", "/tmp/webhook-deliveries.json")

	return config{
		Port:           port,
		MinDelayMs:     minDelayMs,
		MaxDelayMs:     maxDelayMs,
		DeliveriesFile: deliveriesFile,
	}, nil
}

// getEnv returns the environment variable value or the fallback.
// Mirrors cmd/worker/main.go's helper.
func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// getEnvInt parses an integer env var, falling back when unset.
// Mirrors cmd/worker/main.go's helper.
func getEnvInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}

	return parsed, nil
}
