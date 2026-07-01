package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/almeidadiego/webhook-engine/internal/domain"
	"github.com/almeidadiego/webhook-engine/internal/infra/repository"
	"github.com/almeidadiego/webhook-engine/internal/usecase"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type config struct {
	PostgresURL    string
	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	PollInterval   time.Duration
	MaxConcurrency int
	BaseRetryDelay time.Duration
	BatchSize      int
	LogLevel       slog.Level
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		panic(fmt.Errorf("load config: %w", err))
	}

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pgPool, err := newPostgresPool(ctx, cfg.PostgresURL)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	redisClient, err := newRedisClient(ctx, cfg)
	if err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Warn("failed to close redis", "error", err)
		}
	}()

	jobRepo := repository.NewPostgresJobRepository(pgPool)
	idempotencyStore := repository.NewRedisIdempotencyStore(redisClient)

	workerCfg := domain.WorkerConfig{
		MaxConcurrency: cfg.MaxConcurrency,
		BaseRetryDelay: cfg.BaseRetryDelay,
	}

	worker := usecase.NewWorkerService(jobRepo, idempotencyStore, workerCfg)

	logger.Info(
		"worker started",
		"poll_interval", cfg.PollInterval.String(),
		"max_concurrency", cfg.MaxConcurrency,
		"base_retry_delay", cfg.BaseRetryDelay.String(),
	)

	run(ctx, logger, worker, cfg.PollInterval)

	logger.Info("shutting down worker, waiting for in-flight jobs")
	worker.Stop()
	logger.Info("worker finished")
}

func run(
	ctx context.Context,
	logger *slog.Logger,
	worker *usecase.WorkerService,
	pollInterval time.Duration,
) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	worker.ExecuteCycle(ctx)

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutdown solicitado")
			return
		case <-ticker.C:
			worker.ExecuteCycle(ctx)
		}
	}
}

func loadConfig() (config, error) {
	pollInterval, err := getEnvDuration("WORKER_POLL_INTERVAL", 2*time.Second)
	if err != nil {
		return config{}, err
	}

	baseRetryDelay, err := getEnvDuration("WORKER_BASE_RETRY_DELAY", 30*time.Second)
	if err != nil {
		return config{}, err
	}

	maxConcurrency, err := getEnvInt("WORKER_MAX_CONCURRENCY", 10)
	if err != nil {
		return config{}, err
	}

	redisDB, err := getEnvInt("REDIS_DB", 0)
	if err != nil {
		return config{}, err
	}

	batchSize, err := getEnvInt("WORKER_BATCH_SIZE", 20)
	if err != nil {
		return config{}, err
	}

	logLevel, err := parseLogLevel(getEnv("LOG_LEVEL", "info"))
	if err != nil {
		return config{}, err
	}

	postgresURL := os.Getenv("DATABASE_URL")
	if postgresURL == "" {
		return config{}, errors.New("DATABASE_URL is required")
	}

	return config{
		PostgresURL:    postgresURL,
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  getEnv("REDIS_PASSWORD", ""),
		RedisDB:        redisDB,
		PollInterval:   pollInterval,
		MaxConcurrency: maxConcurrency,
		BaseRetryDelay: baseRetryDelay,
		BatchSize:      batchSize,
		LogLevel:       logLevel,
	}, nil
}

func newLogger(level slog.Level) *slog.Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	return slog.New(handler)
}

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

func newRedisClient(ctx context.Context, cfg config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return client, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s inválido: %w", key, err)
	}

	return parsed, nil
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}

	return parsed, nil
}

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
