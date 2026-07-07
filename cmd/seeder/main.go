package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/almeidadiego/webhook-engine/internal/domain"
	"github.com/almeidadiego/webhook-engine/internal/infra/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("config error", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx := context.Background()

	pgPool, err := newPostgresPool(ctx, cfg.PostgresURL)
	if err != nil {
		logger.Error("postgres connection failed", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	repo := repository.NewPostgresJobRepository(pgPool)
	tenantID := cfg.TenantID

	inserted := 0
	conflicts := 0

	for i := 0; i < cfg.Count; i++ {
		job := &domain.ScheduledJob{
			TenantID:       tenantID,
			IdempotencyKey: uuid.New().String(),
			URL:            cfg.TargetURL,
			HTTPMethod:     "POST",
			RequestHeaders: map[string]string{"Content-Type": "application/json"},
			RequestBody:    []byte(fmt.Sprintf(`{"seeder_index":%d,"ts":"%s"}`, i, time.Now().Format(time.RFC3339))),
			ScheduleAt:     time.Now(),
			Status:         domain.StatusPending,
			MaxAttempts:    5,
		}

		result, err := repo.Insert(ctx, job)
		if err != nil {
			if errors.Is(err, domain.ErrIdempotencyKeyExists) {
				conflicts++
				logger.Warn("idempotency conflict (should not happen with UUIDs)", "key", job.IdempotencyKey, "existing_id", result.ID)
				continue
			}
			logger.Error("insert failed", "error", err)
			os.Exit(1)
		}

		inserted++
		logger.Info("job inserted", "id", result.ID, "key", result.IdempotencyKey)
	}

	logger.Info("seeding complete",
		"inserted", inserted,
		"conflicts", conflicts,
		"total_attempted", cfg.Count,
	)
}

type seederConfig struct {
	PostgresURL string
	TargetURL   string
	TenantID    uuid.UUID
	Count       int
}

func loadConfig() (seederConfig, error) {
	postgresURL := os.Getenv("DATABASE_URL")
	if postgresURL == "" {
		return seederConfig{}, errors.New("DATABASE_URL is required")
	}

	tenantIDStr := os.Getenv("SEEDER_TENANT_ID")
	tenantID := uuid.Nil
	if tenantIDStr != "" {
		parsed, err := uuid.Parse(tenantIDStr)
		if err != nil {
			return seederConfig{}, fmt.Errorf("invalid SEEDER_TENANT_ID: %w", err)
		}
		tenantID = parsed
	}

	count := 1
	if countStr := os.Getenv("SEEDER_COUNT"); countStr != "" {
		parsed, err := strconv.Atoi(countStr)
		if err != nil || parsed < 1 {
			return seederConfig{}, fmt.Errorf("invalid SEEDER_COUNT: %s", countStr)
		}
		count = parsed
	}

	targetURL := os.Getenv("SEEDER_TARGET_URL")
	if targetURL == "" {
		targetURL = "http://localhost:9999/webhook"
	}

	return seederConfig{
		PostgresURL: postgresURL,
		TargetURL:   targetURL,
		TenantID:    tenantID,
		Count:       count,
	}, nil
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
