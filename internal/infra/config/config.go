package config

import (
	"os"
	"strconv"
	"time"

	"github.com/almeidadiego/webhook-engine/internal/domain"
)

type Config struct {
	DBURL    string
	RedisURL string
	Worker   domain.WorkerConfig // Domain config embedded here
}

func Load() *Config {
	maxConn := getEnvInt("MAX_CONCURRENCY", 20)

	return &Config{
		DBURL:    os.Getenv("DATABASE_URL"),
		RedisURL: os.Getenv("REDIS_URL"),
		Worker: domain.WorkerConfig{
			MaxConcurrency: maxConn,
			// BatchSize: we fetch double the capacity to ensure
			// the worker always has work while other jobs are in IO
			BatchSize:      maxConn * 2,
			BaseRetryDelay: time.Duration(getEnvInt("RETRY_DELAY_SEC", 10)) * time.Second,
		},
	}
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
