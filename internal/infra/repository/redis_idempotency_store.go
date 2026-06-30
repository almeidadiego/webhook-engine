package repository

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisIdempotencyStore struct {
	Client *redis.Client
}

func NewRedisIdempotencyStore(client *redis.Client) *RedisIdempotencyStore {
	return &RedisIdempotencyStore{
		Client: client,
	}
}

func (s *RedisIdempotencyStore) CheckAndSet(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	cmd := s.Client.SetArgs(ctx, "idemp:"+key, "p", redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	})

	if err := cmd.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return true, nil // já existia
		}

		return false, err // erro real
	}

	return false, nil // gravou com sucesso
}

func (s *RedisIdempotencyStore) UpdateTTL(ctx context.Context, key string, ttl time.Duration) error {
	return s.Client.Expire(ctx, "idemp:"+key, ttl).Err()
}

func (s *RedisIdempotencyStore) Delete(ctx context.Context, key string) error {
	return s.Client.Del(ctx, "idemp:"+key).Err()
}
