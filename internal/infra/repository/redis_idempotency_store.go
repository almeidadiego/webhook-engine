package repository

import (
	"context"
	"errors"
	"net"
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
	cmd := s.Client.SetArgs(
		ctx,
		"idemp:"+key,
		"p",
		redis.SetArgs{
			Mode: "NX",
			TTL:  ttl,
		},
	)

	if err := cmd.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return true, nil // already exists
		}

		return false, err // real error
	}

	return false, nil // successfully written
}

func (s *RedisIdempotencyStore) UpdateTTL(ctx context.Context, key string, ttl time.Duration) error {
	const maxAttempts = 3
	const retryInterval = 2 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check context before each attempt (including the first)
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = s.Client.Expire(ctx, "idemp:"+key, ttl).Err()
		if lastErr == nil {
			return nil // success
		}

		// If error is not transient, fail immediately
		if !isTransientRedisError(lastErr) {
			return lastErr
		}

		// If this was the last attempt, return the error
		if attempt == maxAttempts {
			break
		}

		// Wait before retrying, but respect context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
			// continue to next attempt
		}
	}

	return lastErr
}

func (s *RedisIdempotencyStore) Delete(ctx context.Context, key string) error {
	const maxAttempts = 3
	const retryInterval = 2 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check context before each attempt (including the first)
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = s.Client.Del(ctx, "idemp:"+key).Err()
		if lastErr == nil {
			return nil // success
		}

		// If error is not transient, fail immediately
		if !isTransientRedisError(lastErr) {
			return lastErr
		}

		// If this was the last attempt, return the error
		if attempt == maxAttempts {
			break
		}

		// Wait before retrying, but respect context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
			// continue to next attempt
		}
	}

	return lastErr
}

// isTransientRedisError classifies go-redis errors to determine if a retry is worthwhile.
// Returns true for connection failures and network errors.
// Returns false for context cancellation and permanent errors.
func isTransientRedisError(err error) bool {
	if err == nil {
		return false
	}

	// Context cancellation is not transient — retrying won't help
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Network errors (connection refused, timeout, etc.)
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}

	// go-redis specific connection errors
	if errors.Is(err, redis.ErrClosed) {
		return true
	}

	return false
}
