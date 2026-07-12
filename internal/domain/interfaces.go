package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// JobRepository defines how we persist jobs
type JobRepository interface {
	// Insert creates a new scheduled job. Returns ErrIdempotencyKeyExists
	// if a job with the same tenant_id + idempotency_key already exists.
	// On conflict, the existing job is returned alongside the error.
	Insert(ctx context.Context, job *ScheduledJob) (*ScheduledJob, error)

	// FetchNextPending claims and returns jobs atomically.
	// Uses UPDATE ... FOR UPDATE SKIP LOCKED so multiple workers
	// never process the same job. The returned jobs are already
	// marked as 'processing' with the given workerID.
	FetchNextPending(ctx context.Context, workerID uuid.UUID, limit int) ([]*ScheduledJob, error)

	// Update saves the final or intermediate state (success, failure, or reschedule)
	// Updates status, attempt_count, schedule_at, and last_error_message
	Update(ctx context.Context, job *ScheduledJob) error

	// SaveExecution saves detailed history to the job_executions table
	SaveExecution(ctx context.Context, exec *ExecutionRecord) error
}

type IdempotencyStore interface {
	// CheckAndSet attempts to write the key. Returns true if it already exists (duplicate).
	// The ttl defines how long this initial "lock" will last.
	CheckAndSet(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// UpdateTTL extends the key's lifetime (e.g., from 1 min to 24h after success)
	UpdateTTL(ctx context.Context, key string, ttl time.Duration) error

	// Delete removes the key (used on failure to allow retry)
	Delete(ctx context.Context, key string) error
}
