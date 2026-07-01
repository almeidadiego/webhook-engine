package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// JobRepository defines how we persist jobs
type JobRepository interface {
	// FetchNextPending retrieves jobs ready for execution (pending and schedule_at <= now)
	FetchNextPending(ctx context.Context, limit int) ([]*ScheduledJob, error)

	// Claim marks the job as 'processing', sets worker_id and started_at
	// Here you would use "FOR UPDATE SKIP LOCKED" as discussed
	Claim(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error

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
