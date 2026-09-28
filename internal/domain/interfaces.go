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

	// CompleteJob transitions a job from 'processing' to 'completed'.
	// Returns (true, nil) if the transition succeeded.
	// Returns (false, nil) if the job was no longer in 'processing' state
	// (e.g., reclaimed by the reaper) — the caller lost the claim race.
	// Returns (false, err) on infrastructure failures.
	CompleteJob(ctx context.Context, job *ScheduledJob) (bool, error)

	// RescheduleJob transitions a job from 'processing' to 'pending' with a new
	// schedule_at (for retry with backoff). Returns (false, nil) if the claim
	// was lost to another worker or the reaper.
	RescheduleJob(ctx context.Context, job *ScheduledJob) (bool, error)

	// FailJob transitions a job from 'processing' to 'failed' (terminal state,
	// max attempts exhausted). Returns (false, nil) if the claim was lost.
	FailJob(ctx context.Context, job *ScheduledJob) (bool, error)

	// ReleaseClaim transitions a job from 'processing' back to 'pending' with
	// schedule_at = NOW() for immediate re-pickup. Used when the worker must
	// abort before attempting delivery (e.g., Redis idempotency check failed).
	// Returns (false, nil) if the claim was already lost.
	ReleaseClaim(ctx context.Context, job *ScheduledJob) (bool, error)

	// SaveExecution saves detailed history to the job_executions table
	SaveExecution(ctx context.Context, exec *ExecutionRecord) error

	// MarkDelivered records the external-world fact that the webhook reached the
	// downstream. Unlike status transitions, this is NOT guarded: it is a monotonic
	// fact (NULL -> timestamp), idempotent via COALESCE (first write wins), and
	// independent of claim ownership. This survives a lost claim race.
	MarkDelivered(ctx context.Context, jobID uuid.UUID) error

	// ReclaimStaleJobs finds jobs stuck in 'processing' status longer than staleThreshold
	// and resets them to 'pending' for reprocessing. Uses FOR UPDATE SKIP LOCKED to safely
	// cooperate with other reapers in multi-replica deployments.
	// Returns the number of jobs reclaimed.
	ReclaimStaleJobs(ctx context.Context, staleThreshold time.Duration, limit int) (int64, error)
}

type IdempotencyStore interface {
	// CheckAndSet attempts to write a tenant-scoped idempotency key.
	// Returns true if the key already exists (duplicate for this tenant).
	// The key is scoped as "idemp:{tenantID}:{key}" to prevent cross-tenant collisions.
	CheckAndSet(ctx context.Context, tenantID uuid.UUID, key string, ttl time.Duration) (bool, error)

	// UpdateTTL extends the lifetime of a tenant-scoped idempotency key
	// (e.g., from 5min to 24h after successful delivery).
	UpdateTTL(ctx context.Context, tenantID uuid.UUID, key string, ttl time.Duration) error

	// Delete removes a tenant-scoped idempotency key (used on failure to allow retry).
	Delete(ctx context.Context, tenantID uuid.UUID, key string) error
}
