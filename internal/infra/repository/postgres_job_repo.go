package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/almeidadiego/webhook-engine/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresJobRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresJobRepository(pool *pgxpool.Pool) *PostgresJobRepository {
	return &PostgresJobRepository{pool: pool}
}

func (r *PostgresJobRepository) Insert(ctx context.Context, job *domain.ScheduledJob) (*domain.ScheduledJob, error) {
	headersJSON, err := json.Marshal(job.RequestHeaders)
	if err != nil {
		return nil, err
	}

	query := `
		INSERT INTO scheduled_jobs (
			tenant_id, idempotency_key, url, http_method,
			request_headers, request_body, schedule_at, status,
			attempt_count, max_attempts
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
		RETURNING id, tenant_id, idempotency_key, url, http_method,
		          request_headers, request_body, schedule_at, status,
		          attempt_count, max_attempts, created_at, updated_at`

	var result domain.ScheduledJob
	var rawHeaders []byte
	err = r.pool.QueryRow(ctx, query,
		job.TenantID, job.IdempotencyKey, job.URL, job.HTTPMethod,
		headersJSON, job.RequestBody, job.ScheduleAt, job.Status,
		job.AttemptCount, job.MaxAttempts,
	).Scan(
		&result.ID, &result.TenantID, &result.IdempotencyKey,
		&result.URL, &result.HTTPMethod, &rawHeaders, &result.RequestBody,
		&result.ScheduleAt, &result.Status, &result.AttemptCount,
		&result.MaxAttempts, &result.CreatedAt, &result.UpdatedAt,
	)
	if err != nil {
		if isNoRows(err) {
			existing, fetchErr := r.getByIdempotencyKey(ctx, job.TenantID, job.IdempotencyKey)
			if fetchErr != nil {
				return nil, fetchErr
			}
			return existing, domain.ErrIdempotencyKeyExists
		}
		return nil, err
	}

	json.Unmarshal(rawHeaders, &result.RequestHeaders)
	return &result, nil
}

func (r *PostgresJobRepository) getByIdempotencyKey(ctx context.Context, tenantID uuid.UUID, key string) (*domain.ScheduledJob, error) {
	query := `
		SELECT id, tenant_id, idempotency_key, url, http_method,
		       request_headers, request_body, schedule_at, status,
		       attempt_count, max_attempts, created_at, updated_at
		FROM scheduled_jobs
		WHERE tenant_id = $1 AND idempotency_key = $2`

	var job domain.ScheduledJob
	var rawHeaders []byte
	err := r.pool.QueryRow(ctx, query, tenantID, key).Scan(
		&job.ID, &job.TenantID, &job.IdempotencyKey,
		&job.URL, &job.HTTPMethod, &rawHeaders, &job.RequestBody,
		&job.ScheduleAt, &job.Status, &job.AttemptCount,
		&job.MaxAttempts, &job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		if isNoRows(err) {
			return nil, domain.ErrJobNotFound
		}
		return nil, err
	}

	json.Unmarshal(rawHeaders, &job.RequestHeaders)
	return &job, nil
}

func (r *PostgresJobRepository) FetchNextPending(ctx context.Context, workerID uuid.UUID, limit int) ([]*domain.ScheduledJob, error) {
	query := `
		UPDATE scheduled_jobs
		SET status = 'processing', worker_id = $1, started_at = NOW()
		WHERE id IN (
			SELECT id FROM scheduled_jobs
			WHERE status = 'pending' AND schedule_at <= NOW()
			ORDER BY schedule_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, tenant_id, idempotency_key, url, http_method,
		          request_headers, request_body, attempt_count, max_attempts`

	rows, err := r.pool.Query(ctx, query, workerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*domain.ScheduledJob
	for rows.Next() {
		var j domain.ScheduledJob
		var headers []byte
		err := rows.Scan(&j.ID, &j.TenantID, &j.IdempotencyKey, &j.URL, &j.HTTPMethod, &headers, &j.RequestBody, &j.AttemptCount, &j.MaxAttempts)
		if err != nil {
			return nil, err
		}
		json.Unmarshal(headers, &j.RequestHeaders)
		jobs = append(jobs, &j)
	}

	return jobs, nil
}

// CompleteJob transitions processing → completed with a state guard.
// The WHERE status = 'processing' clause ensures we never overwrite a
// concurrent state change (e.g., reaper reclaim). This is Compare-And-Swap
// at the database level — a foundational pattern for distributed state machines.
func (r *PostgresJobRepository) CompleteJob(ctx context.Context, job *domain.ScheduledJob) (bool, error) {
	query := `
		UPDATE scheduled_jobs
		SET status = 'completed',
		    attempt_count = $1,
		    last_attempt_at = NOW(),
		    last_response_status_code = $2,
		    last_error_message = NULL,
		    worker_id = NULL,
		    started_at = NULL,
		    updated_at = NOW()
		WHERE id = $3 AND status = 'processing'`

	result, err := r.pool.Exec(ctx, query,
		job.AttemptCount, job.LastResponseCode, job.ID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// RescheduleJob transitions processing → pending with a new schedule_at (retry backoff).
// The state guard prevents overwriting a concurrent reaper reclaim.
func (r *PostgresJobRepository) RescheduleJob(ctx context.Context, job *domain.ScheduledJob) (bool, error) {
	query := `
		UPDATE scheduled_jobs
		SET status = 'pending',
		    attempt_count = $1,
		    schedule_at = $2,
		    last_attempt_at = NOW(),
		    last_response_status_code = $3,
		    last_error_message = $4,
		    worker_id = NULL,
		    started_at = NULL,
		    updated_at = NOW()
		WHERE id = $5 AND status = 'processing'`

	result, err := r.pool.Exec(ctx, query,
		job.AttemptCount, job.ScheduleAt, job.LastResponseCode,
		job.LastErrorMessage, job.ID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// FailJob transitions processing → failed (terminal state, max attempts exhausted).
func (r *PostgresJobRepository) FailJob(ctx context.Context, job *domain.ScheduledJob) (bool, error) {
	query := `
		UPDATE scheduled_jobs
		SET status = 'failed',
		    attempt_count = $1,
		    last_attempt_at = NOW(),
		    last_response_status_code = $2,
		    last_error_message = $3,
		    worker_id = NULL,
		    started_at = NULL,
		    updated_at = NOW()
		WHERE id = $4 AND status = 'processing'`

	result, err := r.pool.Exec(ctx, query,
		job.AttemptCount, job.LastResponseCode,
		job.LastErrorMessage, job.ID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// ReleaseClaim transitions processing → pending with schedule_at = NOW()
// for immediate re-pickup. Used when the worker must abort before attempting
// delivery (e.g., Redis idempotency check failed at the start of runJob).
// The state guard ensures we only release a claim we still own.
func (r *PostgresJobRepository) ReleaseClaim(ctx context.Context, job *domain.ScheduledJob) (bool, error) {
	query := `
		UPDATE scheduled_jobs
		SET status = 'pending',
		    schedule_at = NOW(),
		    worker_id = NULL,
		    started_at = NULL,
		    updated_at = NOW()
		WHERE id = $1 AND status = 'processing'`

	result, err := r.pool.Exec(ctx, query, job.ID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// SaveExecution saves detailed history to the job_executions table.
// Uses ON CONFLICT DO NOTHING to handle the rare race where the original
// goroutine's SaveExecution and a re-execution's SaveExecution target the
// same (job_id, attempt_num) pair. First write wins — history is preserved.
func (r *PostgresJobRepository) SaveExecution(ctx context.Context, exec *domain.ExecutionRecord) error {
	query := `
		INSERT INTO job_executions (job_id, attempt_num, started_at, ended_at, duration_ms, response_status_code, error_message, worker_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (job_id, attempt_num) DO NOTHING`

	_, err := r.pool.Exec(ctx, query,
		exec.JobID, exec.AttemptNum, exec.StartedAt, exec.EndedAt,
		exec.DurationMs, exec.ResponseStatusCode, exec.ErrorMessage, exec.WorkerID)

	return err
}

func (r *PostgresJobRepository) ReclaimStaleJobs(ctx context.Context, staleThreshold time.Duration, limit int) (int64, error) {
	query := `
		UPDATE scheduled_jobs
		SET status = 'pending', worker_id = NULL, started_at = NULL, updated_at = NOW()
		WHERE id IN (
			SELECT id FROM scheduled_jobs
			WHERE status = 'processing' AND started_at < NOW() - $1::INTERVAL
			ORDER BY started_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`

	result, err := r.pool.Exec(ctx, query, staleThreshold, limit)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected(), nil
}

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// isTransientError classifies pgx errors to determine if a retry is worthwhile.
// Returns true for connection failures, serialization conflicts, and deadlocks.
// Returns false for permanent errors (integrity violations, data errors) and context cancellation.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// Context cancellation is not transient — retrying won't help
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for pgx-specific SQLSTATE codes
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Class 08: Connection Exception (transient)
		if strings.HasPrefix(pgErr.Code, "08") {
			return true
		}
		// 40001: serialization_failure (transient)
		if pgErr.Code == "40001" {
			return true
		}
		// 40P01: deadlock_detected (transient, Postgres already killed the loser)
		if pgErr.Code == "40P01" {
			return true
		}
	}

	return false
}
