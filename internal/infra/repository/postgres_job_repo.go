package repository

import (
	"context"
	"encoding/json"

	"github.com/almeidadiego/webhook-engine/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresJobRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresJobRepository(pool *pgxpool.Pool) *PostgresJobRepository {
	return &PostgresJobRepository{pool: pool}
}

// FetchNextPending retrieves candidates but does NOT lock them yet.
func (r *PostgresJobRepository) FetchNextPending(ctx context.Context, limit int) ([]*domain.ScheduledJob, error) {
	query := `
		SELECT id, idempotency_key, url, http_method, request_headers, request_body, attempt_count, max_attempts
		FROM scheduled_jobs
		WHERE status = 'pending' AND schedule_at <= NOW()
		ORDER BY schedule_at ASC
		LIMIT $1`

	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*domain.ScheduledJob
	for rows.Next() {
		var j domain.ScheduledJob
		var headers []byte
		err := rows.Scan(&j.ID, &j.IdempotencyKey, &j.URL, &j.HTTPMethod, &headers, &j.RequestBody, &j.AttemptCount, &j.MaxAttempts)
		if err != nil {
			return nil, err
		}
		json.Unmarshal(headers, &j.RequestHeaders)
		jobs = append(jobs, &j)
	}

	return jobs, nil
}

// Claim attempts to lock the job specifically for this worker.
func (r *PostgresJobRepository) Claim(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error {
	query := `
		UPDATE scheduled_jobs
		SET status = 'processing', worker_id = $1, started_at = NOW()
		WHERE id = (
			SELECT id FROM scheduled_jobs 
			WHERE id = $2 AND status = 'pending' 
			FOR UPDATE SKIP LOCKED
		)`

	res, err := r.pool.Exec(ctx, query, workerID, jobID)
	if err != nil {
		return err
	}

	if res.RowsAffected() == 0 {
		return domain.ErrJobAlreadyClaimed // You would need to define this error in the domain
	}

	return nil
}

// Update saves the final state after the attempt
func (r *PostgresJobRepository) Update(ctx context.Context, job *domain.ScheduledJob) error {
	query := `
		UPDATE scheduled_jobs
		SET status = $1, attempt_count = $2, schedule_at = $3, 
		    last_attempt_at = NOW(), last_response_status_code = $4, 
		    last_error_message = $5, worker_id = NULL, started_at = NULL
		WHERE id = $6`

	_, err := r.pool.Exec(ctx, query,
		job.Status, job.AttemptCount, job.ScheduleAt,
		job.LastResponseCode, job.LastErrorMessage, job.ID)

	return err
}

// SaveExecution records history in the audit table
func (r *PostgresJobRepository) SaveExecution(ctx context.Context, exec *domain.ExecutionRecord) error {
	query := `
		INSERT INTO job_executions (job_id, attempt_num, started_at, ended_at, duration_ms, response_status_code, error_message, worker_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := r.pool.Exec(ctx, query,
		exec.JobID, exec.AttemptNum, exec.StartedAt, exec.EndedAt,
		exec.DurationMs, exec.ResponseStatusCode, exec.ErrorMessage, exec.WorkerID)

	return err
}
