package usecase

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/almeidadiego/webhook-engine/internal/domain"

	"github.com/google/uuid"
)

type WorkerService struct {
	repo       domain.JobRepository
	cache      domain.IdempotencyStore
	httpClient *http.Client
	workerID   uuid.UUID
	config     domain.WorkerConfig
	semaphore  chan struct{}
	wg         sync.WaitGroup
}

func NewWorkerService(
	repo domain.JobRepository,
	cache domain.IdempotencyStore,
	cfg domain.WorkerConfig,
) *WorkerService {
	return &WorkerService{
		repo:     repo,
		cache:    cache,
		workerID: uuid.New(),
		config:   cfg,
		// The semaphore limits global concurrency for this instance
		semaphore: make(chan struct{}, cfg.MaxConcurrency),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ExecuteCycle fetches and dispatches pending jobs
func (s *WorkerService) ExecuteCycle(ctx context.Context) {
	slog.Debug("starting fetch cycle", "worker_id", s.workerID)

	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = s.config.MaxConcurrency * 2
	}

	jobs, err := s.repo.FetchNextPending(ctx, s.workerID, batchSize)
	if err != nil {
		slog.Error("failed to fetch jobs", "error", err)
		return
	}

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return
		case s.semaphore <- struct{}{}:
			s.wg.Add(1)
			go func(j *domain.ScheduledJob) {
				defer func() {
					<-s.semaphore
					s.wg.Done()
				}()
				s.runJob(ctx, j)
			}(job)
		}
	}
}

func (s *WorkerService) runJob(ctx context.Context, job *domain.ScheduledJob) {
	isDuplicate, err := s.cache.CheckAndSet(ctx, job.IdempotencyKey, 5*time.Minute)

	if err != nil {
		slog.Error("error accessing redis, releasing claim and aborting", "job_id", job.ID, "error", err)
		// Release the Postgres claim to avoid zombie processing jobs.
		// Reset status back to pending so another worker can pick it up.
		// Use a background context since the original ctx may be cancelled.
		resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		job.Status = domain.StatusPending
		job.WorkerID = nil
		job.StartedAt = nil
		if updateErr := s.repo.Update(resetCtx, job); updateErr != nil {
			slog.Error("failed to release claim after redis error, job may be stuck in processing",
				"job_id", job.ID, "error", updateErr)
		}
		return
	}

	if isDuplicate {
		slog.Warn("idempotency: job already processed or in progress", "key", job.IdempotencyKey)
		return
	}

	execution := &domain.ExecutionRecord{
		JobID:      job.ID,
		AttemptNum: job.AttemptCount + 1,
		StartedAt:  time.Now(),
		WorkerID:   &s.workerID,
	}

	resp, err := s.sendRequest(ctx, job)

	s.manageIdempotencyState(ctx, job.IdempotencyKey, resp, err)

	s.handleCompletion(ctx, job, resp, err, execution)
}

// manageIdempotencyState decide se mantém ou remove a trava no Redis
func (s *WorkerService) manageIdempotencyState(ctx context.Context, key string, resp *http.Response, err error) {
	// On success (2xx), we transform the 5min lock into a 24h seal
	if err == nil && resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := s.cache.UpdateTTL(ctx, key, 24*time.Hour); err != nil {
			slog.Error("error extending idempotency TTL", "key", key, "error", err)
		}
		return
	}

	// On network error or status >= 400, we release the key for the next retry
	// If Delete fails, the original 5 min TTL from CheckAndSet is our fallback.
	if err := s.cache.Delete(ctx, key); err != nil {
		slog.Warn("failed to delete redis lock (waiting for TTL)", "key", key, "error", err)
	}
}

func (s *WorkerService) sendRequest(ctx context.Context, job *domain.ScheduledJob) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, job.HTTPMethod, job.URL, bytes.NewReader(job.RequestBody))
	if err != nil {
		return nil, err
	}

	for k, v := range job.RequestHeaders {
		req.Header.Set(k, v)
	}

	return s.httpClient.Do(req)
}

func (s *WorkerService) handleCompletion(ctx context.Context, job *domain.ScheduledJob, resp *http.Response, err error, exec *domain.ExecutionRecord) {
	now := time.Now()
	exec.EndedAt = &now
	duration := int(now.Sub(exec.StartedAt).Milliseconds())
	exec.DurationMs = &duration

	isError := err != nil || (resp != nil && resp.StatusCode >= 400)

	if isError {
		job.AttemptCount++
		errMsg := "unknown error"
		if err != nil {
			errMsg = err.Error()
		} else if resp != nil {
			errMsg = fmt.Sprintf("http status: %d", resp.StatusCode)
			exec.ResponseStatusCode = &resp.StatusCode
		}

		job.LastErrorMessage = &errMsg
		exec.ErrorMessage = &errMsg

		if job.CanRetry() {
			job.Status = domain.StatusPending
			job.ScheduleAt = job.CalculateNextRetry(s.config.BaseRetryDelay)
			slog.Warn("job failed, scheduling retry", "job_id", job.ID, "next_attempt", job.ScheduleAt)
		} else {
			job.Status = domain.StatusFailed
			slog.Error("job failed permanently", "job_id", job.ID, "attempts", job.AttemptCount)
		}
	} else {
		job.Status = domain.StatusCompleted
		job.LastResponseCode = &resp.StatusCode
		exec.ResponseStatusCode = &resp.StatusCode
		slog.Info("job completed successfully", "job_id", job.ID)
	}

	// Final persistence
	if err := s.repo.Update(ctx, job); err != nil {
		slog.Error("error updating job in the database", "job_id", job.ID, "error", err)
	}
	if err := s.repo.SaveExecution(ctx, exec); err != nil {
		slog.Error("error saving execution history", "job_id", job.ID, "error", err)
	}
}

// Stop waits for in-flight tasks to finish
func (s *WorkerService) Stop() {
	slog.Info("waiting for in-flight webhooks to finish...")
	s.wg.Wait()
}
