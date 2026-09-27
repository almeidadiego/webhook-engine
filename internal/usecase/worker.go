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

// shutdownGraceTimeout bounds the lifetime of in-flight HTTP calls + persistence
// during graceful shutdown. Budget: HTTP (30s) + Redis (5s) + DB writes (5s) + buffer (5s).
const shutdownGraceTimeout = 45 * time.Second

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
		// schedule_at = NOW() makes the job eligible for immediate re-pickup.
		// Use a background context since the original ctx may be cancelled.
		resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, releaseErr := s.repo.ReleaseClaim(resetCtx, job); releaseErr != nil {
			slog.Error("failed to release claim after redis error, job may be stuck in processing",
				"job_id", job.ID, "error", releaseErr)
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

	// Once we commit to the HTTP call, all subsequent operations must survive
	// parent context cancellation (e.g., SIGTERM). The timeout bounds the
	// goroutine lifetime so Stop() -> wg.Wait() always returns.
	detachedCtx, cancel := context.WithTimeout(context.Background(), shutdownGraceTimeout)
	defer cancel()

	resp, err := s.sendRequest(detachedCtx, job)

	s.handleCompletion(detachedCtx, job, resp, err, execution)
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

	// Execution history is always attempted — it's valuable regardless of claim ownership.
	// ON CONFLICT DO NOTHING handles the rare race where both the original goroutine
	// and a re-execution insert the same (job_id, attempt_num). First write wins.
	if saveErr := s.repo.SaveExecution(ctx, exec); saveErr != nil {
		slog.Error("error saving execution history", "job_id", job.ID, "error", saveErr)
	}

	// Persist the state transition with a guard. The guard ensures we never
	// overwrite a concurrent state change (e.g., reaper reclaim) — Compare-And-Swap.
	var claimOwned bool
	var transitionErr error

	switch job.Status {
	case domain.StatusCompleted:
		claimOwned, transitionErr = s.repo.CompleteJob(ctx, job)
	case domain.StatusPending:
		claimOwned, transitionErr = s.repo.RescheduleJob(ctx, job)
	case domain.StatusFailed:
		claimOwned, transitionErr = s.repo.FailJob(ctx, job)
	}

	if transitionErr != nil {
		slog.Error("failed to persist state transition",
			"job_id", job.ID, "target_status", job.Status, "error", transitionErr)
	}

	if !claimOwned {
		// We lost the claim race — another worker or the reaper changed the state
		// before our write. Do NOT touch the idempotency key: the current owner
		// will handle it. Rare by construction (reaper threshold > detachedCtx).
		slog.Warn("lost claim race — state transition blocked by guard",
			"job_id", job.ID, "target_status", job.Status)
		return
	}

	// We own the claim: proceed with idempotency seal-or-release (P3 semantics).
	s.sealOrReleaseIdempotencyKey(ctx, job.IdempotencyKey, resp, err)
}

// sealOrReleaseIdempotencyKey transforms the 5min lock into a 24h seal on success,
// or deletes it on failure to allow retries.
func (s *WorkerService) sealOrReleaseIdempotencyKey(ctx context.Context, key string, resp *http.Response, err error) {
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

// Stop waits for in-flight tasks to finish
func (s *WorkerService) Stop() {
	slog.Info("waiting for in-flight webhooks to finish...")
	s.wg.Wait()
}

// RunReaper reclaims jobs stuck in 'processing' for longer than the configured
// stale threshold, resetting them to 'pending' for reprocessing.
func (s *WorkerService) RunReaper(ctx context.Context) {
	reclaimed, err := s.repo.ReclaimStaleJobs(ctx, s.config.StaleJobThreshold, s.config.BatchSize)
	if err != nil {
		slog.Error("reaper: failed to reclaim stale jobs", "error", err)
		return
	}

	if reclaimed > 0 {
		slog.Warn("reaper: reclaimed stale jobs", "count", reclaimed, "threshold", s.config.StaleJobThreshold)
	} else {
		slog.Debug("reaper: no stale jobs found")
	}
}
