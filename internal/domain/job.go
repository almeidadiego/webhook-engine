package domain

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusCancelled  JobStatus = "cancelled"
)

type ScheduledJob struct {
	ID               uuid.UUID
	TenantID         uuid.UUID
	URL              string
	HTTPMethod       string
	RequestHeaders   map[string]string
	RequestBody      []byte
	ScheduleAt       time.Time
	Status           JobStatus
	AttemptCount     int
	MaxAttempts      int
	WorkerID         *uuid.UUID
	StartedAt        *time.Time
	LastAttemptAt    *time.Time
	LastResponseCode *int
	LastErrorMessage *string
	LastResponseBody []byte
	IdempotencyKey   string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// CanRetry checks if the job can still be retried
func (j *ScheduledJob) CanRetry() bool {
	return j.AttemptCount < j.MaxAttempts && j.Status != StatusCancelled
}

// CalculateNextRetry applies Exponential Backoff with Jitter
func (j *ScheduledJob) CalculateNextRetry(baseDelay time.Duration) time.Time {
	// n = AttemptCount
	// delay = base * 2^n
	expFactor := math.Pow(2, float64(j.AttemptCount))
	delay := float64(baseDelay) * expFactor

	// Adding Jitter (variance of up to 20% of the current delay)
	// This prevents multiple jobs from "waking up" at the same time.
	jitterRange := delay * 0.2
	randomJitter := (rand.Float64() * jitterRange)

	finalDelay := delay + randomJitter

	return time.Now().Add(time.Duration(finalDelay))
}

type ExecutionRecord struct {
	JobID              uuid.UUID
	AttemptNum         int
	StartedAt          time.Time
	EndedAt            *time.Time
	DurationMs         *int
	ResponseStatusCode *int
	ErrorMessage       *string
	ErrorStackTrace    *string
	BodyResponse       *string
	WorkerID           *uuid.UUID
}
