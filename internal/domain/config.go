package domain

import "time"

// WorkerConfig defines the operational settings for a Worker.
// Located in the domain as it dictates business rule behavior.
type WorkerConfig struct {
	// MaxConcurrency defines the number of concurrent goroutines (semaphore).
	MaxConcurrency int

	// BaseRetryDelay is the initial delay for exponential backoff calculation.
	BaseRetryDelay time.Duration

	// BatchSize defines how many jobs the worker tries to fetch from the database per cycle.
	BatchSize int

	// StaleJobThreshold defines how long a job can remain in 'processing' status
	// before the reaper reclaims it. Must exceed the detached context timeout (45s)
	// to prevent races with in-flight goroutines.
	StaleJobThreshold time.Duration
}
