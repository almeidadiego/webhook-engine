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
}
