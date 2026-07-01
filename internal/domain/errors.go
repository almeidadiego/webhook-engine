package domain

import "errors"

var (
	// Job Flow Errors
	ErrJobNotFound       = errors.New("job not found")
	ErrJobAlreadyClaimed = errors.New("job already being processed by another worker")
	ErrJobInvalidStatus  = errors.New("job status does not allow this operation")

	// Infrastructure Errors (Abstracted to the Domain)
	ErrIdempotencyKeyExists = errors.New("idempotency key already processed")
	ErrInternal             = errors.New("an internal error occurred")
)
