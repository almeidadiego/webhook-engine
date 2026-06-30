package domain

import "errors"

var (
	// Erros de Fluxo de Job
	ErrJobNotFound       = errors.New("job not found")
	ErrJobAlreadyClaimed = errors.New("job already being processed by another worker")
	ErrJobInvalidStatus  = errors.New("job status does not allow this operation")

	// Erros de Infraestrutura (Abstraídos para o Domínio)
	ErrIdempotencyKeyExists = errors.New("idempotency key already processed")
	ErrInternal             = errors.New("an internal error occurred")
)
