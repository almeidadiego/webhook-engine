package domain

import "time"

// WorkerConfig define as configurações operacionais do Worker.
// Localizado no domínio pois dita o comportamento da regra de negócio.
type WorkerConfig struct {
	// MaxConcurrency define o número de goroutines simultâneas (semáforo).
	MaxConcurrency int

	// BaseRetryDelay é o tempo inicial para o cálculo de exponential backoff.
	BaseRetryDelay time.Duration

	// BatchSize define quantos jobs o worker tenta buscar do banco por ciclo.
	BatchSize int
}
