package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// JobRepository define como persistimos os jobs
type JobRepository interface {
	// Busca jobs prontos para execução (pending e schedule_at <= agora)
	FetchNextPending(ctx context.Context, limit int) ([]*ScheduledJob, error)

	// Marca o job como 'processing', define o worker_id e started_at
	// Aqui você usaria o "FOR UPDATE SKIP LOCKED" que discutimos
	Claim(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error

	// Salva o estado final ou intermediário (sucesso, falha ou reagendamento)
	// Atualiza status, attempt_count, schedule_at e last_error_message
	Update(ctx context.Context, job *ScheduledJob) error

	// Para o histórico detalhado na tabela job_executions
	SaveExecution(ctx context.Context, exec *ExecutionRecord) error
}

type IdempotencyStore interface {
	// CheckAndSet tenta gravar a chave. Retorna true se já existia (duplicado).
	// O ttl define quanto tempo esse "lock" inicial vai durar.
	CheckAndSet(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// UpdateTTL estende a vida da chave (ex: de 1 min para 24h após sucesso)
	UpdateTTL(ctx context.Context, key string, ttl time.Duration) error

	// Delete remove a chave (usado em caso de falha para liberar o retry)
	Delete(ctx context.Context, key string) error
}
