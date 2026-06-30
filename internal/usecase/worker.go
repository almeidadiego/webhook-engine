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
		// O semáforo limita a concorrência global desta instância
		semaphore: make(chan struct{}, cfg.MaxConcurrency),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ExecuteCycle busca e dispara jobs pendentes
func (s *WorkerService) ExecuteCycle(ctx context.Context) {
	slog.Debug("iniciando ciclo de busca", "worker_id", s.workerID)

	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = s.config.MaxConcurrency * 2
	}

	jobs, err := s.repo.FetchNextPending(ctx, batchSize)
	if err != nil {
		slog.Error("falha ao buscar jobs", "error", err)
		return
	}

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return
		case s.semaphore <- struct{}{}:
			// Ocupamos uma vaga no semáforo antes de tentar o Claim no banco
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
	// 1. Claim no Postgres (Garante que este worker é o dono do registro)
	if err := s.repo.Claim(ctx, job.ID, s.workerID); err != nil {
		return
	}

	slog.Info("job capturado", "job_id", job.ID, "url", job.URL)

	// 2. Lock de Ocupação no Redis (5 minutos)
	// Se outro worker tentar o mesmo Job ou mesma chave de idempotência, ele barra aqui.
	isDuplicate, err := s.cache.CheckAndSet(ctx, job.IdempotencyKey, 5*time.Minute)

	if err != nil {
		slog.Error("erro ao acessar redis", "job_id", job.ID, "error", err)
		return // Falha de infra: paramos para não arriscar duplicidade
	}

	if isDuplicate {
		slog.Warn("idempotência: job já processado ou em andamento", "key", job.IdempotencyKey)
		return
	}

	// 3. Preparação do registro de execução
	execution := &domain.ExecutionRecord{
		JobID:      job.ID,
		AttemptNum: job.AttemptCount + 1,
		StartedAt:  time.Now(),
		WorkerID:   &s.workerID,
	}

	// 4. Executa o Webhook (O momento da verdade)
	resp, err := s.sendRequest(ctx, job)

	// 5. Gestão do Ciclo de Vida da Idempotência (Redis)
	s.manageIdempotencyState(ctx, job.IdempotencyKey, resp, err)

	// 6. Finaliza e persiste o resultado no Banco (Postgres)
	s.handleCompletion(ctx, job, resp, err, execution)
}

// manageIdempotencyState decide se mantém ou remove a trava no Redis
func (s *WorkerService) manageIdempotencyState(ctx context.Context, key string, resp *http.Response, err error) {
	// Se foi sucesso (2xx), transformamos o lock de 5min em selo de 24h
	if err == nil && resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := s.cache.UpdateTTL(ctx, key, 24*time.Hour); err != nil {
			slog.Error("erro ao estender TTL de idempotência", "key", key, "error", err)
		}
		return
	}

	// Se houve erro de rede ou status >= 400, liberamos a chave para o próximo retry
	// Se o Delete falhar, o TTL de 5 min original do CheckAndSet é o nosso fallback.
	if err := s.cache.Delete(ctx, key); err != nil {
		slog.Warn("falha ao deletar lock do redis (esperando TTL)", "key", key, "error", err)
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
			slog.Warn("job falhou, agendando retentativa", "job_id", job.ID, "next_attempt", job.ScheduleAt)
		} else {
			job.Status = domain.StatusFailed
			slog.Error("job falhou definitivamente", "job_id", job.ID, "attempts", job.AttemptCount)
		}
	} else {
		job.Status = domain.StatusCompleted
		job.LastResponseCode = &resp.StatusCode
		exec.ResponseStatusCode = &resp.StatusCode
		slog.Info("job concluído com sucesso", "job_id", job.ID)
	}

	// Persistência final
	if err := s.repo.Update(ctx, job); err != nil {
		slog.Error("erro ao atualizar job no banco", "job_id", job.ID, "error", err)
	}
	if err := s.repo.SaveExecution(ctx, exec); err != nil {
		slog.Error("erro ao salvar histórico de execução", "job_id", job.ID, "error", err)
	}
}

// Stop aguarda a finalização de tarefas em andamento
func (s *WorkerService) Stop() {
	slog.Info("aguardando finalização de webhooks em andamento...")
	s.wg.Wait()
}
