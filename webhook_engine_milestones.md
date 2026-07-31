### Roteiro do Projeto: Webhook Engine (Go + Postgres + Redis)

* **Marco 1 (A Fundação e Execução Básica):**
* **Objetivo:** Criar o fluxo de ponta a ponta (Producer -> Banco -> Consumer -> HTTP Request).
* **Stack:** Go, PostgreSQL, Redis e Docker.
* **Componentes:** Estruturar o `WorkerService` para rodar em background, implementar o lock distribuído no Postgres (`FOR UPDATE SKIP LOCKED`) e garantir a idempotência no Redis (usando `SET NX` com TTL, promovido a 24h em caso de sucesso).
* **Validação:** Subir a infraestrutura via `docker-compose.yml`, inserir uma tarefa manualmente (ou via script simples) e confirmar que o worker a captura e executa.


* **Marco 2 (Resiliência e Ciclo de Vida):**
* **Objetivo:** Garantir que falhas de rede ou serviços fora do ar não destruam os jobs.
* **Componentes:** Implementar *Exponential Backoff* no cálculo da próxima execução (`NextRunAt`), limite máximo de tentativas (`MaxAttempts`) e transição correta de estados (`StatusPending` -> `StatusFailed` -> `StatusCompleted`).


* **Marco 3 (Escala Horizontal e Concorrência):**
* **Objetivo:** Provar a robustez do design de locks.
* **Validação:** Subir múltiplas instâncias do Worker (Worker A, Worker B, Worker C) simultaneamente. Garantir via logs e métricas que a query `SKIP LOCKED` do Postgres e o `SET NX` do Redis impedem completamente o processamento duplicado (Race Conditions).


* **Marco 4 (Engenharia do Caos e Carga):**
* **Objetivo:** Descobrir os gargalos de CPU, memória e conexões com o banco de dados.
* **Validação:** Criar uma API de ingestão rápida e usar uma ferramenta de load testing (como k6) para injetar 5.000+ tarefas simultâneas. Observar o comportamento do `sync.WaitGroup` e dos Semáforos (channels) de limite de concorrência criados no Go.