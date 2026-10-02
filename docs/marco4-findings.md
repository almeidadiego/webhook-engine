# Marco 4 — Caos e Carga: Achados

Resultados dos experimentos de carga e caos do projeto Webhook Engine.

## 1. Gargalos descobertos

### 1.1 Pool de conexões do Postgres (primeiro gargalo)

No teste de carga escalonado (50 → 200 → 500 usuários virtuais via k6):

| Rodada | Usuários | p95 | Throughput | Max pool adquirido |
|---|---|---|---|---|
| 1 | 50 | 4.14ms | 425/s | 15 |
| 2 | 200 | 4.65ms | 1.706/s | 16 |
| 3 | 500 | 5.14ms | 4.278/s | **18 (= max)** |

O `pgxpool` da API saturou em `max_conns=18` na rodada 3 — o teto do pool.
A latência se manteve baixa porque o pool absorveu a carga; o limite apareceria
ao aumentar mais a carga.

### 1.2 Throughput dos workers limitado por `batch_size / poll_interval`

Descoberto ao rodar o primeiro experimento de kill com 100 usuários virtuais:

- A API inseria ~1000/s; os workers drenavam **~40-60/s no total**
- O backlog explodiu para **143.000 jobs** (nunca drenaria em tempo hábil)
- Causa: `ExecuteCycle` claima um batch (`batch_size=20`) e bloqueia até despachá-lo;
  o próximo poll só vem 1s depois → **~20 jobs/s por worker**, independentemente
  dos 10 slots de concorrência interna
- Somado a isso, 1000 inserções/s competindo no banco estrangulam ainda mais os workers

**Alavancas para aumentar o throughput:** `batch_size` (mais por poll) e
`poll_interval` (polls mais frequentes), até o teto da concorrência real
(`concurrency / job_duration`). Em produção, dimensionar a carga à capacidade
real dos workers evita backlog sem controle.

### 1.3 Dummy server single-threaded (gargalo do harness de teste)

O servidor dummy original (Python `HTTPServer`) era **single-threaded**: com
delay por requisição, serializava as requisições concorrentes dos workers,
causando timeouts de HTTP nos workers. Reescrevemos em Go (`cmd/dummy-server`,
`net/http` concorrente) — 5 requisições concorrentes de 500ms passaram a levar
~525ms (em vez de ~2500ms).

## 2. Chaos: kill de worker (SIGKILL)

Config: 3 workers, backlog fixo de 3000 jobs (seeder), dummy com delay 500-800ms,
stale threshold 60s, reaper interval 10s. Victim morto com SIGKILL durante o drain.

| Observação | Valor |
|---|---|
| Survivors vivos ao fim | 2/2 |
| Victim morto | confirmado |
| `reaper: reclaimed stale jobs` | **7** |
| `idempotency: job already processed` (skip-loop) | 36 |
| Panics | 0 |
| Fila drenada | `pending=0, processing=0` em 343s |

**Mecanismo validado:** SIGKILL não roda hooks de saída → os jobs em voo do victim
ficam congelados em `processing` → após o stale threshold, os reapers dos
sobreviventes reclaimam os zumbis → os jobs recuperados skip-loopam na chave de
idempotência (5min) → completam. O sistema se auto-recupera.

## 3. Chaos: restart do Postgres

Config: 3 workers, carga leve via k6 (10 usuários virtuais), outage forçado de 3s
(`stop → sleep → start`) durante a carga.

| Observação | Valor |
|---|---|
| Amostras DOWN (outage real) | **6** (~3s) |
| `failed to fetch jobs` (workers) | 9 |
| `failed to insert job` (API) | 322 |
| k6 `http_req_failed` | 2,10% |
| Processos reiniciados | **0** (API + 3 workers, PIDs originais vivos) |
| Panics | 0 |
| Fila drenada | `pending=0, processing=0` |

**Mecanismo validado:** o `pgxpool` reconecta preguiçosamente; a API surfaceia
erros 5xx durante o outage e se recupera nos próximos inserts; o loop de poll
dos workers absorve as falhas. **Nenhum processo precisa reiniciar** para se
recuperar.

## 4. Validações do Marco 2/3 (já documentadas)

- `delivered_at`: eliminou as duplicatas no cenário de claim-lost (2 → 0) —
  ver `docs/chaos-guard-results.md`
- Guards CAS: bloqueiam transições de estado ilegais
- Idempotência por tenant: alinha Redis com a constraint `(tenant_id, idempotency_key)`

## Scripts

| Script | O que valida |
|---|---|
| `scripts/validate-e2e.sh` | Fluxo ponta a ponta (happy path) |
| `scripts/validate-reaper.sh` | Recuperação de job zumbi |
| `scripts/validate-scale.sh` | Escala multi-worker (sem duplicatas) |
| `scripts/validate-idempotency-race.sh` | Corrida do `SET NX` |
| `scripts/validate-load.sh` | Carga escalonada (gargalos) |
| `scripts/validate-chaos-guard.sh` | Race forçado + guards CAS + duplicatas |
| `scripts/validate-chaos-worker-kill.sh` | SIGKILL de worker + self-healing |
| `scripts/validate-chaos-postgres-restart.sh` | Restart do Postgres + recovery sem restart |
