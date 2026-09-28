# Chaos Guard — Resultados do Experimento

Validação empírica dos *state guards* (Compare-And-Swap) implementados no Marco 2,
via um cenário de caos que força a corrida entre o reaper e as goroutines em processamento.

## Objetivo

Provar, com evidência, que:

1. Os guards `WHERE status='processing'` bloqueiam transições de estado ilegais.
2. O risco residual documentado (entrega duplicada no caminho *claim-lost*) é real.

## Configuração (intencionalmente adversa)

| Parâmetro | Valor | Racional |
|---|---|---|
| `DUMMY_MIN_DELAY_MS` / `MAX` | 1000 / 2000 | Jobs levam 1-2s no downstream |
| `WORKER_STALE_THRESHOLD` | 3s | Reaper reclaima jobs em `processing` > 3s — derrota o timing invariant (60s) |
| `WORKER_REAPER_INTERVAL` | 2s | Varreduras frequentes |
| k6 | 50 VUs × 60s | Carga moderada → backlog → semaphore wait > 3s |
| `GUARD_WAIT_SECONDS` | 330s | Aguarda o TTL de 5min das keys Redis expirar |

Script: `scripts/validate-chaos-guard.sh`

## Resultados

```
total jobs únicos entregues : 2371
total deliveries             : 2373
distribuição (count → jobs)  : {1: 2369, 2: 2}

lost claim race (CAS blocked): 2   (target_status=completed)
idempotency skips             : 356
duplicate deliveries (jobs)   : 2
panics                        : 0
```

## Cadeia causal provada

Os 2 jobs com `lost claim race` (target `completed`) são **exatamente** os 2 jobs
com entrega duplicada. Log do job `a77060cd-9d0c-4902-8405-e116b7a079ca`:

```
22:39:11.472  job completed successfully            <- goroutine entregou (HTTP 200)
22:39:11.474  lost claim race — blocked by guard    <- guard barrou o 'completed'
              target_status=completed                  (reaper já havia resetado para pending)
   ... (~6.6 min: TTL de 5min da key Redis expira) ...
22:45:49.984  job completed successfully            <- RE-EXECUÇÃO = 2ª ENTREGA = DUPLICATA
```

O mesmo padrão ocorre para `db8642df-f2b3-40fa-a36e-380e4d7b57de`.

## Interpretação

| Camada | Comportamento observado |
|---|---|
| **CAS guard (DB)** | Bloqueou as 2 transições `processing→completed` ilegais — estado permaneceu consistente, zero panics |
| **Redis seal (P3)** | 356 skips de idempotência — preveniu duplicatas enquanto as keys estavam vivas (janela de 5min) |
| **Residual risk** | Após o TTL expirar, a re-execução reentregou → 2 duplicatas (comprovado) |

**Lição central:** *state consistency ≠ delivery consistency*. O guard protege o estado
do banco; a entrega é um fato do mundo externo que pode ocorrer mesmo quando a claim é perdida.

## Em produção

O timing invariant (`reaper threshold` 60s > `detachedCtx` 45s) impede a corrida
inteira — os guards são defesa em profundidade, não o mecanismo primário.
O experimento derrotou o invariant de propósito para revelar o comportamento quando ele falha.

## Melhoria futura (backlog)

Mitigação completa: coluna `delivered_at` no banco, escrita incondicionalmente no HTTP 2xx,
permitindo que uma re-claim pule a entrega (fonte de verdade durável, independente da claim).

## Artefatos

Gerados em `/tmp` durante a execução (não versionados):

- `/tmp/chaos-guard-worker-*.log` — logs dos workers (evidência do CAS)
- `/tmp/chaos-guard-queue.log` — amostras de pending/processing/completed
- `/tmp/chaos-guard-k6-output.log` — métricas do k6
- `/tmp/webhook-deliveries.json` — contagem de entregas por job
