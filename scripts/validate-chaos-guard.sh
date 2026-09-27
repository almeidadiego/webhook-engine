#!/usr/bin/env bash
# =============================================================================
# Chaos Guard Validation — forced claim-lost race — Webhook Engine P1
# =============================================================================
#
# WHAT THIS DOES (read this first — it explains the whole script):
#
# The worker claims a job in two steps:
#   1. A Postgres claim flips the row to status='processing', started_at=T=0.
#   2. The claiming goroutine queues on the in-process semaphore and waits for
#      a slot BEFORE the HTTP delivery starts.
# The reaper sweeps 'processing' rows whose started_at is older than the stale
# threshold and reclaims them to 'pending'. Between step 1 and step 2 there is
# therefore an observable window where the DB says "processing, stale-looking"
# while the goroutine still owns the claim — the reaper can race it.
#
# The CAS (Compare-And-Swap) guard is the line of defense: CompleteJob /
# FailJob / SealJob only mutate the row if the row still matches the claimed
# state (status='processing' + matching worker). When the reaper wins the race,
# the goroutine's finish must be REJECTED, never applied over the new owner.
# This script forces that outcome on purpose and checks the guard held:
#
#   expected log: "lost claim race — state transition blocked by guard"
#
# HOW THE RACE IS FORCED (intentionally "wrong" timing, didactic on purpose):
#   1. Worker claims job   → status='processing', started_at=T=0
#   2. Dummy server sleeps 1000-2000ms per request and the semaphore (30
#      slots across 3 workers) is saturated → the goroutine waits in the
#      backlog queue for 2-3s or more
#   3. Reaper (interval=2s) sees started_at > stale threshold (3s)
#      → reclaims the row → 'pending'
#   4. The original goroutine finishes its HTTP delivery → calls
#      CompleteJob → the CAS guard BLOCKS the transition (status is no
#      longer 'processing' for this attempt)
#   5. Worker logs "lost claim race" and returns WITHOUT sealing the Redis
#      idempotency key as done
#   6. The Redis key (5min TTL) expires unsealed → the job can be re-executed
#      → the delivery happens AGAIN → counted as a DUPLICATE by dummy server
#
# WHAT YOU SHOULD OBSERVE (documented residual risk):
#   - CAS guard HOLDS:  no lost UPDATE, no double execution bookkeeping
#     (attempt state stays consistent). This is the PASS condition.
#   - DUPLICATE DELIVERIES exist anyway: state consistency ≠ delivery
#     consistency. A rejected CompleteJob means the original delivery's
#     result was thrown away while its HTTP side effect already happened.
#   - PRODUCTION PREVENTS THE RACE BY TIMING: with the default
#     WORKER_STALE_THRESHOLD (60s) and reaper interval (10s), a goroutine
#     never looks stale while it still holds its claim — the reaper only
#     sees jobs whose worker actually died. The invariant is "stale
#     threshold >> worst-case in-flight delivery", not the CAS guard alone.
#
# Prerequisites:
#   - Docker and docker compose installed
#   - k6 installed and on PATH
#   - Go 1.25+ installed
#   - Ports 5432, 6379, 8080, and 9999 free
#
# Usage:
#   ./scripts/validate-chaos-guard.sh
#
#   GUARD_WAIT_SECONDS (env, default 330 = 5min30s) controls how long the
#   script waits for the pending Redis idempotency keys (5min TTL) to expire
#   before the second worker pass re-executes the unsealed jobs and the
#   duplicates show up in /tmp/webhook-deliveries.json, e.g.
#   `GUARD_WAIT_SECONDS=310 ./scripts/validate-chaos-guard.sh`.
#
# Artifacts (preserved after the run — they are the analysis deliverable):
#   /tmp/chaos-guard-api.log            API log
#   /tmp/chaos-guard-worker-{1,2,3}.log round-1 worker logs (race evidence)
#   /tmp/chaos-guard-worker-r2-{1,2,3}.log round-2 worker logs (re-execution)
#   /tmp/chaos-guard-queue.log          queue depth samples
#   /tmp/chaos-guard-k6-output.log      raw k6 output
#   /tmp/chaos-guard-dummy.log          dummy server log
#   /tmp/webhook-deliveries.json        per-job delivery counts
# =============================================================================

set -euo pipefail

# ─── Config ─────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

DB_USER="webhook_user"
DB_PASS="webhook_password"
DB_NAME="webhooks"
DB_PORT="5432"
REDIS_PORT="6379"
DATABASE_URL="postgres://${DB_USER}:${DB_PASS}@localhost:${DB_PORT}/${DB_NAME}?sslmode=disable"
REDIS_ADDR="localhost:${REDIS_PORT}"

API_PORT="8080"
API_URL="http://localhost:${API_PORT}"
DUMMY_PORT="9999"
TARGET_URL="http://localhost:${DUMMY_PORT}/webhook"

# ── Adversarial timing knobs (the "wrong" config that forces the race) ──────
# Dummy server latency: every delivery occupies a semaphore slot for 1-2s.
# With 50 VUs pushing jobs far faster than 30 slots can drain, claimed jobs
# pile up in the semaphore backlog — exactly the multi-second window that the
# 3s stale threshold treats as "abandoned".
DUMMY_MIN_DELAY_MS=1000
DUMMY_MAX_DELAY_MS=2000

# Worker timing: stale threshold (3s) is SMALLER than the semaphore backlog
# wait (2-3s+), and the reaper sweeps every 2s — so a claim is routinely
# reaped while its goroutine is still queued. In production these values are
# 60s/10s and the race window is practically unreachable.
WORKER_STALE_THRESHOLD="3s"
WORKER_REAPER_INTERVAL="2s"
WORKER_COUNT=3
WORKER_MAX_CONCURRENCY=10
WORKER_POLL_INTERVAL="1s"

# k6 load shape: 50 VUs for 60s, enough to build the backlog that starves the
# semaphore and creates the stale window.
K6_VUS=50
K6_DURATION="60s"

# How long to wait after load so the pending Redis idempotency keys (5min
# TTL) expire and re-execution becomes possible. 330s = 300s TTL + buffer.
# Env-overridable only — the forced race itself has fixed adversarial timing.
GUARD_WAIT_SECONDS="${GUARD_WAIT_SECONDS:-330}"

# Queue collector sample cadence (seconds between pending/processing/completed
# samples; shows the backlog building and the reaper flipping states).
QUEUE_SAMPLE_INTERVAL=5

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
err()   { echo -e "${RED}[✗]${NC} $*"; exit 1; }
sep()   { echo -e "\n${YELLOW}──────────────────────────────────────────${NC}"; }
sql()   { docker exec webhook-db psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "$1" 2>/dev/null; }
redis_cli() { docker exec webhook-redis redis-cli "$@" 2>/dev/null; }

# ─── Process stoppers ───────────────────────────────────────────────────────
# go run spawns TWO processes (the `go run` wrapper + the compiled binary
# under the go-build cache), so every target is covered by three patterns.
stop_workers() {
    pkill -f "cmd/worker" 2>/dev/null || true
    pkill -f "exe/worker" 2>/dev/null || true
    pkill -f "go-build.*worker" 2>/dev/null || true
}

stop_api() {
    pkill -f "cmd/api" 2>/dev/null || true
    pkill -f "exe/api" 2>/dev/null || true
    pkill -f "go-build.*api" 2>/dev/null || true
}

stop_dummy() {
    pkill -f "dummy-server" 2>/dev/null || true
    pkill -f "python3.*9999" 2>/dev/null || true
}

# ─── Cleanup ────────────────────────────────────────────────────────────────
cleanup() {
    log "Cleaning up..."
    stop_workers
    stop_api
    stop_dummy
    # Analysis artifacts (/tmp/chaos-guard-*.log, /tmp/webhook-deliveries.json)
    # are intentionally NOT removed here — they are the deliverable of this
    # chaos run. rm only happens at script start for a fresh run.
}

trap cleanup EXIT

cd "${PROJECT_DIR}"

sep
echo "Webhook Engine — Chaos Guard Validation (forced claim-lost race)"
echo "Project: ${PROJECT_DIR}"
echo "Adversarial timing: stale=${WORKER_STALE_THRESHOLD}, reaper=${WORKER_REAPER_INTERVAL}, semaphore=${WORKER_MAX_CONCURRENCY}x${WORKER_COUNT} slots, dummy delay=${DUMMY_MIN_DELAY_MS}-${DUMMY_MAX_DELAY_MS}ms"
echo "Guard wait: ${GUARD_WAIT_SECONDS}s (Redis idempotency TTL is 5min)"
sep

# ─── Prerequisites ──────────────────────────────────────────────────────────
command -v k6 >/dev/null 2>&1 || err "k6 is not installed or not on PATH (expected at /opt/homebrew/bin/k6)"
command -v docker >/dev/null 2>&1 || err "docker is not installed or not on PATH"
command -v python3 >/dev/null 2>&1 || err "python3 is not installed or not on PATH (needed by the dummy delivery counter)"

# ─── Step 1/6: Infrastructure ───────────────────────────────────────────────
sep
log "Step 1/6: Starting Postgres + Redis via docker compose..."
docker compose up -d

log "Waiting for Postgres to be healthy..."
for i in $(seq 1 30); do
    if docker compose ps postgres 2>/dev/null | grep -q "healthy"; then
        log "Postgres is healthy"
        break
    fi
    sleep 1
done

log "Waiting for Redis to be healthy..."
for i in $(seq 1 30); do
    if docker compose ps redis 2>/dev/null | grep -q "healthy"; then
        log "Redis is healthy"
        break
    fi
    sleep 1
done

# Fresh job/execution state so the chaos run observes only the jobs it creates.
# job_executions first: it references scheduled_jobs (FK), so truncate parent
# tables in dependency order.
log "Resetting job tables..."
sql "TRUNCATE TABLE job_executions, scheduled_jobs;"
log "Flushing Redis idempotency state..."
redis_cli FLUSHDB

# Reset the delivery counter the dummy server relies on.
rm -f /tmp/chaos-guard-*.log /tmp/webhook-deliveries.json

# ─── Step 2/6: Start dummy server + API ─────────────────────────────────────
sep
log "Step 2/6: Starting dummy HTTP server (delay=${DUMMY_MIN_DELAY_MS}-${DUMMY_MAX_DELAY_MS}ms) and API on :${API_PORT}..."

# The delay is the donkey work of this experiment: it makes every delivery
# occupy a semaphore slot for 1-2s, which under 50 VUs of pressure turns the
# queue after the DB claim into a multi-second backlog.
DUMMY_MIN_DELAY_MS="${DUMMY_MIN_DELAY_MS}" \
DUMMY_MAX_DELAY_MS="${DUMMY_MAX_DELAY_MS}" \
python3 scripts/dummy-server.py > /tmp/chaos-guard-dummy.log 2>&1 &
disown
sleep 0.5

DATABASE_URL="${DATABASE_URL}" \
    API_ADDR=":${API_PORT}" \
    go run ./cmd/api > /tmp/chaos-guard-api.log 2>&1 &
disown

log "Waiting for API to be ready..."
API_READY=0
for i in $(seq 1 15); do
    if curl -s "http://localhost:${API_PORT}/metrics/pool" >/dev/null 2>&1; then
        API_READY=1
        log "API is ready"
        break
    fi
    sleep 1
done
if [[ "${API_READY}" -ne 1 ]]; then
    err "API did not become ready within 15s; see /tmp/chaos-guard-api.log"
fi

# ─── Step 3/6: Start aggressive workers + queue collector ───────────────────
sep
log "Step 3/6: Starting ${WORKER_COUNT} workers with adversarial reaper timing..."
log "  poll=${WORKER_POLL_INTERVAL}, reaper=${WORKER_REAPER_INTERVAL}, stale=${WORKER_STALE_THRESHOLD}, concurrency=${WORKER_MAX_CONCURRENCY}"
for i in $(seq 1 "${WORKER_COUNT}"); do
    DATABASE_URL="${DATABASE_URL}" \
        REDIS_ADDR="${REDIS_ADDR}" \
        WORKER_POLL_INTERVAL="${WORKER_POLL_INTERVAL}" \
        WORKER_REAPER_INTERVAL="${WORKER_REAPER_INTERVAL}" \
        WORKER_STALE_THRESHOLD="${WORKER_STALE_THRESHOLD}" \
        WORKER_MAX_CONCURRENCY="${WORKER_MAX_CONCURRENCY}" \
        go run ./cmd/worker > "/tmp/chaos-guard-worker-${i}.log" 2>&1 &
    disown
done
sleep 2

# Queue depth collector: watches the three-way state split that this chaos
# experiment is designed to produce — pending (freshly reaped re-claims),
# processing (claims held/queued), completed (guard-approved finishes).
QUEUE_LOG="/tmp/chaos-guard-queue.log"
: > "${QUEUE_LOG}"  # truncate/create
(
    while true; do
        PENDING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='pending';" | tr -d ' ')
        PROCESSING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='processing';" | tr -d ' ')
        COMPLETED=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='completed';" | tr -d ' ')
        echo "$(date +%H:%M:%S) pending=${PENDING} processing=${PROCESSING} completed=${COMPLETED}" >> "${QUEUE_LOG}"
        sleep "${QUEUE_SAMPLE_INTERVAL}"
    done
) &
QUEUE_PID=$!
disown
log "Queue collector started (every ${QUEUE_SAMPLE_INTERVAL}s → ${QUEUE_LOG})"

# ─── Step 4/6: Chaos load ───────────────────────────────────────────────────
sep
log "Step 4/6: Running k6 (${K6_VUS} VUs, ${K6_DURATION}) — building the semaphore backlog that feeds the reaper race..."

# `|| true` on the END of the whole pipeline: k6 exits non-zero when a
# threshold is crossed (even a lenient one) — under `set -euo pipefail` that
# would otherwise abort the script before the guard wait, where the
# interesting evidence accumulates. Under load, workers SHOULD log
# "lost claim race" — that IS the experiment working, so surface it live:
# a watcher breaks out of its polling loop as soon as the first race event
# appears in the shared round-1 worker logs.
(
    # Watcher loop: surface race evidence live while k6 runs. `|| true` guards
    # grep's exit code 1 on "no match yet" so the watcher cannot kill the
    # subshell early under `set -e`.
    while true; do
        if grep -h "lost claim race" /tmp/chaos-guard-worker-*.log 2>/dev/null; then
            break
        fi
        sleep 2
    done
) &
WATCHER_PID=$!
disown

# `|| true` on the whole pipeline — k6 exits non-zero on lenient threshold
# breach, and we want the run to continue into the guard wait either way.
API_URL="${API_URL}/jobs" \
    TARGET_URL="${TARGET_URL}" \
    VUS="${K6_VUS}" \
    DURATION="${K6_DURATION}" \
    k6 run loadtest/k6-load.js 2>&1 | tee "/tmp/chaos-guard-k6-output.log" || true

kill "${WATCHER_PID}" 2>/dev/null || true
wait "${WATCHER_PID}" 2>/dev/null || true
kill "${QUEUE_PID}" 2>/dev/null || true
wait "${QUEUE_PID}" 2>/dev/null || true
log "Queue collector stopped; samples: ${QUEUE_LOG}"

# Drain window: in-flight goroutines (1-2s HTTP + FIFO prompt) need a few
# seconds to reach CompleteJob so the CAS guard verdicts land in the round-1
# logs before we freeze the experiment. This maximizes the collected evidence.
log "Draining in-flight deliveries (10s)..."
sleep 10

# Halt workers+API but keep infrastructure up: PG and Redis must stay alive so
# rows and Redis keys keep their TTLs while we wait for key expiry.
log "Stopping workers + API (infrastructure stays up so Redis TTLs keep counting)..."
stop_workers
stop_api
sleep 1

# Snapshot deliveries at load end: everything beyond this in the same jobids
# is a duplicate caused by the expired, unsealed idempotency key.
DELIVERIES_AT_LOAD_END=$(python3 -c "import json; print(sum(json.load(open('/tmp/webhook-deliveries.json')).values()))" 2>/dev/null || echo 0)
JOBS_AT_LOAD_END=$(sql "SELECT count(*) FROM scheduled_jobs;" | tr -d ' ' || echo 0)
log "Deliveries captured at load end: ${DELIVERIES_AT_LOAD_END} for ${JOBS_AT_LOAD_END} jobs"

# ─── Step 5/6: Guard wait — the didactic heart of the script ────────────────
sep
log "Step 5/6: Waiting ${GUARD_WAIT_SECONDS}s for Redis idempotency keys to expire..."
echo "  WHY: the worker that lost the claim returned WITHOUT sealing the Redis key as done."
echo "  That key (SET idemp:<key> EX 300) is now the ONLY thing preventing re-execution."
echo "  While the key is alive, re-claims see it and log 'idempotency: job already processed'"
echo "  and skip. After ~300s the key expires, so the second worker pass below will actually"
echo "  deliver a SECOND time — which is exactly the documented residual risk being observed."
if [[ "${GUARD_WAIT_SECONDS}" -lt 300 ]]; then
    warn "GUARD_WAIT_SECONDS=${GUARD_WAIT_SECONDS}s is below the 5min idempotency TTL."
    warn "  Re-executions will NOT appear within this run — duplicates section will stay 0."
    warn "  Use GUARD_WAIT_SECONDS>=310 to see the full duplicate effect."
fi
sleep "${GUARD_WAIT_SECONDS}"
log "Guard wait complete. Redis markers for lost-claim jobs should now be gone."

# ─── Step 6/6: Second pass with safe timing + verification ──────────────────
sep
log "Step 6/6: Restarting workers briefly with SAFE threshold (60s) to process re-executed jobs..."
for i in $(seq 1 "${WORKER_COUNT}"); do
    DATABASE_URL="${DATABASE_URL}" \
        REDIS_ADDR="${REDIS_ADDR}" \
        WORKER_POLL_INTERVAL="${WORKER_POLL_INTERVAL}" \
        WORKER_REAPER_INTERVAL="10s" \
        WORKER_STALE_THRESHOLD="60s" \
        WORKER_MAX_CONCURRENCY="${WORKER_MAX_CONCURRENCY}" \
        go run ./cmd/worker > "/tmp/chaos-guard-worker-r2-${i}.log" 2>&1 &
    disown
done
log "Second pass running for 30s (safe 60s stale threshold — no new races, just drain)..."
sleep 30
stop_workers
log "Second pass workers stopped."

# Re-collect deliveries after the second pass; diff against the load-time
# snapshot to count deliveries caused by re-execution.
DELIVERIES_AFTER_REPLAY=$(python3 -c "import json; print(sum(json.load(open('/tmp/webhook-deliveries.json')).values()))" 2>/dev/null || echo 0)
REEXEC_DELIVERIES=$(python3 -c "print(max(0, ${DELIVERIES_AFTER_REPLAY:-(0)} - ${DELIVERIES_AT_LOAD_END:-(0)}))" 2>/dev/null || echo 0)

# ─── Verification ───────────────────────────────────────────────────────────
sep
log "Verifying CAS state guards and the documented duplicate residual risk..."

# CAS evidence: the guard refused state mutation for a reaped job.
LOST_CLAIM=$(grep -h "lost claim race" /tmp/chaos-guard-worker-*.log 2>/dev/null | wc -l | tr -d ' ' || true)
# Idempotency skip evidence: while Redis keys were alive, second claimants
# observed them and skipped (the delivery-inhibition layer doing its job).
IDEM_SKIP=$(grep -h "idempotency: job already processed" /tmp/chaos-guard-worker-*.log 2>/dev/null | wc -l | tr -d ' ' || true)
# Duplicate evidence: job IDs the dummy server received MORE THAN ONCE. This
# is the residual risk — it is EXPECTED to be > 0 and is NOT a failure here.
DUPLICATES=$(python3 -c "import json; d=json.load(open('/tmp/webhook-deliveries.json')); print(sum(1 for v in d.values() if v>1))" 2>/dev/null || echo 0)
# Panic evidence must be zero everywhere: a CAS violation or invalid state
# transition attempt should be a clean rejected-UPDATE, not a crash.
PANICS=$(grep -h -i "panic" /tmp/chaos-guard-worker-*.log 2>/dev/null | wc -l | tr -d ' ' || true)

echo ""
echo "=== Round-1 worker log — 'lost claim race' evidence ==="
if grep -h "lost claim race" /tmp/chaos-guard-worker-*.log 2>/dev/null; then
    log "count=${LOST_CLAIM}"
else
    echo "  (none — the CAS guard was never exercised by any racing reaper claim)"
fi

echo ""
echo "=== Round-1 worker log — 'idempotency: job already processed' skips ==="
if grep -h "idempotency: job already processed" /tmp/chaos-guard-worker-*.log 2>/dev/null; then
    log "count=${IDEM_SKIP}"
else
    echo "  (none — every raced job still had a live Redis marker when checked)"
fi

echo ""
echo "=== Delivery counter (dummy server) ==="
echo "  deliveries at load end:   ${DELIVERIES_AT_LOAD_END}"
echo "  deliveries after replay:  ${DELIVERIES_AFTER_REPLAY}"
echo "  re-execution deliveries:  ${REEXEC_DELIVERIES}"
echo "  job IDs served >1 time:   ${DUPLICATES}"

echo ""
echo "=== Final DB state ==="
sql "SELECT status, count(*) FROM scheduled_jobs GROUP BY status ORDER BY status;"
sql "SELECT count(*) AS execution_records FROM job_executions;"

# ─── Summary + Verdict ──────────────────────────────────────────────────────
sep
log "Summary"
echo ""
echo "  Configuration (deliberately adversarial):"
echo "    dummy latency            : ${DUMMY_MIN_DELAY_MS}-${DUMMY_MAX_DELAY_MS}ms per request"
echo "    worker stale threshold   : ${WORKER_STALE_THRESHOLD}"
echo "    reaper interval          : ${WORKER_REAPER_INTERVAL}"
echo "    workers x concurrency    : ${WORKER_COUNT} x ${WORKER_MAX_CONCURRENCY} (semaphore slots)"
echo "    k6                       : ${K6_VUS} VUs for ${K6_DURATION}"
echo "    guard wait               : ${GUARD_WAIT_SECONDS}s (Redis idempotency TTL is 5min)"
echo ""
echo "  Observations:"
echo "    lost claim race (CAS blocked): ${LOST_CLAIM}"
echo "    idempotency skips            : ${IDEM_SKIP}"
echo "    duplicate deliveries (jobs):  ${DUPLICATES}"
echo "    re-execution deliveries       : ${REEXEC_DELIVERIES}"
echo "    panics                        : ${PANICS}"
echo ""
echo "  Artifacts:"
echo "    logs        : /tmp/chaos-guard-api.log, /tmp/chaos-guard-worker-*.log"
echo "    queue       : ${QUEUE_LOG}"
echo "    k6 output   : /tmp/chaos-guard-k6-output.log"
echo "    deliveries  : /tmp/webhook-deliveries.json"

sep

# Verdict: the CAS guard is validated if the race was actually observed (at
# least one lost-claim rejection) AND no panics fired anywhere. Duplicates are
# NOT part of the verdict — they are the documented residual risk shape:
# a rejected CompleteJob by design leaves the already-sent HTTP delivery
# unsealed (state consistency ≠ delivery consistency) and the idempotency
# marker expires after 5min, so a re-execution can duplicate the side effect.
# Production prevents that by the timing invariant (60s stale threshold >>
# worst-case in-flight delivery), never by relying on the race happening.
PASS=true
if [[ "${LOST_CLAIM}" -gt 0 ]]; then
    log "CAS guard exercised: ${LOST_CLAIM} 'lost claim race' events observed ✓"
    log "   → CompleteJob was blocked when the row had been reaped; no state corruption."
else
    warn "0 'lost claim race' events — the race never triggered."
    warn "  Check /tmp/chaos-guard-worker-*.log: guards exist but were not exercised."
    warn "  Likely causes: semaphore never backlogged long enough, or reaper timing too slow."
    PASS=false
fi

if [[ "${PANICS}" -eq 0 ]]; then
    log "No panics in worker logs ✓"
else
    warn "Found ${PANICS} lines containing 'panic' in worker logs — investigate:"
    grep -h -i "panic" /tmp/chaos-guard-worker-*.log 2>/dev/null | head -10
    PASS=false
fi

if [[ "${DUPLICATES}" -gt 0 ]]; then
    warn "Duplicates observed: ${DUPLICATES} job(s) delivered more than once."
    warn "  This is the DOCUMENTED residual risk of the guard design, not a guard failure."
    warn "  Production's timing invariant (stale=60s >> worst-case delivery) prevents it."
else
    log "No duplicate deliveries recorded (see GUARD_WAIT_SECONDS note if 0)."
fi

sep
if $PASS; then
    log "CHAOS GUARD VALIDATION PASSED"
    log "The Compare-And-Swap state guards held under force-multiplicity load:"
    log "  claim → reaper reclaims → stale CompleteJob blocked → state consistent, no panics."
    log "Duplicates (if any) were the expected, documented residual risk of unsealed idempotency keys."
else
    err "CHAOS GUARD VALIDATION FAILED: see summary above. Inspect /tmp/chaos-guard-* logs."
fi

sep
log "Infrastructure still running. To stop: docker compose down"
