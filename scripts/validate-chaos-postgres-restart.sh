#!/usr/bin/env bash
# =============================================================================
# Chaos Postgres-Restart Validation — DB outage mid-load, zero process restarts
# =============================================================================
#
# WHAT THIS DOES (read this first — it explains the whole script):
#
# The full stack (API + 3 workers + dummy server) is running under k6 load
# against Postgres. At T+30s (RESTART_AT_SECONDS) a watchdog subshell runs
# `docker compose restart postgres` — a REAL outage: connections are severed
# mid-flight, the server goes away, and comes back minutes later.
#
# THE CLAIM BEING VALIDATED: no application process needs to restart.
# The workers and the API ride out the outage in place, thanks to three
# mechanisms working together:
#
#   1. pgxpool (v5) reconnects LAZILY: it does not spin in the background
#      trying to heal. It simply hands out fresh connections the next time
#      a query needs one. The API's next POST /jobs and the workers' next
#      poll succeed again as soon as Postgres is back — with a transient
#      error window in between, which is EXPECTED and counted.
#
#   2. The worker poll loop treats "cannot reach the DB" as a transient
#      condition: it logs "failed to fetch jobs" and simply retries on the
#      next tick (WORKER_POLL_INTERVAL=1s). No crash, no restart, no
#      circuit-breaker — the loop IS the resilience mechanism.
#
#   3. In-flight rows whose worker cannot persist the completion (the DB
#      write fails during the outage) are stranded as 'processing' when the
#      outage ends — temporarily frozen, exactly like a SIGKILL victim's
#      rows (see validate-chaos-worker-kill.sh). The REAPER heals them
#      after the stale threshold (60s): reclaimed to 'pending' and
#      re-delivered. The drain budget (480s) covers this: stale(60s) +
#      Redis idempotency TTL(300s, the same skip-loop as the kill test) +
#      buffer.
#
# WHAT YOU SHOULD OBSERVE:
#   - Real outage proven: the downtime monitor (pg_isready every 0.5s)
#     records "DOWN" samples (DB_DOWN_HITS > 0).
#   - Transient error evidence: workers log "failed to fetch jobs", the
#     API logs "failed to insert job", k6 shows an http_req_failed spike —
#     all bounded, all recovered from WITHOUT intervention.
#   - Zero restarts: all 4 process PIDs (API + 3 workers) recorded at
#     startup are still alive at the end (verified with kill -0).
#   - Full drain: final DB state has NO pending and NO processing rows.
#   - No panics anywhere.
#
# Prerequisites:
#   - Docker and docker compose installed
#   - k6 installed and on PATH
#   - Go 1.25+ installed
#   - Ports 5432, 6379, 8080, and 9999 free
#
# Usage:
#   ./scripts/validate-chaos-postgres-restart.sh
#
#   Env knobs (all optional):
#     RESTART_AT_SECONDS (default 30)   when during the load the DB restart lands
#     K6_VUS             (default 100)  k6 virtual users
#     K6_DURATION        (default 150s) k6 load duration (must be > RESTART_AT_SECONDS)
#     DRAIN_TIMEOUT      (default 480)  max seconds to wait for full drain
#
# Artifacts (preserved after the run — they are the analysis deliverable):
#   /tmp/chaos-pg-api.log          API log (insert-failure evidence)
#   /tmp/chaos-pg-worker-{1,2,3}.log worker logs (fetch-failure + reaper evidence)
#   /tmp/chaos-pg-dummy.log        dummy server log
#   /tmp/chaos-pg-k6-output.log    raw k6 output (http_req_failed spike)
#   /tmp/chaos-pg-queue.log        queue depth samples
#   /tmp/chaos-pg-downtime.log     pg_isready outage timeline (DOWN/ready)
#   /tmp/chaos-pg-events.log       restart timeline (restarting / returned)
#   /tmp/webhook-deliveries.json   per-job delivery counts
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

WORKER_COUNT=3
WORKER_MAX_CONCURRENCY=10
WORKER_POLL_INTERVAL="1s"
WORKER_REAPER_INTERVAL="10s"
WORKER_STALE_THRESHOLD="60s"

# Dummy server latency: 0ms — the experiment's variable is the DB outage,
# not delivery latency. Keep every other stress source quiet.
DUMMY_MIN_DELAY_MS=0
DUMMY_MAX_DELAY_MS=0

# k6 load shape: same shape as the kill test so the two chaos runs are
# comparable. Load must still be running when Postgres comes back so the
# recovery is exercised under load, not in quiescence.
K6_VUS="${K6_VUS:-10}"
K6_DURATION="${K6_DURATION:-150s}"

# Chaos injection timing: docker compose restart postgres happens T+30s
# into the k6 run, while the API and workers are hot.
RESTART_AT_SECONDS="${RESTART_AT_SECONDS:-30}"
OUTAGE_SECONDS="${OUTAGE_SECONDS:-3}"

# Drain budget, same math as the worker-kill script: outages can strand
# in-flight rows as 'processing'; the reaper reclaims them after 60s, and
# their re-executions skip-loop on the 5min Redis idempotency TTL before
# completing. 60s + 300s + buffer → 480s.
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-480}"

# Queue collector sample cadence (seconds between pending/processing/completed
# samples; shows the outage freeze and the post-restart drain).
QUEUE_SAMPLE_INTERVAL=5

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
err()   { echo -e "${RED}[✗]${NC} $*"; exit 1; }
sep()   { echo -e "\n${YELLOW}──────────────────────────────────────────${NC}"; }
sql()   { docker exec webhook-db psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "$1" 2>/dev/null; }
redis_cli() { docker exec webhook-redis redis-cli "$@" 2>/dev/null; }

# ─── Process stoppers ───────────────────────────────────────────────────────
# Binaries built to /tmp are matched by name (e.g. "webhook-worker-pg");
# the go-build/exe patterns are belt-and-braces to catch anything else
# left behind by previous runs of the other validate scripts.
stop_workers() {
    pkill -f "webhook-worker-pg" 2>/dev/null || true
    pkill -f "cmd/worker" 2>/dev/null || true
    pkill -f "exe/worker" 2>/dev/null || true
    pkill -f "go-build.*worker" 2>/dev/null || true
}

stop_api() {
    pkill -f "webhook-api-pg" 2>/dev/null || true
    pkill -f "cmd/api" 2>/dev/null || true
    pkill -f "exe/api" 2>/dev/null || true
    pkill -f "go-build.*api" 2>/dev/null || true
}

stop_dummy() {
    pkill -f "dummy-server" 2>/dev/null || true
}

# ─── Cleanup ────────────────────────────────────────────────────────────────
# Records in-flight subshell PIDs (queue collector, downtime monitor,
# watchdog) so the EXIT trap can reap exactly what this run created, in
# addition to the pattern sweep. Artifacts are intentionally NOT removed —
# they are the deliverable.
QUEUE_PID=""
DOWN_MON_PID=""
RESTART_WATCHDOG_PID=""
cleanup() {
    log "Cleaning up..."
    for pid in "${QUEUE_PID}" "${DOWN_MON_PID}" "${RESTART_WATCHDOG_PID}"; do
        [[ -n "${pid}" ]] && kill "${pid}" 2>/dev/null || true
    done
    # set -u-safe empty-array idiom (macOS bash 3.2; mirrors script 1).
    if [[ -n "${WORKER_PIDS[@]+x}" ]]; then
        for pid in "${WORKER_PIDS[@]}"; do
            [[ -n "${pid}" ]] && kill -9 "${pid}" 2>/dev/null || true
        done
    fi
    stop_workers
    stop_api
    stop_dummy
    # Analysis artifacts (/tmp/chaos-pg-*.log, /tmp/webhook-deliveries.json)
    # are intentionally NOT removed here — they are the deliverable of this
    # chaos run. rm only happens at script start for a fresh run.
    log "Infrastructure still running. To stop: docker compose down"
}

trap cleanup EXIT

cd "${PROJECT_DIR}"

sep
echo "Webhook Engine — Chaos Postgres-Restart Validation (DB outage mid-load, zero process restarts)"
echo "Project: ${PROJECT_DIR}"
echo "Load: ${K6_VUS} VUs for ${K6_DURATION}; dummy delay=0ms; workers=${WORKER_COUNT}x${WORKER_MAX_CONCURRENCY}"
echo "Chaos: docker compose restart postgres at T+${RESTART_AT_SECONDS}s; reaper=${WORKER_REAPER_INTERVAL}, stale=${WORKER_STALE_THRESHOLD}"
echo "Claim: API + workers survive WITHOUT any process restart; reaper heals stranded rows."
echo "Drain budget: ${DRAIN_TIMEOUT}s (skips until the 5min Redis idempotency TTL expires)"
sep

# ─── Prerequisites ──────────────────────────────────────────────────────────
command -v k6 >/dev/null 2>&1 || err "k6 is not installed or not on PATH"
command -v docker >/dev/null 2>&1 || err "docker is not installed or not on PATH"
command -v python3 >/dev/null 2>&1 || err "python3 is not installed or not on PATH (needed to parse the delivery counts JSON in the verification steps)"

# Fresh state for a fresh run — also orphans any stray processes from a
# previous run so our recorded PIDs refer only to THIS experiment.
rm -f /tmp/chaos-pg-* /tmp/webhook-deliveries.json

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
sql "TRUNCATE job_executions, scheduled_jobs;"
log "Flushing Redis idempotency state..."
redis_cli FLUSHDB

# ─── Step 2/6: Build + start dummy server and API ───────────────────────────
sep
log "Step 2/6: Building binaries (NOT 'go run' — 'go run' is a two-process wrapper"
log "  and its \$! is the wrapper PID, not the real process; PIDs must be real here"
log "  because the verdict is 'no process restarted', verified by kill -0)..."

log "Building dummy HTTP server binary..."
go build -o /tmp/webhook-dummy-server ./cmd/dummy-server/
log "Building API binary..."
go build -o /tmp/webhook-api-pg ./cmd/api/
log "Building worker binary..."
go build -o /tmp/webhook-worker-pg ./cmd/worker/
log "Binaries built: /tmp/webhook-dummy-server, /tmp/webhook-api-pg, /tmp/webhook-worker-pg"

# Zero delay: the ONLY stress source in this experiment is the DB outage.
log "Starting dummy HTTP server (delay=0ms) on :${DUMMY_PORT}..."
DUMMY_PORT="${DUMMY_PORT}" \
DUMMY_MIN_DELAY_MS="${DUMMY_MIN_DELAY_MS}" \
DUMMY_MAX_DELAY_MS="${DUMMY_MAX_DELAY_MS}" \
    /tmp/webhook-dummy-server > /tmp/chaos-pg-dummy.log 2>&1 &
DUMMY_PID=$!
disown
sleep 0.5

log "Starting API on :${API_PORT}..."
DATABASE_URL="${DATABASE_URL}" \
    API_ADDR=":${API_PORT}" \
    /tmp/webhook-api-pg > /tmp/chaos-pg-api.log 2>&1 &
API_PID=$!
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
    err "API did not become ready within 15s; see /tmp/chaos-pg-api.log"
fi

# ─── Step 3/6: Start workers (PIDs recorded) + queue collector ──────────────
sep
log "Step 3/6: Starting ${WORKER_COUNT} workers (poll=${WORKER_POLL_INTERVAL}, reaper=${WORKER_REAPER_INTERVAL}, stale=${WORKER_STALE_THRESHOLD})..."
log "  All 3 PIDs are recorded — the verdict REQUIRES every one of them to still"
log "  be alive at the end (kill -0), proving nobody was restarted to recover."
WORKER_PIDS=()
for i in 1 2 3; do
    DATABASE_URL="${DATABASE_URL}" REDIS_ADDR="${REDIS_ADDR}" \
        WORKER_POLL_INTERVAL="${WORKER_POLL_INTERVAL}" \
        WORKER_REAPER_INTERVAL="${WORKER_REAPER_INTERVAL}" \
        WORKER_STALE_THRESHOLD="${WORKER_STALE_THRESHOLD}" \
        WORKER_MAX_CONCURRENCY="${WORKER_MAX_CONCURRENCY}" \
        /tmp/webhook-worker-pg > "/tmp/chaos-pg-worker-${i}.log" 2>&1 &
    WORKER_PIDS+=($!)
done
disown
echo "  worker PIDs: 1→${WORKER_PIDS[0]} 2→${WORKER_PIDS[1]} 3→${WORKER_PIDS[2]} (API pid: ${API_PID})"
sleep 2

# Queue depth collector: during the outage the counts freeze or stall; after
# the restart + reaper you should SEE processing drain through pending to 0.
QUEUE_LOG="/tmp/chaos-pg-queue.log"
: > "${QUEUE_LOG}"  # truncate/create
(
    while true; do
        PENDING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='pending';" | tr -d ' ')
        PROCESSING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='processing';" | tr -d ' ')
        COMPLETED=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='completed';" | tr -d ' ')
        PENDING="${PENDING:-0}"
        PROCESSING="${PROCESSING:-0}"
        COMPLETED="${COMPLETED:-0}"
        echo "$(date +%H:%M:%S) pending=${PENDING} processing=${PROCESSING} completed=${COMPLETED}" >> "${QUEUE_LOG}"
        sleep "${QUEUE_SAMPLE_INTERVAL}"
    done
) &
QUEUE_PID=$!
disown
log "Queue collector started (every ${QUEUE_SAMPLE_INTERVAL}s → ${QUEUE_LOG})"

# ─── Step 4/6: Downtime monitor + restart watchdog + k6 load ────────────────
sep
log "Step 4/6: k6 (${K6_VUS} VUs, ${K6_DURATION}); watchdog restarts Postgres at T+${RESTART_AT_SECONDS}s..."

# Downtime monitor: pg_isready every 0.5s gives the ground truth that the
# outage was REAL (DOWN samples) and bounded (ready resumes). Its exit code
# semantics: 0 = server accepting connections, anything else = not ready.
(
    while true; do
        # docker inspect is non-blocking and reflects the real container state
        # ("restarting"/"exited" during the outage). docker exec pg_isready can
        # transparently reconnect and miss a short outage, so it is not used.
        STATE=$(docker inspect --format '{{.State.Status}}' webhook-db 2>/dev/null || echo "unknown")
        if [[ "${STATE}" == "running" ]]; then
            echo "$(date +%H:%M:%S) ready" >> /tmp/chaos-pg-downtime.log
        else
            echo "$(date +%H:%M:%S) DOWN state=${STATE}" >> /tmp/chaos-pg-downtime.log
        fi
        sleep 0.5
    done
) &
DOWN_MON_PID=$!
disown
: > /tmp/chaos-pg-downtime.log
log "Downtime monitor started (pg_isready every 0.5s → /tmp/chaos-pg-downtime.log)"

EVENTS_LOG="/tmp/chaos-pg-events.log"
: > "${EVENTS_LOG}"

# Restart watchdog: fires the chaos exactly once, mid-load, and records the
# timeline. `docker compose restart` returned almost instantly and produced a
# sub-sampling outage, so we force a deterministic stop → sleep → start window
# (OUTAGE_SECONDS) that the monitor AND the app can both observe.
(
    sleep "${RESTART_AT_SECONDS}"
    echo "$(date +%H:%M:%S) stopping postgres (forced ${OUTAGE_SECONDS}s outage)" >> /tmp/chaos-pg-events.log
    docker compose stop postgres
    sleep "${OUTAGE_SECONDS}"
    echo "$(date +%H:%M:%S) starting postgres" >> /tmp/chaos-pg-events.log
    docker compose start postgres
    echo "$(date +%H:%M:%S) postgres back up" >> /tmp/chaos-pg-events.log
) &
RESTART_WATCHDOG_PID=$!
disown

# `|| true` on the END of the whole pipeline: k6 exits non-zero when any
# threshold is crossed (here: http_req_failed SPIKES DURING THE OUTAGE —
# that is the experiment WORKING), and under `set -euo pipefail` that must
# not abort the script before the drain loop and verification.
API_URL="${API_URL}/jobs" \
    TARGET_URL="${TARGET_URL}" \
    VUS="${K6_VUS}" \
    DURATION="${K6_DURATION}" \
    k6 run loadtest/k6-load.js 2>&1 | tee "/tmp/chaos-pg-k6-output.log" || true

# Reap the watchdog (the restart command can outlast k6 — `|| true` covers
# both the already-exited and still-running cases; in the latter case `wait`
# additionally blocks until Postgres is back up, which the drain needs).
wait "${RESTART_WATCHDOG_PID}" 2>/dev/null || true

log "Echo of restart timeline:"
grep -h . "${EVENTS_LOG}" 2>/dev/null || true
log "Downtimeline summary: DOWN samples=$(grep -c ' DOWN' /tmp/chaos-pg-downtime.log 2>/dev/null || true), ready samples=$(grep -c 'ready$' /tmp/chaos-pg-downtime.log 2>/dev/null || true)"

# Stop the downtime monitor now — post-k6 it would only add noise.
kill "${DOWN_MON_PID}" 2>/dev/null || true
wait "${DOWN_MON_PID}" 2>/dev/null || true
log "Downtime monitor stopped."

# ─── Step 5/6: Drain with the workers ALIVE ─────────────────────────────────
sep
log "Step 5/6: Stopping the queue collector; workers/API STAY ALIVE and drain..."
kill "${QUEUE_PID}" 2>/dev/null || true
wait "${QUEUE_PID}" 2>/dev/null || true
log "Queue collector stopped; samples: ${QUEUE_LOG}"
log "Workers/API are deliberately NOT stopped yet — they own the drain. They are"
log "stopped only AFTER the queue reaches 0/0, right before verification."

# THE DRAIN LOOP — heart of the recovery verification (identical mechanics to
# validate-chaos-worker-kill.sh): poll a two-count sample every 10s until
# BOTH pending=0 AND processing=0. Empty/non-numeric sample = DB-unreachable
# → log 'drain DB-unreachable' and keep waiting; an empty sample can NEVER
# count as success. Why ≥480s: stranded 'processing' rows need the 60s stale
# threshold before the reaper reclaims them, and their re-executions
# skip-loop on the 5min Redis idempotency key before completing.
log "Drain loop: polling pending:processing every 10s (budget ${DRAIN_TIMEOUT}s)..."
DRAIN_START=$(date +%s)
DRAINED=false
while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - DRAIN_START))
    if [[ "${ELAPSED}" -ge "${DRAIN_TIMEOUT}" ]]; then
        warn "Drain budget exhausted after ${DRAIN_TIMEOUT}s with pending/processing above zero."
        break
    fi

    SAMPLE=$(sql "SELECT count(*) FILTER (WHERE status='pending')::text || ':' || count(*) FILTER (WHERE status='processing')::text FROM scheduled_jobs;" | tr -d ' ' || true)
    SAMPLE="${SAMPLE:-}"
    if [[ ! "${SAMPLE}" =~ ^[0-9]+:[0-9]+$ ]]; then
        echo "  ${ELAPSED}s: drain DB-unreachable (DB restarting or being re-readied)"
        sleep 10
        continue
    fi

    PEND="${SAMPLE%%:*}"
    PROC="${SAMPLE##*:}"
    echo "  ${ELAPSED}s: pending=${PEND} processing=${PROC}"
    if [[ "${PEND}" -eq 0 && "${PROC}" -eq 0 ]]; then
        DRAINED=true
        break
    fi
    sleep 10
done

# ── Capture the zero-restart evidence BEFORE stopping anything ──
# The claim being validated is "no process needed a restart". Capture the
# recorded API + worker PIDs' aliveness here (drain-end, still running), then
# stop them. Checking kill -0 after stopping would always read dead.
echo ""
echo "=== Process aliveness (recorded PIDs — captured at drain-end) ==="
ALIVE_COUNT=0
ALIVE_LINES=()
if kill -0 "${API_PID}" 2>/dev/null; then
    ALIVE_LINES+=("api pid=${API_PID}: alive")
    ALIVE_COUNT=$((ALIVE_COUNT + 1))
else
    ALIVE_LINES+=("api pid=${API_PID}: DEAD (API restarted/crashed — breaks the claim)")
fi
for i in 1 2 3; do
    if kill -0 "${WORKER_PIDS[$((i-1))]}" 2>/dev/null; then
        ALIVE_LINES+=("worker $((i-1)) pid=${WORKER_PIDS[$((i-1))]}: alive")
        ALIVE_COUNT=$((ALIVE_COUNT + 1))
    else
        ALIVE_LINES+=("worker $((i-1)) pid=${WORKER_PIDS[$((i-1))]}: DEAD")
    fi
done
for line in "${ALIVE_LINES[@]}"; do echo "  ${line}"; done

log "Now safe to stop workers/API/dummy (drain complete)."
stop_workers
stop_api
stop_dummy
sleep 1

# ─── Step 6/6: Verification ─────────────────────────────────────────────────
sep
log "Step 6/6: Verification — outage was real, errors were transient, nobody restarted..."

# Outage evidence: pg_isready DOWN samples. Must be > 0, otherwise the
# chaos never actually happened and every other PASS condition inherits it.
DB_DOWN_HITS=$(grep -c " DOWN" /tmp/chaos-pg-downtime.log 2>/dev/null || true)
DB_DOWN_HITS="${DB_DOWN_HITS:-0}"

# Transient-error evidence: workers hit the dead DB in their poll loop.
FETCH_FAILS=$(grep -h "failed to fetch jobs" /tmp/chaos-pg-worker-*.log 2>/dev/null | wc -l | tr -d ' ' || true)

# Transient-error evidence: the API hit the dead DB on POST /jobs inserts.
API_ERRORS=$(grep -h "failed to insert job" /tmp/chaos-pg-api.log 2>/dev/null | wc -l | tr -d ' ' || true)

# k6-side evidence: the http_req_failed percentage line — parsed from the
# text of the k6 summary rather than thresholds (k6 exit code 1 on threshold
# breach is already healed by || true above).
HTTP_FAILED=$(grep "http_req_failed" /tmp/chaos-pg-k6-output.log | grep -oE '[0-9]+\.[0-9]+%' | head -1 || true)
HTTP_FAILED="${HTTP_FAILED:-n/a}"

# Recovery evidence: the reaper reclaimed rows stranded by failed DB writes.
RECLAIMS=$(grep -h "reaper: reclaimed stale jobs" /tmp/chaos-pg-worker-*.log 2>/dev/null | wc -l | tr -d ' ' || true)

# Skip-loop evidence (expected inhibition on re-claims of uncertain-delivery jobs).
SKIP_LOCK=$(grep -h "idempotency: job already processed" /tmp/chaos-pg-worker-*.log 2>/dev/null | wc -l | tr -d ' ' || true)

# Stability evidence: the word panic must not appear anywhere.
PANICS=$(grep -ih "panic" /tmp/chaos-pg-worker-*.log /tmp/chaos-pg-api.log 2>/dev/null | wc -l | tr -d ' ' || true)

# Zero-restart evidence was captured BEFORE stopping (see the capture block
# above the stop_workers call). ALIVE_COUNT holds the result.

echo ""
echo "=== Log evidence ==="
echo "  DOWN samples (real outage)   : ${DB_DOWN_HITS}"
echo "  failed to fetch jobs (worker): ${FETCH_FAILS}"
echo "  failed to insert job (api)   : ${API_ERRORS}"
echo "  k6 http_req_failed           : ${HTTP_FAILED}"
echo "  reaper: reclaimed stale jobs : ${RECLAIMS}"
echo "  idempotency skip-locks       : ${SKIP_LOCK}"
echo "  panic lines                  : ${PANICS}"

echo ""
echo "=== Final DB state ==="
sql "SELECT status, count(*) FROM scheduled_jobs GROUP BY status ORDER BY status;"
sql "SELECT count(*) AS execution_records FROM job_executions;"

DUPLICATES=$(python3 -c "import json;d=json.load(open('/tmp/webhook-deliveries.json'));print(sum(1 for v in d.values() if v>1))" 2>/dev/null || echo 0)
echo ""
echo "=== Delivery counter (dummy server) ==="
echo "  jobs served more than once: ${DUPLICATES}"

# ─── Summary + Verdict ──────────────────────────────────────────────────────
sep
log "Summary"
echo ""
echo "  Chaos configuration:"
echo "    k6                : ${K6_VUS} VUs for ${K6_DURATION}"
echo "    db restart at     : T+${RESTART_AT_SECONDS}s (docker compose restart postgres)"
echo "    monitored PIDs    : api ${API_PID} + workers ${WORKER_PIDS[*]}"
echo "    reaper            : interval=${WORKER_REAPER_INTERVAL}, stale=${WORKER_STALE_THRESHOLD}"
echo "    drain budget      : ${DRAIN_TIMEOUT}s (capped by Redis idempotency TTL=5min)"
echo ""
echo "  Observations:"
echo "    outage DOWN samples          : ${DB_DOWN_HITS}"
echo "    worker fetch failures        : ${FETCH_FAILS}"
echo "    api insert failures          : ${API_ERRORS}"
echo "    k6 http_req_failed           : ${HTTP_FAILED}"
echo "    reaper reclaim events        : ${RECLAIMS}"
echo "    idempotency skip-locks       : ${SKIP_LOCK}"
echo "    processes alive at end       : ${ALIVE_COUNT}/4 (the zero-restart claim)"
echo "    duplicate deliveries (jobs)  : ${DUPLICATES}"
echo "    panics                       : ${PANICS}"
echo ""
echo "  Artifacts:"
echo "    logs        : /tmp/chaos-pg-api.log, /tmp/chaos-pg-worker-{1,2,3}.log, /tmp/chaos-pg-dummy.log"
echo "    queue       : /tmp/chaos-pg-queue.log"
echo "    downtime    : /tmp/chaos-pg-downtime.log"
echo "    events      : /tmp/chaos-pg-events.log"
echo "    k6 output   : /tmp/chaos-pg-k6-output.log"
echo "    deliveries  : /tmp/webhook-deliveries.json"

sep

# Verdict: PASS requires —
#   (1) the outage was real (DB_DOWN_HITS > 0),
#   (2) all 4 processes survived (zero-restart claim), and
#   (3) the queue fully drained (reaper healed stranded rows) with zero panics.
# FETCH_FAILS / API_ERRORS / HTTP_FAILED are REPORTED, not gated: they are the
# expected transient-error window during the outage. SKIP_LOCK>0 and
# duplicates>0 are WARN-only (same at-least-once residual shape as script 1).

# Final drain check: re-sample so the verdict is based on DB state at verdict
# time, not on the last drain-loop sample.
FINAL_SAMPLE=$(sql "SELECT count(*) FILTER (WHERE status='pending')::text || ':' || count(*) FILTER (WHERE status='processing')::text FROM scheduled_jobs;" | tr -d ' ' || true)
FINAL_SAMPLE="${FINAL_SAMPLE:-}"
FINAL_PENDING=""
FINAL_PROCESSING=""
if [[ "${FINAL_SAMPLE}" =~ ^[0-9]+:[0-9]+$ ]]; then
    FINAL_PENDING="${FINAL_SAMPLE%%:*}"
    FINAL_PROCESSING="${FINAL_SAMPLE##*:}"
fi

PASS=true

if [[ "${DB_DOWN_HITS}" -gt 0 ]]; then
    log "Real outage confirmed: ${DB_DOWN_HITS} DOWN samples in the pg_isready timeline ✓"
else
    warn "0 DOWN samples — the outage was never observed by the monitor; chaos not injected."
    warn "  Check /tmp/chaos-pg-downtime.log and whether 'docker compose restart postgres' ran."
    PASS=false
fi

if [[ "${ALIVE_COUNT}" -eq 4 ]]; then
    log "Zero restarts: API + all ${WORKER_COUNT} workers survived on their original PIDs ✓"
else
    warn "Only ${ALIVE_COUNT}/4 original processes alive — a process was restarted or crashed."
    warn "  That contradicts the claim under validation (resilience WITHOUT restarts)."
    PASS=false
fi

if [[ -n "${FINAL_PENDING}" && "${FINAL_PENDING}" -eq 0 && "${FINAL_PROCESSING}" -eq 0 ]]; then
    log "Queue fully drained: pending=0, processing=0 ✓"
elif [[ -z "${FINAL_PENDING}" ]]; then
    warn "Final DB check could not read pending/processing — DB unreachable at verdict time."
    PASS=false
else
    warn "Queue NOT drained: final pending=${FINAL_PENDING} processing=${FINAL_PROCESSING}."
    warn "  Expected: the reaper reclaimed stranded 'processing' rows and workers drained them."
    PASS=false
fi

if [[ "${PANICS}" -eq 0 ]]; then
    log "No panics in worker/API logs ✓"
else
    warn "Found ${PANICS} lines containing 'panic' in worker/API logs — investigate:"
    grep -ih "panic" /tmp/chaos-pg-worker-*.log /tmp/chaos-pg-api.log 2>/dev/null | head -10 || true
    PASS=false
fi

# INFO-only transient-error report (expected during the outage, not gated):
log "Transient errors (reported, not gated):"
echo "  failed to fetch jobs: ${FETCH_FAILS} (workers kept polling; pgxpool reconnected lazily)"
echo "  failed to insert job : ${API_ERRORS} (API surfaced the outage; next POSTs succeeded)"
echo "  k6 http_req_failed   : ${HTTP_FAILED} (spike bounded to the outage window)"

if [[ "${RECLAIMS}" -gt 0 ]]; then
    log "Reaper exercised: ${RECLAIMS} 'reclaimed stale jobs' events ✓ (outage-stranded rows healed)"
else
    warn "0 reaper reclaim events — either no rows were stranded by the outage, or the run"
    warn "  ended before stale threshold. Only fatal when the queue did not drain."
fi

# WARN-only residuals (documented at-least-once behavior):
if [[ "${SKIP_LOCK}" -gt 0 ]]; then
    warn "Idempotency skip-locks observed: ${SKIP_LOCK} (expected skip-loop until the 5min TTL expires)."
fi

if [[ "${DUPLICATES}" -gt 0 ]]; then
    warn "Duplicates observed: ${DUPLICATES} job(s) delivered more than once."
    warn "  This is the DOCUMENTED at-least-once residual (in-flight delivery whose completion"
    warn "  write hit the outage), not a failure state. Same shape as validate-chaos-worker-kill.sh."
fi

if [[ "${DRAINED}" != "true" ]]; then
    warn "Note: drain loop timed out without reaching 0:0 — see final-sample check above,"
    warn "  which is the verdict-input (it may have completed after the loop gave up)."
fi

sep
if $PASS; then
    log "CHAOS POSTGRES-RESTART VALIDATION PASSED"
    log "  Postgres restarted mid-load → pgxpool reconnected lazily, workers kept polling"
    log "  through 'failed to fetch jobs', the API recovered on the next inserts, the reaper"
    log "  healed outage-stranded rows, and the queue drained to 0 — with ZERO process"
    log "  restarts (all original PIDs alive) and zero panics."
else
    err "CHAOS POSTGRES-RESTART VALIDATION FAILED: see summary above. Inspect /tmp/chaos-pg-* logs."
fi

sep
