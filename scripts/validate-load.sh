#!/usr/bin/env bash
# =============================================================================
# Load Test Orchestrator — Webhook Engine Marco 4
# =============================================================================
# Runs 3 escalating k6 load rounds (50 → 200 → 500 VUs) against the ingestion
# API while collecting Postgres pool saturation metrics between rounds.
#
# Prerequisites:
#   - Docker and docker compose installed
#   - k6 installed and on PATH
#   - Go 1.25+ installed
#   - Ports 5432, 6379, 8080, and 9999 free
#
# Usage:
#   ./scripts/validate-load.sh [START_ROUND] [END_ROUND]
#
#   START_ROUND / END_ROUND (optional, default 1 / 3) select a sub-range of
#   the escalation ladder so individual rounds can be re-run in isolation,
#   e.g. `./scripts/validate-load.sh 3 3` runs only the 500 VU round.
#
#   SUSTAIN_DURATION (env, default "60s") controls how long each round holds
#   peak load, e.g. `SUSTAIN_DURATION=120s ./scripts/validate-load.sh`.
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
WORKER_CONCURRENCY=10
POLL_INTERVAL="1s"
REAPER_INTERVAL="10s"
STALE_THRESHOLD="60s"
QUEUE_SAMPLE_INTERVAL=5   # seconds between queue depth samples (0 to disable)

# Env-overridable: how long each round sustains peak VUs.
SUSTAIN_DURATION="${DURATION:-60s}"

# Escalation ladder: round number → target VUs.
# Uses a case statement instead of declare -A for macOS bash 3.2 compatibility.
round_vus() {
    case "$1" in
        1) echo 50 ;;
        2) echo 200 ;;
        3) echo 500 ;;
        *) echo "" ;;
    esac
}

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

# ─── Cleanup ────────────────────────────────────────────────────────────────
cleanup() {
    log "Cleaning up..."
    pkill -f "cmd/api" 2>/dev/null || true
    pkill -f "go-build.*api" 2>/dev/null || true
    pkill -f "exe/api" 2>/dev/null || true
    pkill -f "cmd/worker" 2>/dev/null || true
    pkill -f "exe/worker" 2>/dev/null || true
    pkill -f "go-build.*worker" 2>/dev/null || true
    pkill -f "webhook-worker" 2>/dev/null || true
    pkill -f "dummy-server" 2>/dev/null || true
    pkill -f "python3.*9999" 2>/dev/null || true
    # Result artifacts (k6-output, pool samples, summary JSON) are preserved
    # intentionally — they are the analysis deliverable of the load test.
    rm -f /tmp/api-load.log /tmp/worker-*-load.log /tmp/webhook-deliveries.json
}
trap cleanup EXIT

cd "${PROJECT_DIR}"

sep
echo "Webhook Engine — Load Test Orchestrator (Marco 4)"
echo "Project: ${PROJECT_DIR}"
echo "Rounds: 50 → 200 → 500 VUs, sustain=${SUSTAIN_DURATION}"
sep

# ─── Prerequisites ──────────────────────────────────────────────────────────
command -v k6 >/dev/null 2>&1 || err "k6 is not installed or not on PATH (expected at /opt/homebrew/bin/k6)"
command -v docker >/dev/null 2>&1 || err "docker is not installed or not on PATH"

START_ROUND="${1:-1}"
END_ROUND="${2:-3}"
if [[ ! "${START_ROUND}" =~ ^[1-3]$ ]] || [[ ! "${END_ROUND}" =~ ^[1-3]$ ]]; then
    err "START_ROUND/END_ROUND must be integers in the range 1-3 (got '${START_ROUND}' / '${END_ROUND}')"
fi
if [[ "${START_ROUND}" -gt "${END_ROUND}" ]]; then
    err "START_ROUND (${START_ROUND}) cannot be greater than END_ROUND (${END_ROUND})"
fi
log "Rounds to run: ${START_ROUND}..${END_ROUND}"

# ─── Step 1: Infrastructure ─────────────────────────────────────────────────
sep
log "Step 1/4: Starting Postgres + Redis via docker compose..."
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

# Reset the delivery counter the dummy server relies on.
rm -f /tmp/webhook-deliveries.json

# ─── Step 2: Start Services ─────────────────────────────────────────────────
sep
log "Step 2/4: Starting dummy server, API, and ${WORKER_COUNT} workers..."

log "Starting dummy HTTP server on port ${DUMMY_PORT}..."
DUMMY_MIN_DELAY_MS="${DUMMY_MIN_DELAY_MS:-0}" \
DUMMY_MAX_DELAY_MS="${DUMMY_MAX_DELAY_MS:-0}" \
python3 scripts/dummy-server.py &
disown
sleep 0.5

log "Starting ingestion API on :${API_PORT}..."
DATABASE_URL="${DATABASE_URL}" \
    API_ADDR=":${API_PORT}" \
    go run ./cmd/api > /tmp/api-load.log 2>&1 &
disown

log "Waiting for API to be ready..."
API_READY=0
for i in $(seq 1 10); do
    if curl -s "http://localhost:${API_PORT}/metrics/pool" >/dev/null 2>&1; then
        API_READY=1
        log "API is ready"
        break
    fi
    sleep 1
done
if [[ "${API_READY}" -ne 1 ]]; then
    err "API did not become ready within 10s; see /tmp/api-load.log"
fi

log "Starting ${WORKER_COUNT} workers (concurrency=${WORKER_CONCURRENCY})..."
for i in $(seq 1 "${WORKER_COUNT}"); do
    DATABASE_URL="${DATABASE_URL}" \
        REDIS_ADDR="${REDIS_ADDR}" \
        WORKER_POLL_INTERVAL="${POLL_INTERVAL}" \
        WORKER_REAPER_INTERVAL="${REAPER_INTERVAL}" \
        WORKER_STALE_THRESHOLD="${STALE_THRESHOLD}" \
        WORKER_MAX_CONCURRENCY="${WORKER_CONCURRENCY}" \
        go run ./cmd/worker > "/tmp/worker-${i}-load.log" 2>&1 &
    disown
done
log "Services started; logs: /tmp/api-load.log, /tmp/worker-{1,2,3}-load.log"

# ─── Step 3: Rounds Loop ────────────────────────────────────────────────────
sep
log "Step 3/4: Running load rounds..."

ROUND_RESULTS=()

for ROUND in $(seq "${START_ROUND}" "${END_ROUND}"); do
    VUS="$(round_vus "$ROUND")"
    if [[ -z "${VUS}" ]]; then
        err "Invalid round number: ${ROUND}"
    fi
    sep
    log "Round ${ROUND}/3: ${VUS} VUs (sustain=${SUSTAIN_DURATION})"

    # Fresh database state per round so latency reflects each load level in
    # isolation rather than accumulated backlog from earlier rounds.
    if [[ "${ROUND}" -gt "${START_ROUND}" ]]; then
        log "Resetting DB + Redis before round..."
        sql "TRUNCATE TABLE job_executions, scheduled_jobs;"
        redis_cli FLUSHDB
        sleep 2
    fi

    # Background collector samples the pool saturation endpoint every 10s.
    (
        while true; do
            curl -s "http://localhost:${API_PORT}/metrics/pool" >> "/tmp/pool-round${ROUND}.log"
            echo "" >> "/tmp/pool-round${ROUND}.log"
            sleep 10
        done
    ) &
    POOL_PID=$!
    disown

    # Queue depth collector: samples pending/processing/completed counts every QUEUE_SAMPLE_INTERVAL seconds
    # This reveals semaphore saturation (processing capped at MaxConcurrency * workers) and
    # queue backlog (pending growing monotonically when producer > consumer).
    QUEUE_LOG="/tmp/queue-round${ROUND}.log"
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

    log "Running k6 with ${VUS} VUs..."
    # `|| true` on the whole pipeline lets a round finish even if k6 exits
    # non-zero (e.g. a lenient threshold was crossed), so the summary still
    # runs and we can observe degradation instead of aborting the ladder.
    API_URL="${API_URL}/jobs" \
        TARGET_URL="${TARGET_URL}" \
        VUS="${VUS}" \
        DURATION="${SUSTAIN_DURATION}" \
        k6 run --summary-export="/tmp/k6-summary-round${ROUND}.json" \
        loadtest/k6-load.js 2>&1 | tee "/tmp/k6-output-round${ROUND}.log" || true

    kill "${POOL_PID}" 2>/dev/null || true
    wait "${POOL_PID}" 2>/dev/null || true
    kill "${QUEUE_PID}" 2>/dev/null || true
    wait "${QUEUE_PID}" 2>/dev/null || true
    log "Pool collector stopped; samples: /tmp/pool-round${ROUND}.log"

    # Extract headline metrics from the k6 output. The real percentiles live on
    # the `http_req_duration..............:` line (not the threshold line above
    # it); p99 is only printed in the threshold block, so grab it there. Each
    # pipeline ends with `|| true`: under `set -o pipefail`, the upstream grep
    # receives SIGPIPE when `head -1` closes the pipe early, which would
    # otherwise abort the whole script before the next round.
    P95=$(grep "http_req_duration\.\.\.\.\.\." "/tmp/k6-output-round${ROUND}.log" | grep -oE 'p\(95\)=[0-9.]+m?s' | head -1 | sed 's/p(95)=//' || true)
    P99=$(grep "'p(99)" "/tmp/k6-output-round${ROUND}.log" | grep -oE 'p\(99\)=[0-9.]+m?s' | head -1 | sed 's/p(99)=//' || true)
    HTTP_REQS=$(grep "http_reqs\.\.\.\." "/tmp/k6-output-round${ROUND}.log" | grep -oE '[0-9]+\.[0-9]+/s' | head -1 || true)
    HTTP_FAILED=$(grep "http_req_failed\.\.\." "/tmp/k6-output-round${ROUND}.log" | grep -oE '[0-9]+\.[0-9]+%' | head -1 || true)

    # Max concurrently acquired DB connections observed across the round.
    MAX_ACQUIRED=$(grep -oE '"acquired_conns":[0-9]+' "/tmp/pool-round${ROUND}.log" | sed 's/"acquired_conns"://' | sort -n | tail -1)
    MAX_ACQUIRED="${MAX_ACQUIRED:-0}"

    # Queue metrics: max concurrent processing (semaphore saturation signal)
    # and final pending count (backlog growth signal)
    MAX_PROCESSING=$(grep -oE 'processing=[0-9]+' "${QUEUE_LOG}" | sed 's/processing=//' | sort -n | tail -1 || echo "0")
    LAST_PENDING=$(grep -oE 'pending=[0-9]+' "${QUEUE_LOG}" | sed 's/pending=//' | tail -1 || echo "0")
    LAST_COMPLETED=$(grep -oE 'completed=[0-9]+' "${QUEUE_LOG}" | sed 's/completed=//' | tail -1 || echo "0")

    ROUND_RESULTS+=("Round ${ROUND} (${VUS} VUs): p95=${P95:-n/a}, p99=${P99:-n/a}, reqs/s=${HTTP_REQS:-n/a}, errors=${HTTP_FAILED:-n/a}, max_pool_acquired=${MAX_ACQUIRED:-0}, max_processing=${MAX_PROCESSING:-0}, final_pending=${LAST_PENDING:-0}, final_completed=${LAST_COMPLETED:-0}")
done

# ─── Step 4: Summary ────────────────────────────────────────────────────────
sep
log "Step 4/4: Summary"

echo ""
echo "=== Load Test Results ==="
for RESULT in "${ROUND_RESULTS[@]}"; do
    echo "  ${RESULT}"
done

echo ""
echo "=== API Errors (if any) ==="
if grep -iE "error|level=ERROR" /tmp/api-load.log | grep -v "level=INFO" >/dev/null 2>&1; then
    grep -iE "error|level=ERROR" /tmp/api-load.log | grep -v "level=INFO" | head -20
else
    echo "  No ERROR lines found in API log"
fi

echo ""
echo "=== Reaper Activity (stale job reclamation) ==="
REAPER_HITS=$(grep -h "reaper: reclaimed" /tmp/worker-*-load.log 2>/dev/null | wc -l | tr -d ' ' || true)
if [[ "${REAPER_HITS}" -gt 0 ]]; then
    grep -h "reaper: reclaimed" /tmp/worker-*-load.log | head -10
else
    echo "  No 'reaper: reclaimed' activity observed (expected — jobs should be consumed normally)"
fi

sep
log "LOAD TEST ROUNDS COMPLETE"
warn "Chaos-scenario next steps (not run here):"
warn "  1. Kill a worker mid-round to observe re-queueing / reaper reclamation."
warn "  2. Restart Postgres mid-round to observe pool reconnect and error handling."
sep
