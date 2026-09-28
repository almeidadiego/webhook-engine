#!/usr/bin/env bash
# =============================================================================
# Idempotency Race Validation Script — Webhook Engine Marco 3 Scenario B
# =============================================================================
# Validates: forced SET NX race. A job stuck in 'processing' with an old
#            started_at has its idempotency key pre-seeded in Redis. The worker
#            must reaper-skip it and NOT re-deliver.
#
# Prerequisites:
#   - Docker and docker compose installed
#   - Go 1.25+ installed
#   - Ports 5432, 6379, and 9999 free
#
# Usage:
#   ./scripts/validate-idempotency-race.sh
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

TARGET_URL="http://localhost:9999/webhook"
IDEMPOTENCY_KEY="race-test-key"
TENANT_ID="a1b2c3d4-e5f6-7890-abcd-ef1234567890"
DEAD_WORKER_ID="00000000-0000-0000-0000-000000000099"

# ─── Cleanup ────────────────────────────────────────────────────────────────
cleanup() {
    log "Cleaning up..."
    pkill -f "cmd/worker" 2>/dev/null || true
    pkill -f "exe/worker" 2>/dev/null || true
    pkill -f "go-build.*worker" 2>/dev/null || true
    pkill -f "webhook-worker" 2>/dev/null || true
    pkill -f "dummy-server" 2>/dev/null || true
    rm -f /tmp/webhook-deliveries.json
}
trap cleanup EXIT

cd "${PROJECT_DIR}"

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

sep
echo "Webhook Engine — Idempotency Race Validation"
echo "Project: ${PROJECT_DIR}"
sep

# ─── Step 1: Start Infrastructure ───────────────────────────────────────────
log "Step 1/6: Starting Postgres + Redis via docker compose..."
docker compose down -v --remove-orphans 2>/dev/null || true
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

TABLE_COUNT=$(sql "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('scheduled_jobs','job_executions');")
if [[ "${TABLE_COUNT}" -lt 2 ]]; then
    err "Schema not applied. Expected 2 tables, got ${TABLE_COUNT}"
fi
log "Schema verified: scheduled_jobs + job_executions tables exist"

# ─── Step 2: Start Dummy Server ─────────────────────────────────────────────
sep
log "Step 2/6: Building + starting dummy HTTP server on port 9999 (counts deliveries)..."
# The Go dummy server is concurrent by construction (one goroutine per request
# in net/http), unlike the old single-threaded Python HTTPServer which
# serialized delayed requests and became the bottleneck of load tests.
# Binary name contains "dummy-server" so the cleanup pkill pattern matches it.
go build -o /tmp/webhook-dummy-server ./cmd/dummy-server/
/tmp/webhook-dummy-server &
HTTP_PID=$!
disown
sleep 0.5

# ─── Step 3: Insert Processing Job with Idempotency Key ───────────────────────
sep
log "Step 3/6: Inserting processing job with pre-seeded Redis idempotency key..."

sql "
  INSERT INTO scheduled_jobs (
      tenant_id, idempotency_key, url, http_method,
      request_headers, request_body, schedule_at, status,
      attempt_count, max_attempts, worker_id, started_at, last_attempt_at
  ) VALUES (
      '${TENANT_ID}', '${IDEMPOTENCY_KEY}', '${TARGET_URL}', 'POST',
      '{\"Content-Type\": \"application/json\"}', '{\"test\":\"race\"}', NOW(), 'processing',
      1, 5, '${DEAD_WORKER_ID}', NOW() - INTERVAL '2 minutes', NOW() - INTERVAL '2 minutes'
  );
"

JOB_ID=$(sql "SELECT id FROM scheduled_jobs WHERE idempotency_key = '${IDEMPOTENCY_KEY}';")
JOB_ID="${JOB_ID// /}"
if [[ -z "${JOB_ID}" ]]; then
    err "Failed to retrieve job ID"
fi
log "Inserted job ID: ${JOB_ID}"

sql "
  INSERT INTO job_executions (job_id, attempt_num, started_at, ended_at, duration_ms, worker_id, response_status_code)
  VALUES (
      '${JOB_ID}',
      1,
      NOW() - INTERVAL '2 minutes',
      NOW() - INTERVAL '2 minutes',
      1000,
      '${DEAD_WORKER_ID}',
      200
  );
"

redis_cli SET "idemp:${IDEMPOTENCY_KEY}" "p" EX 300

INITIAL_STATUS=$(sql "SELECT status FROM scheduled_jobs WHERE id='${JOB_ID}';")
INITIAL_STATUS="${INITIAL_STATUS// /}"
EXEC_BEFORE=$(sql "SELECT count(*) FROM job_executions WHERE job_id='${JOB_ID}';")
EXEC_BEFORE="${EXEC_BEFORE// /}"
TTL_BEFORE=$(redis_cli TTL "idemp:${IDEMPOTENCY_KEY}")
TTL_BEFORE="${TTL_BEFORE// /}"

log "Initial state: status=${INITIAL_STATUS} | executions=${EXEC_BEFORE} | redis_ttl=${TTL_BEFORE}s"

# ─── Step 4: Start Worker with Reaper ─────────────────────────────────────────
sep
log "Step 4/6: Starting worker with reaper (poll=1s, reaper=10s, stale=60s)..."

DATABASE_URL="${DATABASE_URL}" \
    REDIS_ADDR="${REDIS_ADDR}" \
    WORKER_POLL_INTERVAL="1s" \
    WORKER_REAPER_INTERVAL="10s" \
    WORKER_STALE_THRESHOLD="60s" \
    WORKER_MAX_CONCURRENCY=5 \
    go run ./cmd/worker > /tmp/worker-race.log 2>&1 &
disown
sleep 0.5

# ─── Step 5: Wait for Reaper Cycle ────────────────────────────────────────────
sep
log "Step 5/6: Waiting 30s for reaper cycle + worker processing..."
sleep 30

# ─── Step 6: Verify Results ─────────────────────────────────────────────────
sep
log "Step 6/6: Verifying results..."

pkill -f "cmd/worker" 2>/dev/null || true
pkill -f "exe/worker" 2>/dev/null || true
    pkill -f "go-build.*worker" 2>/dev/null || true
pkill -f "webhook-worker" 2>/dev/null || true
pkill -f "dummy-server" 2>/dev/null || true

FINAL_STATUS=$(sql "SELECT status FROM scheduled_jobs WHERE id='${JOB_ID}';")
FINAL_STATUS="${FINAL_STATUS// /}"
EXEC_AFTER=$(sql "SELECT count(*) FROM job_executions WHERE job_id='${JOB_ID}';")
EXEC_AFTER="${EXEC_AFTER// /}"

DELIVERIES=0
if [[ -f /tmp/webhook-deliveries.json ]]; then
    DELIVERIES=$(python3 -c "import json; print(sum(json.load(open('/tmp/webhook-deliveries.json')).values()))")
    DELIVERIES="${DELIVERIES// /}"
fi

echo ""
echo "=== Summary ==="
echo "  Job status:           ${FINAL_STATUS}"
echo "  Execution records:    ${EXEC_AFTER}"
echo "  HTTP deliveries:      ${DELIVERIES}"
echo ""
echo "=== Worker log (idempotency-related lines) ==="
grep -i "idempotency" /tmp/worker-race.log || true

sep

if [[ "${FINAL_STATUS}" == "processing" ]]; then
    log "Job remained 'processing' (was skipped due to idempotency key) ✓"
else
    err "Expected job status 'processing', got '${FINAL_STATUS}'"
fi

if [[ "${EXEC_AFTER}" -eq 1 ]]; then
    log "Execution records still == 1 (no re-delivery) ✓"
else
    err "Expected 1 execution record, got ${EXEC_AFTER}"
fi

if [[ "${DELIVERIES}" -eq 0 ]]; then
    log "HTTP deliveries == 0 (no re-delivery) ✓"
else
    err "Expected 0 HTTP deliveries, got ${DELIVERIES}"
fi

if grep -q "idempotency: job already processed" /tmp/worker-race.log; then
    log "Worker log contains 'idempotency: job already processed' ✓"
else
    err "Worker log does not contain 'idempotency: job already processed'"
fi

sep
log "VALIDATION PASSED"
log "Idempotency race: stale job + Redis key → worker skipped → no re-delivery"
sep
