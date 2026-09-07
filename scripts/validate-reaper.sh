#!/usr/bin/env bash
# =============================================================================
# Zombie Reaper Validation Script — Webhook Engine P1
# =============================================================================
# Validates: insert zombie job (processing + old started_at) → reaper reclaims
#            → worker picks up → dispatches → completes
#
# Prerequisites:
#   - Docker and docker compose installed
#   - Go 1.25+ installed
#   - Ports 5432, 6379, and 9999 free
#
# Usage:
#   chmod +x scripts/validate-reaper.sh
#   ./scripts/validate-reaper.sh
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

# Fixed UUIDs for reproducibility
ZOMBIE_JOB_ID="deadbeef-1111-2222-3333-444455556666"
DEAD_WORKER_ID="00000000-0000-0000-0000-000000000001"
TENANT_ID="a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# Reaper config (fast values for testing)
REAPER_INTERVAL="5s"
STALE_THRESHOLD="10s"
POLL_INTERVAL="2s"

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
    pkill -f "webhook-engine.*worker" 2>/dev/null || true
    pkill -f "dummy-server" 2>/dev/null || true
}
trap cleanup EXIT

cd "${PROJECT_DIR}"

sep
echo "Webhook Engine — Zombie Reaper Validation"
echo "Project: ${PROJECT_DIR}"
sep

# ─── Step 1: Start Infrastructure ───────────────────────────────────────────
log "Step 1/5: Starting Postgres + Redis via docker compose..."
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

# Verify schema
TABLE_COUNT=$(sql "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('scheduled_jobs','job_executions');")
if [[ "${TABLE_COUNT}" -lt 2 ]]; then
    err "Schema not applied. Expected 2 tables, got ${TABLE_COUNT}"
fi
log "Schema verified: scheduled_jobs + job_executions tables exist"

# ─── Step 2: Insert Zombie Job ──────────────────────────────────────────────
sep
log "Step 2/5: Inserting zombie job (simulates worker crash during processing)..."

sql "
  INSERT INTO scheduled_jobs (
    id, tenant_id, idempotency_key, url, http_method,
    request_headers, request_body, schedule_at, status,
    attempt_count, max_attempts, worker_id, started_at, last_attempt_at
  ) VALUES (
    '${ZOMBIE_JOB_ID}',
    '${TENANT_ID}',
    'zombie-test-key',
    'http://localhost:9999/webhook',
    'POST',
    '{\"Content-Type\": \"application/json\"}',
    '{\"test\":\"zombie\"}',
    NOW(),
    'processing',
    1,
    5,
    '${DEAD_WORKER_ID}',
    NOW() - INTERVAL '5 minutes',
    NOW() - INTERVAL '5 minutes'
  );
"

# Insert the fake execution record for the original (crashed) attempt
sql "
  INSERT INTO job_executions (job_id, attempt_num, started_at, ended_at, duration_ms, worker_id, error_message)
  VALUES (
    '${ZOMBIE_JOB_ID}',
    1,
    NOW() - INTERVAL '5 minutes',
    NOW() - INTERVAL '5 minutes',
    30000,
    '${DEAD_WORKER_ID}',
    'worker crash simulated (SIGKILL)'
  );
"

# Verify insertion
INITIAL_STATUS=$(sql "SELECT status FROM scheduled_jobs WHERE id='${ZOMBIE_JOB_ID}';")
EXEC_BEFORE=$(sql "SELECT count(*) FROM job_executions WHERE job_id='${ZOMBIE_JOB_ID}';")
log "Zombie job inserted: status=${INITIAL_STATUS} | attempt_count=1 | execution records=${EXEC_BEFORE}"
log "Job has been stuck in 'processing' for 5 minutes — reaper should reclaim it"

# ─── Step 3: Start Worker ───────────────────────────────────────────────────
sep
log "Step 3/5: Starting worker with reaper (interval=${REAPER_INTERVAL}, threshold=${STALE_THRESHOLD})..."

log "Starting dummy HTTP server on port 9999 (counts deliveries)..."
python3 scripts/dummy-server.py &
HTTP_PID=$!
disown
sleep 0.5

# Build worker binary
log "Building worker binary..."
go build -o /tmp/webhook-worker ./cmd/worker/

log "Starting worker..."
DATABASE_URL="${DATABASE_URL}" \
    REDIS_ADDR="${REDIS_ADDR}" \
    WORKER_POLL_INTERVAL="${POLL_INTERVAL}" \
    WORKER_REAPER_INTERVAL="${REAPER_INTERVAL}" \
    WORKER_STALE_THRESHOLD="${STALE_THRESHOLD}" \
    WORKER_MAX_CONCURRENCY=5 \
    /tmp/webhook-worker &
WORKER_PID=$!

log "Worker PID: ${WORKER_PID}"

# ─── Step 4: Wait for Reaper + Processing ───────────────────────────────────
sep
log "Step 4/5: Waiting for reaper cycle + worker processing..."
log "  Reaper interval: ${REAPER_INTERVAL}"
log "  Worker poll:     ${POLL_INTERVAL}"

# Wait long enough for:
#   1 reaper cycle (5s) + 1 poll cycle (2s) + HTTP request + buffer
sleep 15

# ─── Step 5: Verify Recovery ────────────────────────────────────────────────
sep
log "Step 5/5: Verifying zombie job recovery..."

echo ""
echo "=== Job State ==="
sql "SELECT id, status, attempt_count, worker_id FROM scheduled_jobs WHERE id='${ZOMBIE_JOB_ID}';"

echo ""
echo "=== Execution Records ==="
sql "SELECT attempt_num, response_status_code, error_message, duration_ms
     FROM job_executions
     WHERE job_id='${ZOMBIE_JOB_ID}'
     ORDER BY attempt_num;"

echo ""
echo "=== Full Status ==="
sql "SELECT status, count(*) FROM scheduled_jobs GROUP BY status ORDER BY status;"

FINAL_STATUS=$(sql "SELECT status FROM scheduled_jobs WHERE id='${ZOMBIE_JOB_ID}';")
EXEC_AFTER=$(sql "SELECT count(*) FROM job_executions WHERE job_id='${ZOMBIE_JOB_ID}';")
PROCESSING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='processing';")

echo ""
echo "=== Summary ==="
echo "  Zombie job status:     ${FINAL_STATUS}"
echo "  Execution records:     ${EXEC_AFTER}"
echo "  Processing (stale):    ${PROCESSING}"

sep

# ─── Verdict ────────────────────────────────────────────────────────────────
PASS=true

if [[ "${FINAL_STATUS}" == *"completed"* ]]; then
    log "Zombie job recovered: completed ✓"
else
    warn "Expected status='completed', got '${FINAL_STATUS}'"
    PASS=false
fi

if [[ "${EXEC_AFTER}" -eq 2 ]]; then
    log "Execution records: 2 (attempt 1=crashed + attempt 2=success) ✓"
else
    warn "Expected 2 execution records, got ${EXEC_AFTER}"
    PASS=false
fi

if [[ "${PROCESSING}" -eq 0 ]]; then
    log "No stale processing jobs remain ✓"
else
    warn "Expected 0 processing jobs, got ${PROCESSING}"
    PASS=false
fi

sep

if $PASS; then
    log "VALIDATION PASSED"
    log "Reaper flow: zombie(processing) → reaper reclaims → pending → worker picks up → completed"
    log "The zombie reaper correctly detects and recovers jobs stuck in 'processing' status."
else
    err "VALIDATION FAILED: Check worker output above for errors."
fi

sep
log "Infrastructure still running. To stop: docker compose down"
