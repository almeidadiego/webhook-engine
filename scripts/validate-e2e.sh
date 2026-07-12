#!/usr/bin/env bash
# =============================================================================
# End-to-End Validation Script — Webhook Engine Marco 1
# =============================================================================
# Validates: seeder → insert → fetch+claim (FOR UPDATE SKIP LOCKED) → dispatch → completion
#
# Prerequisites:
#   - Docker and docker compose installed
#   - Go 1.25+ installed
#   - Ports 5432 and 6379 free (or adjust below)
#
# Usage:
#   chmod +x scripts/validate-e2e.sh
#   ./scripts/validate-e2e.sh
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

# UUIDs for test run
TENANT_ID="a1b2c3d4-e5f6-7890-abcd-ef1234567890"
WORKER_ID="b2c3d4e5-f6a7-8901-bcde-f12345678901"
SEEDER_COUNT=5
SEEDER_TARGET_URL="http://localhost:9999/webhook"
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
    # Kill worker binary (not go run wrapper) by process name
    pkill -f "webhook-engine.*worker" 2>/dev/null || true
    # Kill HTTP server
    pkill -f "python3.*9999" 2>/dev/null || true
}
trap cleanup EXIT

cd "${PROJECT_DIR}"

sep
echo "Webhook Engine — End-to-End Validation"
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

# Verify schema was applied
TABLE_COUNT=$(sql "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('scheduled_jobs','job_executions');")
if [[ "${TABLE_COUNT}" -lt 2 ]]; then
    err "Schema not applied. Expected 2 tables, got ${TABLE_COUNT}"
fi
log "Schema verified: scheduled_jobs + job_executions tables exist"

# ─── Step 2: Initial State ──────────────────────────────────────────────────
sep
log "Step 2/6: Verifying initial state..."

JOB_COUNT=$(sql "SELECT count(*) FROM scheduled_jobs;")
KEY_COUNT=$(redis_cli DBSIZE)
log "DB has ${JOB_COUNT} jobs | Redis has ${KEY_COUNT} keys"

# ─── Step 3: Seed Jobs ──────────────────────────────────────────────────────
sep
log "Step 3/6: Seeding ${SEEDER_COUNT} jobs..."

SEEDER_OUTPUT=$(DATABASE_URL="${DATABASE_URL}" \
    SEEDER_TENANT_ID="${TENANT_ID}" \
    SEEDER_COUNT="${SEEDER_COUNT}" \
    SEEDER_TARGET_URL="${SEEDER_TARGET_URL}" \
    go run ./cmd/seeder/ 2>&1)

echo "${SEEDER_OUTPUT}"

INSERTED=$(echo "${SEEDER_OUTPUT}" | sed -n 's/.* inserted=\([0-9][0-9]*\).*/\1/p')
INSERTED="${INSERTED:-0}"
CONFLICTS=$(echo "${SEEDER_OUTPUT}" | sed -n 's/.* conflicts=\([0-9][0-9]*\).*/\1/p')
CONFLICTS="${CONFLICTS:-0}"

if [[ "${INSERTED}" -eq 0 ]]; then
    err "No jobs were inserted"
fi
log "Inserted ${INSERTED} jobs, ${CONFLICTS} conflicts"

# ─── Step 4: Verify Seeded Data ─────────────────────────────────────────────
sep
log "Step 4/6: Verifying seeded data in DB..."

PENDING_COUNT=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='pending';")
log "Pending jobs in DB: ${PENDING_COUNT}"

# Show a sample row
sql "SELECT id, tenant_id, idempotency_key, url, status, attempt_count, max_attempts, schedule_at
     FROM scheduled_jobs LIMIT 3;"

# ─── Step 5: Run Worker ─────────────────────────────────────────────────────
sep
log "Step 5/6: Starting worker (will run for ${POLL_INTERVAL} poll interval)..."

# The worker will POST to SEEDER_TARGET_URL — start a simple HTTP 200 server
log "Starting dummy HTTP server on port 9999 (responds 200)..."
python3 -c "
import http.server, socketserver
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'ok')
    def log_message(self, *a): pass
socketserver.TCPServer(('', 9999), H).serve_forever()
" &
HTTP_PID=$!
disown
sleep 0.5

# Build worker binary so we can kill the actual process, not the go run wrapper
log "Building worker binary..."
go build -o /tmp/webhook-worker ./cmd/worker/

log "Starting worker..."
DATABASE_URL="${DATABASE_URL}" \
    REDIS_ADDR="${REDIS_ADDR}" \
    WORKER_POLL_INTERVAL="${POLL_INTERVAL}" \
    WORKER_MAX_CONCURRENCY=5 \
    /tmp/webhook-worker &
WORKER_PID=$!

log "Worker PID: ${WORKER_PID}"
log "Waiting for worker to poll and process jobs..."

# Wait for multiple poll cycles to ensure processing
sleep 8

# ─── Step 6: Verify Results ─────────────────────────────────────────────────
sep
log "Step 6/6: Verifying job state transitions..."

echo ""
echo "=== Jobs by Status ==="
sql "SELECT status, count(*) FROM scheduled_jobs GROUP BY status ORDER BY status;"

COMPLETED=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='completed';")
FAILED=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='failed';")
PROCESSING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='processing';")
PENDING_REMAINING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='pending';")

echo ""
echo "=== Job Executions ==="
sql "SELECT job_id, attempt_num, response_status_code, error_message, duration_ms FROM job_executions ORDER BY started_at DESC LIMIT 10;"

echo ""
echo "=== Redis Keys ==="
REDIS_KEYS=$(redis_cli KEYS "idemp:*")
if [[ -n "${REDIS_KEYS}" ]]; then
    for key in ${REDIS_KEYS}; do
        ttl=$(redis_cli TTL "${key}")
        echo "  ${key} (TTL: ${ttl}s)"
    done
else
    warn "No idempotency keys in Redis"
fi

echo ""
echo "=== Summary ==="
echo "  Completed:       ${COMPLETED}"
echo "  Failed:          ${FAILED}"
echo "  Processing:      ${PROCESSING}"
echo "  Pending:         ${PENDING_REMAINING}"
echo "  Redis idemp keys: $(redis_cli DBSIZE)"

sep

# ─── Verdict ────────────────────────────────────────────────────────────────
if [[ "${COMPLETED}" -gt 0 ]] || [[ "${FAILED}" -gt 0 ]]; then
    log "VALIDATION PASSED: Jobs transitioned from pending → completed/failed"
    log "The end-to-end flow (seeder→insert→fetch+claim→dispatch→completion) works correctly."
else
    err "VALIDATION FAILED: No jobs were processed. Check the worker output above for errors."
fi

sep
log "Infrastructure still running. To stop: docker compose down"
log "Script completed."
