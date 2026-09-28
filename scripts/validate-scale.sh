#!/usr/bin/env bash
# =============================================================================
# Scale Validation Script — Webhook Engine Marco 3 Scenario A
# =============================================================================
# Validates: 1000 jobs, 3 workers, no duplicate deliveries, even load distribution
#
# Prerequisites:
#   - Docker and docker compose installed
#   - Go 1.25+ installed
#   - Ports 5432, 6379, and 9999 free
#
# Usage:
#   ./scripts/validate-scale.sh
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

JOB_COUNT=1000
WORKER_COUNT=3
WORKER_CONCURRENCY=10
TARGET_URL="http://localhost:9999/webhook"
POLL_INTERVAL="1s"
TIMEOUT=60

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
    pkill -f "cmd/worker" 2>/dev/null || true
    pkill -f "exe/worker" 2>/dev/null || true
    pkill -f "go-build.*worker" 2>/dev/null || true
    pkill -f "webhook-worker" 2>/dev/null || true
    pkill -f "dummy-server" 2>/dev/null || true
    rm -f /tmp/webhook-deliveries.json
}
trap cleanup EXIT

cd "${PROJECT_DIR}"

sep
echo "Webhook Engine — Scale Validation"
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

# ─── Step 3: Seed Jobs ──────────────────────────────────────────────────────
sep
log "Step 3/6: Seeding ${JOB_COUNT} jobs..."

SEEDER_OUTPUT=$(DATABASE_URL="${DATABASE_URL}" \
    SEEDER_COUNT="${JOB_COUNT}" \
    SEEDER_TARGET_URL="${TARGET_URL}" \
    go run ./cmd/seeder 2>&1)

echo "${SEEDER_OUTPUT}"

INSERTED=$(echo "${SEEDER_OUTPUT}" | sed -n 's/.* inserted=\([0-9][0-9]*\).*/\1/p')
INSERTED="${INSERTED:-0}"
CONFLICTS=$(echo "${SEEDER_OUTPUT}" | sed -n 's/.* conflicts=\([0-9][0-9]*\).*/\1/p')
CONFLICTS="${CONFLICTS:-0}"

if [[ "${INSERTED}" -ne "${JOB_COUNT}" ]]; then
    err "Expected ${JOB_COUNT} jobs inserted, got ${INSERTED}"
fi
log "Inserted ${INSERTED} jobs, ${CONFLICTS} conflicts"

# ─── Step 4: Start Workers ──────────────────────────────────────────────────
sep
log "Step 4/6: Starting ${WORKER_COUNT} workers (concurrency=${WORKER_CONCURRENCY})..."

for i in $(seq 1 "${WORKER_COUNT}"); do
    DATABASE_URL="${DATABASE_URL}" \
        REDIS_ADDR="${REDIS_ADDR}" \
        WORKER_POLL_INTERVAL="${POLL_INTERVAL}" \
        WORKER_REAPER_INTERVAL="10s" \
        WORKER_STALE_THRESHOLD="60s" \
        WORKER_MAX_CONCURRENCY="${WORKER_CONCURRENCY}" \
        go run ./cmd/worker > "/tmp/worker-${i}.log" 2>&1 &
    disown
done

log "Workers started; logs: /tmp/worker-{1,2,3}.log"

# ─── Step 5: Wait for Completion ────────────────────────────────────────────
sep
log "Step 5/6: Waiting up to ${TIMEOUT}s for all jobs to complete..."

START=$(date +%s)
while true; do
    COMPLETED=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='completed';")
    COMPLETED="${COMPLETED// /}"
    if [[ "${COMPLETED}" -eq "${JOB_COUNT}" ]]; then
        log "All ${JOB_COUNT} jobs completed"
        break
    fi

    NOW=$(date +%s)
    ELAPSED=$((NOW - START))
    if [[ "${ELAPSED}" -ge "${TIMEOUT}" ]]; then
        err "Timeout: only ${COMPLETED}/${JOB_COUNT} jobs completed after ${TIMEOUT}s"
    fi

    sleep 1
done

# ─── Step 6: Verify Results ─────────────────────────────────────────────────
sep
log "Step 6/6: Verifying results..."

# Kill workers and dummy server before final checks
pkill -f "cmd/worker" 2>/dev/null || true
pkill -f "exe/worker" 2>/dev/null || true
    pkill -f "go-build.*worker" 2>/dev/null || true
pkill -f "webhook-worker" 2>/dev/null || true
pkill -f "dummy-server" 2>/dev/null || true

EXECUTIONS=$(sql "SELECT count(*) FROM job_executions;")
EXECUTIONS="${EXECUTIONS// /}"
DUPLICATES=$(sql "SELECT COUNT(*) FROM (SELECT job_id FROM job_executions GROUP BY job_id HAVING COUNT(*) > 1) AS d;")
DUPLICATES="${DUPLICATES// /}"
DISTINCT_WORKERS=$(sql "SELECT count(DISTINCT worker_id) FROM job_executions;")
DISTINCT_WORKERS="${DISTINCT_WORKERS// /}"

DELIVERIES=$(python3 -c "import json; print(sum(json.load(open('/tmp/webhook-deliveries.json')).values()))")
DELIVERIES="${DELIVERIES// /}"

echo ""
echo "=== Summary ==="
echo "  Jobs seeded:          ${JOB_COUNT}"
echo "  Execution records:    ${EXECUTIONS}"
echo "  Duplicate jobs:       ${DUPLICATES}"
echo "  Distinct workers:     ${DISTINCT_WORKERS}"
echo "  HTTP deliveries:      ${DELIVERIES}"

sep

if [[ "${EXECUTIONS}" -eq "${JOB_COUNT}" ]]; then
    log "Execution records == ${JOB_COUNT} ✓"
else
    err "Expected ${JOB_COUNT} execution records, got ${EXECUTIONS}"
fi

if [[ "${DUPLICATES}" -eq 0 ]]; then
    log "No duplicate job executions ✓"
else
    err "Found ${DUPLICATES} duplicate job executions"
fi

if [[ "${DISTINCT_WORKERS}" -eq "${WORKER_COUNT}" ]]; then
    log "All ${WORKER_COUNT} workers participated ✓"
else
    err "Expected ${WORKER_COUNT} distinct workers, got ${DISTINCT_WORKERS}"
fi

if [[ "${DELIVERIES}" -eq "${JOB_COUNT}" ]]; then
    log "HTTP deliveries == ${JOB_COUNT} ✓"
else
    err "Expected ${JOB_COUNT} HTTP deliveries, got ${DELIVERIES}"
fi

sep
log "VALIDATION PASSED"
log "Scale flow: ${JOB_COUNT} jobs → ${WORKER_COUNT} workers → ${JOB_COUNT} unique deliveries"
sep
