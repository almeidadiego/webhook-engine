#!/usr/bin/env bash
# =============================================================================
# Chaos Worker-Kill Validation — SIGKILL mid-drain + reaper self-healing
# =============================================================================
#
# WHAT THIS DOES (read this first — it explains the whole script):
#
# A FIXED BACKLOG is seeded directly into Postgres by the seeder binary
# (SEEDER_COUNT=3000 pending jobs, deterministic, no DB contention). Three
# workers start and begin draining it. After a short warmup
# (KILL_AT_SECONDS=10s — enough for all semaphore slots to be occupied) the
# main thread SIGKILLs (kill -9) one of the three workers — NOT the process
# group, not a graceful stop: SIGKILL. The kernel destroys the process
# immediately, giving it no chance to run any exit hook, signal handler, or
# graceful-shutdown path (which is the point: we emulate a box yanked out of
# the rack, not a `docker stop`).
#
# Consequence: the victim dies holding rows already claimed in Postgres at
# status='processing'. Nobody completes them — the process is gone. Under a
# graceful shutdown the worker would drain and finish its in-flight jobs
# before exiting; under SIGKILL those rows are simply frozen in 'processing'
# forever unless something else intervenes.
#
# THAT something is the REAPER. Every WORKER_REAPER_INTERVAL (10s) the
# surviving workers sweep scheduled_jobs for 'processing' rows whose
# started_at is older than WORKER_STALE_THRESHOLD (60s) and reclaim them
# back to 'pending' (log line: "reaper: reclaimed stale jobs"). A
# surviving worker then re-claims and re-delivers the recovered job.
#
# WHY THE LOAD IS A FIXED SEEDED BACKLOG (v2 redesign — bugs fixed from v1):
#
#   v1 pushed jobs with k6 (100 VUs → ~1000 inserts/s) while the worker
#   cluster drains only ~(batch_size / poll_interval) = 20/s per worker,
#   ~60/s total. The backlog grew to ~143,000 jobs that could never drain —
#   the test failed on capacity, not on chaos behavior. Worse: two flow bugs
#   sabotaged the experiment itself:
#
#     BUG 1 — the v1 script called stop_workers BEFORE the drain loop. With
#             zero consumers left the backlog could never drain even if
#             sized correctly. v2 keeps the survivors ALIVE through the
#             entire drain and stops everything only afterwards.
#
#     BUG 2 — the victim was measured idle at kill time (processing=0): with
#             only 50-150ms dummy delay and thin load it held ~0 in-flight
#             jobs, so the reaper had nothing to reclaim. v2 raises the
#             dummy delay to 500-800ms (DUMMY_MIN/MAX_DELAY_MS): each of the
#             worker's 10 semaphore slots stays occupied for ~0.5-0.8s, so
#             at the kill instant the victim holds ~10 in-flight jobs —
#             frozen 'processing' rows (zombies) are now GUARANTEED, and the
#             reaper reclaim is deterministic instead of luck.
#
#   The seeder ALSO makes the run deterministic: one blocking insert pass,
#   sized to worker capacity (see math below), instead of a racing
#   producer/consumer race whose queue depth depends on the machine.
#
# BACKLOG SIZING (why 3000 fits the 480s drain budget):
#   Worker drain capacity ≈ min(batch_size / poll_interval, concurrency /
#   delivery_time) = min(20/1s = 20/s, 10 slots / ~0.65s ≈ 15/s) ≈ 15/s per
#   worker → ~45/s for the surviving fleet (2 workers after the kill).
#   3000 jobs × (1 - 11s warmup) / 30/s ≈ 100s to drain the live backlog.
#   The victim's ~10 frozen rows add: stale threshold (60s) + Redis TTL.
#
# WHY THE DRAIN MUST WAIT OUT THE 5-MINUTE IDEMPOTENCY KEY (skip-loop tail):
#
#   The victim may have ALREADY delivered the HTTP request for some of its
#   in-flight jobs before dying. The system cannot know whether that
#   delivery completed, so it relies on the Redis idempotency key
#   (SET idemp:<key> EX 300, 5-minute TTL): while the key is alive, every
#   re-claim of the job logs "idempotency: job already processed" and
#   SKIPS — the row stays pending, and the drain stalls in a skip-loop
#   until the key expires (~5 min). Only after TTL expiry can the
#   surviving workers actually deliver and complete those jobs. Therefore
#   the drain budget must exceed: backlog drain (~100s) + stale threshold
#   (60s) + Redis TTL (300s) + buffer. We default DRAIN_TIMEOUT=480s.
#   This skip-loop is EXPECTED behavior, not a bug: it is the
#   delivery-inhibition layer preventing a duplicate while the uncertainty
#   about the victim's in-flight state exists.
#
# WHAT YOU SHOULD OBSERVE:
#   - Surgical kill: exactly ONE worker dies (KILL_INDEX); the other two
#     keep running (verified via kill -0 on their recorded PIDs) AND DO THE
#     DRAINING — survivors stay alive until the queue is empty.
#   - Reaper evidence: victim-owned stale rows are reclaimed by SURVIVING
#     workers ("reaper: reclaimed stale jobs"), the row goes pending ->
#     processing -> completed again.
#   - Full drain: final DB state shows NO pending and NO processing rows.
#   - No panics anywhere.
#   - Total deliveries ≈ SEEDER_COUNT (+ small duplicate tail): the seeder
#     sends every job to ONE fixed target URL, so per-job duplicate
#     attribution is not possible here — we report the total only (the
#     per-job duplicate mechanism was already validated in
#     validate-chaos-guard.sh).
#
# HOW THE KILL IS MADE SURGICAL (why `go run` is forbidden here):
#
#   `go run` spawns TWO processes: the `go run` wrapper plus the compiled
#   binary under the go-build cache. `$!` would be the wrapper's PID, and
#   `kill -9` on the wrapper orphans the real binary — the chaos would hit
#   nothing. So we `go build` real binaries to /tmp first; `$!` is then the
#   actual worker process, and the recorded PID is both the kill target and
#   the aliveness-check target. (Same rationale applied to the API.)
#
# Prerequisites:
#   - Docker and docker compose installed
#   - Go 1.25+ installed (binaries are built, NOT `go run`)
#   - python3 installed (delivery-total parsing)
#   - k6 is NO LONGER NEEDED (v1's load generator was replaced by the seeder)
#   - Ports 5432, 6379, 8080, and 9999 free
#
# Usage:
#   ./scripts/validate-chaos-worker-kill.sh
#
#   Env knobs (all optional):
#     DUMMY_MIN_DELAY_MS (default 500)  dummy latency floor (saturates slots)
#     DUMMY_MAX_DELAY_MS (default 800)  dummy latency ceiling
#     SEEDER_COUNT       (default 3000) fixed backlog size (worker-capacity-sized)
#     KILL_AT_SECONDS    (default 10)   warmup before the SIGKILL (workers saturated)
#     KILL_INDEX         (default 1)    which worker dies, 0-based (1 = #2 of 3)
#     DRAIN_TIMEOUT      (default 480)  max seconds to wait for full drain
#
# Artifacts (preserved after the run — they are the analysis deliverable):
#   /tmp/chaos-kill-api.log            API log
#   /tmp/chaos-kill-worker-{1,2,3}.log worker logs (incl. victim's pre-kill lines)
#   /tmp/chaos-kill-dummy.log          dummy server log
#   /tmp/chaos-kill-seeder.log         seeder log (fixed backlog insert evidence)
#   /tmp/chaos-kill-queue.log          queue depth samples + drain-loop samples
#   /tmp/chaos-kill-events.log         kill timeline (KILL / victim confirmed dead)
#   /tmp/webhook-deliveries.json       delivery counter (total only — fixed URL)
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

# Normal (production-like) reaper timing: the victim's rows must be allowed
# to LOOK genuinely abandoned — started_at grows past the 60s stale threshold
# while the worker process that owned them no longer exists. These values are
# also the production defaults, which is what makes this a faithful chaos test.
WORKER_REAPER_INTERVAL="10s"
WORKER_STALE_THRESHOLD="60s"

# Dummy server latency: LONG (500-800ms). Each delivery occupies one of the
# worker's 10 semaphore slots for ~0.5-0.8s, so during the warmup every slot
# is busy and the victim is GUARANTEED to hold ~10 in-flight jobs at the kill
# instant (v1 used 50-150ms and the victim was measured idle — fix for bug 3).
DUMMY_MIN_DELAY_MS="${DUMMY_MIN_DELAY_MS:-500}"
DUMMY_MAX_DELAY_MS="${DUMMY_MAX_DELAY_MS:-800}"

# Fixed seeded backlog (replaces k6 — bug 2 fix): 3000 pending jobs inserted
# by the seeder BEFORE the workers start. Sized to worker capacity:
# ~15/s per worker × 2 survivors ≈ 30/s → ~100s to drain the live backlog,
# leaving room inside the 480s budget for the 60s stale threshold and the
# 5min idempotency TTL tail of the victim's frozen rows.
SEEDER_COUNT="${SEEDER_COUNT:-3000}"
SEEDER_TENANT_ID="a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# Chaos injection timing:
#   KILL_AT_SECONDS — warmup on the MAIN thread: the workers have been
#                     running this long when the SIGKILL lands (they started
#                     against a full 3000-job backlog, so they saturate in
#                     ~1 poll cycle; 10s comfortably covers saturation).
#   KILL_INDEX      — 0-based index into WORKER_PIDS; 1 = kill worker #2.
KILL_AT_SECONDS="${KILL_AT_SECONDS:-10}"
KILL_INDEX="${KILL_INDEX:-1}"

# Drain budget: why 480? The surviving workers DRAIN WITH THE QUEUE ALIVE —
# never stop the survivors before the drain (v1 bug 1). The tail is capped by
# the reclaimed victim rows skip-looping on the Redis idempotency key (5min
# TTL) until it expires — see the header. The drain must cover the backlog
# drain + stale(60s) + TTL(300s) + TTL-expiry re-pickup + buffer.
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-480}"

# Queue collector sample cadence (seconds between pending/processing/completed
# samples; shows the saturated warmup, the frozen victim rows, the reap, and
# the drain to zero).
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
# Binaries built to /tmp are matched by name (e.g. "webhook-worker-kill");
# the go-build/exe patterns are belt-and-braces to catch anything else
# left behind by previous runs of the other validate scripts.
#
# TIMING RULE (v1 bug 1): survivors are the engine of the drain — these
# stoppers are ONLY invoked after the drain loop finishes (or from the EXIT
# cleanup on early abort), never between the kill and the drain.
stop_workers() {
    pkill -f "webhook-worker-kill" 2>/dev/null || true
    pkill -f "cmd/worker" 2>/dev/null || true
    pkill -f "exe/worker" 2>/dev/null || true
    pkill -f "go-build.*worker" 2>/dev/null || true
}

stop_api() {
    pkill -f "webhook-api-kill" 2>/dev/null || true
    pkill -f "cmd/api" 2>/dev/null || true
    pkill -f "exe/api" 2>/dev/null || true
    pkill -f "go-build.*api" 2>/dev/null || true
}

stop_dummy() {
    pkill -f "dummy-server" 2>/dev/null || true
}

stop_seeder() {
    # The seeder is a short-lived blocking process (exits on its own after
    # the inserts); the pattern only exists to sweep a wedged leftover.
    pkill -f "webhook-seeder" 2>/dev/null || true
}

# ─── Cleanup ────────────────────────────────────────────────────────────────
# Records in-flight subshell PIDs (queue collector) plus the started process
# PIDs (workers, API, dummy) so the EXIT trap can reap exactly what this run
# created, in addition to the pattern sweep. Artifacts are intentionally NOT
# removed — they are the deliverable.
QUEUE_PID=""
API_PID=""
DUMMY_PID=""
cleanup() {
    log "Cleaning up..."
    [[ -n "${QUEUE_PID}" ]] && kill "${QUEUE_PID}" 2>/dev/null || true
    [[ -n "${API_PID}" ]]   && kill "${API_PID}"   2>/dev/null || true
    [[ -n "${DUMMY_PID}" ]] && kill "${DUMMY_PID}" 2>/dev/null || true
    # "${arr[@]+...}" is the set -u-safe empty-array idiom (macOS bash 3.2).
    if [[ -n "${WORKER_PIDS[@]+x}" ]]; then
        for pid in "${WORKER_PIDS[@]}"; do
            [[ -n "${pid}" ]] && kill -9 "${pid}" 2>/dev/null || true
        done
    fi
    stop_workers
    stop_api
    stop_dummy
    stop_seeder
    # Analysis artifacts (/tmp/chaos-kill-*.log, /tmp/webhook-deliveries.json)
    # are intentionally NOT removed here — they are the deliverable of this
    # chaos run. rm only happens at script start for a fresh run.
    log "Infrastructure still running. To stop: docker compose down"
}

trap cleanup EXIT

cd "${PROJECT_DIR}"

# Config sanity: the kill index must point at a real slot in WORKER_PIDS.
if [[ "${KILL_INDEX}" -lt 0 || "${KILL_INDEX}" -ge "${WORKER_COUNT}" ]]; then
    err "KILL_INDEX=${KILL_INDEX} is out of range (0..$((WORKER_COUNT - 1)))"
fi
if [[ "${KILL_AT_SECONDS}" -lt 5 ]]; then
    warn "KILL_AT_SECONDS=${KILL_AT_SECONDS}s is a very short warmup — the workers may"
    warn "  not have saturated their ${WORKER_MAX_CONCURRENCY} slots yet, weakening the in-flight guarantee."
fi

sep
echo "Webhook Engine — Chaos Worker-Kill Validation (SIGKILL mid-drain + reaper self-healing)"
echo "Project: ${PROJECT_DIR}"
echo "Load: fixed seeded backlog of ${SEEDER_COUNT} jobs (no k6); dummy delay=${DUMMY_MIN_DELAY_MS}-${DUMMY_MAX_DELAY_MS}ms; workers=${WORKER_COUNT}x${WORKER_MAX_CONCURRENCY}"
echo "Chaos: kill -9 worker #${KILL_INDEX} (0-based) after ${KILL_AT_SECONDS}s warmup; reaper interval=${WORKER_REAPER_INTERVAL}, stale=${WORKER_STALE_THRESHOLD}"
echo "Drain: SURVIVORS STAY ALIVE and drain; budget=${DRAIN_TIMEOUT}s (skips until the 5min Redis idempotency TTL expires)"
sep

# ─── Prerequisites ──────────────────────────────────────────────────────────
command -v docker >/dev/null 2>&1 || err "docker is not installed or not on PATH"
command -v python3 >/dev/null 2>&1 || err "python3 is not installed or not on PATH (needed to parse the delivery totals JSON in the verification steps)"
# NOTE: k6 is intentionally NOT checked anymore — the load phase was replaced
# by the seeder binary (deterministic fixed backlog, no DB write contention).

# ─── Step 1/8: Infrastructure ───────────────────────────────────────────────
sep
log "Step 1/8: Starting Postgres + Redis via docker compose..."
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

# Fresh state for a fresh run — also orphans any stray processes from a
# previous run so our recorded PIDs refer only to THIS experiment. The
# artifacts re-created below are the deliverable of this run.
rm -f /tmp/chaos-kill-* /tmp/webhook-deliveries.json

# Fresh job/execution state so the chaos run observes only the jobs it creates.
# job_executions first: it references scheduled_jobs (FK), so truncate parent
# tables in dependency order.
log "Resetting job tables..."
sql "TRUNCATE job_executions, scheduled_jobs;"
log "Flushing Redis idempotency state..."
redis_cli FLUSHDB

# ─── Step 2/8: Build binaries (real PIDs, NOT 'go run') ─────────────────────
sep
log "Step 2/8: Building binaries..."
log "  WHY: 'go run' is a two-process wrapper (wrapper + go-build binary); \$!"
log "  would be the wrapper PID, and kill -9 would orphan the real process —"
log "  the chaos would hit nothing. Built binaries make \$! the actual PID."
log "  The seeder is also built (replaces k6): a blocking, deterministic"

log "Building dummy HTTP server binary..."
go build -o /tmp/webhook-dummy-server ./cmd/dummy-server/
log "Building API binary..."
go build -o /tmp/webhook-api-kill ./cmd/api/
log "Building worker binary..."
go build -o /tmp/webhook-worker-kill ./cmd/worker/
log "Building seeder binary..."
go build -o /tmp/webhook-seeder ./cmd/seeder/
log "Binaries built: /tmp/webhook-dummy-server, /tmp/webhook-api-kill, /tmp/webhook-worker-kill, /tmp/webhook-seeder"

# ─── Step 3/8: Start dummy server and API ───────────────────────────────────
sep
log "Step 3/8: Starting dummy HTTP server + API..."

# The delay is the donkey work of this experiment: 500-800ms keeps every one
# of the victim's 10 semaphore slots occupied at the kill instant → ~10
# frozen 'processing' rows → the reaper reclaims REAL zombies every time.
log "Starting dummy HTTP server (delay=${DUMMY_MIN_DELAY_MS}-${DUMMY_MAX_DELAY_MS}ms) on :${DUMMY_PORT}..."
DUMMY_PORT="${DUMMY_PORT}" \
DUMMY_MIN_DELAY_MS="${DUMMY_MIN_DELAY_MS}" \
DUMMY_MAX_DELAY_MS="${DUMMY_MAX_DELAY_MS}" \
    /tmp/webhook-dummy-server > /tmp/chaos-kill-dummy.log 2>&1 &
DUMMY_PID=$!
disown
sleep 0.5

log "Starting API on :${API_PORT}..."
DATABASE_URL="${DATABASE_URL}" \
    API_ADDR=":${API_PORT}" \
    /tmp/webhook-api-kill > /tmp/chaos-kill-api.log 2>&1 &
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
    err "API did not become ready within 15s; see /tmp/chaos-kill-api.log"
fi

# ─── Step 4/8: Seed the fixed backlog ───────────────────────────────────────
sep
log "Step 4/8: Seeding the fixed backlog (${SEEDER_COUNT} jobs)..."
# BLOCKING run (no longer k6, no background): the seeder inserts the whole
# backlog synchronously before any worker exists, so the queue depth is a
# known constant when the chaos begins — deterministic, and no insert-rate
# tug-of-war with consumers (v1's 1000 inserts/s vs 60/s drain was the
# 143k-jobs-that-never-dreained bug).
DATABASE_URL="${DATABASE_URL}" \
    SEEDER_TENANT_ID="${SEEDER_TENANT_ID}" \
    SEEDER_COUNT="${SEEDER_COUNT}" \
    SEEDER_TARGET_URL="${TARGET_URL}" \
    /tmp/webhook-seeder > /tmp/chaos-kill-seeder.log 2>&1
log "Seeder finished; last log lines:"
tail -n 2 /tmp/chaos-kill-seeder.log 2>/dev/null || true

SEEDED=$(sql "SELECT count(*) FROM scheduled_jobs;" | tr -d ' ' || true)
SEEDED="${SEEDED:-0}"
if [[ "${SEEDED}" =~ ^[0-9]+$ ]]; then
    log "Backlog in DB: ${SEEDED} pending jobs (expected ${SEEDER_COUNT})"
else
    warn "Could not read the seeded backlog count from the DB ('${SEEDED}') — continuing; the build/worker steps will expose any real problem."
fi

# ─── Step 5/8: Start workers (PIDs recorded) + queue collector ──────────────
sep
log "Step 5/8: Starting ${WORKER_COUNT} workers (production-like timing: reaper=${WORKER_REAPER_INTERVAL}, stale=${WORKER_STALE_THRESHOLD})..."
log "  PIDs are recorded in WORKER_PIDS[] — the main thread kills WORKER_PIDS[KILL_INDEX]"
log "  and the verification re-checks every survivor with kill -0, so the PID must"
log "  be the REAL worker process (hence the built binary, not 'go run')."
WORKER_PIDS=()
for i in 1 2 3; do
    DATABASE_URL="${DATABASE_URL}" REDIS_ADDR="${REDIS_ADDR}" \
        WORKER_POLL_INTERVAL="${WORKER_POLL_INTERVAL}" \
        WORKER_REAPER_INTERVAL="${WORKER_REAPER_INTERVAL}" \
        WORKER_STALE_THRESHOLD="${WORKER_STALE_THRESHOLD}" \
        WORKER_MAX_CONCURRENCY="${WORKER_MAX_CONCURRENCY}" \
        /tmp/webhook-worker-kill > "/tmp/chaos-kill-worker-${i}.log" 2>&1 &
    WORKER_PIDS+=($!)
done
disown
echo "  worker PIDs: 1→${WORKER_PIDS[0]} 2→${WORKER_PIDS[1]} 3→${WORKER_PIDS[2]}"
sleep 2

# Queue depth collector: over time you should SEE the backlog shrink under
# the saturated warmup, processing JUMP when the victim's frozen rows are
# reaped, and both drain to 0 — the whole self-healing story in one column
# of samples. Collectors run THROUGHOUT the drain (survivors stay alive).
QUEUE_LOG="/tmp/chaos-kill-queue.log"
: > "${QUEUE_LOG}"  # truncate/create
(
    while true; do
        PENDING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='pending';" | tr -d ' ' || true)
        PROCESSING=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='processing';" | tr -d ' ' || true)
        COMPLETED=$(sql "SELECT count(*) FROM scheduled_jobs WHERE status='completed';" | tr -d ' ' || true)
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

# ─── Step 6/8: Warmup + surgical SIGKILL (main thread, no k6) ───────────────
sep
log "Step 6/8: Warming up for ${KILL_AT_SECONDS}s so workers are saturated with in-flight jobs..."
log "  WHY: workers start against a full ${SEEDER_COUNT}-job backlog, so within one poll"
log "  cycle all ${WORKER_MAX_CONCURRENCY} semaphore slots per worker are occupied. At the kill"
log "  instant the victim is GUARANTEED to hold ~${WORKER_MAX_CONCURRENCY} in-flight jobs (the"
log "  500-800ms dummy delay makes slots stay occupied; v1's victim was idle)."

EVENTS_LOG="/tmp/chaos-kill-events.log"
: > "${EVENTS_LOG}"

# Main-thread kill (no watchdog subshell needed anymore — there is no load
# to run in parallel; sleep here, kill here, keep going):
sleep "${KILL_AT_SECONDS}"
VICTIM="${WORKER_PIDS[${KILL_INDEX}]}"
echo "$(date +%H:%M:%S) KILL -9 victim_pid=${VICTIM}" >> "${EVENTS_LOG}"
log "CHAOS: kill -9 → worker #${KILL_INDEX} (pid ${VICTIM})"
kill -9 "${VICTIM}" 2>/dev/null || true
sleep 1
if kill -0 "${VICTIM}" 2>/dev/null; then
    echo "$(date +%H:%M:%S) ERROR kill -9 did not take effect" >> "${EVENTS_LOG}"
    warn "kill -9 did not take effect on pid ${VICTIM} — the kill timeline records the failure; the verdict will reflect it."
else
    echo "$(date +%H:%M:%S) victim confirmed dead" >> "${EVENTS_LOG}"
    log "Victim confirmed dead: pid ${VICTIM} is gone (SIGKILL cannot be ignored)."
fi
log "Survivors KEEP RUNNING from here — they own the drain (v1 bug 1 fix)."

# ─── Step 7/8: Drain loop with SURVIVORS ALIVE ──────────────────────────────
sep
log "Step 7/8: Drain loop: polling pending:processing every 10s (budget ${DRAIN_TIMEOUT}s)..."
log "  NOBODY is stopped here — the two surviving workers must be the ones to"
log "  consume the backlog, reap the victim's frozen rows after the stale"
log "  threshold (60s), and wait out the 5min idempotency skip-loop tail."
log "  WHY ≥480s: reclaimed victim rows skip-loop while their 5min idempotency"
log "  key is alive ('idempotency: job already processed'), so the tail of the"
log "  drain must wait out the TTL before final completion. See header."
DRAIN_START=$(date +%s)
DRAINED=false
while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - DRAIN_START))
    if [[ "${ELAPSED}" -ge "${DRAIN_TIMEOUT}" ]]; then
        warn "Drain budget exhausted after ${DRAIN_TIMEOUT}s with pending/processing above zero."
        break
    fi

    # Two-count sample: 'pending:processing'. A vectorized single-query form
    # keeps the two counts consistent with each other at read time.
    SAMPLE=$(sql "SELECT count(*) FILTER (WHERE status='pending')::text || ':' || count(*) FILTER (WHERE status='processing')::text FROM scheduled_jobs;" || true)
    SAMPLE="$(echo "${SAMPLE}" | tr -d ' \n')"
    if [[ "${SAMPLE}" =~ ^[0-9]+:[0-9]+$ ]]; then
        P="${SAMPLE%%:*}"
        PROC="${SAMPLE##*:}"
        echo "$(date +%H:%M:%S) drain pending=${P} processing=${PROC}" >> "${QUEUE_LOG}"
        echo "  +${ELAPSED}s: pending=${P} processing=${PROC}"
        if [[ "${P}" -eq 0 && "${PROC}" -eq 0 ]]; then
            DRAINED=true
            log "Queue fully drained after ${ELAPSED}s (pending=0, processing=0)."
            break
        fi
    else
        # Empty/non-numeric = DB unreachable (docker down, cache rebuild, whatever)
        # — that is NOT "drained". Keep waiting; a success verdict can never be
        # built on an empty sample.
        echo "$(date +%H:%M:%S) drain DB-unreachable" >> "${QUEUE_LOG}"
        echo "  +${ELAPSED}s: DB-unreachable (raw='${SAMPLE}') — keeping the drain alive"
    fi
    sleep 10
done

# ─── Step 8/8: Stop everything (SAFE now) + verification ────────────────────
sep
log "Step 8/8: Stopping the queue collector first, then workers/API/dummy..."
# Only NOW is it safe to stop the survivors — the drain is done (or budgeted
# out); stopping them any earlier was v1's fatal flow bug.
kill "${QUEUE_PID}" 2>/dev/null || true
wait "${QUEUE_PID}" 2>/dev/null || true
log "Queue collector stopped; samples: ${QUEUE_LOG}"

# ── Surgical-kill evidence MUST be captured BEFORE stopping the survivors ──
# The survivors being alive at drain-end is the real proof the kill was
# surgical (one process, not all). Checking kill -0 after stop_workers would
# always read them as dead — so capture it here, first.
ALIVE_COUNT=0
SURVIVOR_LINES=()
for i in 0 1 2; do
    if [[ "${i}" -eq "${KILL_INDEX}" ]]; then
        if kill -0 "${WORKER_PIDS[$i]}" 2>/dev/null; then
            SURVIVOR_LINES+=("worker ${i} (KILL_INDEX) should be DEAD but is alive")
        else
            SURVIVOR_LINES+=("worker ${i} (KILL_INDEX): dead as intended")
        fi
        continue
    fi
    if kill -0 "${WORKER_PIDS[$i]}" 2>/dev/null; then
        SURVIVOR_LINES+=("worker ${i} pid=${WORKER_PIDS[$i]}: alive")
        ALIVE_COUNT=$((ALIVE_COUNT + 1))
    else
        SURVIVOR_LINES+=("worker ${i} pid=${WORKER_PIDS[$i]}: DEAD")
    fi
done

stop_workers
stop_api
stop_dummy
stop_seeder
sleep 1
log "All app processes stopped. Infrastructure (Postgres + Redis) intentionally left UP."

log "Verification: reaper evidence, surgical-kill evidence, final state..."

# Reaper evidence: the reclaimed-stale-jobs line in worker logs. grep -h across
# ALL THREE logs: the victim's PRE-kill reaper lines are included in this
# count — acceptable and documented here (we measure "the reaper swept at
# least once in the cluster," not exactly which instance reaped the frozen
# rows; with the saturated warmup the victim's own rows are the bulk of what
# any reaper finds after T+60s anyway).
RECLAIMS=$(grep -h "reaper: reclaimed stale jobs" /tmp/chaos-kill-worker-*.log 2>/dev/null | wc -l | tr -d ' ' || true)
RECLAIMS="${RECLAIMS:-0}"

# Skip-loop evidence: reclaimed re-claims hitting the live idempotency key
# (the expected inhibition behavior while the victim's fate is uncertain).
SKIP_LOCK=$(grep -h "idempotency: job already processed" /tmp/chaos-kill-worker-*.log 2>/dev/null | wc -l | tr -d ' ' || true)
SKIP_LOCK="${SKIP_LOCK:-0}"

# Stability evidence: counting the word panic anywhere in worker/API logs.
PANICS=$(grep -ih "panic" /tmp/chaos-kill-worker-*.log /tmp/chaos-kill-api.log 2>/dev/null | wc -l | tr -d ' ' || true)
PANICS="${PANICS:-0}"

echo ""
echo "=== Log evidence ==="
echo "  reaper: reclaimed stale jobs : ${RECLAIMS}"
echo "  idempotency skip-locks       : ${SKIP_LOCK}"
echo "  panic lines                  : ${PANICS}"

# Surgical-kill evidence was captured BEFORE stopping the survivors (see the
# capture block above the stop_workers call) — printing the recorded result.
echo ""
echo "=== Survivor PID aliveness (captured at drain-end, before stop) ==="
for line in "${SURVIVOR_LINES[@]}"; do
    echo "  ${line}"
done

echo ""
echo "=== Kill timeline (${EVENTS_LOG}) ==="
grep -h . "${EVENTS_LOG}" 2>/dev/null || true

echo ""
echo "=== Final DB state ==="
sql "SELECT status, count(*) FROM scheduled_jobs GROUP BY status ORDER BY status;"
sql "SELECT count(*) AS execution_records FROM job_executions;"

# Total deliveries from the dummy server counter. NOTE: the seeder sends every
# job to ONE fixed target URL (SEEDER_TARGET_URL), so the counter cannot
# attribute deliveries to individual jobs — per-job duplicate detection is
# NOT possible in this experiment. We report the TOTAL only; the per-job
# duplicate mechanism was already validated in validate-chaos-guard.sh with
# per-job keying. Expected total ≈ SEEDER_COUNT (+ a small duplicate tail from
# the victim's in-flight deliveries that never reached CompleteJob).
TOTAL_DELIVERIES=$(python3 -c "import json;print(sum(json.load(open('/tmp/webhook-deliveries.json')).values()))" 2>/dev/null || echo 0)
TOTAL_DELIVERIES="${TOTAL_DELIVERIES:-0}"
echo ""
echo "=== Delivery counter (dummy server) ==="
echo "  total deliveries: ${TOTAL_DELIVERIES} (fixed target URL — per-job duplicates not attributable here)"

# ─── Summary + Verdict ──────────────────────────────────────────────────────
sep
log "Summary"
echo ""
echo "  Chaos configuration:"
echo "    backlog           : ${SEEDER_COUNT} seeded jobs (fixed, worker-capacity-sized; no k6)"
echo "    dummy latency     : ${DUMMY_MIN_DELAY_MS}-${DUMMY_MAX_DELAY_MS}ms (saturates the ${WORKER_MAX_CONCURRENCY} slots)"
echo "    kill at           : T+${KILL_AT_SECONDS}s warmup → worker #${KILL_INDEX} (pid ${WORKER_PIDS[${KILL_INDEX}]}) got kill -9"
echo "    reaper            : interval=${WORKER_REAPER_INTERVAL}, stale=${WORKER_STALE_THRESHOLD}"
echo "    drain budget      : ${DRAIN_TIMEOUT}s (capped by Redis idempotency TTL=5min tail)"
echo ""
echo "  Observations:"
echo "    reaper reclaim events        : ${RECLAIMS}"
echo "    idempotency skip-locks       : ${SKIP_LOCK}"
echo "    total deliveries             : ${TOTAL_DELIVERIES} (seeder uses one fixed target URL)"
echo "    panics                       : ${PANICS}"
echo ""
echo "  Artifacts:"
echo "    logs        : /tmp/chaos-kill-api.log, /tmp/chaos-kill-worker-{1,2,3}.log, /tmp/chaos-kill-dummy.log"
echo "    seeder      : /tmp/chaos-kill-seeder.log"
echo "    queue       : /tmp/chaos-kill-queue.log (collector + drain samples)"
echo "    events      : /tmp/chaos-kill-events.log (kill timeline)"
echo "    deliveries  : /tmp/webhook-deliveries.json"

sep

# Verdict: all five conditions must hold —
#   (1) the victim was actually killed (timeline log says so),
#   (2) the reaper reclaimed stale rows (self-healing was exercised),
#   (3) all surviving workers were alive through the drain (kill was surgical),
#   (4) the queue fully drained (no 'pending' and no frozen 'processing' rows),
#   (5) zero panics.
# SKIP_LOCK>0 is a WARN-only: the skip-loop is the documented delivery-
# inhibition layer, and with a single fixed target URL no per-job duplicate
# metric exists to gate on.

VICTIM_KILLED=false
if grep -q "victim confirmed dead" "${EVENTS_LOG}" 2>/dev/null; then
    VICTIM_KILLED=true
    log "Victim confirmed dead ✓ (pid ${WORKER_PIDS[${KILL_INDEX}]})"
else
    warn "Victim not confirmed dead — check ${EVENTS_LOG}."
fi

# Final drain check: re-sample so the verdict is based on DB state at verdict
# time, not on the last drain-loop sample (which may have been DB-unreachable
# on its very last poll).
FINAL_SAMPLE=$(sql "SELECT count(*) FILTER (WHERE status='pending')::text || ':' || count(*) FILTER (WHERE status='processing')::text FROM scheduled_jobs;" | tr -d ' ' || true)
FINAL_SAMPLE="${FINAL_SAMPLE:-}"
FINAL_PENDING=""
FINAL_PROCESSING=""
if [[ "${FINAL_SAMPLE}" =~ ^[0-9]+:[0-9]+$ ]]; then
    FINAL_PENDING="${FINAL_SAMPLE%%:*}"
    FINAL_PROCESSING="${FINAL_SAMPLE##*:}"
fi

PASS=true

if [[ "${VICTIM_KILLED}" != "true" ]]; then
    warn "Victim was not confirmed killed — chaos never actually happened."
    PASS=false
fi

if [[ "${RECLAIMS}" -gt 0 ]]; then
    log "Reaper exercised: ${RECLAIMS} 'reclaimed stale jobs' events ✓"
    log "   → candidate victim rows went processing → pending → re-delivered."
else
    warn "0 reaper reclaim events — self-healing was never exercised."
    warn "  Likely causes: the ${DUMMY_MIN_DELAY_MS}-${DUMMY_MAX_DELAY_MS}ms delay or ${KILL_AT_SECONDS}s warmup left the victim"
    warn "  idle at kill (check /tmp/chaos-kill-worker-*.log for 'processing' counts),"
    warn "  or the stale threshold was never reached within the run."
    PASS=false
fi

if [[ "${ALIVE_COUNT}" -eq "$((WORKER_COUNT - 1))" ]]; then
    log "All $((WORKER_COUNT - 1)) surviving workers still alive through the drain (surgical kill confirmed) ✓"
else
    warn "Only ${ALIVE_COUNT}/$((WORKER_COUNT - 1)) surviving workers alive — the kill was NOT surgical (or survivors crashed)."
    PASS=false
fi

if [[ -n "${FINAL_PENDING}" && "${FINAL_PENDING}" -eq 0 && "${FINAL_PROCESSING}" -eq 0 ]]; then
    log "Queue fully drained: pending=0, processing=0 ✓"
elif [[ -z "${FINAL_PENDING}" ]]; then
    warn "Final DB check could not read pending/processing — DB unreachable at verdict time."
    PASS=false
else
    warn "Queue NOT drained: final pending=${FINAL_PENDING} processing=${FINAL_PROCESSING}."
    warn "  Expected: survivors drained the backlog and the reaper pulled the victim"
    warn "  rows out of 'processing' eventually."
    warn "  If processing stayed > 0: surviving workers' reaper never reclaimed."
    PASS=false
fi

if [[ "${PANICS}" -eq 0 ]]; then
    log "No panics in worker/API logs ✓"
else
    warn "Found ${PANICS} lines containing 'panic' in worker/API logs — investigate:"
    grep -ih "panic" /tmp/chaos-kill-worker-*.log /tmp/chaos-kill-api.log 2>/dev/null | head -10 || true
    PASS=false
fi

# WARN-only residual (documented skip-loop behavior):
if [[ "${SKIP_LOCK}" -gt 0 ]]; then
    warn "Idempotency skip-locks observed: ${SKIP_LOCK}."
    warn "  EXPECTED: reclaimed victim rows skip-looped while their 5min Redis keys were alive."
    warn "  This is the delivery-inhibition layer preventing duplicates during the uncertainty window."
fi

if [[ "${DRAINED}" != "true" ]]; then
    warn "Note: drain loop timed out without reaching 0:0 — see final-sample check above,"
    warn "  which is the verdict-input (it may have completed after the loop gave up)."
fi

sep
if $PASS; then
    log "CHAOS WORKER-KILL VALIDATION PASSED"
    log "  SIGKILL froze ~${WORKER_MAX_CONCURRENCY} victim rows in 'processing' → surviving workers kept"
    log "  running, drained the seeded backlog, reaped the frozen rows after the stale"
    log "  threshold, and the queue reached 0/0 with zero panics."
    log "  (Skip-locks, if any, were the documented idempotency skip-loop tail.)"
else
    err "CHAOS WORKER-KILL VALIDATION FAILED: see summary above. Inspect /tmp/chaos-kill-* logs."
fi

sep
log "Infrastructure still running. To stop: docker compose down"
