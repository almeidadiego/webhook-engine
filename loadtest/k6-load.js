/**
 * k6 Load Test for Webhook Engine Ingestion API
 *
 * PURPOSE: Inject jobs via POST /jobs to stress-test the API and database
 * under varying load levels (50 → 200 → 500 VUs).
 *
 * USAGE:
 *   API_URL=http://localhost:8080/jobs \
 *   TARGET_URL=http://localhost:9999/webhook \
 *   VUS=200 \
 *   DURATION=60s \
 *   k6 run loadtest/k6-load.js
 *
 * PARAMETERIZATION:
 *   - API_URL: Ingestion API endpoint (default: http://localhost:8080/jobs)
 *   - TARGET_URL: Downstream webhook target (default: http://localhost:9999/webhook)
 *   - VUS: Number of virtual users (default: 50)
 *   - DURATION: Sustain time at peak load (default: 60s)
 */

import http from 'k6/http';
import { check, sleep } from 'k6';

// Read configuration from environment variables
const API_URL = __ENV.API_URL || 'http://localhost:8080/jobs';
const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:9999/webhook';
const VUS = parseInt(__ENV.VUS || '50', 10);
const DURATION = __ENV.DURATION || '60s';

/**
 * k6 Test Configuration
 *
 * STAGES: Ramp-up (10s) → Sustain (DURATION) → Ramp-down (10s)
 * This simulates a realistic load pattern where traffic builds up,
 * sustains at peak, then gracefully decreases.
 *
 * THRESHOLDS: Lenient pass/fail criteria for discovery-focused testing.
 * - p(95) < 1s: 95% of requests should complete in under 1 second
 * - p(99) < 2s: 99% of requests should complete in under 2 seconds
 * - error rate < 5%: Less than 5% of requests should fail
 *
 * These thresholds are intentionally lenient to allow the test to complete
 * even under degradation. The real insight comes from observing the actual
 * metrics (p95 latency, pool saturation) rather than pass/fail status.
 */
export const options = {
  scenarios: {
    load_test: {
      executor: 'ramping-vus',
      exec: 'default',
      startVUs: 0,
      stages: [
        { duration: '10s', target: VUS },      // Ramp-up: 0 → VUS over 10s
        { duration: DURATION, target: VUS },   // Sustain: VUS for DURATION
        { duration: '10s', target: 0 },        // Ramp-down: VUS → 0 over 10s
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<1000', 'p(99)<2000'],  // p95 < 1s, p99 < 2s
    'http_req_failed': ['rate<0.05'],                    // < 5% error rate
  },
};

/**
 * Default Function (executed by each VU in a loop)
 *
 * Each iteration:
 * 1. Generates a unique idempotency key using __VU (virtual user ID) + __ITER (iteration counter) + timestamp
 * 2. Constructs a realistic webhook job payload
 * 3. POSTs to the ingestion API
 * 4. Validates response (201 Created expected)
 * 5. Sleeps briefly to simulate realistic user behavior (not pure busy-loop)
 *
 * IDEMPOTENCY KEY STRATEGY:
 * Using __VU + __ITER + Date.now() ensures uniqueness across:
 * - Different VUs (each VU has a unique __VU ID)
 * - Different iterations (each iteration increments __ITER)
 * - Different test runs (Date.now() adds millisecond precision)
 */
export default function () {
  const idempotencyKey = `job-vu${__VU}-iter${__ITER}-${Date.now()}`;

  const payload = JSON.stringify({
    tenant_id: '00000000-0000-0000-0000-000000000001',
    idempotency_key: idempotencyKey,
    url: TARGET_URL,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Test': 'k6-load-test',
    },
    body: {
      event: 'test.load',
      timestamp: new Date().toISOString(),
      vu_id: __VU,
      iteration: __ITER,
    },
    schedule_at: new Date().toISOString(),  // Schedule immediately
    max_attempts: 5,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: {
      type: 'job-create',
    },
  };

  const res = http.post(API_URL, payload, params);

  check(res, {
    'status is 201 Created': (r) => r.status === 201,
    'response has job ID': (r) => {
      if (r.status !== 201) return false;
      const body = r.json();
      return body.id && typeof body.id === 'string';
    },
    'response time < 500ms': (r) => r.timings.duration < 500,
  });

  sleep(0.1);
}
