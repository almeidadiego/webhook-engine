#!/usr/bin/env python3
"""
Dummy HTTP server that counts requests per job ID.
Writes counts to /tmp/webhook-deliveries.json on each request.

REALISTIC LATENCY SIMULATION:
By default, the server responds instantly (0ms delay). To simulate real-world
downstream webhook targets (Stripe, Slack, etc.), set DUMMY_MIN_DELAY_MS and
DUMMY_MAX_DELAY_MS env vars. The server will sleep for a random duration in
[min, max] milliseconds before responding.

This is used in load tests to reveal semaphore saturation, queue backlog, and
producer/consumer decoupling in the webhook engine.

Usage:
    python3 scripts/dummy-server.py                                        # instant (baseline)
    DUMMY_MIN_DELAY_MS=100 DUMMY_MAX_DELAY_MS=300 python3 scripts/dummy-server.py  # realistic
    DUMMY_MIN_DELAY_MS=500 DUMMY_MAX_DELAY_MS=1500 python3 scripts/dummy-server.py # stressed
"""
import json
import os
import random
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

DELIVERIES_FILE = "/tmp/webhook-deliveries.json"

# Read delay configuration from environment (default: 0-0ms = instant response)
MIN_DELAY_MS = int(os.getenv('DUMMY_MIN_DELAY_MS', '0'))
MAX_DELAY_MS = int(os.getenv('DUMMY_MAX_DELAY_MS', '0'))


class CountingHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Simulate realistic downstream latency BEFORE processing
        # This makes the worker hold the semaphore slot for the full request duration,
        # which is the realistic behavior of external webhook targets.
        if MAX_DELAY_MS > 0:
            delay_ms = random.randint(MIN_DELAY_MS, MAX_DELAY_MS)
            time.sleep(delay_ms / 1000.0)

        # Count the request by job ID (extracted from URL path)
        counts = {}
        if os.path.exists(DELIVERIES_FILE):
            with open(DELIVERIES_FILE, 'r') as f:
                counts = json.load(f)

        job_id = self.path.split('/')[-1] if '/' in self.path else self.path
        counts[job_id] = counts.get(job_id, 0) + 1

        with open(DELIVERIES_FILE, 'w') as f:
            json.dump(counts, f, indent=2)

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')

    def log_message(self, format, *args):
        pass


if __name__ == '__main__':
    if os.path.exists(DELIVERIES_FILE):
        os.remove(DELIVERIES_FILE)

    print(f"Dummy server listening on port 9999 (delay: {MIN_DELAY_MS}-{MAX_DELAY_MS}ms)")
    server = HTTPServer(('0.0.0.0', 9999), CountingHandler)
    server.serve_forever()
