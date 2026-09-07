#!/usr/bin/env python3
"""
Dummy HTTP server that counts requests per job ID.
Writes counts to /tmp/webhook-deliveries.json on each request.
"""
import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

DELIVERIES_FILE = "/tmp/webhook-deliveries.json"


class CountingHandler(BaseHTTPRequestHandler):
    def do_POST(self):
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
    server = HTTPServer(('0.0.0.0', 9999), CountingHandler)
    print("Dummy server listening on port 9999")
    server.serve_forever()
