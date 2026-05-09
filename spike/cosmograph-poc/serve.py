#!/usr/bin/env python3
"""Spike server: serve files + proxy /cypher to Loveliness on :8080.

Run:  python3 serve.py [port]   # default 8765
"""
import http.server
import socketserver
import sys
import urllib.request
import urllib.error

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
LOVELINESS = "http://localhost:8080"


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        if self.path != "/cypher":
            self.send_error(404, "Not Found")
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        upstream_headers = {"Content-Type": "text/plain"}
        # Forward Accept so the browser can negotiate Arrow vs JSON.
        # Without this, the proxy effectively pins every request to
        # the upstream default (JSON), which makes the DuckDB-WASM
        # smoke test impossible to run through the spike server.
        if self.headers.get("Accept"):
            upstream_headers["Accept"] = self.headers["Accept"]
        try:
            req = urllib.request.Request(
                LOVELINESS + "/cypher",
                data=body,
                headers=upstream_headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = resp.read()
                status = resp.status
                ctype = resp.headers.get("Content-Type", "application/json")
        except urllib.error.HTTPError as e:
            payload = e.read()
            status = e.code
            ctype = "application/json"
        except Exception as e:
            payload = ('{"error":"proxy failure: ' + str(e).replace('"', "'") + '"}').encode()
            status = 502
            ctype = "application/json"
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def end_headers(self):
        # Add CORS to all (file) responses so dev tools don't complain
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def log_message(self, fmt, *args):
        sys.stderr.write("[serve] " + (fmt % args) + "\n")


with socketserver.ThreadingTCPServer(("", PORT), Handler) as httpd:
    print(f"spike server: http://localhost:{PORT}/  (proxy /cypher -> {LOVELINESS}/cypher)")
    httpd.serve_forever()
