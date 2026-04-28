"""
Minimal HTTP listener for Railway deployments.

Railway expects a process that binds $PORT and stays running. This is not the
GUI inventory app; it only keeps the service healthy. Run inventory locally or
via Railway shell / a future API if needed.
"""

import json
import os
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

PORT = int(os.environ.get("PORT", "8080"))


class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path in ("/", "/health"):
            body = {
                "status": "ok",
                "service": "folder-inventory",
                "note": "Inventory runs via create_inventory_reta.py locally or with custom job config.",
            }
            payload = json.dumps(body).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_error(404, "Not Found")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
    print(f"listening on 0.0.0.0:{PORT}", flush=True)
    server.serve_forever()
