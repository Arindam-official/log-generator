"""Simple log generator.

Prints one JSON log line to stdout every LOG_INTERVAL seconds, so
`docker logs -f log-generator` always shows output. In addition it tries,
best-effort, to ship each line to a TCP endpoint (Logstash). If that endpoint
is down the generator keeps logging and retries the connection in the
background - it never blocks and never exits.
"""

import json
import os
import random
import socket
import sys
import time
import uuid
from datetime import datetime, timezone

HOST = os.getenv("LOGSTASH_HOST", "host.docker.internal")
PORT = int(os.getenv("LOGSTASH_PORT", "8091"))
INTERVAL = float(os.getenv("LOG_INTERVAL", "3"))
SERVICE = os.getenv("SERVICE_NAME", "demo-app")
# Set TCP_OUTPUT=false to only print to stdout.
TCP_OUTPUT = os.getenv("TCP_OUTPUT", "true").lower() not in ("false", "0", "no")
# Don't hammer a dead endpoint: at most one connect attempt per this many secs.
RETRY_EVERY = float(os.getenv("RETRY_EVERY", "15"))
CONNECT_TIMEOUT = 2

LEVELS = ["INFO", "INFO", "INFO", "DEBUG", "WARN", "ERROR"]
ENDPOINTS = ["/api/login", "/api/orders", "/api/users", "/health", "/api/payments"]
METHODS = ["GET", "POST", "PUT", "DELETE"]
MESSAGES = {
    "INFO": ["request completed", "user authenticated", "cache hit"],
    "DEBUG": ["payload parsed", "db pool acquired", "cache miss"],
    "WARN": ["slow response detected", "retrying upstream call", "rate limit near"],
    "ERROR": ["upstream timeout", "database connection failed", "unhandled exception"],
}


def build_event():
    level = random.choice(LEVELS)
    status = 200
    if level == "WARN":
        status = random.choice([301, 400, 429])
    elif level == "ERROR":
        status = random.choice([500, 502, 503])

    return {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE,
        "level": level,
        "message": random.choice(MESSAGES[level]),
        "trace_id": str(uuid.uuid4()),
        "http": {
            "method": random.choice(METHODS),
            "path": random.choice(ENDPOINTS),
            "status": status,
            "duration_ms": round(random.uniform(5, 1800), 2),
        },
        "client_ip": f"10.0.{random.randint(0, 4)}.{random.randint(1, 254)}",
        "user_id": f"user-{random.randint(1000, 1050)}",
    }


class Shipper:
    """Best-effort TCP sender. Never raises, never blocks the log loop."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
        self.next_attempt = 0.0

    def _connect(self):
        now = time.monotonic()
        if now < self.next_attempt:
            return False
        self.next_attempt = now + RETRY_EVERY
        try:
            self.sock = socket.create_connection(
                (self.host, self.port), timeout=CONNECT_TIMEOUT
            )
            log(f"connected to {self.host}:{self.port}")
            return True
        except OSError as exc:
            log(f"{self.host}:{self.port} unreachable ({exc}) - stdout only, "
                f"retrying in {RETRY_EVERY:.0f}s")
            self.sock = None
            return False

    def send(self, line):
        if self.sock is None and not self._connect():
            return
        try:
            self.sock.sendall(line)
        except OSError as exc:
            log(f"send failed ({exc}) - will reconnect")
            self.close()

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None


def log(msg):
    print(f"[generator] {msg}", flush=True)


def main():
    log(f"service={SERVICE} interval={INTERVAL}s tcp={TCP_OUTPUT} "
        f"target={HOST}:{PORT}")
    shipper = Shipper(HOST, PORT) if TCP_OUTPUT else None

    while True:
        event = build_event()
        line = json.dumps(event)
        print(line, flush=True)          # always visible in `docker logs`
        if shipper is not None:
            shipper.send((line + "\n").encode("utf-8"))
        time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
