"""Simple log generator.

Emits one JSON log line every LOG_INTERVAL seconds to a TCP socket
(Logstash) and to stdout. Reconnects automatically if Logstash restarts.
"""

import json
import os
import random
import socket
import sys
import time
import uuid
from datetime import datetime, timezone

HOST = os.getenv("LOGSTASH_HOST", "logstash")
PORT = int(os.getenv("LOGSTASH_PORT", "8091"))
INTERVAL = float(os.getenv("LOG_INTERVAL", "3"))
SERVICE = os.getenv("SERVICE_NAME", "demo-app")

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


def connect():
    """Block until a connection to Logstash is established."""
    while True:
        try:
            sock = socket.create_connection((HOST, PORT), timeout=10)
            print(f"[generator] connected to {HOST}:{PORT}", flush=True)
            return sock
        except OSError as exc:
            print(f"[generator] waiting for {HOST}:{PORT} ({exc})", flush=True)
            time.sleep(3)


def main():
    print(
        f"[generator] service={SERVICE} target={HOST}:{PORT} interval={INTERVAL}s",
        flush=True,
    )
    sock = connect()
    while True:
        event = build_event()
        line = (json.dumps(event) + "\n").encode("utf-8")
        try:
            sock.sendall(line)
            print(json.dumps(event), flush=True)
        except OSError as exc:
            print(f"[generator] send failed ({exc}), reconnecting", flush=True)
            try:
                sock.close()
            except OSError:
                pass
            sock = connect()
            continue
        time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
