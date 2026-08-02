"""Real-world-style JSON log generator.

Emits structured JSON log lines to stdout every LOG_INTERVAL seconds.
~80 % of events are successful 2xx responses; ~20 % are 5xx errors.
Each log line follows an ECS-inspired schema that Logstash / Elasticsearch
can ingest without any pipeline transforms.

Environment variables
---------------------
LOGSTASH_HOST   host to ship TCP lines to  (default: host.docker.internal)
LOGSTASH_PORT   TCP port                   (default: 8091)
LOG_INTERVAL    seconds between events     (default: 3)
SERVICE_NAME    value of service.name      (default: demo-app)
TCP_OUTPUT      set 'false' to stdout only (default: true)
RETRY_EVERY     seconds between reconnects (default: 15)
ERROR_RATE      float 0-1, fraction of 5xx (default: 0.20)
"""

import json
import os
import random
import socket
import sys
import time
import uuid
from datetime import datetime, timezone

# ── Configuration ─────────────────────────────────────────────────────────────
HOST          = os.getenv("LOGSTASH_HOST", "host.docker.internal")
PORT          = int(os.getenv("LOGSTASH_PORT", "8091"))
INTERVAL      = float(os.getenv("LOG_INTERVAL", "3"))
SERVICE       = os.getenv("SERVICE_NAME", "demo-app")
TCP_OUTPUT    = os.getenv("TCP_OUTPUT", "true").lower() not in ("false", "0", "no")
RETRY_EVERY   = float(os.getenv("RETRY_EVERY", "15"))
ERROR_RATE    = float(os.getenv("ERROR_RATE", "0.60"))   # fraction of 5xx events
CONNECT_TIMEOUT = 2

# ── Static data pools ─────────────────────────────────────────────────────────
SERVICE_VERSION = "3.1.4"

ENDPOINTS = [
    ("GET",    "/api/v1/users",               "list_users"),
    ("GET",    "/api/v1/users/{id}",          "get_user"),
    ("POST",   "/api/v1/users",               "create_user"),
    ("PUT",    "/api/v1/users/{id}",          "update_user"),
    ("DELETE", "/api/v1/users/{id}",          "delete_user"),
    ("GET",    "/api/v1/orders",              "list_orders"),
    ("GET",    "/api/v1/orders/{id}",         "get_order"),
    ("POST",   "/api/v1/orders",              "create_order"),
    ("PATCH",  "/api/v1/orders/{id}/status",  "patch_order_status"),
    ("GET",    "/api/v1/products",            "list_products"),
    ("GET",    "/api/v1/products/{id}",       "get_product"),
    ("POST",   "/api/v1/auth/login",          "auth_login"),
    ("POST",   "/api/v1/auth/logout",         "auth_logout"),
    ("POST",   "/api/v1/auth/refresh",        "auth_refresh"),
    ("GET",    "/api/v1/payments",            "list_payments"),
    ("POST",   "/api/v1/payments",            "create_payment"),
    ("GET",    "/api/v1/payments/{id}",       "get_payment"),
    ("GET",    "/health",                     "health_check"),
    ("GET",    "/metrics",                    "metrics"),
    ("GET",    "/api/v1/notifications",       "list_notifications"),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "okhttp/4.12.0",
    "axios/1.7.2 Node.js/20.14.0",
    "python-httpx/0.27.0",
    "PostmanRuntime/7.39.0",
    "curl/8.7.1",
]

# 2xx outcomes
SUCCESS_STATUSES = [
    (200, "OK"),
    (200, "OK"),
    (200, "OK"),
    (201, "Created"),
    (204, "No Content"),
]

# 5xx outcomes
ERROR_STATUSES = [
    (500, "Internal Server Error"),
    (500, "Internal Server Error"),
    (502, "Bad Gateway"),
    (503, "Service Unavailable"),
    (504, "Gateway Timeout"),
]

ERROR_CATALOG = [
    {
        "type":    "DatabaseConnectionError",
        "message": "Connection pool exhausted: max_connections=50 reached",
        "stack":   (
            "Traceback (most recent call last):\n"
            '  File "/app/db/pool.py", line 83, in acquire\n'
            "    raise PoolExhaustedError(f'max_connections={MAX_CONN} reached')\n"
            "DatabaseConnectionError: Connection pool exhausted"
        ),
        "logger":  "app.db.pool",
    },
    {
        "type":    "UpstreamTimeoutError",
        "message": "Upstream service did not respond within 5000ms",
        "stack":   (
            "Traceback (most recent call last):\n"
            '  File "/app/clients/payment_gateway.py", line 47, in charge\n'
            "    response = await session.post(url, timeout=5.0)\n"
            "asyncio.exceptions.TimeoutError\n"
            "UpstreamTimeoutError: Upstream service did not respond within 5000ms"
        ),
        "logger":  "app.clients.payment_gateway",
    },
    {
        "type":    "UnhandledExceptionError",
        "message": "NullPointerException in order processing pipeline",
        "stack":   (
            "Traceback (most recent call last):\n"
            '  File "/app/services/order_service.py", line 122, in process\n'
            "    total = sum(item['price'] for item in order['items'])\n"
            "TypeError: 'NoneType' object is not iterable"
        ),
        "logger":  "app.services.order_service",
    },
    {
        "type":    "CacheBackendError",
        "message": "Redis READONLY - replica in failover state",
        "stack":   (
            "Traceback (most recent call last):\n"
            '  File "/app/cache/redis_client.py", line 61, in set\n'
            "    self._client.set(key, value, ex=ttl)\n"
            "redis.exceptions.ReadOnlyError: READONLY You can't write against a read only replica"
        ),
        "logger":  "app.cache.redis_client",
    },
    {
        "type":    "AuthServiceUnavailable",
        "message": "Auth service returned 503 after 3 retries",
        "stack":   (
            "Traceback (most recent call last):\n"
            '  File "/app/middleware/auth.py", line 38, in verify_token\n'
            "    resp = await auth_client.introspect(token)\n"
            "ServiceUnavailableError: Auth service returned 503 after 3 retries"
        ),
        "logger":  "app.middleware.auth",
    },
    {
        "type":    "DiskIOError",
        "message": "Failed to write audit log: No space left on device",
        "stack":   (
            "Traceback (most recent call last):\n"
            '  File "/app/audit/logger.py", line 19, in write\n'
            "    fh.write(record)\n"
            "OSError: [Errno 28] No space left on device"
        ),
        "logger":  "app.audit.logger",
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rand_id(prefix="", length=8):
    return prefix + uuid.uuid4().hex[:length]


def _resolve_path(template):
    return template.replace("{id}", str(random.randint(10000, 99999)))


def _rand_ip():
    return f"10.{random.randint(0,4)}.{random.randint(0,9)}.{random.randint(1,254)}"


# ── Event builders ─────────────────────────────────────────────────────────────

def build_success_event():
    method, path_tpl, operation = random.choice(ENDPOINTS)
    path = _resolve_path(path_tpl)
    status, status_text = random.choice(SUCCESS_STATUSES)

    resp_bytes = 0 if status == 204 else random.randint(200, 8192)
    req_bytes  = random.randint(0, 1024) if method in ("POST", "PUT", "PATCH") else 0
    duration_ms = round(random.uniform(8, 450), 2)

    return {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "log": {
            "level":  "INFO",
            "logger": f"app.routers.{operation}",
        },
        "message": f"{method} {path} {status} {duration_ms}ms",
        "service": {
            "name":        SERVICE,
            "version":     SERVICE_VERSION,
            "environment": "production",
        },
        "http": {
            "request": {
                "id":         _rand_id("req-", 12),
                "method":     method,
                "path":       path,
                "bytes":      req_bytes,
                "user_agent": random.choice(USER_AGENTS),
            },
            "response": {
                "status_code": status,
                "status_text": status_text,
                "bytes":       resp_bytes,
                "duration_ms": duration_ms,
            },
        },
        "client": {
            "ip":   _rand_ip(),
            "port": random.randint(30000, 65535),
        },
        "user": {
            "id":         f"user-{random.randint(1000, 9999)}",
            "session_id": _rand_id("sess-"),
        },
        "trace": {
            "id":      str(uuid.uuid4()),
            "span_id": uuid.uuid4().hex[:16],
        },
        "event": {
            "outcome": "success",
            "dataset": "application.access",
        },
        "error": None,
    }


def build_error_event():
    method, path_tpl, operation = random.choice(ENDPOINTS)
    path = _resolve_path(path_tpl)
    status, status_text = random.choice(ERROR_STATUSES)
    err = random.choice(ERROR_CATALOG)

    # 504 timeouts are slow by definition
    if status == 504:
        duration_ms = round(random.uniform(5000, 30000), 2)
    else:
        duration_ms = round(random.uniform(500, 5000), 2)

    req_bytes = random.randint(0, 1024) if method in ("POST", "PUT", "PATCH") else 0

    return {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "log": {
            "level":  "ERROR",
            "logger": err["logger"],
        },
        "message": f"{method} {path} {status} {duration_ms}ms — {err['message']}",
        "service": {
            "name":        SERVICE,
            "version":     SERVICE_VERSION,
            "environment": "production",
        },
        "http": {
            "request": {
                "id":         _rand_id("req-", 12),
                "method":     method,
                "path":       path,
                "bytes":      req_bytes,
                "user_agent": random.choice(USER_AGENTS),
            },
            "response": {
                "status_code": status,
                "status_text": status_text,
                "bytes":       0,
                "duration_ms": duration_ms,
            },
        },
        "client": {
            "ip":   _rand_ip(),
            "port": random.randint(30000, 65535),
        },
        "user": {
            "id":         f"user-{random.randint(1000, 9999)}",
            "session_id": _rand_id("sess-"),
        },
        "trace": {
            "id":      str(uuid.uuid4()),
            "span_id": uuid.uuid4().hex[:16],
        },
        "event": {
            "outcome": "failure",
            "dataset": "application.access",
        },
        "error": {
            "type":        err["type"],
            "message":     err["message"],
            "stack_trace": err["stack"],
        },
    }


def build_event():
    if random.random() < ERROR_RATE:
        return build_error_event()
    return build_success_event()


# ── TCP Shipper ────────────────────────────────────────────────────────────────

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
            _log(f"connected to {self.host}:{self.port}")
            return True
        except OSError as exc:
            _log(
                f"{self.host}:{self.port} unreachable ({exc}) — stdout only, "
                f"retrying in {RETRY_EVERY:.0f}s"
            )
            self.sock = None
            return False

    def send(self, line: bytes):
        if self.sock is None and not self._connect():
            return
        try:
            self.sock.sendall(line)
        except OSError as exc:
            _log(f"send failed ({exc}) — will reconnect")
            self.close()

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None


# ── Entry point ───────────────────────────────────────────────────────────────

def _log(msg):
    print(f"[generator] {msg}", flush=True)


def main():
    _log(
        f"service={SERVICE} version={SERVICE_VERSION} "
        f"interval={INTERVAL}s tcp={TCP_OUTPUT} "
        f"target={HOST}:{PORT} error_rate={ERROR_RATE:.0%}"
    )
    shipper = Shipper(HOST, PORT) if TCP_OUTPUT else None

    while True:
        event = build_event()
        line  = json.dumps(event)
        print(line, flush=True)
        if shipper is not None:
            shipper.send((line + "\n").encode("utf-8"))
        time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
