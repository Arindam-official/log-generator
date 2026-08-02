"""Real-world-style mixed-format log generator.

Emits log lines to stdout every LOG_INTERVAL seconds.
60 % of events are 5xx errors, 40 % are 2xx successes (configurable).
5xx errors are a mix of:
  - Structured JSON  (app-layer errors, ~60 % of 5xx)
  - Raw non-JSON     (proxy/infra layer: nginx HTML, plain text, XML,
                      Java stacktrace, PHP fatal, empty body — ~40 % of 5xx)

Environment variables
---------------------
LOGSTASH_HOST   host to ship TCP lines to      (default: host.docker.internal)
LOGSTASH_PORT   TCP port                       (default: 8091)
LOG_INTERVAL    seconds between events         (default: 3)
SERVICE_NAME    value of service.name          (default: demo-app)
TCP_OUTPUT      set 'false' to stdout only     (default: true)
RETRY_EVERY     seconds between reconnects     (default: 15)
ERROR_RATE      fraction of events that are 5xx (default: 0.60)
NON_JSON_RATE   fraction of 5xx that are raw   (default: 0.40)
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
ERROR_RATE    = float(os.getenv("ERROR_RATE",    "0.60"))  # fraction of events that are 5xx
NON_JSON_RATE = float(os.getenv("NON_JSON_RATE", "0.40"))  # fraction of 5xx that emit raw (non-JSON)
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

# ── JSON error catalog (app-layer structured errors) ─────────────────────────
JSON_ERROR_CATALOG = [
    # --- original 6 ---
    {
        "type":    "DatabaseConnectionError",
        "message": "Connection pool exhausted: max_connections=50 reached",
        "stack":   'File "/app/db/pool.py", line 83, in acquire — PoolExhaustedError',
        "logger":  "app.db.pool",
    },
    {
        "type":    "UpstreamTimeoutError",
        "message": "Upstream service did not respond within 5000ms",
        "stack":   'File "/app/clients/payment_gateway.py", line 47, in charge — asyncio.TimeoutError',
        "logger":  "app.clients.payment_gateway",
    },
    {
        "type":    "UnhandledExceptionError",
        "message": "TypeError: \'NoneType\' object is not iterable in order processing",
        "stack":   'File "/app/services/order_service.py", line 122, in process — TypeError',
        "logger":  "app.services.order_service",
    },
    {
        "type":    "CacheBackendError",
        "message": "Redis READONLY - replica in failover state",
        "stack":   'File "/app/cache/redis_client.py", line 61, in set — redis.ReadOnlyError',
        "logger":  "app.cache.redis_client",
    },
    {
        "type":    "AuthServiceUnavailable",
        "message": "Auth service returned 503 after 3 retries",
        "stack":   'File "/app/middleware/auth.py", line 38, in verify_token — ServiceUnavailableError',
        "logger":  "app.middleware.auth",
    },
    {
        "type":    "DiskIOError",
        "message": "Failed to write audit log: [Errno 28] No space left on device",
        "stack":   'File "/app/audit/logger.py", line 19, in write — OSError',
        "logger":  "app.audit.logger",
    },
    # --- new: 10 additional types ---
    {
        "type":    "CircuitBreakerOpenError",
        "message": "Circuit breaker OPEN for inventory-service after 5 consecutive failures",
        "stack":   'File "/app/resilience/circuit_breaker.py", line 54, in call — CircuitBreakerOpenError',
        "logger":  "app.resilience.circuit_breaker",
    },
    {
        "type":    "MessageQueueError",
        "message": "Kafka producer: topic order-events — LEADER_NOT_AVAILABLE",
        "stack":   'File "/app/events/producer.py", line 31, in publish — KafkaError: LEADER_NOT_AVAILABLE',
        "logger":  "app.events.producer",
    },
    {
        "type":    "OutOfMemoryError",
        "message": "Worker killed by OOM killer: RSS 2.1 GB exceeded limit",
        "stack":   'File "/app/workers/report_worker.py", line 88, in run — MemoryError',
        "logger":  "app.workers.report_worker",
    },
    {
        "type":    "DeadlockDetected",
        "message": "PostgreSQL deadlock detected on table orders — transaction rolled back",
        "stack":   'File "/app/db/session.py", line 67, in commit — psycopg2.errors.DeadlockDetected',
        "logger":  "app.db.session",
    },
    {
        "type":    "SSLHandshakeError",
        "message": "SSL handshake failed: certificate has expired (notAfter=Aug 1 00:00:00 2026 GMT)",
        "stack":   'File "/app/clients/base.py", line 22, in _get_session — ssl.SSLCertVerificationError',
        "logger":  "app.clients.base",
    },
    {
        "type":    "ConfigurationError",
        "message": "Required env var PAYMENT_API_SECRET not set — service cannot start handler",
        "stack":   'File "/app/config.py", line 14, in load — KeyError: PAYMENT_API_SECRET',
        "logger":  "app.config",
    },
    {
        "type":    "SerializationError",
        "message": "Protobuf deserialization failed: unexpected field tag 0 in UserEvent",
        "stack":   'File "/app/serializers/proto.py", line 39, in decode — google.protobuf.message.DecodeError',
        "logger":  "app.serializers.proto",
    },
    {
        "type":    "ThreadPoolExhausted",
        "message": "Executor queue full (max_workers=32, queue_size=500) — request rejected",
        "stack":   'File "/app/server/executor.py", line 78, in submit — RuntimeError: queue full',
        "logger":  "app.server.executor",
    },
    {
        "type":    "DNSResolutionError",
        "message": "getaddrinfo failed for payments-svc.internal: Name or service not known",
        "stack":   'File "/app/clients/payments.py", line 18, in connect — socket.gaierror: [Errno -2]',
        "logger":  "app.clients.payments",
    },
    {
        "type":    "GRPCStatusError",
        "message": "gRPC call to user-svc failed: RESOURCE_EXHAUSTED — rate limit exceeded",
        "stack":   'File "/app/clients/grpc_user.py", line 55, in get_user — grpc.RpcError: RESOURCE_EXHAUSTED',
        "logger":  "app.clients.grpc_user",
    },
]

# ── Non-JSON raw error catalog (proxy / infra layer) ──────────────────────────
# Each entry is a callable that returns a raw string given (method, path, status).
NON_JSON_CATALOG = [
    # nginx HTML 502
    lambda m, p, s: (
        '<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n'
        '<body><center><h1>502 Bad Gateway</h1></center>\r\n'
        '<hr><center>nginx/1.24.0</center>\r\n</body>\r\n</html>'
    ),
    # nginx HTML 503
    lambda m, p, s: (
        '<html>\r\n<head><title>503 Service Temporarily Unavailable</title></head>\r\n'
        '<body><center><h1>503 Service Temporarily Unavailable</h1></center>\r\n'
        '<hr><center>nginx/1.24.0</center>\r\n</body>\r\n</html>'
    ),
    # nginx HTML 504
    lambda m, p, s: (
        '<html>\r\n<head><title>504 Gateway Time-out</title></head>\r\n'
        '<body><center><h1>504 Gateway Time-out</h1></center>\r\n'
        '<hr><center>nginx/1.24.0</center>\r\n</body>\r\n</html>'
    ),
    # Envoy / Istio plain text upstream error
    lambda m, p, s: (
        f'upstream connect error or disconnect/reset before headers. '
        f'reset reason: connection timeout'
    ),
    # HAProxy plain text
    lambda m, p, s: (
        f'<html><body><h1>503 Service Unavailable</h1>\n'
        f'No server is available to handle this request.\n</body></html>'
    ),
    # AWS ALB / ELB XML
    lambda m, p, s: (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<ErrorResponse>\n'
        '  <Error>\n'
        f'    <Code>ServiceUnavailable</Code>\n'
        f'    <Message>Service {s} — please retry your request</Message>\n'
        '  </Error>\n'
        '</ErrorResponse>'
    ),
    # Java / Tomcat unhandled exception plain text
    lambda m, p, s: (
        f'java.lang.NullPointerException\n'
        f'\tat com.example.app.OrderService.process(OrderService.java:142)\n'
        f'\tat com.example.app.OrderController.createOrder(OrderController.java:88)\n'
        f'\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n'
        f'\tat org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:897)'
    ),
    # PHP-FPM fatal error plain text
    lambda m, p, s: (
        f'[{datetime.now(timezone.utc).strftime("%d-%b-%Y %H:%M:%S")} UTC] '
        f'PHP Fatal error:  Uncaught Error: Call to a member function getId() on null '
        f'in /var/www/html/src/Controller/PaymentController.php:67\n'
        f'Stack trace:\n'
        f'#0 /var/www/html/public/index.php(23): App\\Kernel->handle()\n'
        f'#1 {{main}}'
    ),
    # Empty body (connection reset — zero-byte response)
    lambda m, p, s: "",
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


def build_json_error_event():
    """Structured JSON error — emitted by the application layer."""
    method, path_tpl, operation = random.choice(ENDPOINTS)
    path    = _resolve_path(path_tpl)
    status, status_text = random.choice(ERROR_STATUSES)
    err     = random.choice(JSON_ERROR_CATALOG)

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


def build_raw_error_line():
    """Raw non-JSON string — emitted by nginx/proxy/infra layer."""
    method, path_tpl, _ = random.choice(ENDPOINTS)
    path   = _resolve_path(path_tpl)
    status, _ = random.choice(ERROR_STATUSES)
    template   = random.choice(NON_JSON_CATALOG)
    return template(method, path, status)


def build_event():
    """Return either a dict (JSON) or a raw string (non-JSON)."""
    if random.random() < ERROR_RATE:
        if random.random() < NON_JSON_RATE:
            return build_raw_error_line()   # raw string
        return build_json_error_event()     # dict
    return build_success_event()            # dict


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
        # build_event() returns either a dict (JSON) or a raw string (non-JSON)
        if isinstance(event, dict):
            line = json.dumps(event)
        else:
            line = event  # already a raw string (HTML / plain text / XML / empty)
        print(line, flush=True)
        if shipper is not None:
            shipper.send((line + "\n").encode("utf-8"))
        time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
