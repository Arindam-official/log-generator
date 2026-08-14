"""Real-world-style mixed-format log generator.

Emits log lines to stdout every LOG_INTERVAL seconds.
60 % of events are errors, 40 % are 2xx successes (configurable).
Errors are a mix of three shapes:
  - Spring Boot console lines (app-layer, the `TS LEVEL 1 --- [thread] logger : msg`
                      format, with flattened HTML / XML / stacktrace bodies)
  - Structured JSON  (app-layer errors, ECS-ish nested fields)
  - Raw non-JSON     (proxy/infra layer: nginx HTML, plain text, XML,
                      Java stacktrace, PHP fatal, empty body)

Sequence numbers
----------------
EVERY emitted line carries a monotonically increasing sequence number starting
at 1, so a downstream collector can be checked for dropped lines:

  JSON lines    -> top-level  "seq": 42
  all others    -> literal    seq=42   (inside a [seq=42] marker)

Extract them from either shape with:

  docker logs log-generator | grep -oE '"seq": *[0-9]+|seq=[0-9]+' \
                            | grep -oE '[0-9]+'

The counter restarts at 1 whenever the process restarts.

Environment variables
---------------------
LOGSTASH_HOST   host to ship TCP lines to      (default: host.docker.internal)
LOGSTASH_PORT   TCP port                       (default: 8091)
LOG_INTERVAL    seconds between events         (default: 3)
SERVICE_NAME    value of service.name          (default: demo-app)
TCP_OUTPUT      set 'false' to stdout only     (default: true)
RETRY_EVERY     seconds between reconnects     (default: 15)
ERROR_RATE      fraction of events that are errors     (default: 0.60)
SPRING_RATE     fraction of errors as Spring lines     (default: 0.45)
NON_JSON_RATE   fraction of errors as raw infra output (default: 0.25)
                (the remainder of errors are structured JSON)

All identifiers below (references, tenant/partner ids, request ids, hosts)
are randomly generated at runtime. Nothing here is taken from a real system.
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
# Each instance keeps its OWN sequence counter starting at 1, so every line
# also carries an instance id — otherwise 5 containers all emit seq=1,2,3...
INSTANCE      = os.getenv("INSTANCE_ID") or socket.gethostname()
TCP_OUTPUT    = os.getenv("TCP_OUTPUT", "true").lower() not in ("false", "0", "no")
RETRY_EVERY   = float(os.getenv("RETRY_EVERY", "15"))
ERROR_RATE    = float(os.getenv("ERROR_RATE",    "0.60"))  # fraction of events that are errors
SPRING_RATE   = float(os.getenv("SPRING_RATE",   "0.45"))  # fraction of errors as Spring console lines
NON_JSON_RATE = float(os.getenv("NON_JSON_RATE", "0.25"))  # fraction of errors as raw infra output
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

# ── Spring Boot console error catalog (app layer) ─────────────────────────────
# Mimics the classic Spring Boot pattern:
#   %d{ISO8601} %5p 1 --- [%15.15t] %-40.40logger{39} : %m
# with the message body carrying a flattened upstream payload (HTML / XML /
# JSON / stacktrace on a single line, <EOL> where the newlines were).

SPRING_THREADS = [
    "http-nio-8080-exec-1",
    "http-nio-8080-exec-4",
    "http-nio-8080-exec-7",
    "http-nio-8080-exec-10",
    "http-nio-8090-exec-2",
    "http-nio-8090-exec-9",
    "ntContainer#0-1",
    "ntContainer#2-1",
    "ntContainer#5-1",
    "kafka-consumer-0-C-1",
    "scheduling-1",
    "scheduling-3",
    "task-2",
    "task-6",
    "pool-4-thread-1",
]

EDGE_POPS = ["EDGE01-P1", "EDGE07-P2", "EDGE12-P1", "EDGE23-P3", "EDGE31-P2"]


def _rand_token(length, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"):
    return "".join(random.choice(alphabet) for _ in range(length))


def _rand_request_id():
    """Base64-ish opaque id, same silhouette as an edge-proxy request id."""
    alphabet = ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz0123456789-_")
    return _rand_token(54, alphabet) + "=="


def _rand_ref():
    """Synthetic booking-style reference. Shape only — not a real reference."""
    return (f"{random.randint(100000000, 999999999)}-{_rand_token(6)}-"
            f"RS{random.randint(10000000, 99999999)}_{random.randint(10000000, 99999999)}")


def _today():
    return datetime.now(timezone.utc).date().isoformat()


def _flat_html_error(status, title, blurb):
    """Single-line HTML error body with <EOL> in place of newlines."""
    return (
        '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" '
        '"http://www.w3.org/TR/html4/loose.dtd"><EOL><HTML><HEAD>'
        '<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=iso-8859-1"><EOL>'
        f'<TITLE>ERROR: {title}</TITLE><EOL></HEAD><BODY><EOL>'
        f'<H1>{status} ERROR</H1><EOL><H2>{title}</H2><EOL>'
        '<HR noshade size="1px"><EOL>'
        f'{blurb}<EOL><BR clear="all"><EOL>'
        'If this persists, review the edge configuration for this origin before '
        'retrying the request.<EOL><BR clear="all"><EOL>'
        '<HR noshade size="1px"><EOL><PRE><EOL>'
        f'Generated by edge-proxy ({random.choice(EDGE_POPS)})<EOL>'
        f'Request ID: {_rand_request_id()}<EOL></PRE><EOL>'
        '<ADDRESS><EOL></ADDRESS><EOL></BODY></HTML>'
    )


def _flat_json_error(code, detail):
    return (f'{{"error":{{"code":"{code}","detail":"{detail}",'
            f'"traceId":"{uuid.uuid4().hex}"}}}}')


def _flat_soap_fault(code, reason):
    return (
        '<?xml version="1.0" encoding="UTF-8"?><EOL>'
        '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><EOL>'
        '<soap:Body><EOL><soap:Fault><EOL>'
        f'<faultcode>soap:{code}</faultcode><EOL>'
        f'<faultstring>{reason}</faultstring><EOL>'
        '</soap:Fault><EOL></soap:Body><EOL></soap:Envelope>'
    )


def _flat_stacktrace(exception, message, frames):
    return (f'{exception}: {message}<EOL>'
            + '<EOL>'.join(f'\tat {f}' for f in frames))


# Each entry returns (level, logger_fqcn, message).
SPRING_ERROR_CATALOG = [
    lambda: (
        "ERROR",
        "com.example.platform.core.svc.notify.WebhookDispatchSvc",
        f"Exception in sending webhook callback for eventRef: {_rand_ref()} "
        f"from:  to:  partnerId: partner-{random.randint(100, 999)} with detail: "
        f'400 Bad Request: "'
        + _flat_html_error(400, "The request could not be satisfied.",
                           "Bad request.") + '"',
    ),
    lambda: (
        "ERROR",
        "com.example.platform.billing.svc.InvoiceSyncMgr",
        f"Error in Invoice Sync API Call for INV-{random.randint(100000, 999999)}-{_today()} "
        f"with message 500 Error",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.catalog.svc.PriceFeedMgr",
        f"Error in Price Feed API Call for SKU-{random.randint(10000, 99999)}-{_today()} "
        f"with message 503 Error",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.gateway.svc.PaymentCaptureSvc",
        f"Capture failed for paymentRef: {_rand_token(10)} "
        f"amount: {random.randint(15, 4200)}.{random.randint(0, 99):02d} EUR with detail: "
        f'502 Bad Gateway: "'
        + _flat_html_error(502, "The request could not be satisfied.",
                           "We can't connect to the origin for this app at this time.") + '"',
    ),
    lambda: (
        "ERROR",
        "com.example.platform.inventory.job.StockReserveJob",
        f"Reservation timed out for reservationRef: {_rand_token(8)} after 5000ms — "
        f"java.net.SocketTimeoutException: Read timed out",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.messaging.KafkaOffsetCommitter",
        f"Offset commit failed for topic order-events partition {random.randint(0, 11)} "
        f"groupId: order-processor — "
        f"org.apache.kafka.clients.consumer.CommitFailedException: rebalance in progress",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.storage.svc.DocumentUploadSvc",
        f"Upload failed for documentId: doc-{uuid.uuid4().hex[:12]} bucket: internal-docs — "
        f"S3Exception: Access Denied (Service: S3, Status Code: 403, "
        f"Request ID: {_rand_token(16)})",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.security.svc.TokenRefreshSvc",
        f"Token refresh rejected for clientId: svc-{random.randint(1000, 9999)} with detail: "
        f'401 Unauthorized: "'
        + _flat_json_error("TOKEN_EXPIRED", "refresh token expired or revoked") + '"',
    ),
    lambda: (
        "ERROR",
        "com.example.platform.report.worker.PdfRenderWorker",
        f"Rendering aborted for reportId: rpt-{random.randint(100000, 999999)} after 30000ms — "
        f"java.util.concurrent.TimeoutException: renderer did not return",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.core.svc.schedule.RetryScheduler",
        f"Giving up on task {uuid.uuid4().hex[:10]} after 5 attempts — last error: "
        f"java.net.ConnectException: Connection refused (Connection refused)",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.integration.PartnerSoapClient",
        f"SOAP fault from partner endpoint for correlationId: {uuid.uuid4()} with detail: "
        + _flat_soap_fault("Server", "Backend unavailable, try again later"),
    ),
    lambda: (
        "ERROR",
        "com.example.platform.core.db.LedgerRepository",
        f"Deadlock while updating ledger row {random.randint(100000, 999999)} — "
        f"org.postgresql.util.PSQLException: ERROR: deadlock detected; "
        f"Detail: Process {random.randint(1000, 9999)} waits for ShareLock",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.core.cache.SessionCacheSvc",
        f"Redis SETEX failed for sessionId: sess-{uuid.uuid4().hex[:12]} — "
        f"redis.clients.jedis.exceptions.JedisConnectionException: Unexpected end of stream",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.core.svc.mail.MailRelaySvc",
        f"Relay refused message for messageId: msg-{uuid.uuid4().hex[:12]} — "
        f"jakarta.mail.SendFailedException: 550 5.7.1 Relay access denied",
    ),
    lambda: (
        "ERROR",
        "com.example.platform.orders.svc.OrderProcessor",
        f"Unhandled failure processing orderRef: {_rand_ref()} — "
        + _flat_stacktrace(
            "java.lang.NullPointerException",
            "Cannot invoke \"Segment.getCode()\" because \"segment\" is null",
            [
                "com.example.platform.orders.svc.OrderProcessor.process(OrderProcessor.java:214)",
                "com.example.platform.orders.web.OrderController.create(OrderController.java:96)",
                "org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:897)",
                "java.base/java.lang.Thread.run(Thread.java:1583)",
            ],
        ),
    ),
    lambda: (
        " WARN",
        "com.example.platform.ratelimit.QuotaGuard",
        f"Downstream quota exceeded for tenantId: tenant-{random.randint(100, 999)} "
        f"with message 429 Too Many Requests — backing off "
        f"{random.choice([500, 1000, 2000, 5000])}ms",
    ),
    lambda: (
        " WARN",
        "com.example.platform.core.svc.health.DependencyProbe",
        f"Health probe degraded for dependency: pricing-engine "
        f"latency={random.randint(1500, 9000)}ms threshold=1000ms — marking DOWN",
    ),
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

_seq = 0


def _next_seq():
    """Monotonic line counter, 1-based, reset on process start."""
    global _seq
    _seq += 1
    return _seq


def _spring_ts():
    """ISO8601 with millisecond precision, as Spring Boot prints it."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _abbrev_logger(fqcn):
    """com.example.app.FooSvc -> c.e.a.FooSvc (Logback's logger{39} style)."""
    parts = fqcn.split(".")
    return ".".join([p[0] for p in parts[:-1]] + [parts[-1]])


def _marker(seq):
    """Identity marker carried by every non-JSON line."""
    return f"[inst={INSTANCE} seq={seq}]"


def _format_spring(seq, level, logger, thread, message):
    # %15.15t truncates the thread name from the left, %-40.40logger pads right.
    return (
        f"{_spring_ts()} {level:>5} 1 --- [{thread[-15:]:>15}] "
        f"{_abbrev_logger(logger)[:40]:<40} : {_marker(seq)} {message}"
    )


def _rand_id(prefix="", length=8):
    return prefix + uuid.uuid4().hex[:length]


def _resolve_path(template):
    return template.replace("{id}", str(random.randint(10000, 99999)))


def _rand_ip():
    return f"10.{random.randint(0,4)}.{random.randint(0,9)}.{random.randint(1,254)}"


# ── Event builders ─────────────────────────────────────────────────────────────

def build_success_event(seq):
    method, path_tpl, operation = random.choice(ENDPOINTS)
    path = _resolve_path(path_tpl)
    status, status_text = random.choice(SUCCESS_STATUSES)

    resp_bytes = 0 if status == 204 else random.randint(200, 8192)
    req_bytes  = random.randint(0, 1024) if method in ("POST", "PUT", "PATCH") else 0
    duration_ms = round(random.uniform(8, 450), 2)

    return {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "instance": INSTANCE,
        "seq": seq,
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


def build_json_error_event(seq):
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
        "instance": INSTANCE,
        "seq": seq,
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


def build_raw_error_line(seq):
    """Raw non-JSON string — emitted by nginx/proxy/infra layer."""
    method, path_tpl, _ = random.choice(ENDPOINTS)
    path   = _resolve_path(path_tpl)
    status, _ = random.choice(ERROR_STATUSES)
    template   = random.choice(NON_JSON_CATALOG)
    body = template(method, path, status)
    # The seq marker goes first so even a zero-byte body stays identifiable.
    return f"{_marker(seq)} {body}".rstrip()


def build_spring_error_line(seq):
    """Spring Boot console line — app layer, single line, may embed HTML/XML."""
    level, logger, message = random.choice(SPRING_ERROR_CATALOG)()
    thread = random.choice(SPRING_THREADS)
    return _format_spring(seq, level, logger, thread, message)


def build_event(seq):
    """Return either a dict (JSON) or a raw string (non-JSON)."""
    if random.random() < ERROR_RATE:
        roll = random.random()
        if roll < SPRING_RATE:
            return build_spring_error_line(seq)          # raw string
        if roll < SPRING_RATE + NON_JSON_RATE:
            return build_raw_error_line(seq)             # raw string
        return build_json_error_event(seq)               # dict
    return build_success_event(seq)                      # dict


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
        f"instance={INSTANCE} service={SERVICE} version={SERVICE_VERSION} "
        f"interval={INTERVAL}s tcp={TCP_OUTPUT} "
        f"target={HOST}:{PORT} error_rate={ERROR_RATE:.0%} "
        f"spring={SPRING_RATE:.0%} raw={NON_JSON_RATE:.0%}"
    )
    _log(
        f"sequence starts at 1 for instance '{INSTANCE}' — JSON lines carry "
        f'"instance"/"seq", others carry [inst={INSTANCE} seq=N]'
    )
    shipper = Shipper(HOST, PORT) if TCP_OUTPUT else None

    while True:
        event = build_event(_next_seq())
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
