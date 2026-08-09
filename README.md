# Log generator

A small Python service that produces one log event every 3 seconds, in a
deliberately mixed set of real-world shapes: Spring Boot console lines,
structured JSON, and raw proxy/infra output (HTML, XML, stacktraces, empty
bodies). Every line is numbered so you can detect dropped lines downstream.

Every line goes to **stdout**, so `docker logs -f log-generator` always shows
output. On top of that it ships each line, best-effort, to TCP port **8091**
(your Logstash input). If nothing is listening there the service keeps logging
anyway and retries the connection every 15s - it never blocks and never exits.

## Run with Docker

The stack starts **five identical containers at once**, differing only in how
often they emit:

| Service | Container | `LOG_INTERVAL` | `INSTANCE_ID` | Events/day |
|---|---|---|---|---|
| `gen-03s` | `log-generator-03s` | 3 s | `g03` | 28,800 |
| `gen-08s` | `log-generator-08s` | 8 s | `g08` | 10,800 |
| `gen-15s` | `log-generator-15s` | 15 s | `g15` | 5,760 |
| `gen-22s` | `log-generator-22s` | 22 s | `g22` | 3,927 |
| `gen-30s` | `log-generator-30s` | 30 s | `g30` | 2,880 |

```bash
git clone https://github.com/Arindam-official/simple_test.git
cd simple_test
docker compose up -d --build     # all five
docker compose logs -f           # follow all five, interleaved
docker compose logs -f gen-03s   # follow just the fastest one
docker compose down              # stop all five
```

All five share one image (`log-generator:latest`) and one set of tuning knobs;
only the interval and instance id differ. To change the intervals, edit
`LOG_INTERVAL` on the service in `docker-compose.yml`. To run a single
container instead, `docker compose up -d gen-03s`.

By default it targets `host.docker.internal:8091` — i.e. Logstash running on
the Docker host. Point it somewhere else with a `.env` file or inline vars:

```bash
LOGSTASH_HOST=10.0.0.5 docker compose up -d --build
```

If your Logstash runs in another compose stack, attach to its network instead
and set `LOGSTASH_HOST` to that service name.

## Run without Docker

```bash
LOGSTASH_HOST=localhost python3 generator/app.py
```

No dependencies — standard library only.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `LOGSTASH_HOST` | `host.docker.internal` | Target host |
| `LOGSTASH_PORT` | `8091` | Target TCP port |
| `LOG_INTERVAL` | `3` | Seconds between logs |
| `SERVICE_NAME` | `demo-app` | Value of the `service` field |
| `INSTANCE_ID` | container hostname | Stamped on every line; keeps the five sequence counters apart |
| `TCP_OUTPUT` | `true` | Set `false` to log to stdout only |
| `RETRY_EVERY` | `15` | Seconds between reconnect attempts |
| `ERROR_RATE` | `0.60` | Fraction of events that are errors |
| `SPRING_RATE` | `0.45` | Fraction of errors emitted as Spring Boot console lines |
| `NON_JSON_RATE` | `0.25` | Fraction of errors emitted as raw proxy/infra output |

The remaining errors (`1 - SPRING_RATE - NON_JSON_RATE`, 30 % by default) are
structured JSON.

## Sequence numbers

Every emitted line carries a monotonically increasing sequence number starting
at **1**, so you can tell exactly which lines a downstream collector dropped.
Each container counts **independently**, so every line is also stamped with its
instance id — otherwise all five streams would emit `seq=1, 2, 3…` and collide.

| Line shape | Where the identity lives |
|---|---|
| JSON | top-level fields `"instance": "g03", "seq": 42` |
| Spring / raw | literal marker `[inst=g03 seq=42]` |

Find gaps per instance across all five containers at once:

```bash
docker compose logs --no-color \
  | grep -oE 'inst=g[0-9]+ seq=[0-9]+|"instance": "g[0-9]+", "seq": [0-9]+' \
  | grep -oE 'g[0-9]+|[0-9]+$' | paste - - \
  | sort -k1,1 -k2,2n \
  | awk '{ if ($1 == inst && $2 != prev+1) print "gap:", $1, prev+1, "..", $2-1; inst=$1; prev=$2 }'
```

Compare that against the same extraction run over what actually landed in
Logstash/Loki — anything missing on the receiving side is a skipped line. In
Loki you can also just group by the `container_name` label instead of `inst`.

The counter resets to 1 on every container restart, so a stream that suddenly
restarts at 1 means the container bounced, not that lines were lost. Note that
raw proxy bodies
(nginx HTML, PHP fatals, Java stacktraces) genuinely span multiple physical
lines; only the **first** line of such a body carries the `[seq=N]` marker,
which is exactly what you want for testing multiline handling.

## Output shapes

**Spring Boot console line** — app layer, single line, sometimes with a
flattened HTML/XML/JSON upstream body (`<EOL>` marks where newlines were):

```
2026-08-09T20:16:13.471Z ERROR 1 --- [ntContainer#0-1] c.e.p.c.s.n.WebhookDispatchSvc           : [seq=132] Exception in sending webhook callback for eventRef: 270803923-1TMYM2-RS55108901_54114555 from:  to:  partnerId: partner-142 with detail: 400 Bad Request: "<!DOCTYPE HTML ...<EOL><HTML>...</HTML>"
2026-08-09T20:16:13.469Z  WARN 1 --- [nio-8080-exec-7] c.e.p.r.QuotaGuard                       : [seq=1] Downstream quota exceeded for tenantId: tenant-468 with message 429 Too Many Requests — backing off 5000ms
```

17 distinct Spring error types ship out of the box: webhook dispatch, invoice
sync, price feed, payment capture, stock reservation, Kafka offset commit,
document upload, token refresh, PDF render, retry scheduler, SOAP partner
fault, ledger deadlock, Redis session cache, mail relay, unhandled NPE,
downstream quota, and dependency health probe. All identifiers (references,
tenant/partner ids, request ids) are randomly generated at runtime — no real
values are baked into the generator.

**Structured JSON:**

```json
{"@timestamp":"2026-08-09T20:16:13.469261+00:00","seq":2,"log":{"level":"INFO","logger":"app.routers.get_order"},"message":"GET /api/v1/orders/86111 200 415.61ms","service":{"name":"demo-app","version":"3.1.4","environment":"production"},"http":{"...":"..."}}
```

**Raw proxy/infra output** — nginx HTML, Envoy plain text, ALB XML, Java
stacktrace, PHP fatal, or an empty body, each prefixed with `[seq=N]`.

## Logstash input

Because the stream is deliberately mixed, `codec => json_lines` alone will
choke on the non-JSON lines. Take them as plain lines and parse conditionally:

```
input { tcp { port => 8091 codec => line } }

filter {
  if [message] =~ /^\{/ {
    json { source => "message" }
  } else {
    grok {
      match => { "message" => "%{TIMESTAMP_ISO8601:ts}%{SPACE}%{LOGLEVEL:level} %{NUMBER:pid} --- \[%{DATA:thread}\] %{DATA:logger}%{SPACE}: \[seq=%{NUMBER:seq:int}\] %{GREEDYDATA:msg}" }
      tag_on_failure => ["_rawline"]
    }
  }
}
```
