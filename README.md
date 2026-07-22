# Log generator

A small Python service that produces one JSON log line every 3 seconds and
sends it to TCP port **8091** (your Logstash input), while also printing it to
stdout.

## Run with Docker

```bash
git clone https://github.com/Arindam-official/simple_test.git
cd simple_test
docker compose up -d --build
docker compose logs -f
```

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

The service waits (retrying every 3s) until the port accepts connections, and
reconnects automatically if it goes away.

## Sample output

```json
{"@timestamp":"2026-07-22T12:26:20.121671+00:00","service":"demo-app","level":"ERROR","message":"upstream timeout","trace_id":"25f1c196-...","http":{"method":"DELETE","path":"/api/orders","status":500,"duration_ms":179.8},"client_ip":"10.0.0.147","user_id":"user-1032"}
```

Lines are newline-delimited JSON, so the matching Logstash input is:

```
input { tcp { port => 8091 codec => json_lines } }
```
