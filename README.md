# ELK log-generator demo

A dockerised Python app that emits one JSON log line **every 3 seconds** to
**Logstash on TCP port 8091**. Logstash stores them in Elasticsearch (daily
index `app-logs-YYYY.MM.dd`) and also writes raw files to `./logs/`.

```
log-generator --TCP 8091--> logstash --> elasticsearch --> kibana (5601)
                                     \-> ./logs/app-logs-YYYY.MM.dd.log
```

## Run

```bash
git clone <this-repo> && cd <this-repo>
docker compose up -d --build
docker compose logs -f log-generator
```

First boot takes ~1 minute (Elasticsearch health check gates Logstash/Kibana).

## Verify logs are stored

```bash
# indices exist
curl 'http://localhost:9200/_cat/indices/app-logs-*?v'

# latest documents
curl 'http://localhost:9200/app-logs-*/_search?size=5&sort=@timestamp:desc&pretty'

# raw files on disk
tail -f logs/app-logs-*.log
```

Kibana: <http://localhost:5601> → **Stack Management → Data Views** → create a
data view with pattern `app-logs-*` and time field `@timestamp`, then browse in
**Discover**.

## Config

Environment variables on the `log-generator` service in `docker-compose.yml`:

| Variable | Default | Meaning |
|---|---|---|
| `LOGSTASH_HOST` | `logstash` | Target host |
| `LOGSTASH_PORT` | `8091` | Target TCP port |
| `LOG_INTERVAL` | `3` | Seconds between logs |
| `SERVICE_NAME` | `demo-app` | `service` field value |

## Send logs from outside Docker

Port 8091 is published on the host, so anything can feed it:

```bash
echo '{"level":"INFO","message":"hello from host","service":"manual"}' | nc localhost 8091
```

Or run the generator locally:

```bash
LOGSTASH_HOST=localhost python generator/app.py
```

## Already have an ELK stack?

Delete the `elasticsearch`, `logstash`, and `kibana` services from
`docker-compose.yml`, keep `log-generator`, and point it at your Logstash:

```yaml
environment:
  - LOGSTASH_HOST=your-logstash-host
  - LOGSTASH_PORT=8091
```

Make sure that Logstash has a `tcp { port => 8091 codec => json_lines }` input.

## Stop

```bash
docker compose down        # keep data
docker compose down -v     # wipe Elasticsearch data too
```
