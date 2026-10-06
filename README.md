# LogScope Python

[![CI](https://github.com/denfry/logscope-python/actions/workflows/ci.yml/badge.svg)](https://github.com/denfry/logscope-python/actions/workflows/ci.yml)

LogScope is a bounded log-ingestion and search service built around FastAPI, PostgreSQL, Elasticsearch, Redis, Docker Compose, and GitLab CI. It accepts structured log batches, persists indexing jobs durably, and exposes typed full-text and exact-filter search.

## Quick start

Requirements: Docker Engine with Compose v2 and Python 3.12 (for the smoke script).

```bash
cp .env.example .env          # use `copy` in cmd.exe
docker compose up --build -d
python scripts/smoke.py
docker compose down -v
```

The API listens on <http://localhost:8080>. See [Local setup](#local-setup) to run the API outside Docker.

## Architecture

```text
client
  -> FastAPI API
       -> PostgreSQL: ingestion_batches + index_jobs
       -> Redis: fixed-window rate limiting
       -> Elasticsearch: searchable log documents
  -> leased worker -> Elasticsearch bulk upsert
```

- **PostgreSQL** is the source of truth for accepted batches and indexing jobs.
- **Elasticsearch** stores normalized documents using deterministic SHA-256 IDs. Re-ingesting the same event updates the same document.
- **Redis** is used only for distributed request rate-limit state. If Redis is unavailable, requests fail open and a Prometheus counter records the event.
- **Worker leases** use `FOR UPDATE SKIP LOCKED`. Expired processing leases become pending again, so an interrupted worker does not permanently strand jobs.

## API

### Ingest a batch

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H 'content-type: application/json' \
  -d '{
    "source": "gateway",
    "records": [{
      "event_id": "evt-1",
      "timestamp": "2026-08-31T10:00:00Z",
      "level": "error",
      "service": "payments",
      "environment": "prod",
      "message": "card declined",
      "trace_id": "trace-1",
      "fields": {"status": 402}
    }]
  }'
```

The response is `202 Accepted` and contains `batch_id`, `accepted_records`, and deterministic `document_ids`. Records with the same document ID in one batch are coalesced before job creation.

### Track a batch

```bash
curl http://localhost:8080/v1/ingest/<batch_id>
```

Statuses are `pending`, `completed`, `partial`, and `failed`. A partial batch means every job reached a terminal state and at least one indexed job and one failed job remain.

### Search

```bash
curl 'http://localhost:8080/v1/search?q=card%20declined&service=payments&limit=20'
curl 'http://localhost:8080/v1/search/facets?environment=prod'
```

Supported filters are full-text `q`, `levels`, `service`, `environment`, `source`, `from_ts`, `to_ts`, `limit`, and an opaque `cursor`. Search uses fixed ordering: `timestamp DESC`, then `document_id ASC`. The cursor is URL-safe base64 JSON containing only the last timestamp and document ID.

### Health and metrics

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
curl http://localhost:8080/metrics
```

`/health` checks PostgreSQL, Elasticsearch, and Redis. `/ready` includes worker dependency readiness. Dependency failures return `503` without exposing connection strings or backend response bodies.

## Local setup

Requirements: Python 3.12, Docker Engine with Compose v2.

```bash
python3.12 -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
python -m pip install -e ".[dev]"
cp .env.example .env
python -m app.main
```

For the complete local stack:

```bash
docker compose up --build -d
docker compose ps
python scripts/smoke.py
docker compose down -v
```

Compose starts PostgreSQL, Elasticsearch, Redis, the API, and the worker only after dependency health checks pass. Local Compose credentials are placeholders intended for the isolated development stack; do not reuse them outside it.

## Configuration

All supported environment variables are listed in [`.env.example`](.env.example). Important bounds include:

- `INGEST_MAX_BODY_BYTES`: maximum ingest request body, default 1 MiB.
- `INGEST_MAX_RECORDS`: maximum records in one request, default 1000.
- `RECORD_MAX_MESSAGE_CHARS`: maximum message length, default 4096.
- `RECORD_MAX_FIELDS_DEPTH`: maximum structured field depth, default 5.
- `SEARCH_MAX_LIMIT`: maximum page size, default 100.
- `SEARCH_MAX_QUERY_CHARS`: maximum full-text query length, default 256.
- `SEARCH_MAX_RANGE_SECONDS`: maximum time range, default 31 days.
- `WORKER_LEASE_SECONDS`: lease duration before recovery, default 60 seconds.
- `WORKER_MAX_ATTEMPTS`: terminal failure threshold, default 3.
- `TRUSTED_PROXY_CIDRS`: optional comma-separated proxy networks. Forwarded client addresses are ignored unless the socket peer belongs to one of these networks.

## Indexing behavior

The index mapping explicitly defines timestamp, level, service, environment, source, event IDs, tracing IDs, and message fields. Arbitrary client-controlled Elasticsearch DSL is not accepted. Structured `fields` are retained as an object after bounded validation; payloads are not written to application logs.

Transient Elasticsearch bulk failures (HTTP 429 and 5xx) are retried with bounded exponential backoff and jitter until `WORKER_MAX_ATTEMPTS`. Mapping and other terminal failures are marked failed immediately. Error classes are sanitized and truncated before storage.

## Project layout

```text
app/api.py, app/main.py   FastAPI routes and process entry points (API and worker)
app/domain/               Normalization, query building, retry policy
app/services/             Ingest and search use cases
app/repositories/         PostgreSQL job and batch persistence
app/adapters/             Elasticsearch and Redis rate-limit adapters
migrations/               SQL schema
scripts/smoke.py          End-to-end smoke check against the Compose stack
tests/                    unit, api and integration (Testcontainers) suites
```

## Testing

Unit and API tests run without external services:

```bash
python -m pytest -m 'not integration' --cov=app --cov-fail-under=80
python -m ruff check app tests
python -m mypy app
python -m bandit -q -r app
```

Integration tests start real PostgreSQL and Elasticsearch containers through Testcontainers:

```bash
python -m pytest -m integration -q
```

The GitLab pipeline (`.gitlab-ci.yml`) separates `lint`, `unit`, `integration`, `build`, and `smoke` stages. The smoke path performs ingest, batch completion polling, search-hit polling, health, readiness, and metrics checks.

## Security notes

- Secrets are supplied through environment variables; `.env` is ignored and only `.env.example` is tracked.
- Request and search inputs are bounded by Pydantic models and settings limits.
- Search filters are allowlisted; raw Elasticsearch query fragments and client field names are not accepted.
- Database access uses bound parameters. The worker never logs record payloads.
- Dependency error responses are generic. Health output contains statuses only.
- Compose disables Elasticsearch security only for the isolated local stack. A deployed environment requires its own authentication, TLS, network policy, secret storage, and operational controls.

## Non-goals

This repository does not provide authentication, multi-tenant authorization, alerting, schema evolution for arbitrary log formats, long-term retention policy, or a managed production deployment. It intentionally keeps the durable queue in PostgreSQL instead of introducing a separate broker.
