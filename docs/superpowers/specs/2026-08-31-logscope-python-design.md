# LogScope Python Design Specification

## Goal

LogScope is a production-shaped log ingestion and search service. It accepts bounded JSON log batches, records ingestion state in PostgreSQL, indexes normalized documents into Elasticsearch asynchronously, and exposes typed search and operational endpoints.

The repository is intentionally self-contained and honest about scope: it demonstrates ingestion, durable state transitions, idempotent indexing, retry behavior, filtering, aggregations, health checks, tests, Docker Compose, and GitLab CI. It does not claim production traffic, customer usage, or benchmark results that have not been measured.

## Architecture

```text
HTTP client
  |
  | POST /v1/ingest
  v
FastAPI API
  | validate + persist batch/job state
  | enqueue indexing job
  v
PostgreSQL ------------------------------+
  | batch, log metadata, job attempts     |
  +---------------------------------------+
                                          |
                                          v
                                  Indexing worker
                                    | normalize
                                    | bulk upsert
                                    | retry transient failures
                                    v
                                  Elasticsearch

GET /v1/search -> typed Elasticsearch query -> hits + facets + cursor
Redis -> distributed rate-limit state for API requests
```

The first implementation uses a PostgreSQL-backed durable job table rather than adding a second broker. The API transaction creates the batch and its jobs atomically; the worker claims pending jobs with row locks and `SKIP LOCKED`. This preserves the required durable boundary without introducing an unneeded queue service. Redis is used only for rate limiting and is not a source of business truth.

The API and worker share domain schemas, repositories, normalization, and indexing ports. Infrastructure adapters implement those ports. HTTP handlers never construct raw Elasticsearch DSL or SQL strings directly.

## Data model

### PostgreSQL

`ingestion_batches`

- `id UUID PRIMARY KEY`
- `source VARCHAR(128) NOT NULL`
- `status VARCHAR(32) NOT NULL` (`pending`, `processing`, `completed`, `partial`, `failed`)
- `document_count INTEGER NOT NULL CHECK (document_count >= 0)`
- `indexed_count INTEGER NOT NULL CHECK (indexed_count >= 0)`
- `failed_count INTEGER NOT NULL CHECK (failed_count >= 0)`
- `created_at TIMESTAMPTZ NOT NULL`
- `updated_at TIMESTAMPTZ NOT NULL`

`index_jobs`

- `id BIGSERIAL PRIMARY KEY`
- `batch_id UUID REFERENCES ingestion_batches(id) ON DELETE CASCADE`
- `document_id VARCHAR(256) NOT NULL`
- `payload JSONB NOT NULL`
- `status VARCHAR(32) NOT NULL` (`pending`, `processing`, `indexed`, `failed`)
- `attempts INTEGER NOT NULL CHECK (attempts >= 0)`
- `available_at TIMESTAMPTZ NOT NULL`
- `last_error TEXT`
- `locked_at TIMESTAMPTZ`
- `created_at TIMESTAMPTZ NOT NULL`
- `updated_at TIMESTAMPTZ NOT NULL`
- `UNIQUE (batch_id, document_id)`

The unique key makes retried ingestion idempotent for the same source batch and deterministic document ID. Job claims update status and attempt metadata in one transaction. The worker resets abandoned `processing` jobs after a configurable lease timeout.

`log_records` are not duplicated into PostgreSQL; the canonical searchable document is in Elasticsearch. PostgreSQL stores enough payload and job state to retry indexing without asking the client to resend data.

### Elasticsearch

Index name: `logscope-logs-v1`.

Document fields:

- `document_id` keyword
- `batch_id` keyword
- `source` keyword
- `timestamp` date
- `level` keyword
- `service` keyword
- `environment` keyword
- `trace_id` keyword
- `request_id` keyword
- `message` text with a keyword subfield where useful
- `fields` flattened/object for structured attributes

Index creation is explicit and versioned. The worker uses deterministic IDs and bulk index/upsert operations. Search uses a typed request model and a fixed allowlist of filters and sort fields.

## Ingestion flow

1. FastAPI validates a request with Pydantic models.
2. Body, batch size, message length, and structured field depth are bounded before persistence.
3. The service normalizes each event and derives a deterministic `document_id` from `source` and the event's supplied `event_id`; when no event ID is supplied, it derives one from canonical event content plus source.
4. One PostgreSQL transaction inserts the batch and `index_jobs` with `ON CONFLICT (batch_id, document_id) DO NOTHING`.
5. The API returns `202 Accepted` with the batch ID and counts.
6. The worker claims jobs, indexes them in bounded bulk chunks, and records indexed/failed state.
7. Batch status is derived from job state: `completed` when all jobs are indexed, `partial` when both indexed and failed jobs exist, and `failed` when all jobs are terminal failures.

Responses never include raw database or Elasticsearch exception text.

## Search API

`GET /v1/search`

Supported query parameters:

- `q`: optional full-text query, max 256 characters
- `level`: repeated or comma-separated allowlisted levels
- `service`: repeated or comma-separated service names, max 32 values
- `environment`: repeated or comma-separated environments
- `trace_id`: exact match
- `request_id`: exact match
- `from`: RFC3339 timestamp, inclusive
- `to`: RFC3339 timestamp, exclusive
- `cursor`: opaque base64url search-after cursor
- `limit`: integer from 1 to 100, default 25

Sort order is `timestamp DESC, document_id ASC`. The cursor encodes only the last sort values and is validated before use. Search returns `hits`, `next_cursor`, `total_relation`, and `facets` for level and service. No arbitrary field names, scripts, or raw DSL are accepted from clients.

`GET /v1/search/facets` exposes the same bounded filters and returns only aggregations. It shares the same query builder and limits.

## Operational endpoints

- `GET /health`: process and dependency status; returns `200` when the process is alive, `503` when a required dependency is unavailable.
- `GET /ready`: returns `200` only when PostgreSQL and Elasticsearch are reachable and the worker can claim work.
- `GET /metrics`: Prometheus text exposition for request counts, ingest counts, job transitions, indexing latency, retries, and search failures.
- `GET /v1/ingest/{batch_id}`: batch status and counts.

## Failure handling

- Validation failures return structured `400` responses and do not write data.
- Duplicate document IDs within one batch are coalesced before persistence.
- Duplicate requests with the same source and event IDs are idempotent through the PostgreSQL unique key and Elasticsearch deterministic IDs.
- PostgreSQL transaction failure prevents a `202` response.
- Worker claims have a lease. A process crash leaves jobs recoverable after the lease expires.
- Elasticsearch bulk failures are classified as transient or terminal. Transient failures retry with exponential backoff and jitter, bounded by `INDEX_MAX_ATTEMPTS`; terminal failures become `failed` with sanitized error class and truncated message.
- A failed Elasticsearch write never becomes `indexed`.
- Redis outage fails open for rate limiting with a metric and log signal; PostgreSQL and Elasticsearch outages fail readiness and return sanitized `503` responses.
- Graceful shutdown stops new claims, lets the active indexing operation finish until the shutdown deadline, and leaves uncommitted jobs pending or lease-recoverable.

## Security and privacy

- Secrets are environment-only. `.env` and local overrides are ignored; `.env.example` contains development placeholders only.
- No raw log payload is emitted to application logs. Structured fields are not copied into metric labels.
- Request bodies, batch size, message size, query size, cursor size, and result limits are enforced.
- SQL uses SQLAlchemy expressions and bound parameters.
- Elasticsearch queries are generated from typed allowlists; arbitrary DSL is not accepted.
- Rate limiting uses the socket peer address or an explicitly trusted proxy configuration, never an unconditional client-provided forwarding header.
- Docker runs as a non-root user. Compose credentials are local development values and are not suitable for public deployment.
- CI uses masked GitLab variables and never prints secrets.

## Testing strategy

Unit tests cover normalization, deterministic IDs, cursor encoding/decoding, query precedence, retry classification, batch status transitions, and rate-limit boundaries.

API tests use `httpx` and dependency overrides to verify `202` ingestion, validation failures, duplicate coalescing, batch status, search filters, cursor pagination, facets, health states, and sanitized dependency errors.

Integration tests use Testcontainers for PostgreSQL and Elasticsearch. They apply migrations, persist an ingestion batch, run the real worker against the real Elasticsearch adapter, verify idempotent indexing, and query the indexed documents.

Compose smoke starts API, worker, PostgreSQL, Elasticsearch, and Redis, then exercises ingest → indexing → search and checks health/metrics. The smoke test asserts observable behavior rather than timings or fabricated throughput.

GitLab CI runs Ruff, mypy, Bandit, unit tests, integration tests, Docker build, and the Compose smoke job. Dependency and container versions are pinned in project configuration where practical.

## Non-goals

- Authentication, multi-tenant authorization, billing, alerting, log tailing, retention management, arbitrary Elasticsearch DSL, Kafka, and a hosted SaaS control plane.
- Claims about production scale, customer adoption, or commercial experience.
- A second queue/broker before the PostgreSQL durable job table proves insufficient for the requested scope.
