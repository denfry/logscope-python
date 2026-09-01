# LogScope Python Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and publish a tested FastAPI log ingestion/search service with durable PostgreSQL jobs, Elasticsearch indexing, Redis rate limiting, Docker Compose, and GitLab CI.

**Architecture:** FastAPI accepts bounded log batches and commits `ingestion_batches` plus `index_jobs` atomically in PostgreSQL. A worker claims jobs with row locks and `SKIP LOCKED`, normalizes records, and bulk-upserts deterministic Elasticsearch documents with retries and lease recovery. Search uses typed filters, fixed sort order, cursor pagination, and bounded facets; Redis is used only for distributed rate limiting.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2 async, asyncpg, Alembic-style SQL migrations, Elasticsearch Python client 8.x, Redis asyncio client, pytest, pytest-asyncio, httpx, Testcontainers, Ruff, mypy, Bandit, Docker Compose, GitLab CI.

**Spec:** `docs/superpowers/specs/2026-08-31-logscope-python-design.md`

## Global Constraints

- Python runtime MUST be 3.12+.
- HTTP request bodies, batches, messages, structured fields, cursors, and result limits MUST be bounded.
- PostgreSQL MUST remain the source of ingestion/job truth; Redis MUST NOT store business state.
- Ingestion MUST persist the batch and all index jobs in one PostgreSQL transaction.
- Index document IDs MUST be deterministic and duplicate ingestion MUST be idempotent.
- Workers MUST claim jobs with a lease and `FOR UPDATE SKIP LOCKED`.
- Elasticsearch queries MUST be built from typed allowlists; clients MUST NOT submit raw DSL, field names, or scripts.
- Errors returned to clients MUST be sanitized and logs MUST NOT include raw log payloads.
- New behavior MUST follow RED → GREEN → REFACTOR; each test must fail before its production implementation.
- No real secrets, customer data, production usage, or unmeasured benchmark claims may enter the repository.
- Every meaningful slice MUST end with a focused test command and a conventional commit without co-author attribution.

---

### Task 1: Scaffold Python service and configuration

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `.dockerignore`
- Create: `.env.example`
- Create: `app/__init__.py`
- Create: `app/config.py`
- Create: `tests/unit/test_config.py`

**Interfaces:**
- Produces `app.config.Settings` loaded from environment with typed fields: `database_url`, `elasticsearch_url`, `elasticsearch_index`, `redis_url`, `http_host`, `http_port`, `worker_poll_interval`, `worker_batch_size`, `worker_lease_seconds`, `worker_max_attempts`, `ingest_max_body_bytes`, `ingest_max_records`, `record_max_message_chars`, `search_max_limit`, `search_max_query_chars`, `rate_limit_requests`, `rate_limit_window_seconds`, and `shutdown_timeout_seconds`.
- `get_settings() -> Settings` is an `@lru_cache` function used by API and worker entrypoints.

- [ ] **Step 1: Write the failing configuration tests**

```python
from pydantic import ValidationError
import pytest
from app.config import Settings


def test_settings_loads_safe_local_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://app:app@postgres:5432/logscope")
    monkeypatch.setenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
    settings = Settings()
    assert settings.ingest_max_records == 1000
    assert settings.search_max_limit == 100
    assert settings.worker_max_attempts == 3


def test_settings_rejects_invalid_limits() -> None:
    with pytest.raises(ValidationError):
        Settings(
            DATABASE_URL="postgresql+asyncpg://app:app@postgres:5432/logscope",
            ELASTICSEARCH_URL="http://elasticsearch:9200",
            INGEST_MAX_RECORDS=0,
        )
```

- [ ] **Step 2: Run the focused tests and verify the expected missing-module failure**

Run: `pytest tests/unit/test_config.py -q`
Expected: collection fails because `app.config` does not exist.

- [ ] **Step 3: Add the minimal settings model and tool configuration**

`Settings` MUST use `BaseSettings` with aliases matching the uppercase environment names, positive integer validators, and a `field_validator` that rejects `search_max_limit > 100` and `ingest_max_records > 1000`. `pyproject.toml` MUST define runtime dependencies, test/dev dependency groups, Ruff, mypy, pytest asyncio mode, and a `logscope-api`/`logscope-worker` script entrypoint.

- [ ] **Step 4: Run focused and static checks**

Run: `pytest tests/unit/test_config.py -q && ruff check app tests`
Expected: PASS with no diagnostics.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml .gitignore .dockerignore .env.example app tests/unit/test_config.py
git commit -m "build: scaffold LogScope configuration"
```

### Task 2: Add database schema and durable job repository

**Files:**
- Create: `migrations/001_init.sql`
- Create: `app/db.py`
- Create: `app/models.py`
- Create: `app/repositories/jobs.py`
- Create: `tests/unit/test_job_state.py`
- Create: `tests/integration/test_jobs_postgres.py`

**Interfaces:**
- `app.db.create_engine(settings) -> AsyncEngine` and `create_session_factory(engine) -> async_sessionmaker[AsyncSession]`.
- `JobRepository.create_batch(session, source, records) -> BatchRecord`.
- `JobRepository.claim_batch(session, limit, lease_seconds) -> list[JobRecord]`.
- `JobRepository.mark_indexed(session, job_id)`, `mark_retry(session, job_id, available_at, error_class)`, and `mark_failed(session, job_id, error_class)`.
- `JobRepository.get_batch(session, batch_id) -> BatchRecord | None`.

- [ ] **Step 1: Write state-transition tests before repository code**

```python
from app.models import BatchStatus, JobStatus
from app.repositories.jobs import derive_batch_status


def test_batch_is_partial_when_indexed_and_failed_jobs_remain() -> None:
    assert derive_batch_status(total=3, indexed=2, failed=1) is BatchStatus.PARTIAL


def test_batch_is_pending_when_no_job_is_terminal() -> None:
    assert derive_batch_status(total=2, indexed=0, failed=0) is BatchStatus.PENDING
```

- [ ] **Step 2: Run the state tests and verify the expected missing-module failure**

Run: `pytest tests/unit/test_job_state.py -q`
Expected: collection fails because `app.models` does not exist.

- [ ] **Step 3: Create schema and repository implementation**

`001_init.sql` MUST create `ingestion_batches` and `index_jobs` with UUID batch IDs, JSONB payloads, status checks, timestamp columns, `(batch_id, document_id)` uniqueness, indexes on `(status, available_at)` and `batch_id`, and a migration marker table. Repository claims MUST execute:

```sql
SELECT id, batch_id, document_id, payload, attempts
FROM index_jobs
WHERE status IN ('pending', 'processing')
  AND available_at <= now()
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT :limit
```

Claimed rows MUST be updated to `processing`, increment `attempts`, and set `locked_at` in the same transaction. Rows whose lease is older than `lease_seconds` MUST be reset to `pending` before new claims. Duplicate jobs MUST use `ON CONFLICT (batch_id, document_id) DO NOTHING`.

- [ ] **Step 4: Add PostgreSQL integration coverage**

The Testcontainers test MUST start PostgreSQL, apply `migrations/001_init.sql`, create a batch with two records including a duplicate document ID, assert one job per unique ID, claim it, mark it indexed, and assert `get_batch` reports `completed`.

- [ ] **Step 5: Run focused checks**

Run: `pytest tests/unit/test_job_state.py -q`
Expected: PASS.

Run: `pytest tests/integration/test_jobs_postgres.py -q`
Expected: PASS when Docker is available; without Docker the test MUST fail clearly rather than silently skip in the explicit integration command.

- [ ] **Step 6: Commit**

```bash
git add migrations app/db.py app/models.py app/repositories tests/unit/test_job_state.py tests/integration/test_jobs_postgres.py
git commit -m "feat: add durable ingestion job repository"
```

### Task 3: Implement normalization and deterministic document IDs

**Files:**
- Create: `app/domain/__init__.py`
- Create: `app/domain/normalization.py`
- Create: `app/schemas.py`
- Create: `tests/unit/test_normalization.py`

**Interfaces:**
- `LogRecordInput` validates `timestamp`, `level`, `service`, `environment`, optional IDs, `message`, and bounded `fields`.
- `normalize_record(source: str, record: LogRecordInput) -> NormalizedLogRecord`.
- `deterministic_document_id(source: str, record: LogRecordInput) -> str` returns a stable SHA-256 hex ID.

- [ ] **Step 1: Write failing normalization tests**

```python
from app.domain.normalization import deterministic_document_id, normalize_record
from app.schemas import LogRecordInput


def test_document_id_is_stable_for_same_source_and_event() -> None:
    record = LogRecordInput.model_validate({
        "event_id": "evt-1",
        "timestamp": "2026-08-31T10:00:00Z",
        "level": "ERROR",
        "service": "payments",
        "environment": "prod",
        "message": "card declined",
    })
    assert deterministic_document_id("gateway", record) == deterministic_document_id("gateway", record)


def test_normalization_lowercases_level_and_copies_trace_fields() -> None:
    record = LogRecordInput.model_validate({
        "timestamp": "2026-08-31T10:00:00Z",
        "level": "WARN",
        "service": "api",
        "environment": "dev",
        "trace_id": "trace-1",
        "message": "slow request",
    })
    normalized = normalize_record("stdout", record)
    assert normalized.level == "warn"
    assert normalized.trace_id == "trace-1"
```

- [ ] **Step 2: Run focused tests and verify the expected failure**

Run: `pytest tests/unit/test_normalization.py -q`
Expected: collection fails because normalization and schemas are absent.

- [ ] **Step 3: Implement typed schemas and normalization**

The validator MUST reject blank source/service/environment, messages over `record_max_message_chars`, unsupported levels, invalid RFC3339 timestamps, and fields deeper than five object levels. IDs without a supplied `event_id` MUST hash canonical JSON with sorted keys, the source, and the normalized timestamp. The normalized output MUST retain original structured fields without logging them.

- [ ] **Step 4: Run focused tests and boundary checks**

Run: `pytest tests/unit/test_normalization.py -q`
Expected: PASS, including oversize and invalid-input cases.

- [ ] **Step 5: Commit**

```bash
git add app/domain app/schemas.py tests/unit/test_normalization.py
git commit -m "feat: normalize logs with stable document ids"
```

### Task 4: Add Elasticsearch adapter and safe query builder

**Files:**
- Create: `app/adapters/elasticsearch.py`
- Create: `app/domain/query.py`
- Create: `tests/unit/test_query_builder.py`
- Create: `tests/unit/test_cursor.py`

**Interfaces:**
- `SearchFilters` contains only the documented fields and bounded values.
- `build_query(filters: SearchFilters) -> dict[str, object]` returns an internal Elasticsearch query.
- `encode_cursor(timestamp: datetime, document_id: str) -> str` and `decode_cursor(cursor: str) -> tuple[datetime, str]` use URL-safe base64 JSON with strict size and field validation.
- `ElasticsearchAdapter.ensure_index()`, `bulk_upsert(documents)`, `search(filters)`, and `facets(filters)`.

- [ ] **Step 1: Write failing query and cursor tests**

```python
from app.domain.query import SearchFilters, build_query


def test_query_combines_full_text_and_exact_filters() -> None:
    query = build_query(SearchFilters(q="timeout", levels=["error"], service="api"))
    clauses = query["bool"]["must"]
    assert any("multi_match" in clause for clause in clauses)
    assert {"term": {"level": "error"}} in query["bool"]["filter"]
    assert {"term": {"service": "api"}} in query["bool"]["filter"]


def test_query_rejects_arbitrary_field_names() -> None:
    filters = SearchFilters.model_validate({"q": "x", "field": "__source"})
    assert not hasattr(filters, "field")
```

- [ ] **Step 2: Run focused tests and verify expected missing-module failures**

Run: `pytest tests/unit/test_query_builder.py tests/unit/test_cursor.py -q`
Expected: collection fails because query and cursor modules are absent.

- [ ] **Step 3: Implement the allowlisted query builder and cursor**

The query builder MUST emit `bool.must` only for `q`, `bool.filter` for exact/time filters, fixed `sort` `[{"timestamp": "desc"}, {"document_id": "asc"}]`, and `search_after` only from a validated cursor. It MUST never interpolate client field names or raw DSL. Cursor decoding MUST reject invalid base64, extra keys, non-RFC3339 timestamps, and values over 512 bytes.

- [ ] **Step 4: Implement Elasticsearch adapter**

Use the official async Elasticsearch client. `ensure_index()` MUST create `logscope-logs-v1` with explicit mappings and return successfully when it already exists. `bulk_upsert()` MUST inspect each bulk item, classify transient HTTP 429/5xx failures separately from terminal mapping errors, and return per-document results without leaking payloads. Search MUST return only the documented hit fields and aggregation buckets.

- [ ] **Step 5: Run focused tests**

Run: `pytest tests/unit/test_query_builder.py tests/unit/test_cursor.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/adapters/elasticsearch.py app/domain/query.py tests/unit/test_query_builder.py tests/unit/test_cursor.py
git commit -m "feat: add safe Elasticsearch search adapter"
```

### Task 5: Implement ingestion and search services

**Files:**
- Create: `app/services/ingest.py`
- Create: `app/services/search.py`
- Modify: `app/repositories/jobs.py`
- Create: `tests/unit/test_ingest_service.py`
- Create: `tests/unit/test_search_service.py`

**Interfaces:**
- `IngestService.ingest(source: str, records: list[LogRecordInput]) -> IngestResult`.
- `IngestService.get_batch(batch_id: UUID) -> BatchResult | None`.
- `SearchService.search(filters: SearchFilters) -> SearchResult`.
- `SearchService.facets(filters: SearchFilters) -> FacetResult`.

- [ ] **Step 1: Write failing service tests**

```python
import pytest
from app.services.ingest import IngestService


@pytest.mark.asyncio
async def test_ingest_coalesces_duplicate_document_ids_before_transaction(fake_uow) -> None:
    service = IngestService(fake_uow)
    result = await service.ingest("gateway", [fake_record("evt-1"), fake_record("evt-1")])
    assert result.accepted_records == 1
    assert fake_uow.created_job_document_ids == [result.document_ids[0]]
```

- [ ] **Step 2: Run focused tests and verify expected failures**

Run: `pytest tests/unit/test_ingest_service.py tests/unit/test_search_service.py -q`
Expected: collection or assertion failures because services are absent.

- [ ] **Step 3: Implement transaction-bound ingestion**

Normalize records, deduplicate by deterministic document ID, open one async session transaction, insert the batch and jobs, and commit before returning. A transaction exception MUST propagate as a typed internal error and MUST NOT produce an accepted result. The service MUST return counts and batch ID only.

- [ ] **Step 4: Implement search service**

Validate `from < to`, maximum time range of 31 days, query size, filter counts, and result limit. Invoke the adapter with the typed filters, map hits to response schemas, and generate the next cursor from the final hit sort values. Elasticsearch errors MUST become `SearchDependencyError` without raw response text.

- [ ] **Step 5: Run focused tests**

Run: `pytest tests/unit/test_ingest_service.py tests/unit/test_search_service.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/services app/repositories/jobs.py tests/unit/test_ingest_service.py tests/unit/test_search_service.py
git commit -m "feat: add ingestion and search services"
```

### Task 6: Add FastAPI routes, health, metrics, and Redis limiter

**Files:**
- Create: `app/api.py`
- Create: `app/health.py`
- Create: `app/metrics.py`
- Create: `app/adapters/rate_limit.py`
- Create: `app/main.py`
- Create: `tests/api/test_api.py`
- Create: `tests/api/test_health.py`

**Interfaces:**
- Routes: `POST /v1/ingest`, `GET /v1/ingest/{batch_id}`, `GET /v1/search`, `GET /v1/search/facets`, `GET /health`, `GET /ready`, `GET /metrics`.
- `create_app(settings: Settings, dependencies: AppDependencies | None = None) -> FastAPI`.
- `RedisRateLimiter.allow(key: str) -> bool`, failing open with a metric when Redis is unavailable.

- [ ] **Step 1: Write failing API tests**

```python
import pytest
from httpx import ASGITransport, AsyncClient
from app.main import create_app


@pytest.mark.asyncio
async def test_ingest_returns_202_and_batch_id(fake_dependencies) -> None:
    app = create_app(fake_dependencies.settings, fake_dependencies)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/ingest", json={
            "source": "gateway",
            "records": [{
                "event_id": "evt-1",
                "timestamp": "2026-08-31T10:00:00Z",
                "level": "error",
                "service": "api",
                "environment": "test",
                "message": "timeout",
            }],
        })
    assert response.status_code == 202
    assert response.json()["accepted_records"] == 1
```

- [ ] **Step 2: Run the API tests and verify the expected missing-route failure**

Run: `pytest tests/api/test_api.py tests/api/test_health.py -q`
Expected: collection fails because `app.main` and routes are absent.

- [ ] **Step 3: Implement routes and dependency wiring**

Handlers MUST use Pydantic request/response schemas, body limits, dependency-injected services, and exception handlers for validation, dependency, not-found, and unexpected errors. `/health` checks process plus dependency connectivity and returns `503` when required dependencies fail. `/ready` additionally checks that the worker repository can claim work. `/metrics` returns Prometheus text and MUST NOT include log contents or user-controlled labels.

- [ ] **Step 4: Implement Redis limiter and request middleware**

Use a Redis Lua script or atomic `INCR` plus `EXPIRE` for a fixed window. Key by socket peer unless `TRUSTED_PROXY_CIDRS` explicitly permits a forwarded address. Limit ingest and search separately. Redis errors MUST allow the request and increment `logscope_rate_limit_fail_open_total`.

- [ ] **Step 5: Run API checks**

Run: `pytest tests/api -q && ruff check app tests && mypy app`
Expected: PASS with no raw dependency errors in responses.

- [ ] **Step 6: Commit**

```bash
git add app/api.py app/health.py app/metrics.py app/adapters/rate_limit.py app/main.py tests/api
git commit -m "feat: expose LogScope HTTP API"
```

### Task 7: Add worker loop, retries, and lease recovery

**Files:**
- Create: `app/worker.py`
- Create: `app/domain/retry.py`
- Modify: `app/repositories/jobs.py`
- Create: `tests/unit/test_retry.py`
- Create: `tests/unit/test_worker.py`

**Interfaces:**
- `RetryPolicy.next_delay(attempt: int) -> float` with bounded exponential delay and jitter.
- `Worker.run(stop_event: asyncio.Event) -> None`.
- `Worker.process_once() -> int` claims, indexes, and transitions a bounded batch.

- [ ] **Step 1: Write failing retry/worker tests**

```python
from app.domain.retry import RetryPolicy


def test_retry_delay_is_bounded() -> None:
    policy = RetryPolicy(base_seconds=0.01, max_seconds=0.05, jitter_seconds=0)
    assert policy.next_delay(20) == 0.05
```

```python
@pytest.mark.asyncio
async def test_worker_marks_terminal_bulk_failure_failed(fake_worker_dependencies) -> None:
    worker = fake_worker_dependencies.worker(max_attempts=2)
    fake_worker_dependencies.elasticsearch.bulk_result = {"job-1": "terminal"}
    processed = await worker.process_once()
    assert processed == 1
    assert fake_worker_dependencies.jobs.failed_ids == ["job-1"]
```

- [ ] **Step 2: Run focused tests and verify expected failures**

Run: `pytest tests/unit/test_retry.py tests/unit/test_worker.py -q`
Expected: collection fails because retry and worker modules are absent.

- [ ] **Step 3: Implement worker lifecycle**

`process_once()` MUST claim at most `worker_batch_size`, call `ensure_index()`, bulk upsert normalized payloads, mark successes indexed, schedule transient failures with `available_at`, and mark terminal failures with a sanitized error class truncated to 256 characters. Retry delays MUST use `asyncio.sleep` and stop promptly on cancellation. The loop MUST poll at `worker_poll_interval`, stop claims after shutdown begins, and leave unfinished jobs lease-recoverable.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unit/test_retry.py tests/unit/test_worker.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/worker.py app/domain/retry.py app/repositories/jobs.py tests/unit/test_retry.py tests/unit/test_worker.py
git commit -m "feat: add leased indexing worker"
```

### Task 8: Add application lifecycle and real Elasticsearch integration

**Files:**
- Modify: `app/main.py`
- Create: `tests/integration/test_search_elasticsearch.py`
- Create: `tests/api/conftest.py`
- Create: `tests/conftest.py`

**Interfaces:**
- FastAPI lifespan MUST call `ensure_index()` before readiness and close Elasticsearch, Redis, and SQLAlchemy resources during shutdown.
- Integration fixture returns real PostgreSQL and Elasticsearch adapters.

- [ ] **Step 1: Write the failing integration flow**

```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingest_worker_search_round_trip(real_stack) -> None:
    batch = await real_stack.ingest.ingest("gateway", [real_stack.record("evt-1")])
    assert batch.accepted_records == 1
    await real_stack.worker.process_once()
    result = await real_stack.search.search(real_stack.filters(q="timeout"))
    assert [hit.document_id for hit in result.hits] == [batch.document_ids[0]]
```

- [ ] **Step 2: Run the integration test and verify the expected fixture failure**

Run: `pytest -m integration tests/integration/test_search_elasticsearch.py -q`
Expected: fixture/import failure because the real stack fixture is not implemented.

- [ ] **Step 3: Implement Testcontainers fixtures and lifespan**

Start PostgreSQL and Elasticsearch 8.15 with security disabled only inside the test container, apply `001_init.sql`, wait for Elasticsearch cluster health, and use the real adapters. The test MUST index the same event twice and assert one searchable document, then assert a full-text hit, exact filters, and facets. Test teardown MUST stop containers.

- [ ] **Step 4: Run integration verification**

Run: `pytest -m integration tests/integration/test_search_elasticsearch.py -q`
Expected: PASS with Docker available.

- [ ] **Step 5: Commit**

```bash
git add app/main.py tests/conftest.py tests/api/conftest.py tests/integration/test_search_elasticsearch.py
git commit -m "test: cover real Elasticsearch round trip"
```

### Task 9: Add Docker Compose, smoke test, GitLab CI, and README

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `scripts/smoke.py`
- Create: `.gitlab-ci.yml`
- Create: `README.md`
- Modify: `.env.example`
- Create: `tests/test_smoke_contract.py`

**Interfaces:**
- Compose services: `api`, `worker`, `postgres`, `elasticsearch`, `redis`.
- `scripts/smoke.py` MUST perform ingest → worker wait → search, then check `/health`, `/ready`, and `/metrics`.
- GitLab stages MUST be `lint`, `unit`, `integration`, `build`, `smoke`.

- [ ] **Step 1: Write the smoke contract test**

```python
def test_smoke_script_uses_only_documented_endpoints() -> None:
    text = Path("scripts/smoke.py").read_text(encoding="utf-8")
    assert "/v1/ingest" in text
    assert "/v1/search" in text
    assert "localhost" in text
    assert "password" not in text.lower()
```

- [ ] **Step 2: Run the contract test and verify the expected missing-script failure**

Run: `pytest tests/test_smoke_contract.py -q`
Expected: failure because `scripts/smoke.py` does not exist.

- [ ] **Step 3: Implement container and CI files**

`Dockerfile` MUST use a slim Python 3.12 base, install from locked dependency metadata, copy only application/migrations, run as non-root, and launch with `python -m app.main`. Compose MUST pin image tags, use healthchecks, set local placeholder credentials via environment defaults, mount no host secrets, and start API/worker only after dependencies are healthy. `scripts/smoke.py` MUST use standard-library JSON parsing, retry boundedly on batch status, assert a returned search hit, and exit nonzero on any failed contract.

`.gitlab-ci.yml` MUST run:

```yaml
lint: ruff check app tests && mypy app && bandit -q -r app
unit: pytest -m 'not integration' --cov=app --cov-fail-under=80
integration: pytest -m integration -q
build: docker build -t logscope:$CI_COMMIT_SHA .
smoke: docker compose up -d && python scripts/smoke.py
```

The actual file MUST include service health waiting and `docker compose down -v` in `after_script`.

- [ ] **Step 4: Write README with exact, honest behavior**

README MUST document architecture, durable job semantics, API examples, setup, environment variables, Compose commands, worker retry/lease behavior, Elasticsearch mappings/query limits, testing commands, security notes, and explicit non-goals. It MUST NOT claim users, production deployment, throughput, latency, or benchmark results.

- [ ] **Step 5: Run local checks**

Run: `pytest tests/test_smoke_contract.py -q && ruff check app tests && mypy app && bandit -q -r app && docker compose config --quiet`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Dockerfile docker-compose.yml scripts/smoke.py .gitlab-ci.yml README.md .env.example tests/test_smoke_contract.py
git commit -m "docs(ci): add LogScope operations and verification"
```

### Task 10: Full verification, security review, and publication

**Files:**
- Modify: `README.md` only if verification commands or measured output are incorrect.
- Modify: profile repository README after publication.

- [ ] **Step 1: Run the complete verification matrix**

Run:

```bash
pytest -m 'not integration' --cov=app --cov-fail-under=80
ruff check app tests
mypy app
bandit -q -r app
docker compose config --quiet
pytest -m integration -q
docker compose up --build -d
python scripts/smoke.py
docker compose down -v
```

Expected: every command exits zero; integration and smoke require Docker.

- [ ] **Step 2: Review secrets and history**

Search tracked files and Git history for private keys, cloud access-key formats, unmasked credentials, `.env` files, raw log payload logging, arbitrary Elasticsearch DSL, and unsafe SQL interpolation. Expected results: no real secrets; only explicitly documented local placeholders in `.env.example`/Compose; no raw payload logging or unbounded client-controlled fields.

- [ ] **Step 3: Review final repository state**

Run: `git diff --check` and `git status --short`.
Expected: no whitespace errors and no uncommitted files except intentionally generated local artifacts, which MUST be removed before publication.

- [ ] **Step 4: Publish LogScope repository**

```bash
gh repo create denfry/logscope-python --public \
  --description "FastAPI log ingestion and Elasticsearch search service" \
  --source=. --remote=origin --push
```

- [ ] **Step 5: Update and publish profile entry**

Add a compact entry to the existing profile README after the selected-work section, link `https://github.com/denfry/logscope-python`, list `FastAPI`, `PostgreSQL`, `Elasticsearch`, `Redis`, `Docker`, and `GitLab CI`, and describe only implemented behavior. Commit with `docs: add LogScope to portfolio` and push the profile repository.

- [ ] **Step 6: Commit any final documentation correction**

If verification changes README content, commit it before pushing the repository; otherwise retain the staged commit history above. Never rewrite history or add co-author attribution.
