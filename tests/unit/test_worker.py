from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.adapters.elasticsearch import BulkItemResult
from app.models import JobRecord, JobStatus
from app.worker import Worker


class FakeSession:
    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeJobs:
    def __init__(self, jobs: list[JobRecord]) -> None:
        self.jobs = jobs
        self.indexed_ids: list[int] = []
        self.failed_ids: list[int] = []
        self.retry_ids: list[int] = []

    async def claim_batch(
        self, _session: object, _limit: int, _lease_seconds: int
    ) -> list[JobRecord]:
        return self.jobs

    async def mark_indexed(self, _session: object, job_id: int) -> None:
        self.indexed_ids.append(job_id)

    async def mark_failed(self, _session: object, job_id: int, _error_class: str) -> None:
        self.failed_ids.append(job_id)

    async def mark_retry(
        self, _session: object, job_id: int, _available_at: datetime, _error_class: str
    ) -> None:
        self.retry_ids.append(job_id)


class FakeElasticsearch:
    def __init__(self, result: list[BulkItemResult]) -> None:
        self.result = result
        self.ensure_index_calls = 0

    async def ensure_index(self) -> None:
        self.ensure_index_calls += 1

    async def bulk_upsert(self, _documents: list[dict[str, object]]) -> list[BulkItemResult]:
        return self.result


def make_job(job_id: int, attempts: int = 1) -> JobRecord:
    return JobRecord(
        id=job_id,
        batch_id=__import__("uuid").uuid4(),
        document_id=f"job-{job_id}",
        payload={"document_id": f"job-{job_id}", "message": "timeout"},
        status=JobStatus.PROCESSING,
        attempts=attempts,
        available_at=datetime.now(UTC),
        locked_at=datetime.now(UTC),
        last_error=None,
    )


def make_worker(jobs: FakeJobs, elasticsearch: FakeElasticsearch, max_attempts: int = 2) -> Worker:
    return Worker(
        session_factory=lambda: FakeSession(),  # type: ignore[arg-type]
        repository=jobs,  # type: ignore[arg-type]
        elasticsearch=elasticsearch,  # type: ignore[arg-type]
        batch_size=10,
        lease_seconds=60,
        max_attempts=max_attempts,
        retry_policy=SimpleNamespace(next_delay=lambda _attempt: 0.01),  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_worker_marks_successful_bulk_item_indexed() -> None:
    jobs = FakeJobs([make_job(1)])
    elasticsearch = FakeElasticsearch([BulkItemResult("job-1", True, False)])
    worker = make_worker(jobs, elasticsearch)
    assert await worker.process_once() == 1
    assert jobs.indexed_ids == [1]
    assert elasticsearch.ensure_index_calls == 1


@pytest.mark.asyncio
async def test_worker_marks_terminal_bulk_failure_failed() -> None:
    jobs = FakeJobs([make_job(1)])
    elasticsearch = FakeElasticsearch([BulkItemResult("job-1", False, False, "mapping_error")])
    worker = make_worker(jobs, elasticsearch, max_attempts=2)
    assert await worker.process_once() == 1
    assert jobs.failed_ids == [1]
    assert jobs.retry_ids == []


@pytest.mark.asyncio
async def test_worker_retries_transient_failure_until_max_attempts() -> None:
    jobs = FakeJobs([make_job(1, attempts=1)])
    elasticsearch = FakeElasticsearch([BulkItemResult("job-1", False, True, "timeout")])
    worker = make_worker(jobs, elasticsearch, max_attempts=2)
    await worker.process_once()
    assert jobs.retry_ids == [1]

    jobs.jobs[0] = make_job(1, attempts=2)
    await worker.process_once()
    assert jobs.failed_ids == [1]
