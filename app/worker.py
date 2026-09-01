from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.elasticsearch import BulkItemResult, ElasticsearchAdapter
from app.config import Settings
from app.db import apply_migrations, close_engine, create_engine, create_session_factory
from app.domain.retry import RetryPolicy
from app.repositories.jobs import JobRepository


class WorkerRepository(Protocol):
    async def claim_batch(
        self, session: AsyncSession, limit: int, lease_seconds: int
    ) -> list[Any]: ...

    async def mark_indexed(self, session: AsyncSession, job_id: int) -> None: ...

    async def mark_retry(
        self,
        session: AsyncSession,
        job_id: int,
        available_at: datetime,
        error_class: str,
    ) -> None: ...

    async def mark_failed(self, session: AsyncSession, job_id: int, error_class: str) -> None: ...


class WorkerElasticsearch(Protocol):
    async def ensure_index(self) -> None: ...

    async def bulk_upsert(self, documents: list[dict[str, Any]]) -> list[BulkItemResult]: ...


class WorkerRetryPolicy(Protocol):
    def next_delay(self, attempt: int) -> float: ...


_ERROR_CLASS_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def sanitize_error_class(error_class: str) -> str:
    sanitized = _ERROR_CLASS_RE.sub("_", error_class).strip("_")
    return (sanitized or "indexing_error")[:256]


class Worker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        repository: WorkerRepository,
        elasticsearch: WorkerElasticsearch,
        batch_size: int,
        lease_seconds: int,
        max_attempts: int,
        retry_policy: WorkerRetryPolicy,
        poll_interval: float = 0.5,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._elasticsearch = elasticsearch
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._retry_policy = retry_policy
        self._poll_interval = poll_interval

    async def process_once(self) -> int:
        async with self._session_factory() as session:
            jobs = await self._repository.claim_batch(
                session, self._batch_size, self._lease_seconds
            )
            if not jobs:
                return 0
            await self._elasticsearch.ensure_index()
            results = await self._elasticsearch.bulk_upsert([job.payload for job in jobs])
            result_by_document_id = {result.document_id: result for result in results}
            for job in jobs:
                result = result_by_document_id.get(job.document_id)
                if result is None:
                    result = BulkItemResult(job.document_id, False, True, "missing_bulk_result")
                await self._transition_job(session, job, result)
            return len(jobs)

    async def _transition_job(
        self, session: AsyncSession, job: Any, result: BulkItemResult
    ) -> None:
        if result.indexed:
            await self._repository.mark_indexed(session, job.id)
            return
        error_class = sanitize_error_class(result.error_class or "indexing_error")
        if result.transient and job.attempts < self._max_attempts:
            delay = self._retry_policy.next_delay(job.attempts)
            await self._repository.mark_retry(
                session,
                job.id,
                datetime.now(UTC) + timedelta(seconds=delay),
                error_class,
            )
            return
        await self._repository.mark_failed(session, job.id, error_class)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.process_once()
            if stop_event.is_set():
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval)
            except TimeoutError:
                continue


async def run_worker_process(settings: Settings) -> None:
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    repository = JobRepository()
    elasticsearch = ElasticsearchAdapter(settings)
    await apply_migrations(engine)
    await elasticsearch.ensure_index()
    worker = Worker(
        session_factory=session_factory,
        repository=repository,
        elasticsearch=elasticsearch,
        batch_size=settings.worker_batch_size,
        lease_seconds=settings.worker_lease_seconds,
        max_attempts=settings.worker_max_attempts,
        retry_policy=RetryPolicy(),
        poll_interval=settings.worker_poll_interval,
    )
    try:
        await worker.run(asyncio.Event())
    finally:
        await elasticsearch.close()
        await close_engine(engine)
