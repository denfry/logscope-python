from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.normalization import normalize_record
from app.models import BatchRecord
from app.repositories.jobs import JobRepository, RecordPayload
from app.schemas import LogRecordInput


class IngestUnitOfWork(Protocol):
    async def __aenter__(self) -> IngestUnitOfWork: ...

    async def __aexit__(self, *args: object) -> None: ...

    async def create_batch(self, source: str, records: list[RecordPayload]) -> BatchRecord: ...

    async def get_batch(self, batch_id: UUID) -> BatchRecord | None: ...


class DatabaseIngestUnitOfWork:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        repository: JobRepository,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> DatabaseIngestUnitOfWork:
        self._session = self._session_factory()
        await self._session.__aenter__()
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._session is not None:
            await self._session.__aexit__(*args)

    def _require_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("unit of work is not active")
        return self._session

    async def create_batch(self, source: str, records: list[RecordPayload]) -> BatchRecord:
        return await self._repository.create_batch(self._require_session(), source, records)

    async def get_batch(self, batch_id: UUID) -> BatchRecord | None:
        return await self._repository.get_batch(self._require_session(), batch_id)


@dataclass(frozen=True, slots=True)
class IngestResult:
    batch_id: str
    accepted_records: int
    document_ids: list[str]


@dataclass(frozen=True, slots=True)
class BatchResult:
    batch_id: str
    source: str
    total_records: int
    indexed_records: int
    failed_records: int
    status: str

    @classmethod
    def from_record(cls, batch: BatchRecord) -> BatchResult:
        return cls(
            batch_id=str(batch.id),
            source=batch.source,
            total_records=batch.total_records,
            indexed_records=batch.indexed_records,
            failed_records=batch.failed_records,
            status=batch.status.value,
        )


UowFactory = Callable[[], AbstractAsyncContextManager[IngestUnitOfWork]]


class IngestService:
    def __init__(self, uow_factory: UowFactory) -> None:
        self._uow_factory = uow_factory

    async def ingest(self, source: str, records: list[LogRecordInput]) -> IngestResult:
        normalized_by_id: dict[str, RecordPayload] = {}
        for record in records:
            normalized = normalize_record(source, record)
            normalized_by_id[normalized.document_id] = (
                normalized.document_id,
                normalized.as_payload(),
            )
        async with self._uow_factory() as uow:
            batch = await uow.create_batch(source, list(normalized_by_id.values()))
        return IngestResult(
            batch_id=str(batch.id),
            accepted_records=batch.total_records,
            document_ids=list(normalized_by_id),
        )

    async def get_batch(self, batch_id: UUID) -> BatchResult | None:
        async with self._uow_factory() as uow:
            batch = await uow.get_batch(batch_id)
        return None if batch is None else BatchResult.from_record(batch)
