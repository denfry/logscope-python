from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from app.models import BatchRecord, BatchStatus
from app.schemas import LogRecordInput
from app.services.ingest import IngestService


class FakeSession:
    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.created_job_document_ids: list[str] = []
        self.batch_id = uuid4()

    async def __aenter__(self) -> FakeUnitOfWork:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def create_batch(
        self, source: str, records: list[tuple[str, dict[str, object]]]
    ) -> BatchRecord:
        self.created_job_document_ids = [document_id for document_id, _ in records]
        now = datetime.now(UTC)
        return BatchRecord(
            id=self.batch_id,
            source=source,
            total_records=len(records),
            indexed_records=0,
            failed_records=0,
            status=BatchStatus.PENDING,
            created_at=now,
            updated_at=now,
        )


class FakeUnitOfWorkFactory:
    def __init__(self) -> None:
        self.uow = FakeUnitOfWork()

    def __call__(self) -> FakeUnitOfWork:
        return self.uow


def make_record(event_id: str) -> LogRecordInput:
    return LogRecordInput.model_validate(
        {
            "event_id": event_id,
            "timestamp": "2026-08-31T10:00:00Z",
            "level": "INFO",
            "service": "api",
            "environment": "dev",
            "message": "request finished",
        }
    )


@pytest.mark.asyncio
async def test_ingest_coalesces_duplicate_document_ids_before_transaction() -> None:
    fake_uow_factory = FakeUnitOfWorkFactory()
    service = IngestService(fake_uow_factory)
    result = await service.ingest("gateway", [make_record("evt-1"), make_record("evt-1")])
    assert result.accepted_records == 1
    assert fake_uow_factory.uow.created_job_document_ids == [result.document_ids[0]]
    assert UUID(result.batch_id) == fake_uow_factory.uow.batch_id


@pytest.mark.asyncio
async def test_ingest_preserves_distinct_records() -> None:
    fake_uow_factory = FakeUnitOfWorkFactory()
    service = IngestService(fake_uow_factory)
    result = await service.ingest("gateway", [make_record("evt-1"), make_record("evt-2")])
    assert result.accepted_records == 2
    assert len(result.document_ids) == 2
