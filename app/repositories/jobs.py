from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, TypeVar, cast
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BatchRecord, BatchStatus, JobRecord, JobStatus

RecordPayload = tuple[str, Mapping[str, Any]]
_ResultT = TypeVar("_ResultT")


async def _in_transaction(
    session: AsyncSession,
    operation: Callable[[], Awaitable[_ResultT]],
) -> _ResultT:
    if session.in_transaction():
        return await operation()
    async with session.begin():
        return await operation()


def derive_batch_status(*, total: int, indexed: int, failed: int) -> BatchStatus:
    if indexed == total:
        return BatchStatus.COMPLETED
    if failed == total:
        return BatchStatus.FAILED
    if indexed + failed == total:
        return BatchStatus.PARTIAL
    return BatchStatus.PENDING


def _batch_from_row(row: Mapping[str, Any]) -> BatchRecord:
    return BatchRecord(
        id=row["id"],
        source=row["source"],
        total_records=row["total_records"],
        indexed_records=row["indexed_records"],
        failed_records=row["failed_records"],
        status=BatchStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _job_from_row(row: Mapping[str, Any]) -> JobRecord:
    return JobRecord(
        id=row["id"],
        batch_id=row["batch_id"],
        document_id=row["document_id"],
        payload=row["payload"],
        status=JobStatus(row["status"]),
        attempts=row["attempts"],
        available_at=row["available_at"],
        locked_at=row["locked_at"],
        last_error=row["last_error"],
    )


class JobRepository:
    async def create_batch(
        self,
        session: AsyncSession,
        source: str,
        records: Sequence[RecordPayload],
    ) -> BatchRecord:
        batch_id = uuid4()
        unique_records = dict(records)

        async def operation() -> BatchRecord:
            await session.execute(
                text(
                    """
                    INSERT INTO ingestion_batches (id, source, total_records)
                    VALUES (:id, :source, :total_records)
                    """
                ),
                {"id": batch_id, "source": source, "total_records": len(unique_records)},
            )
            for document_id, payload in unique_records.items():
                await session.execute(
                    text(
                        """
                        INSERT INTO index_jobs (batch_id, document_id, payload)
                        VALUES (:batch_id, :document_id, CAST(:payload AS jsonb))
                        ON CONFLICT (batch_id, document_id) DO NOTHING
                        """
                    ),
                    {
                        "batch_id": batch_id,
                        "document_id": document_id,
                        "payload": json.dumps(payload, separators=(",", ":")),
                    },
                )
            batch = await self.get_batch(session, batch_id)
            if batch is None:
                raise RuntimeError("created batch was not readable in the same transaction")
            return batch

        return await _in_transaction(session, operation)

    async def claim_batch(
        self,
        session: AsyncSession,
        limit: int,
        lease_seconds: int,
    ) -> list[JobRecord]:
        async def operation() -> list[JobRecord]:
            await session.execute(
                text(
                    """
                    UPDATE index_jobs
                    SET status = 'pending', locked_at = NULL, updated_at = now()
                    WHERE status = 'processing'
                      AND locked_at IS NOT NULL
                      AND locked_at < now() - make_interval(secs => :lease_seconds)
                    """
                ),
                {"lease_seconds": lease_seconds},
            )
            result = await session.execute(
                text(
                    """
                    SELECT id, batch_id, document_id, payload, status, attempts,
                           available_at, locked_at, last_error
                    FROM index_jobs
                    WHERE status IN ('pending', 'processing')
                      AND available_at <= now()
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            )
            claimed: list[JobRecord] = []
            for row in result.mappings():
                updated = await session.execute(
                    text(
                        """
                        UPDATE index_jobs
                        SET status = 'processing',
                            attempts = attempts + 1,
                            locked_at = now(),
                            available_at = now() + make_interval(secs => :lease_seconds),
                            updated_at = now()
                        WHERE id = :id
                        RETURNING id, batch_id, document_id, payload, status, attempts,
                                  available_at, locked_at, last_error
                        """
                    ),
                    {"id": row["id"], "lease_seconds": lease_seconds},
                )
                updated_row = updated.mappings().one()
                claimed.append(_job_from_row(cast(Mapping[str, Any], updated_row)))
            return claimed

        return await _in_transaction(session, operation)

    async def mark_indexed(self, session: AsyncSession, job_id: int) -> None:
        await self._mark_terminal(session, job_id, JobStatus.INDEXED, None)

    async def mark_retry(
        self,
        session: AsyncSession,
        job_id: int,
        available_at: datetime,
        error_class: str,
    ) -> None:
        await self._mark_terminal(session, job_id, JobStatus.PENDING, error_class, available_at)

    async def mark_failed(self, session: AsyncSession, job_id: int, error_class: str) -> None:
        await self._mark_terminal(session, job_id, JobStatus.FAILED, error_class)

    async def _mark_terminal(
        self,
        session: AsyncSession,
        job_id: int,
        status: JobStatus,
        error_class: str | None,
        available_at: datetime | None = None,
    ) -> None:
        async def operation() -> None:
            result = await session.execute(
                text(
                    """
                    UPDATE index_jobs
                    SET status = :status,
                        locked_at = NULL,
                        available_at = COALESCE(:available_at, available_at),
                        last_error = :last_error,
                        updated_at = now()
                    WHERE id = :job_id
                    RETURNING batch_id
                    """
                ),
                {
                    "status": status.value,
                    "available_at": available_at,
                    "last_error": error_class,
                    "job_id": job_id,
                },
            )
            row = result.mappings().first()
            if row is not None:
                await self._refresh_batch(session, row["batch_id"])

        await _in_transaction(session, operation)

    async def _refresh_batch(self, session: AsyncSession, batch_id: UUID) -> None:
        await session.execute(
            text(
                """
                WITH counts AS (
                    SELECT
                        COUNT(*) FILTER (WHERE status = 'indexed') AS indexed_records,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed_records
                    FROM index_jobs
                    WHERE batch_id = :batch_id
                )
                UPDATE ingestion_batches AS batches
                SET indexed_records = counts.indexed_records,
                    failed_records = counts.failed_records,
                    status = CASE
                        WHEN counts.indexed_records = batches.total_records THEN 'completed'
                        WHEN counts.failed_records = batches.total_records THEN 'failed'
                        WHEN counts.indexed_records + counts.failed_records = batches.total_records
                            THEN 'partial'
                        ELSE 'pending'
                    END,
                    updated_at = now()
                FROM counts
                WHERE batches.id = :batch_id
                """
            ),
            {"batch_id": batch_id},
        )

    async def get_batch(self, session: AsyncSession, batch_id: UUID) -> BatchRecord | None:
        result = await session.execute(
            text(
                """
                SELECT id, source, total_records, indexed_records, failed_records,
                       status, created_at, updated_at
                FROM ingestion_batches
                WHERE id = :batch_id
                """
            ),
            {"batch_id": batch_id},
        )
        row = result.mappings().first()
        return None if row is None else _batch_from_row(cast(Mapping[str, Any], row))

    async def count_jobs(self, session: AsyncSession, batch_id: UUID) -> int:
        result = await session.execute(
            text("SELECT COUNT(*) AS count FROM index_jobs WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )
        return int(result.scalar_one())
