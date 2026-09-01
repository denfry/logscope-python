from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class BatchStatus(StrEnum):
    PENDING = "pending"
    PARTIAL = "partial"
    COMPLETED = "completed"
    FAILED = "failed"


class JobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    INDEXED = "indexed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class BatchRecord:
    id: UUID
    source: str
    total_records: int
    indexed_records: int
    failed_records: int
    status: BatchStatus
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class JobRecord:
    id: int
    batch_id: UUID
    document_id: str
    payload: dict[str, Any]
    status: JobStatus
    attempts: int
    available_at: datetime
    locked_at: datetime | None
    last_error: str | None
