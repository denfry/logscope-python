from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.schemas import LogRecordInput


@dataclass(frozen=True, slots=True)
class NormalizedLogRecord:
    document_id: str
    source: str
    timestamp: datetime
    level: str
    service: str
    environment: str
    event_id: str | None
    trace_id: str | None
    span_id: str | None
    message: str
    fields: dict[str, Any]

    def as_payload(self) -> dict[str, Any]:
        return {
            "document_id": self.document_id,
            "source": self.source,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "level": self.level,
            "service": self.service,
            "environment": self.environment,
            "event_id": self.event_id,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "message": self.message,
            "fields": self.fields,
        }


def _normalized_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def deterministic_document_id(source: str, record: LogRecordInput) -> str:
    source_value = source.strip()
    if not source_value:
        raise ValueError("source must not be blank")
    if record.event_id:
        identity: dict[str, Any] = {"source": source_value, "event_id": record.event_id}
    else:
        identity = {
            "source": source_value,
            "timestamp": _normalized_timestamp(record.timestamp),
            "level": record.level,
            "service": record.service,
            "environment": record.environment,
            "message": record.message,
            "fields": record.fields,
            "trace_id": record.trace_id,
            "span_id": record.span_id,
        }
    canonical = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_record(source: str, record: LogRecordInput) -> NormalizedLogRecord:
    source_value = source.strip()
    if not source_value:
        raise ValueError("source must not be blank")
    document_id = deterministic_document_id(source_value, record)
    timestamp = record.timestamp.astimezone(UTC)
    return NormalizedLogRecord(
        document_id=document_id,
        source=source_value,
        timestamp=timestamp,
        level=record.level,
        service=record.service,
        environment=record.environment,
        event_id=record.event_id,
        trace_id=record.trace_id,
        span_id=record.span_id,
        message=record.message,
        fields=record.fields,
    )
