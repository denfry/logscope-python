from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas import LogLevel

_CURSOR_MAX_BYTES = 512
_CURSOR_KEYS = {"timestamp", "document_id"}


def _as_rfc3339(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an RFC3339 timezone")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class SearchFilters(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    q: str | None = Field(default=None, min_length=1, max_length=256)
    levels: list[LogLevel] = Field(default_factory=list, max_length=5)
    service: str | None = Field(default=None, min_length=1, max_length=128)
    environment: str | None = Field(default=None, min_length=1, max_length=64)
    source: str | None = Field(default=None, min_length=1, max_length=128)
    from_ts: datetime | None = None
    to_ts: datetime | None = None
    limit: int = Field(default=50, gt=0, le=100)
    cursor: str | None = Field(default=None, max_length=_CURSOR_MAX_BYTES)

    @field_validator("from_ts", "to_ts")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("timestamp must include an RFC3339 timezone")
        return value

    @field_validator("levels", mode="before")
    @classmethod
    def normalize_levels(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [part.strip().lower() for part in value.split(",") if part.strip()]
        return value

    @field_validator("q", "service", "environment", "source")
    @classmethod
    def reject_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("filter must not be blank")
        return value


def encode_cursor(timestamp: datetime, document_id: str) -> str:
    if not document_id or len(document_id) > 256:
        raise ValueError("document_id has invalid length")
    payload = json.dumps(
        {"timestamp": _as_rfc3339(timestamp), "document_id": document_id},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    if len(encoded.encode("ascii")) > _CURSOR_MAX_BYTES:
        raise ValueError("cursor exceeds maximum size")
    return encoded


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        cursor_bytes = cursor.encode("ascii")
    except (AttributeError, UnicodeError) as error:
        raise ValueError("cursor is invalid") from error
    if not cursor_bytes or len(cursor_bytes) > _CURSOR_MAX_BYTES:
        raise ValueError("cursor exceeds maximum size")
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError, binascii.Error) as error:
        raise ValueError("cursor is invalid") from error
    if not isinstance(payload, dict) or set(payload) != _CURSOR_KEYS:
        raise ValueError("cursor fields are invalid")
    timestamp_value = payload.get("timestamp")
    document_id = payload.get("document_id")
    if not isinstance(timestamp_value, str) or not isinstance(document_id, str):
        raise ValueError("cursor fields are invalid")
    if not document_id or len(document_id) > 256:
        raise ValueError("cursor document_id is invalid")
    try:
        timestamp = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
        _as_rfc3339(timestamp)
    except ValueError as error:
        raise ValueError("cursor timestamp is invalid") from error
    return timestamp.astimezone(UTC), document_id


def build_query(filters: SearchFilters) -> dict[str, Any]:
    must: list[dict[str, Any]] = []
    exact_filters: list[dict[str, Any]] = []
    if filters.q is not None:
        must.append(
            {
                "multi_match": {
                    "query": filters.q,
                    "fields": ["message", "service", "environment", "trace_id", "span_id"],
                }
            }
        )
    exact_filters.extend({"term": {"level": level}} for level in filters.levels)
    for field in ("service", "environment", "source"):
        value = getattr(filters, field)
        if value is not None:
            exact_filters.append({"term": {field: value}})
    if filters.from_ts is not None or filters.to_ts is not None:
        timestamp_range: dict[str, str] = {}
        if filters.from_ts is not None:
            timestamp_range["gte"] = _as_rfc3339(filters.from_ts)
        if filters.to_ts is not None:
            timestamp_range["lte"] = _as_rfc3339(filters.to_ts)
        exact_filters.append({"range": {"timestamp": timestamp_range}})
    query: dict[str, Any] = {
        "bool": {"must": must, "filter": exact_filters},
        "sort": [{"timestamp": "desc"}, {"document_id": "asc"}],
    }
    if filters.cursor is not None:
        timestamp, document_id = decode_cursor(filters.cursor)
        query["search_after"] = [_as_rfc3339(timestamp), document_id]
    return query
