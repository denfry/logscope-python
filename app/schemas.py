from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

LogLevel = Literal["debug", "info", "warn", "error", "fatal"]


def _field_depth(value: Any, depth: int = 0) -> int:
    if not isinstance(value, dict):
        if isinstance(value, list):
            return max((_field_depth(item, depth) for item in value), default=depth)
        return depth
    return max((_field_depth(child, depth + 1) for child in value.values()), default=depth)


class LogRecordInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    timestamp: datetime
    level: LogLevel
    service: str = Field(min_length=1, max_length=128)
    environment: str = Field(min_length=1, max_length=64)
    event_id: str | None = Field(default=None, min_length=1, max_length=256)
    trace_id: str | None = Field(default=None, min_length=1, max_length=256)
    span_id: str | None = Field(default=None, min_length=1, max_length=256)
    message: str = Field(min_length=1, max_length=4096)
    fields: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include an RFC3339 timezone")
        return value

    @field_validator("level", mode="before")
    @classmethod
    def normalize_level(cls, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        normalized = value.strip().lower()
        if normalized == "warning":
            normalized = "warn"
        return normalized

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, value: dict[str, Any]) -> dict[str, Any]:
        if _field_depth(value) > 5:
            raise ValueError("fields nesting exceeds five levels")
        return value
