from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.domain.normalization import deterministic_document_id, normalize_record
from app.schemas import LogRecordInput


def make_record(**overrides: object) -> LogRecordInput:
    payload: dict[str, object] = {
        "timestamp": "2026-08-31T10:00:00Z",
        "level": "INFO",
        "service": "api",
        "environment": "dev",
        "message": "request finished",
    }
    payload.update(overrides)
    return LogRecordInput.model_validate(payload)


def test_document_id_is_stable_for_same_source_and_event() -> None:
    record = make_record(event_id="evt-1")
    first = deterministic_document_id("gateway", record)
    second = deterministic_document_id("gateway", record)
    assert first == second


def test_document_id_changes_between_sources() -> None:
    record = make_record(event_id="evt-1")
    first = deterministic_document_id("gateway-a", record)
    second = deterministic_document_id("gateway-b", record)
    assert first != second


def test_normalization_lowercases_level_and_copies_trace_fields() -> None:
    record = make_record(level="WARN", trace_id="trace-1", span_id="span-1")
    normalized = normalize_record("stdout", record)
    assert normalized.level == "warn"
    assert normalized.trace_id == "trace-1"
    assert normalized.span_id == "span-1"
    assert normalized.timestamp == datetime(2026, 8, 31, 10, 0, tzinfo=UTC)


def test_normalization_rejects_unsupported_level_and_naive_timestamp() -> None:
    with pytest.raises(ValidationError):
        make_record(level="notice")
    with pytest.raises(ValidationError):
        make_record(timestamp="2026-08-31T10:00:00")


def test_normalization_rejects_deep_fields_and_oversize_message() -> None:
    deep_fields: dict[str, object] = {"a": {"b": {"c": {"d": {"e": {"f": "too deep"}}}}}}
    with pytest.raises(ValidationError):
        make_record(fields=deep_fields)
    with pytest.raises(ValidationError):
        make_record(message="x" * 4097)


def test_normalization_rejects_blank_source() -> None:
    with pytest.raises(ValueError, match="source"):
        normalize_record(" ", make_record())
