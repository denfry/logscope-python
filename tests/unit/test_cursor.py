from datetime import UTC, datetime

import pytest

from app.domain.query import decode_cursor, encode_cursor


def test_cursor_round_trips_timestamp_and_document_id() -> None:
    timestamp = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)
    cursor = encode_cursor(timestamp, "doc-1")
    assert decode_cursor(cursor) == (timestamp, "doc-1")
    assert len(cursor) <= 512


@pytest.mark.parametrize(
    "cursor",
    ["not-base64", "e30", "eyJ0aW1lc3RhbXAiOiJub3QtYS10aW1lc3RhbXAiLCJkb2N1bWVudF9pZCI6ImRvYyJ9"],
)
def test_cursor_rejects_invalid_values(cursor: str) -> None:
    with pytest.raises(ValueError):
        decode_cursor(cursor)


def test_cursor_rejects_extra_keys_and_oversized_values() -> None:
    timestamp = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)
    with pytest.raises(ValueError):
        decode_cursor(encode_cursor(timestamp, "x") + "extra")
    with pytest.raises(ValueError):
        encode_cursor(timestamp, "x" * 513)
