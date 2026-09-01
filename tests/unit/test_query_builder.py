from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.domain.query import SearchFilters, build_query


def test_query_combines_full_text_and_exact_filters() -> None:
    query = build_query(SearchFilters(q="timeout", levels=["error"], service="api"))
    clauses = query["bool"]["must"]
    assert any("multi_match" in clause for clause in clauses)
    assert {"term": {"level": "error"}} in query["bool"]["filter"]
    assert {"term": {"service": "api"}} in query["bool"]["filter"]


def test_query_ignores_arbitrary_field_names() -> None:
    filters = SearchFilters.model_validate({"q": "x", "field": "__source"})
    assert not hasattr(filters, "field")
    assert "__source" not in str(build_query(filters))


def test_query_adds_time_range_sort_and_cursor() -> None:
    filters = SearchFilters(
        from_ts=datetime(2026, 8, 1, tzinfo=UTC),
        to_ts=datetime(2026, 8, 31, tzinfo=UTC),
        cursor="eyJkb2N1bWVudF9pZCI6ImRvYy0xIiwidGltZXN0YW1wIjoiMjAyNi0wOC0zMVQxMDowMDowMFoifQ",
    )
    query = build_query(filters)
    assert query["bool"]["filter"][-1] == {
        "range": {
            "timestamp": {
                "gte": "2026-08-01T00:00:00Z",
                "lte": "2026-08-31T00:00:00Z",
            }
        }
    }
    assert query["sort"] == [{"timestamp": "desc"}, {"document_id": "asc"}]
    assert query["search_after"] == ["2026-08-31T10:00:00Z", "doc-1"]


def test_query_rejects_oversized_filter_values() -> None:
    with pytest.raises(ValidationError):
        SearchFilters(q="x" * 257)
    with pytest.raises(ValidationError):
        SearchFilters(levels=["error"] * 6)
