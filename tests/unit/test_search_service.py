from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.adapters.elasticsearch import SearchHit, SearchPage
from app.domain.query import SearchFilters
from app.services.search import SearchService, SearchValidationError


class FakeSearchAdapter:
    def __init__(self) -> None:
        self.filters: SearchFilters | None = None

    async def search(self, filters: SearchFilters) -> SearchPage:
        self.filters = filters
        return SearchPage(
            hits=[
                SearchHit(
                    document_id="doc-1",
                    source={
                        "document_id": "doc-1",
                        "timestamp": "2026-08-31T10:00:00Z",
                        "level": "error",
                        "service": "api",
                        "environment": "prod",
                        "message": "timeout",
                        "fields": {"status": 504},
                    },
                    sort=["2026-08-31T10:00:00Z", "doc-1"],
                )
            ],
            total=1,
        )

    async def facets(self, filters: SearchFilters) -> dict[str, list[dict[str, object]]]:
        self.filters = filters
        return {"levels": [{"key": "error", "doc_count": 1}]}


@pytest.mark.asyncio
async def test_search_maps_document_and_returns_next_cursor() -> None:
    adapter = FakeSearchAdapter()
    service = SearchService(adapter, max_range_seconds=31 * 24 * 60 * 60)
    result = await service.search(SearchFilters(q="timeout"))
    assert result.total == 1
    assert result.hits[0]["document_id"] == "doc-1"
    assert result.next_cursor is not None
    assert "fields" in result.hits[0]


@pytest.mark.asyncio
async def test_search_rejects_reversed_and_oversized_ranges() -> None:
    adapter = FakeSearchAdapter()
    service = SearchService(adapter, max_range_seconds=31 * 24 * 60 * 60)
    with pytest.raises(SearchValidationError):
        await service.search(
            SearchFilters(
                from_ts=datetime(2026, 8, 31, tzinfo=UTC),
                to_ts=datetime(2026, 8, 30, tzinfo=UTC),
            )
        )
    with pytest.raises(SearchValidationError):
        await service.search(
            SearchFilters(
                from_ts=datetime(2026, 1, 1, tzinfo=UTC),
                to_ts=datetime(2026, 3, 1, tzinfo=UTC),
            )
        )


@pytest.mark.asyncio
async def test_facets_use_typed_filters() -> None:
    adapter = FakeSearchAdapter()
    service = SearchService(adapter, max_range_seconds=31 * 24 * 60 * 60)
    result = await service.facets(SearchFilters(service="api"))
    assert result["levels"][0]["key"] == "error"
    assert adapter.filters is not None
    assert adapter.filters.service == "api"
