from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from app.adapters.elasticsearch import SearchHit, SearchPage
from app.domain.query import SearchFilters, encode_cursor


class SearchAdapter(Protocol):
    async def search(self, filters: SearchFilters) -> SearchPage: ...

    async def facets(self, filters: SearchFilters) -> dict[str, list[dict[str, Any]]]: ...


class SearchValidationError(ValueError):
    pass


class SearchService:
    def __init__(self, adapter: SearchAdapter, max_range_seconds: int) -> None:
        self._adapter = adapter
        self._max_range_seconds = max_range_seconds

    def _validate_filters(self, filters: SearchFilters) -> None:
        if filters.from_ts is None or filters.to_ts is None:
            return
        if filters.from_ts >= filters.to_ts:
            raise SearchValidationError("from_ts must be earlier than to_ts")
        duration = (filters.to_ts - filters.from_ts).total_seconds()
        if duration > self._max_range_seconds:
            raise SearchValidationError("search range exceeds the configured maximum")

    async def search(self, filters: SearchFilters) -> SearchResult:
        self._validate_filters(filters)
        page = await self._adapter.search(filters)
        hits = [_document_for_response(hit) for hit in page.hits]
        next_cursor = _next_cursor(page.hits)
        return SearchResult(total=page.total, hits=hits, next_cursor=next_cursor)

    async def facets(self, filters: SearchFilters) -> dict[str, list[dict[str, Any]]]:
        self._validate_filters(filters)
        return await self._adapter.facets(filters)


class SearchResult:
    def __init__(self, total: int, hits: list[dict[str, Any]], next_cursor: str | None) -> None:
        self.total = total
        self.hits = hits
        self.next_cursor = next_cursor


def _document_for_response(hit: SearchHit) -> dict[str, Any]:
    return {
        "document_id": hit.document_id,
        **hit.source,
    }


def _next_cursor(hits: list[SearchHit]) -> str | None:
    if not hits:
        return None
    sort = hits[-1].sort
    if len(sort) < 2:
        return None
    timestamp_value, document_id = sort[0], sort[1]
    if isinstance(timestamp_value, datetime):
        timestamp = timestamp_value
    elif isinstance(timestamp_value, str):
        timestamp = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
    else:
        return None
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        return None
    return encode_cursor(timestamp.astimezone(UTC), str(document_id))
