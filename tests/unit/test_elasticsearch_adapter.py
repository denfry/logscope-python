from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.adapters.elasticsearch import ElasticsearchAdapter, SearchDependencyError
from app.config import Settings
from app.domain.query import SearchFilters, encode_cursor


class FakeIndices:
    def __init__(self, exists: bool) -> None:
        self.exists_value = exists
        self.created: dict[str, object] | None = None

    async def exists(self, **_kwargs: object) -> bool:
        return self.exists_value

    async def create(self, **kwargs: object) -> None:
        self.created = kwargs


class FakeElasticsearchClient:
    def __init__(self, *, index_exists: bool = False) -> None:
        self.indices = FakeIndices(index_exists)
        self.bulk_request: dict[str, object] | None = None
        self.bulk_response: dict[str, object] = {"items": []}
        self.bulk_error: Exception | None = None
        self.search_response: dict[str, object] = {"hits": {"total": 0, "hits": []}}
        self.search_error: Exception | None = None
        self.search_requests: list[dict[str, object]] = []
        self.closed = False

    async def bulk(self, **kwargs: object) -> dict[str, object]:
        self.bulk_request = kwargs
        if self.bulk_error is not None:
            raise self.bulk_error
        return self.bulk_response

    async def search(self, **kwargs: object) -> dict[str, object]:
        self.search_requests.append(kwargs)
        if self.search_error is not None:
            raise self.search_error
        return self.search_response

    async def close(self) -> None:
        self.closed = True


def settings() -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://app:app@localhost:5432/logscope",
        ELASTICSEARCH_URL="http://localhost:9200",
    )


@pytest.mark.asyncio
async def test_ensure_index_creates_explicit_mapping() -> None:
    client = FakeElasticsearchClient()
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    await adapter.ensure_index()
    assert client.indices.created is not None
    assert client.indices.created["index"] == "logscope-logs-v1"
    assert client.indices.created["mappings"]


@pytest.mark.asyncio
async def test_ensure_index_is_idempotent() -> None:
    client = FakeElasticsearchClient(index_exists=True)
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    await adapter.ensure_index()
    assert client.indices.created is None


@pytest.mark.asyncio
async def test_bulk_upsert_classifies_item_failures() -> None:
    client = FakeElasticsearchClient()
    client.bulk_response = {
        "items": [
            {"index": {"status": 201}},
            {"index": {"status": 429, "error": {"type": "too_many_requests"}}},
            {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
        ]
    }
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    documents = [
        {"document_id": "ok"},
        {"document_id": "retry"},
        {"document_id": "failed"},
    ]
    results = await adapter.bulk_upsert(documents)
    assert [result.indexed for result in results] == [True, False, False]
    assert [result.transient for result in results] == [False, True, False]
    assert client.bulk_request is not None


@pytest.mark.asyncio
async def test_search_and_facets_map_only_documented_values() -> None:
    client = FakeElasticsearchClient()
    client.search_response = {
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "hits": [
                {
                    "_id": "doc-1",
                    "_source": {
                        "document_id": "doc-1",
                        "timestamp": "2026-08-31T10:00:00Z",
                        "level": "error",
                        "service": "api",
                        "message": "timeout",
                        "secret": "discarded",
                    },
                    "sort": ["2026-08-31T10:00:00Z", "doc-1"],
                }
            ],
        },
        "aggregations": {
            "levels": {"buckets": [{"key": "error", "doc_count": 1}]},
            "services": {"buckets": []},
            "environments": {"buckets": []},
        },
    }
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    page = await adapter.search(SearchFilters(q="timeout"))
    assert page.total == 1
    assert page.hits[0].source["message"] == "timeout"
    assert "secret" not in page.hits[0].source
    facets = await adapter.facets(SearchFilters(q="timeout"))
    assert facets["levels"][0]["key"] == "error"
    await adapter.close()
    assert client.closed


@pytest.mark.asyncio
async def test_bulk_upsert_handles_empty_and_missing_items() -> None:
    client = FakeElasticsearchClient()
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    assert await adapter.bulk_upsert([]) == []
    results = await adapter.bulk_upsert([{"document_id": "missing"}])
    assert results[0].transient is True
    assert results[0].error_class == "elasticsearch_response"


@pytest.mark.asyncio
async def test_bulk_upsert_maps_request_failure_as_transient() -> None:
    client = FakeElasticsearchClient()
    client.bulk_error = OSError("connection lost")
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    results = await adapter.bulk_upsert([{"document_id": "retry"}])
    assert results[0].transient is True
    assert results[0].error_class == "elasticsearch_request"


@pytest.mark.asyncio
async def test_search_rejects_backend_error_without_raw_details() -> None:
    client = FakeElasticsearchClient()
    client.search_error = OSError("private backend detail")
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    with pytest.raises(SearchDependencyError, match="unavailable"):
        await adapter.search(SearchFilters(q="timeout"))


@pytest.mark.asyncio
async def test_search_passes_validated_search_after() -> None:
    client = FakeElasticsearchClient()
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    cursor = encode_cursor(datetime(2026, 8, 31, tzinfo=UTC), "doc-1")
    await adapter.search(SearchFilters(cursor=cursor))
    assert client.search_requests[-1]["search_after"] == ["2026-08-31T00:00:00Z", "doc-1"]


@pytest.mark.asyncio
async def test_facets_reject_backend_error_without_raw_details() -> None:
    client = FakeElasticsearchClient()
    client.search_error = OSError("private backend detail")
    adapter = ElasticsearchAdapter(settings(), client=client)  # type: ignore[arg-type]
    with pytest.raises(SearchDependencyError, match="unavailable"):
        await adapter.facets(SearchFilters())
