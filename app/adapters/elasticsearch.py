from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ApiError

from app.config import Settings
from app.domain.query import SearchFilters, build_query

INDEX_MAPPINGS: dict[str, Any] = {
    "properties": {
        "document_id": {"type": "keyword"},
        "source": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "level": {"type": "keyword"},
        "service": {"type": "keyword"},
        "environment": {"type": "keyword"},
        "event_id": {"type": "keyword"},
        "trace_id": {"type": "keyword"},
        "span_id": {"type": "keyword"},
        "message": {"type": "text"},
        "fields": {"type": "object", "dynamic": True},
    }
}


class SearchDependencyError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BulkItemResult:
    document_id: str
    indexed: bool
    transient: bool
    error_class: str | None = None


@dataclass(frozen=True, slots=True)
class SearchHit:
    document_id: str
    source: dict[str, Any]
    sort: list[Any]


@dataclass(frozen=True, slots=True)
class SearchPage:
    hits: list[SearchHit]
    total: int


class ElasticsearchAdapter:
    def __init__(self, settings: Settings, client: AsyncElasticsearch | None = None) -> None:
        self.index = settings.elasticsearch_index
        if client is not None:
            self.client = client
            return
        kwargs: dict[str, Any] = {}
        if settings.elasticsearch_username and settings.elasticsearch_password:
            kwargs["basic_auth"] = (
                settings.elasticsearch_username,
                settings.elasticsearch_password,
            )
        self.client = AsyncElasticsearch(settings.elasticsearch_url, **kwargs)

    async def ensure_index(self) -> None:
        try:
            exists = await self.client.indices.exists(index=self.index)
            if not exists:
                await self.client.indices.create(
                    index=self.index,
                    settings={"number_of_shards": 1, "number_of_replicas": 0},
                    mappings=INDEX_MAPPINGS,
                )
        except ApiError as error:
            error_body = getattr(getattr(error, "meta", None), "body", {})
            error_type = (
                error_body.get("error", {}).get("type")
                if isinstance(error_body, dict)
                else None
            )
            if error_type != "resource_already_exists_exception":
                raise

    async def bulk_upsert(self, documents: list[dict[str, Any]]) -> list[BulkItemResult]:
        if not documents:
            return []
        operations: list[dict[str, Any]] = []
        for document in documents:
            operations.append(
                {"index": {"_index": self.index, "_id": document["document_id"]}}
            )
            operations.append(document)
        try:
            response = await self.client.bulk(operations=operations, refresh=False)
        except (ApiError, OSError) as error:
            status_code = getattr(error, "status_code", 503)
            return [
                BulkItemResult(
                    document_id=document["document_id"],
                    indexed=False,
                    transient=status_code == 429 or status_code >= 500,
                    error_class="elasticsearch_request",
                )
                for document in documents
            ]

        results: list[BulkItemResult] = []
        for document, item in zip(documents, response.get("items", []), strict=False):
            action = item.get("index", {})
            status = int(action.get("status", 500))
            if 200 <= status < 300:
                results.append(BulkItemResult(document["document_id"], True, False))
                continue
            error = action.get("error", {})
            error_type = error.get("type") if isinstance(error, dict) else None
            results.append(
                BulkItemResult(
                    document_id=document["document_id"],
                    indexed=False,
                    transient=status == 429 or status >= 500,
                    error_class=error_type or "elasticsearch_indexing",
                )
            )
        if len(results) < len(documents):
            seen = {result.document_id for result in results}
            results.extend(
                BulkItemResult(document["document_id"], False, True, "elasticsearch_response")
                for document in documents
                if document["document_id"] not in seen
            )
        return results

    async def search(self, filters: SearchFilters) -> SearchPage:
        body = build_query(filters)
        request: dict[str, Any] = {
            "index": self.index,
            "query": body["bool"],
            "sort": body["sort"],
            "size": filters.limit,
        }
        if "search_after" in body:
            request["search_after"] = body["search_after"]
        try:
            response = await self.client.search(**request)
        except (ApiError, OSError) as error:
            raise SearchDependencyError("elasticsearch search unavailable") from error
        hit_data = response.get("hits", {})
        hits: list[SearchHit] = []
        for hit in hit_data.get("hits", []):
            source = hit.get("_source", {})
            if not isinstance(source, dict):
                source = {}
            hits.append(
                SearchHit(
                    document_id=str(source.get("document_id") or hit.get("_id", "")),
                    source={
                        key: source.get(key)
                        for key in (
                            "document_id",
                            "source",
                            "timestamp",
                            "level",
                            "service",
                            "environment",
                            "event_id",
                            "trace_id",
                            "span_id",
                            "message",
                            "fields",
                        )
                        if key in source
                    },
                    sort=list(hit.get("sort", [])),
                )
            )
        total_value = hit_data.get("total", 0)
        total = int(total_value.get("value", 0) if isinstance(total_value, dict) else total_value)
        return SearchPage(hits=hits, total=total)

    async def facets(self, filters: SearchFilters) -> dict[str, list[dict[str, Any]]]:
        body = build_query(filters)
        request: dict[str, Any] = {
            "index": self.index,
            "query": body["bool"],
            "size": 0,
            "aggs": {
                "levels": {"terms": {"field": "level", "size": 10}},
                "services": {"terms": {"field": "service", "size": 20}},
                "environments": {"terms": {"field": "environment", "size": 20}},
            },
        }
        try:
            response = await self.client.search(**request)
        except (ApiError, OSError) as error:
            raise SearchDependencyError("elasticsearch facets unavailable") from error
        aggregations = response.get("aggregations", {})
        return {
            name: list(aggregations.get(name, {}).get("buckets", []))
            for name in ("levels", "services", "environments")
        }

    async def close(self) -> None:
        await self.client.close()
