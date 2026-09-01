from __future__ import annotations

import ipaddress
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from app.adapters.elasticsearch import SearchDependencyError
from app.config import Settings
from app.domain.query import SearchFilters
from app.metrics import HTTP_REQUESTS, metrics_payload
from app.schemas import LogRecordInput
from app.services.ingest import IngestService
from app.services.search import SearchService, SearchValidationError


class RateLimiter(Protocol):
    async def allow(self, key: str) -> bool: ...


class HealthProbe(Protocol):
    async def check(self, *, include_worker: bool = False) -> tuple[bool, dict[str, Any]]: ...


LifecycleHook = Callable[[], Awaitable[None]]


@dataclass(slots=True)
class AppDependencies:
    ingest: IngestService
    search: SearchService
    rate_limiter: RateLimiter
    health: HealthProbe
    startup: LifecycleHook | None = None
    shutdown: LifecycleHook | None = None


class IngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1, max_length=128)
    records: list[LogRecordInput] = Field(min_length=1, max_length=1000)


class IngestResponse(BaseModel):
    batch_id: str
    accepted_records: int
    document_ids: list[str]


class BatchResponse(BaseModel):
    batch_id: str
    source: str
    total_records: int
    indexed_records: int
    failed_records: int
    status: str


class SearchHitResponse(BaseModel):
    document_id: str
    source: str | None = None
    timestamp: str
    level: str
    service: str
    environment: str
    event_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    message: str
    fields: dict[str, Any] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    total: int
    hits: list[SearchHitResponse]
    next_cursor: str | None = None


class FacetResponse(BaseModel):
    levels: list[dict[str, Any]] = Field(default_factory=list)
    services: list[dict[str, Any]] = Field(default_factory=list)
    environments: list[dict[str, Any]] = Field(default_factory=list)


def _trusted_proxy_address(request: Request, settings: Settings) -> str | None:
    peer = request.client.host if request.client else None
    if not peer or not settings.trusted_proxy_cidrs:
        return None
    try:
        peer_ip = ipaddress.ip_address(peer)
        networks = [
            ipaddress.ip_network(value.strip())
            for value in settings.trusted_proxy_cidrs.split(",")
            if value.strip()
        ]
    except ValueError:
        return None
    if not any(peer_ip in network for network in networks):
        return None
    forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
    try:
        return str(ipaddress.ip_address(forwarded))
    except ValueError:
        return None


def _client_key(request: Request, settings: Settings) -> str:
    return _trusted_proxy_address(request, settings) or (
        request.client.host if request.client else "unknown"
    )


def _build_filters(
    *,
    q: str | None,
    levels: list[str] | None,
    service: str | None,
    environment: str | None,
    source: str | None,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
    cursor: str | None,
) -> SearchFilters:
    return SearchFilters(
        q=q,
        levels=levels or [],
        service=service,
        environment=environment,
        source=source,
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
        cursor=cursor,
    )


def register_routes(app: FastAPI, settings: Settings, dependencies: AppDependencies) -> None:
    router = APIRouter()

    @router.post("/v1/ingest", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
    async def ingest(request: Request, payload: IngestRequest) -> IngestResponse:
        key = f"ingest:{_client_key(request, settings)}"
        if not await dependencies.rate_limiter.allow(key):
            raise HTTPException(status_code=429, detail="rate limit exceeded")
        result = await dependencies.ingest.ingest(payload.source, payload.records)
        return IngestResponse(
            batch_id=result.batch_id,
            accepted_records=result.accepted_records,
            document_ids=result.document_ids,
        )

    @router.get("/v1/ingest/{batch_id}", response_model=BatchResponse)
    async def get_batch(batch_id: UUID) -> BatchResponse:
        result = await dependencies.ingest.get_batch(batch_id)
        if result is None:
            raise HTTPException(status_code=404, detail="batch not found")
        return BatchResponse(
            batch_id=result.batch_id,
            source=result.source,
            total_records=result.total_records,
            indexed_records=result.indexed_records,
            failed_records=result.failed_records,
            status=result.status,
        )

    async def search_filters(
        q: str | None = None,
        levels: list[str] | None = None,
        service: str | None = None,
        environment: str | None = None,
        source: str | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> SearchFilters:
        return _build_filters(
            q=q,
            levels=levels,
            service=service,
            environment=environment,
            source=source,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            cursor=cursor,
        )

    search_filters_dependency = Depends(search_filters)

    @router.get("/v1/search", response_model=SearchResponse)
    async def search(
        request: Request,
        filters: SearchFilters = search_filters_dependency,
    ) -> SearchResponse:
        key = f"search:{_client_key(request, settings)}"
        if not await dependencies.rate_limiter.allow(key):
            raise HTTPException(status_code=429, detail="rate limit exceeded")
        result = await dependencies.search.search(filters)
        return SearchResponse(
            total=result.total,
            hits=[SearchHitResponse(**hit) for hit in result.hits],
            next_cursor=result.next_cursor,
        )

    @router.get("/v1/search/facets", response_model=FacetResponse)
    async def facets(
        request: Request,
        filters: SearchFilters = search_filters_dependency,
    ) -> FacetResponse:
        key = f"search:{_client_key(request, settings)}"
        if not await dependencies.rate_limiter.allow(key):
            raise HTTPException(status_code=429, detail="rate limit exceeded")
        return FacetResponse(**(await dependencies.search.facets(filters)))

    @router.get("/health")
    async def health() -> Response:
        healthy, payload = await dependencies.health.check()
        return JSONResponse(payload, status_code=200 if healthy else 503)

    @router.get("/ready")
    async def ready() -> Response:
        healthy, payload = await dependencies.health.check(include_worker=True)
        return JSONResponse(payload, status_code=200 if healthy else 503)

    @router.get("/metrics")
    async def metrics() -> Response:
        payload, content_type = metrics_payload()
        return Response(content=payload, media_type=content_type)

    app.include_router(router)

    @app.exception_handler(SearchValidationError)
    async def search_validation_error(
        _request: Request, exc: SearchValidationError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(SearchDependencyError)
    async def search_dependency_error(
        _request: Request, _exc: SearchDependencyError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503, content={"detail": "search dependency unavailable"}
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    @app.middleware("http")
    async def request_metrics(
        request: Request, call_next: Callable[..., Awaitable[Response]]
    ) -> Response:
        HTTP_REQUESTS.inc()
        if request.url.path == "/v1/ingest" and request.method == "POST":
            content_length = request.headers.get("content-length")
            try:
                too_large = (
                    content_length is not None
                    and int(content_length) > settings.ingest_max_body_bytes
                )
            except ValueError:
                too_large = False
            if too_large:
                return JSONResponse(status_code=413, content={"detail": "request body too large"})
            body = await request.body()
            if len(body) > settings.ingest_max_body_bytes:
                return JSONResponse(status_code=413, content={"detail": "request body too large"})
        return await call_next(request)
