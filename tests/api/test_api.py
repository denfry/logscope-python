from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import AppDependencies
from app.config import Settings
from app.main import create_app
from app.services.search import SearchResult


class FakeRateLimiter:
    def __init__(self, allowed: bool = True) -> None:
        self.allowed = allowed

    async def allow(self, _key: str) -> bool:
        return self.allowed


class FakeHealth:
    async def check(self, *, include_worker: bool = False) -> tuple[bool, dict[str, object]]:
        return True, {"status": "ok", "dependencies": {"worker": "ok" if include_worker else "n/a"}}


class FakeIngest:
    async def ingest(self, _source: str, _records: list[object]) -> SimpleNamespace:
        return SimpleNamespace(
            batch_id=str(uuid4()), accepted_records=1, document_ids=["doc-1"]
        )

    async def get_batch(self, _batch_id: object) -> None:
        return None


class FakeSearch:
    async def search(self, _filters: object) -> SearchResult:
        return SearchResult(
            total=1,
            hits=[
                {
                    "document_id": "doc-1",
                    "timestamp": "2026-08-31T10:00:00Z",
                    "level": "error",
                    "service": "api",
                    "environment": "test",
                    "message": "timeout",
                    "fields": {},
                }
            ],
            next_cursor=None,
        )

    async def facets(self, _filters: object) -> dict[str, list[dict[str, object]]]:
        return {"levels": [], "services": [], "environments": []}


@pytest.fixture
def settings() -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://app:app@localhost:5432/logscope",
        ELASTICSEARCH_URL="http://localhost:9200",
    )


@pytest.fixture
def app_dependencies() -> AppDependencies:
    return AppDependencies(FakeIngest(), FakeSearch(), FakeRateLimiter(), FakeHealth())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_ingest_returns_202_and_batch_id(
    settings: Settings, app_dependencies: AppDependencies
) -> None:
    app = create_app(settings, app_dependencies)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/v1/ingest",
            json={
                "source": "gateway",
                "records": [
                    {
                        "event_id": "evt-1",
                        "timestamp": "2026-08-31T10:00:00Z",
                        "level": "error",
                        "service": "api",
                        "environment": "test",
                        "message": "timeout",
                    }
                ],
            },
        )
    assert response.status_code == 202
    assert response.json()["accepted_records"] == 1
    assert response.json()["batch_id"]


@pytest.mark.asyncio
async def test_search_returns_typed_document(
    settings: Settings, app_dependencies: AppDependencies
) -> None:
    app = create_app(settings, app_dependencies)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/v1/search", params={"q": "timeout", "service": "api"})
    assert response.status_code == 200
    assert response.json()["hits"][0]["document_id"] == "doc-1"


@pytest.mark.asyncio
async def test_rate_limit_returns_429(settings: Settings) -> None:
    dependencies = AppDependencies(FakeIngest(), FakeSearch(), FakeRateLimiter(False), FakeHealth())  # type: ignore[arg-type]
    app = create_app(settings, dependencies)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/v1/search")
    assert response.status_code == 429


@pytest.mark.asyncio
async def test_oversize_ingest_body_returns_413(
    settings: Settings, app_dependencies: AppDependencies
) -> None:
    settings.ingest_max_body_bytes = 32
    app = create_app(settings, app_dependencies)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/ingest", content=b"x" * 33)
    assert response.status_code == 413
