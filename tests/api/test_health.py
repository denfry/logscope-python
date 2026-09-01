from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import AppDependencies
from app.config import Settings
from app.main import create_app
from app.services.search import SearchResult


class UnavailableHealth:
    async def check(self, *, include_worker: bool = False) -> tuple[bool, dict[str, object]]:
        return False, {
            "status": "degraded",
            "dependencies": {"postgres": "unavailable", "worker": "unavailable"},
        }


class NoopLimiter:
    async def allow(self, _key: str) -> bool:
        return True


class NoopIngest:
    async def ingest(self, _source: str, _records: list[object]) -> object:
        raise AssertionError("not called")

    async def get_batch(self, _batch_id: object) -> None:
        return None


class NoopSearch:
    async def search(self, _filters: object) -> SearchResult:
        raise AssertionError("not called")

    async def facets(self, _filters: object) -> dict[str, list[dict[str, object]]]:
        raise AssertionError("not called")


@pytest.mark.asyncio
async def test_health_returns_503_when_required_dependency_fails() -> None:
    settings = Settings(
        DATABASE_URL="postgresql+asyncpg://app:app@localhost:5432/logscope",
        ELASTICSEARCH_URL="http://localhost:9200",
    )
    dependencies = AppDependencies(NoopIngest(), NoopSearch(), NoopLimiter(), UnavailableHealth())  # type: ignore[arg-type]
    app = create_app(settings, dependencies)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        health = await client.get("/health")
        ready = await client.get("/ready")
    assert health.status_code == 503
    assert ready.status_code == 503
    assert health.json()["dependencies"]["postgres"] == "unavailable"
