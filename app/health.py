from __future__ import annotations

from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


class Pingable(Protocol):
    async def ping(self) -> Any: ...


class ElasticsearchPingable(Protocol):
    async def ping(self) -> Any: ...


class HealthChecker:
    def __init__(
        self,
        engine: AsyncEngine,
        elasticsearch: ElasticsearchPingable,
        redis: Pingable,
    ) -> None:
        self._engine = engine
        self._elasticsearch = elasticsearch
        self._redis = redis

    async def check(self, *, include_worker: bool = False) -> tuple[bool, dict[str, Any]]:
        statuses: dict[str, str] = {}
        checks = (
            ("postgres", self._check_postgres),
            ("elasticsearch", self._check_elasticsearch),
            ("redis", self._check_redis),
        )
        for name, check in checks:
            try:
                await check()
            except Exception:
                statuses[name] = "unavailable"
            else:
                statuses[name] = "ok"
        if include_worker:
            statuses["worker"] = "ok" if statuses.get("postgres") == "ok" else "unavailable"
        healthy = all(value == "ok" for value in statuses.values())
        return healthy, {"status": "ok" if healthy else "degraded", "dependencies": statuses}

    async def _check_postgres(self) -> None:
        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def _check_elasticsearch(self) -> None:
        await self._elasticsearch.ping()

    async def _check_redis(self) -> None:
        await self._redis.ping()
