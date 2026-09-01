from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import cast

import uvicorn
from fastapi import FastAPI

from app.adapters.elasticsearch import ElasticsearchAdapter
from app.adapters.rate_limit import RedisRateLimiter
from app.api import AppDependencies, register_routes
from app.config import Settings, get_settings
from app.db import apply_migrations, close_engine, create_engine, create_session_factory
from app.health import HealthChecker, Pingable
from app.repositories.jobs import JobRepository
from app.services.ingest import DatabaseIngestUnitOfWork, IngestService
from app.services.search import SearchService


def build_dependencies(settings: Settings) -> AppDependencies:
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    repository = JobRepository()
    elasticsearch = ElasticsearchAdapter(settings)
    limiter = RedisRateLimiter(settings)

    def make_uow() -> DatabaseIngestUnitOfWork:
        return DatabaseIngestUnitOfWork(session_factory, repository)

    ingest = IngestService(make_uow)
    search = SearchService(elasticsearch, settings.search_max_range_seconds)
    health = HealthChecker(engine, elasticsearch.client, cast(Pingable, limiter.redis))

    async def startup() -> None:
        await apply_migrations(engine)
        await elasticsearch.ensure_index()

    async def shutdown() -> None:
        await elasticsearch.close()
        await limiter.close()
        await close_engine(engine)

    return AppDependencies(
        ingest=ingest,
        search=search,
        rate_limiter=limiter,
        health=health,
        startup=startup,
        shutdown=shutdown,
    )


def create_app(settings: Settings, dependencies: AppDependencies | None = None) -> FastAPI:
    app_dependencies = dependencies or build_dependencies(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if app_dependencies.startup is not None:
            await app_dependencies.startup()
        try:
            yield
        finally:
            if app_dependencies.shutdown is not None:
                await app_dependencies.shutdown()

    app = FastAPI(title="LogScope", version="0.1.0", lifespan=lifespan)
    register_routes(app, settings, app_dependencies)
    return app


def run_api() -> None:
    settings = get_settings()
    uvicorn.run(
        create_app(settings),
        host=settings.http_host,
        port=settings.http_port,
        log_level="info",
    )


def run_worker() -> None:
    from app.worker import run_worker_process

    asyncio.run(run_worker_process(get_settings()))

if __name__ == "__main__":
    run_api()
