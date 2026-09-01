from __future__ import annotations

from dataclasses import dataclass

import pytest_asyncio
from testcontainers.elasticsearch import ElasticSearchContainer
from testcontainers.postgres import PostgresContainer

from app.adapters.elasticsearch import ElasticsearchAdapter
from app.config import Settings
from app.db import apply_migrations, close_engine, create_engine, create_session_factory
from app.domain.retry import RetryPolicy
from app.repositories.jobs import JobRepository
from app.services.ingest import DatabaseIngestUnitOfWork, IngestService
from app.services.search import SearchService
from app.worker import Worker


@dataclass(slots=True)
class RealStack:
    settings: Settings
    ingest: IngestService
    search: SearchService
    worker: Worker
    elasticsearch: ElasticsearchAdapter


@pytest_asyncio.fixture
async def real_stack():
    with (
        PostgresContainer("postgres:16.4-alpine") as postgres,
        ElasticSearchContainer(
            "docker.elastic.co/elasticsearch/elasticsearch:8.15.3",
            mem_limit="1g",
        ).with_env("ES_JAVA_OPTS", "-Xms512m -Xmx512m") as elasticsearch_container,
    ):
        database_url = postgres.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://", 1
        )
        settings = Settings(
            DATABASE_URL=database_url,
            ELASTICSEARCH_URL=elasticsearch_container.get_url(),
        )
        engine = create_engine(settings)
        adapter = ElasticsearchAdapter(settings)
        try:
            await apply_migrations(engine)
            await adapter.ensure_index()
            session_factory = create_session_factory(engine)
            repository = JobRepository()
            ingest = IngestService(
                lambda: DatabaseIngestUnitOfWork(session_factory, repository)
            )
            search = SearchService(adapter, settings.search_max_range_seconds)
            worker = Worker(
                session_factory=session_factory,
                repository=repository,
                elasticsearch=adapter,
                batch_size=settings.worker_batch_size,
                lease_seconds=settings.worker_lease_seconds,
                max_attempts=settings.worker_max_attempts,
                retry_policy=RetryPolicy(jitter_seconds=0),
            )
            yield RealStack(settings, ingest, search, worker, adapter)
        finally:
            await adapter.close()
            await close_engine(engine)
