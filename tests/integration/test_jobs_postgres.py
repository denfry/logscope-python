from __future__ import annotations

from uuid import UUID

import pytest
from testcontainers.postgres import PostgresContainer

from app.config import Settings
from app.db import apply_migrations, close_engine, create_engine, create_session_factory
from app.models import BatchStatus, JobStatus
from app.repositories.jobs import JobRepository


@pytest.mark.integration
@pytest.mark.asyncio
async def test_job_repository_persists_and_claims_unique_jobs() -> None:
    with PostgresContainer("postgres:16.4-alpine") as postgres:
        database_url = postgres.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://", 1
        )
        settings = Settings(
            DATABASE_URL=database_url,
            ELASTICSEARCH_URL="http://localhost:9200",
        )
        engine = create_engine(settings)
        try:
            await apply_migrations(engine)
            session_factory = create_session_factory(engine)
            repository = JobRepository()
            async with session_factory() as session:
                batch = await repository.create_batch(
                    session,
                    source="gateway-a",
                    records=[
                        ("doc-1", {"message": "first"}),
                        ("doc-1", {"message": "duplicate"}),
                        ("doc-2", {"message": "second"}),
                    ],
                )
                assert isinstance(batch.id, UUID)
                assert batch.total_records == 2
                assert await repository.count_jobs(session, batch.id) == 2

                claimed = await repository.claim_batch(session, limit=10, lease_seconds=60)
                assert [job.document_id for job in claimed] == ["doc-1", "doc-2"]
                assert all(job.status is JobStatus.PROCESSING for job in claimed)

                await repository.mark_indexed(session, claimed[0].id)
                await repository.mark_indexed(session, claimed[1].id)
                refreshed = await repository.get_batch(session, batch.id)
                assert refreshed is not None
                assert refreshed.status is BatchStatus.COMPLETED
                assert refreshed.indexed_records == 2
        finally:
            await close_engine(engine)
