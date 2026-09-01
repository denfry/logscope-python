from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import Settings

_MIGRATION_NAME = "001_init.sql"
_MIGRATION_CANDIDATES = (
    Path(__file__).resolve().parents[1] / "migrations" / _MIGRATION_NAME,
    Path.cwd() / "migrations" / _MIGRATION_NAME,
)
MIGRATION_PATH = next(
    (candidate for candidate in _MIGRATION_CANDIDATES if candidate.is_file()),
    _MIGRATION_CANDIDATES[0],
)


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def apply_migrations(engine: AsyncEngine) -> None:
    migration_sql = MIGRATION_PATH.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in migration_sql.split(";") if statement.strip()]
    async with engine.begin() as connection:
        for statement in statements:
            await connection.execute(text(statement))
        await connection.execute(
            text(
                """
                INSERT INTO schema_migrations (version)
                VALUES (:version)
                ON CONFLICT (version) DO NOTHING
                """
            ),
            {"version": "001_init"},
        )


async def close_engine(engine: AsyncEngine) -> None:
    await engine.dispose()


def row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row._mapping)
