from __future__ import annotations

import pytest

from app.config import Settings


@pytest.fixture
def api_settings() -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://app:app@localhost:5432/logscope",
        ELASTICSEARCH_URL="http://localhost:9200",
    )
