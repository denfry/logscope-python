import pytest
from pydantic import ValidationError

from app.config import Settings


def test_settings_loads_safe_local_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://app:app@postgres:5432/logscope")
    monkeypatch.setenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")

    settings = Settings()

    assert settings.ingest_max_records == 1000
    assert settings.search_max_limit == 100
    assert settings.worker_max_attempts == 3


def test_settings_rejects_invalid_limits() -> None:
    with pytest.raises(ValidationError):
        Settings(
            DATABASE_URL="postgresql+asyncpg://app:app@postgres:5432/logscope",
            ELASTICSEARCH_URL="http://elasticsearch:9200",
            INGEST_MAX_RECORDS=0,
        )
