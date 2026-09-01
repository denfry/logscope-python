from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    database_url: str = Field(alias="DATABASE_URL")
    elasticsearch_url: str = Field(alias="ELASTICSEARCH_URL")
    elasticsearch_index: str = Field(default="logscope-logs-v1", alias="ELASTICSEARCH_INDEX")
    elasticsearch_username: str | None = Field(default=None, alias="ELASTICSEARCH_USERNAME")
    elasticsearch_password: str | None = Field(default=None, alias="ELASTICSEARCH_PASSWORD")
    redis_url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")
    http_host: str = Field(default="127.0.0.1", alias="HTTP_HOST")
    http_port: int = Field(default=8080, alias="HTTP_PORT", gt=0, le=65535)
    worker_poll_interval: float = Field(default=0.5, alias="WORKER_POLL_INTERVAL", gt=0, le=60)
    worker_batch_size: int = Field(default=50, alias="WORKER_BATCH_SIZE", gt=0, le=100)
    worker_lease_seconds: int = Field(default=60, alias="WORKER_LEASE_SECONDS", gt=0, le=3600)
    worker_max_attempts: int = Field(default=3, alias="WORKER_MAX_ATTEMPTS", gt=0, le=10)
    ingest_max_body_bytes: int = Field(
        default=1_048_576, alias="INGEST_MAX_BODY_BYTES", gt=0, le=10_485_760
    )
    ingest_max_records: int = Field(
        default=1000, alias="INGEST_MAX_RECORDS", gt=0, le=1000
    )
    record_max_message_chars: int = Field(
        default=4096, alias="RECORD_MAX_MESSAGE_CHARS", gt=0, le=32_768
    )
    record_max_fields_depth: int = Field(default=5, alias="RECORD_MAX_FIELDS_DEPTH", gt=0, le=10)
    search_max_limit: int = Field(default=100, alias="SEARCH_MAX_LIMIT", gt=0, le=100)
    search_max_query_chars: int = Field(default=256, alias="SEARCH_MAX_QUERY_CHARS", gt=0, le=4096)
    search_max_range_seconds: int = Field(default=2_678_400, alias="SEARCH_MAX_RANGE_SECONDS", gt=0)
    rate_limit_requests: int = Field(default=120, alias="RATE_LIMIT_REQUESTS", gt=0)
    rate_limit_window_seconds: int = Field(default=60, alias="RATE_LIMIT_WINDOW_SECONDS", gt=0)
    shutdown_timeout_seconds: int = Field(
        default=15, alias="SHUTDOWN_TIMEOUT_SECONDS", gt=0, le=300
    )
    trusted_proxy_cidrs: str = Field(default="", alias="TRUSTED_PROXY_CIDRS")

    @field_validator("database_url", "elasticsearch_url")
    @classmethod
    def require_nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must not be blank")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
