from __future__ import annotations

import pytest

from app.adapters.rate_limit import RedisRateLimiter
from app.config import Settings


class FakeRedis:
    def __init__(self, result: int = 1, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.closed = False

    async def eval(self, *_args: object) -> int:
        if self.error is not None:
            raise self.error
        return self.result

    async def aclose(self) -> None:
        self.closed = True


def settings() -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://app:app@localhost:5432/logscope",
        ELASTICSEARCH_URL="http://localhost:9200",
        RATE_LIMIT_REQUESTS=2,
    )


@pytest.mark.asyncio
async def test_limiter_rejects_after_fixed_window_limit() -> None:
    redis = FakeRedis(result=3)
    limiter = RedisRateLimiter(settings(), client=redis)
    assert await limiter.allow("search:127.0.0.1") is False
    await limiter.close()
    assert redis.closed


@pytest.mark.asyncio
async def test_limiter_fails_open_when_redis_is_unavailable() -> None:
    limiter = RedisRateLimiter(settings(), client=FakeRedis(error=OSError("down")))
    assert await limiter.allow("search:127.0.0.1") is True
