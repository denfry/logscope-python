from __future__ import annotations

from typing import Any

from redis.asyncio import from_url

from app.config import Settings
from app.metrics import RATE_LIMIT_FAIL_OPEN

_FIXED_WINDOW_SCRIPT = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
"""


class RedisRateLimiter:
    def __init__(self, settings: Settings, client: Any = None) -> None:
        self.limit = settings.rate_limit_requests
        self.window_seconds = settings.rate_limit_window_seconds
        self.redis: Any = client or from_url(settings.redis_url, decode_responses=True)

    async def allow(self, key: str) -> bool:
        try:
            count = await self.redis.eval(
                _FIXED_WINDOW_SCRIPT,
                1,
                f"logscope:rate:{key}",
                str(self.window_seconds),
            )
            return int(count) <= self.limit
        except Exception:
            RATE_LIMIT_FAIL_OPEN.inc()
            return True

    async def close(self) -> None:
        await self.redis.aclose()
