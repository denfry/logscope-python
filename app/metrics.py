from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

RATE_LIMIT_FAIL_OPEN = Counter(
    "logscope_rate_limit_fail_open_total",
    "Requests allowed because the Redis rate limiter was unavailable",
)
HTTP_REQUESTS = Counter(
    "logscope_http_requests_total",
    "HTTP requests handled by LogScope",
)


def metrics_payload() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
