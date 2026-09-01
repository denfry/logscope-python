from __future__ import annotations

import pytest

from app.health import HealthChecker


class FakeConnection:
    async def __aenter__(self) -> FakeConnection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, _query: object) -> None:
        return None


class FakeEngine:
    def connect(self) -> FakeConnection:
        return FakeConnection()


class FakePing:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def ping(self) -> None:
        if self.error is not None:
            raise self.error


@pytest.mark.asyncio
async def test_health_reports_all_dependencies_and_worker() -> None:
    checker = HealthChecker(FakeEngine(), FakePing(), FakePing())  # type: ignore[arg-type]
    healthy, payload = await checker.check(include_worker=True)
    assert healthy is True
    assert payload["dependencies"] == {
        "postgres": "ok",
        "elasticsearch": "ok",
        "redis": "ok",
        "worker": "ok",
    }


@pytest.mark.asyncio
async def test_health_is_degraded_without_backend_details() -> None:
    checker = HealthChecker(
        FakeEngine(), FakePing(OSError("backend detail")), FakePing()
    )  # type: ignore[arg-type]
    healthy, payload = await checker.check()
    assert healthy is False
    assert payload["dependencies"]["elasticsearch"] == "unavailable"
    assert "backend detail" not in str(payload)
