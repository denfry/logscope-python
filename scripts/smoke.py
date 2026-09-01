from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

BASE_URL = os.environ.get("LOGSCOPE_URL", "http://localhost:8080").rstrip("/")
TIMEOUT_SECONDS = float(os.environ.get("SMOKE_TIMEOUT_SECONDS", "90"))


def request_json(
    method: str, path: str, payload: dict[str, Any] | None = None
) -> tuple[int, Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{BASE_URL}{path}", data=body, headers=headers, method=method
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        raw = response.read()
        return response.status, json.loads(raw.decode("utf-8")) if raw else None


def request_bytes(method: str, path: str) -> tuple[int, bytes]:
    request = urllib.request.Request(
        f"{BASE_URL}{path}", headers={"Accept": "*/*"}, method=method
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.status, response.read()


def wait_for(check: Any, description: str) -> Any:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, urllib.error.HTTPError, json.JSONDecodeError) as error:
            last_error = error
        time.sleep(1)
    raise RuntimeError(f"timed out waiting for {description}: {last_error}")


def main() -> None:
    health = wait_for(lambda: request_json("GET", "/health")[1], "health")
    if health.get("status") != "ok":
        raise RuntimeError(f"health is not ok: {health.get('status')}")
    ready = wait_for(lambda: request_json("GET", "/ready")[1], "readiness")
    if ready.get("status") != "ok":
        raise RuntimeError(f"readiness is not ok: {ready.get('status')}")

    _, accepted = request_json(
        "POST",
        "/v1/ingest",
        {
            "source": "smoke",
            "records": [
                {
                    "event_id": "smoke-event-1",
                    "timestamp": "2026-08-31T10:00:00Z",
                    "level": "error",
                    "service": "smoke",
                    "environment": "local",
                    "message": "smoke timeout",
                    "fields": {"status": 504},
                }
            ],
        },
    )
    if accepted.get("accepted_records") != 1:
        raise RuntimeError("ingest did not accept exactly one record")

    batch_id = accepted["batch_id"]

    def completed_batch() -> Any:
        _, batch = request_json("GET", f"/v1/ingest/{batch_id}")
        return batch if batch.get("status") in {"completed", "partial", "failed"} else None

    batch = wait_for(completed_batch, "batch completion")
    if batch.get("status") != "completed":
        raise RuntimeError(f"batch did not complete: {batch.get('status')}")

    query = urllib.parse.urlencode({"q": "smoke timeout", "service": "smoke"})

    def search_hit() -> Any:
        _, result = request_json("GET", f"/v1/search?{query}")
        return result if result.get("hits") else None

    result = wait_for(search_hit, "indexed search hit")
    if result["hits"][0]["message"] != "smoke timeout":
        raise RuntimeError("search returned an unexpected document")

    print(f"smoke ok: batch={batch_id} hits={len(result['hits'])}")
    _, metrics_payload = request_bytes("GET", "/metrics")
    if b"logscope_http_requests_total" not in metrics_payload:
        raise RuntimeError("metrics response did not contain the request counter")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
