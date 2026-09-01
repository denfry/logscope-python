from __future__ import annotations

import pytest

from app.domain.query import SearchFilters
from app.schemas import LogRecordInput


def make_record(event_id: str) -> LogRecordInput:
    return LogRecordInput.model_validate(
        {
            "event_id": event_id,
            "timestamp": "2026-08-31T10:00:00Z",
            "level": "ERROR",
            "service": "api",
            "environment": "prod",
            "message": "database timeout",
            "fields": {"status": 504},
        }
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingest_worker_search_round_trip(real_stack) -> None:
    first = await real_stack.ingest.ingest("gateway", [make_record("evt-1")])
    second = await real_stack.ingest.ingest("gateway", [make_record("evt-1")])
    assert first.accepted_records == 1
    assert second.document_ids == first.document_ids

    assert await real_stack.worker.process_once() == 2
    result = await real_stack.search.search(
        SearchFilters(q="database timeout", service="api", environment="prod")
    )
    assert result.total == 1
    assert [hit["document_id"] for hit in result.hits] == [first.document_ids[0]]
    assert result.hits[0]["message"] == "database timeout"

    facets = await real_stack.search.facets(SearchFilters(service="api"))
    assert facets["levels"][0]["key"] == "error"
