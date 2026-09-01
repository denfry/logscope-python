from pathlib import Path


def test_smoke_script_uses_only_documented_endpoints() -> None:
    text = Path("scripts/smoke.py").read_text(encoding="utf-8")
    assert "/v1/ingest" in text
    assert "/v1/search" in text
    assert "localhost" in text
    assert "password" not in text.lower()
