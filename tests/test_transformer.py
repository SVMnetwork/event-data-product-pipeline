from src.transformer import deduplicate


def _event(row: int, ingestion_timestamp: str, amount: float) -> dict:
    return {
        "event_id": "evt-1",
        "ingestion_timestamp": ingestion_timestamp,
        "amount": amount,
        "_source_row": row,
    }


def test_deduplication_keeps_latest_ingestion() -> None:
    older = _event(1, "2026-01-01T00:00:00Z", 10.0)
    newer = _event(2, "2026-01-02T00:00:00Z", 20.0)

    kept, rejected = deduplicate([newer, older])

    assert kept == [newer]
    assert rejected[0][0] == older
    assert "superseded by input row 2" in rejected[0][1]


def test_deduplication_uses_input_row_as_stable_tie_breaker() -> None:
    first = _event(1, "2026-01-01T00:00:00Z", 10.0)
    second = _event(2, "2026-01-01T00:00:00Z", 20.0)

    kept, _ = deduplicate([first, second])

    assert kept == [second]
