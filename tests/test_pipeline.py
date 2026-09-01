import json
from pathlib import Path

from src.main import run_pipeline


def test_pipeline_end_to_end_and_idempotent(tmp_path: Path) -> None:
    source = tmp_path / "events.json"
    output = tmp_path / "output"
    source.write_text(
        json.dumps(
            [
                {
                    "event_id": "evt-1",
                    "source_system": " CRM ",
                    "customer_id": "cust-1",
                    "event_type": "Account Updated",
                    "event_timestamp": "2026-01-01T01:00:00Z",
                    "amount": 10,
                    "currency": "usd",
                    "ingestion_timestamp": "2026-01-01T01:01:00Z",
                },
                {
                    "event_id": "evt-1",
                    "source_system": "crm",
                    "customer_id": "cust-1",
                    "event_type": "account_updated",
                    "event_timestamp": "2026-01-01T01:00:00Z",
                    "amount": 11,
                    "currency": "USD",
                    "ingestion_timestamp": "2026-01-01T01:02:00Z",
                },
                {
                    "event_id": "evt-2",
                    "source_system": "web",
                    "customer_id": None,
                    "event_type": "purchase",
                    "event_timestamp": "bad",
                    "amount": -1,
                    "currency": "BTC",
                    "ingestion_timestamp": "2026-01-01T01:02:00Z",
                },
            ]
        ),
        encoding="utf-8",
    )

    first_summary = run_pipeline(source, output)
    first_curated = (output / "curated" / "events.jsonl").read_bytes()
    first_quarantine = (output / "quarantine" / "events.jsonl").read_bytes()
    second_summary = run_pipeline(source, output)

    assert first_summary == second_summary
    assert first_curated == (output / "curated" / "events.jsonl").read_bytes()
    assert first_quarantine == (output / "quarantine" / "events.jsonl").read_bytes()
    assert first_summary == {
        "input_file": "events.json",
        "input_records": 3,
        "curated_records": 1,
        "quarantined_records": 2,
        "duplicate_records": 1,
        "status": "success",
    }
    curated = json.loads(first_curated)
    assert curated["amount"] == 11.0
    assert curated["currency"] == "USD"
