from src.transformer import normalize_record
from src.validator import validate_record


def valid_record() -> dict:
    return {
        "event_id": "evt-1",
        "source_system": "crm",
        "customer_id": "cust-1",
        "event_type": "updated",
        "event_timestamp": "2026-01-15T10:30:00Z",
        "amount": 12.5,
        "currency": "USD",
        "ingestion_timestamp": "2026-01-15T10:31:00Z",
    }


def test_valid_record_has_no_errors() -> None:
    assert validate_record(valid_record()) == []


def test_reports_multiple_validation_errors() -> None:
    record = valid_record()
    record.update(customer_id=None, amount="12.50", currency="BTC", event_timestamp="yesterday")

    errors = validate_record(record)

    assert "customer_id: required" in errors
    assert "amount: expected number" in errors
    assert "currency: unsupported value 'BTC'" in errors
    assert any(error.startswith("event_timestamp: invalid timestamp") for error in errors)


def test_normalization_canonicalizes_values() -> None:
    record = valid_record()
    record.update(
        source_system=" CRM ",
        event_type="Account Updated",
        currency="usd",
        amount=12.5,
        event_timestamp="2026-01-15T10:30:00-05:00",
    )

    normalized = normalize_record(record)

    assert normalized["source_system"] == "crm"
    assert normalized["event_type"] == "account_updated"
    assert normalized["currency"] == "USD"
    assert normalized["amount"] == 12.5
    assert normalized["event_timestamp"] == "2026-01-15T15:30:00Z"
