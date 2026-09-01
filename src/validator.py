"""Record-level data contract validation."""

from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation
from typing import Any

from src.transformer import parse_timestamp

REQUIRED_FIELDS = (
    "event_id",
    "source_system",
    "customer_id",
    "event_type",
    "event_timestamp",
    "amount",
    "currency",
    "ingestion_timestamp",
)
ALLOWED_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD", "JPY"}


def validate_record(value: Any) -> list[str]:
    """Return all contract violations rather than failing on the first error."""
    if not isinstance(value, dict):
        return ["invalid_record_type: expected JSON object"]

    errors: list[str] = []
    for field in REQUIRED_FIELDS:
        if field not in value or value[field] is None:
            errors.append(f"{field}: required")

    for field in ("event_id", "source_system", "customer_id", "event_type"):
        if field in value and value[field] is not None:
            if not isinstance(value[field], str) or not value[field].strip():
                errors.append(f"{field}: expected non-empty string")

    currency = value.get("currency")
    if currency is not None:
        if not isinstance(currency, str):
            errors.append("currency: expected string")
        elif currency not in ALLOWED_CURRENCIES:
            errors.append(f"currency: unsupported value {currency!r}")

    amount = value.get("amount")
    if amount is not None:
        if isinstance(amount, bool) or not isinstance(amount, int | float | Decimal):
            errors.append("amount: expected number")
        else:
            try:
                decimal_amount = Decimal(str(amount))
                if not decimal_amount.is_finite() or not math.isfinite(float(decimal_amount)):
                    errors.append("amount: must be finite")
                elif decimal_amount < 0:
                    errors.append("amount: must be non-negative")
            except (InvalidOperation, ValueError, OverflowError):
                errors.append("amount: expected finite number")

    for field in ("event_timestamp", "ingestion_timestamp"):
        timestamp = value.get(field)
        if timestamp is not None:
            if not isinstance(timestamp, str):
                errors.append(f"{field}: expected ISO-8601 string")
            else:
                try:
                    parse_timestamp(timestamp)
                except ValueError as exc:
                    errors.append(f"{field}: invalid timestamp ({exc})")
    return errors
