"""Normalization and deterministic duplicate handling."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

NORMALIZED_TEXT_FIELDS = ("event_id", "customer_id")


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and require an explicit timezone."""
    candidate = value.strip()
    if candidate.endswith(("Z", "z")):
        candidate = candidate[:-1] + "+00:00"
    parsed = datetime.fromisoformat(candidate)
    if parsed.tzinfo is None:
        raise ValueError("timezone offset is required")
    return parsed.astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Return a copy with canonical text, timestamps, currency, and amount."""
    normalized = dict(record)
    for field in NORMALIZED_TEXT_FIELDS:
        if isinstance(normalized.get(field), str):
            normalized[field] = normalized[field].strip()

    for field in ("source_system", "event_type"):
        if isinstance(normalized.get(field), str):
            text = normalized[field].strip().lower()
            normalized[field] = re.sub(r"[\s-]+", "_", text)

    if isinstance(normalized.get("currency"), str):
        normalized["currency"] = normalized["currency"].strip().upper()

    for field in ("event_timestamp", "ingestion_timestamp"):
        if isinstance(normalized.get(field), str):
            try:
                normalized[field] = format_timestamp(parse_timestamp(normalized[field]))
            except ValueError:
                pass  # Validation reports the field-specific reason.

    amount = normalized.get("amount")
    if isinstance(amount, int | float | Decimal) and not isinstance(amount, bool):
        try:
            normalized["amount"] = float(Decimal(str(amount)).quantize(Decimal("0.01")))
        except (ValueError, ArithmeticError):
            pass
    return normalized


def deduplicate(
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
    """Keep the latest-ingested row per event_id; highest input row breaks ties."""
    by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_event[record["event_id"]].append(record)

    kept: list[dict[str, Any]] = []
    rejected: list[tuple[dict[str, Any], str]] = []
    for event_id in sorted(by_event):
        candidates = by_event[event_id]
        winner = max(
            candidates,
            key=lambda item: (parse_timestamp(item["ingestion_timestamp"]), item["_source_row"]),
        )
        kept.append(winner)
        for candidate in candidates:
            if candidate is not winner:
                reason = f"duplicate_event_id: superseded by input row {winner['_source_row']}"
                rejected.append((candidate, reason))
    return kept, rejected
