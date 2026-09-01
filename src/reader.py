"""Input readers for JSON arrays and newline-delimited JSON."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class InputError(Exception):
    """Raised when an input cannot be processed as a dataset."""


@dataclass(frozen=True)
class RawRecord:
    row_number: int
    value: Any
    read_error: str | None = None


def read_records(path: Path) -> list[RawRecord]:
    """Read a JSON array or JSONL file, retaining row-level parse failures."""
    if not path.exists():
        raise InputError(f"Input file does not exist: {path}")
    if not path.is_file():
        raise InputError(f"Input path is not a file: {path}")

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise InputError(f"Unable to read input file {path}: {exc}") from exc
    if not text.strip():
        raise InputError(f"Input file is empty: {path}")

    # A leading '[' unambiguously identifies the supported JSON-array format.
    if text.lstrip().startswith("["):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise InputError(
                f"Malformed JSON array at line {exc.lineno}, column {exc.colno}: {exc.msg}"
            ) from exc
        if not isinstance(payload, list):  # Defensive; the leading '[' normally guarantees this.
            raise InputError("JSON input must contain an array of records")
        return [RawRecord(row_number=index, value=value) for index, value in enumerate(payload, 1)]

    records: list[RawRecord] = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            records.append(RawRecord(line_number, json.loads(line)))
        except json.JSONDecodeError as exc:
            records.append(
                RawRecord(
                    line_number,
                    line,
                    f"malformed_json: column {exc.colno}: {exc.msg}",
                )
            )
    if not records:
        raise InputError(f"Input file contains no records: {path}")
    return records
