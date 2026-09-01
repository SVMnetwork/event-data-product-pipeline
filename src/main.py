"""CLI entry point for the local event curation pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from src.reader import InputError, read_records
from src.transformer import deduplicate, normalize_record
from src.validator import validate_record
from src.writer import write_json, write_jsonl

LOGGER = logging.getLogger("event_pipeline")


def run_pipeline(input_path: Path, output_dir: Path) -> dict[str, Any]:
    LOGGER.info("Reading input from %s", input_path)
    raw_records = read_records(input_path)
    valid: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []

    for raw in raw_records:
        if raw.read_error:
            quarantine.append(
                {
                    "source_file": input_path.name,
                    "source_row": raw.row_number,
                    "record": raw.value,
                    "rejection_reasons": [raw.read_error],
                }
            )
            continue
        normalized = normalize_record(raw.value) if isinstance(raw.value, dict) else raw.value
        errors = validate_record(normalized)
        if errors:
            quarantine.append(
                {
                    "source_file": input_path.name,
                    "source_row": raw.row_number,
                    "record": raw.value,
                    "rejection_reasons": errors,
                }
            )
            continue
        curated = {
            field: normalized[field]
            for field in (
                "event_id",
                "source_system",
                "customer_id",
                "event_type",
                "event_timestamp",
                "amount",
                "currency",
                "ingestion_timestamp",
            )
        }
        curated["_source_file"] = input_path.name
        curated["_source_row"] = raw.row_number
        curated["_raw_record"] = raw.value
        valid.append(curated)

    curated_records, duplicates = deduplicate(valid)
    for duplicate, reason in duplicates:
        quarantine.append(
            {
                "source_file": duplicate["_source_file"],
                "source_row": duplicate["_source_row"],
                "record": duplicate["_raw_record"],
                "rejection_reasons": [reason],
            }
        )

    for curated in curated_records:
        del curated["_raw_record"]

    curated_records.sort(key=lambda row: row["event_id"])
    quarantine.sort(key=lambda row: row["source_row"])
    summary = {
        "input_file": input_path.name,
        "input_records": len(raw_records),
        "curated_records": len(curated_records),
        "quarantined_records": len(quarantine),
        "duplicate_records": len(duplicates),
        "status": "success",
    }
    write_jsonl(output_dir / "curated" / "events.jsonl", curated_records)
    write_jsonl(output_dir / "quarantine" / "events.jsonl", quarantine)
    write_json(output_dir / "run_summary.json", summary)
    LOGGER.info(
        "Completed: input=%d curated=%d quarantined=%d duplicates=%d",
        len(raw_records),
        len(curated_records),
        len(quarantine),
        len(duplicates),
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and curate event data")
    parser.add_argument("--input", required=True, type=Path, help="JSON array or JSONL input path")
    parser.add_argument("--output", required=True, type=Path, help="Output directory")
    parser.add_argument(
        "--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR")
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    try:
        run_pipeline(args.input, args.output)
    except (InputError, OSError) as exc:
        LOGGER.error("Pipeline failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
