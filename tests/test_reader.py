from pathlib import Path

import pytest

from src.reader import InputError, read_records


def test_jsonl_malformed_row_is_retained_for_quarantine(tmp_path: Path) -> None:
    source = tmp_path / "events.jsonl"
    source.write_text('{"event_id": "one"}\n{"broken":\n', encoding="utf-8")

    records = read_records(source)

    assert len(records) == 2
    assert records[0].read_error is None
    assert records[1].read_error.startswith("malformed_json")


def test_missing_input_raises_clear_error(tmp_path: Path) -> None:
    with pytest.raises(InputError, match="does not exist"):
        read_records(tmp_path / "missing.json")
