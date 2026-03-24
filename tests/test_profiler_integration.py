"""Integration-style tests for end-to-end profiler execution."""

import json

from src.data_profiler.adapters.sqlite_adapter import SQLiteAdapter
from src.data_profiler.config import ProfilerConfig
from src.data_profiler.profiler import DataProfiler


def test_profiler_writes_jsonl(sqlite_db, tmp_path):
    """Verify that a profiling run emits a JSONL output file."""
    output_path = tmp_path / "profiles.jsonl"
    profiler = DataProfiler(
        SQLiteAdapter(str(sqlite_db)),
        ProfilerConfig(output_path=str(output_path), max_workers=2),
    )
    summary = profiler.profile_all_tables()
    assert summary["table_count_profiled"] == 1
    assert output_path.exists()

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["profile"]["table"] == "users"
    assert payload["profile"]["row_count"] == 4
