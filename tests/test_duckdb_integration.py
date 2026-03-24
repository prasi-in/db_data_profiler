"""Integration-style tests for end-to-end DuckDB profiler execution."""

import json
import pytest

pytest.importorskip("duckdb")

from src.data_profiler.adapters.duckdb_adapter import DuckDBAdapter
from src.data_profiler.config import ProfilerConfig
from src.data_profiler.profiler import DataProfiler


def test_duckdb_profiler_writes_jsonl(duckdb_db, tmp_path):
    """Verify that a DuckDB profiling run emits a JSONL output file."""
    output_path = tmp_path / "duck_profiles.jsonl"
    profiler = DataProfiler(
        DuckDBAdapter(str(duckdb_db)),
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
