"""DuckDB behavior-focused tests mirroring the SQLite behavior checks."""

import json
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from src.data_profiler.adapters.duckdb_adapter import DuckDBAdapter
from src.data_profiler.config import ProfilerConfig
from src.data_profiler.profiler import DataProfiler


def _create_multi_table_duckdb(db_path: Path, table_count: int = 4, rows_per_table: int = 50) -> Path:
    """Create a synthetic multi-table DuckDB database for behavior tests."""
    con = duckdb.connect(str(db_path))
    for i in range(table_count):
        table = f"events_{i}"
        con.execute(f"CREATE TABLE {table} (id INTEGER, metric INTEGER, amount DOUBLE, label VARCHAR)")
        rows = []
        for j in range(rows_per_table):
            metric = None if j % 10 == 0 else j
            amount = float(j) * 1.5
            label = f"label_{j % 3}"
            rows.append((j, metric, amount, label))
        con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", rows)
    con.close()
    return db_path


def test_duckdb_performance_sampling_changes_sample_size_used(duckdb_db, tmp_path):
    """Verify that smaller sample sizes affect recorded sample size for DuckDB."""
    output_path = tmp_path / "duck_profiles_sampling.jsonl"
    profiler = DataProfiler(
        DuckDBAdapter(str(duckdb_db)),
        ProfilerConfig(output_path=str(output_path), sample_rows=2, max_workers=1),
    )
    profiler.profile_all_tables()
    payload = json.loads(output_path.read_text(encoding="utf-8").strip().splitlines()[0])
    columns = payload["profile"]["columns"]
    assert all(col["sample_size_used"] == 2 for col in columns)


def test_duckdb_performance_parallel_workers_profile_all_tables(tmp_path):
    """Verify that multiple DuckDB tables can be profiled with concurrency."""
    db_path = _create_multi_table_duckdb(tmp_path / "parallel.duckdb", table_count=6, rows_per_table=20)
    output_path = tmp_path / "duck_profiles_parallel.jsonl"
    profiler = DataProfiler(
        DuckDBAdapter(str(db_path)),
        ProfilerConfig(output_path=str(output_path), max_workers=4),
    )
    summary = profiler.profile_all_tables()
    assert summary["table_count_discovered"] == 6
    assert summary["table_count_profiled"] == 6
    assert summary["failures"] == []
    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 6


def test_duckdb_configurability_histograms_and_bins_are_reflected(duckdb_db, tmp_path):
    """Verify histogram-related config changes the emitted DuckDB output."""
    output_path = tmp_path / "duck_profiles_hist.jsonl"
    profiler = DataProfiler(
        DuckDBAdapter(str(duckdb_db)),
        ProfilerConfig(
            output_path=str(output_path),
            include_histograms=True,
            histogram_bins=4,
            max_workers=1,
        ),
    )
    profiler.profile_all_tables()
    payload = json.loads(output_path.read_text(encoding="utf-8").strip().splitlines()[0])
    columns = {c["name"]: c for c in payload["profile"]["columns"]}
    assert columns["age"]["histogram"] is not None
    assert len(columns["age"]["histogram"]) == 4
    assert columns["name"]["histogram"] is None


def test_duckdb_resilience_resume_skips_already_profiled_tables(tmp_path):
    """Verify reruns skip completed DuckDB tables when resume is enabled."""
    db_path = _create_multi_table_duckdb(tmp_path / "resume.duckdb", table_count=3, rows_per_table=10)
    output_path = tmp_path / "duck_profiles_resume.jsonl"

    profiler_first = DataProfiler(
        DuckDBAdapter(str(db_path)),
        ProfilerConfig(output_path=str(output_path), max_workers=2, resume=True),
    )
    first_summary = profiler_first.profile_all_tables()
    assert first_summary["table_count_profiled"] == 3
    assert first_summary["table_count_skipped"] == 0

    profiler_second = DataProfiler(
        DuckDBAdapter(str(db_path)),
        ProfilerConfig(output_path=str(output_path), max_workers=2, resume=True),
    )
    second_summary = profiler_second.profile_all_tables()
    assert second_summary["table_count_profiled"] == 0
    assert second_summary["table_count_skipped"] == 3

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3


def test_duckdb_output_jsonl_is_persistent_and_downstream_readable(duckdb_db, tmp_path):
    """Verify DuckDB JSONL output persists and can be parsed downstream."""
    output_path = tmp_path / "duck_profiles_output.jsonl"
    profiler = DataProfiler(
        DuckDBAdapter(str(duckdb_db)),
        ProfilerConfig(output_path=str(output_path), max_workers=1),
    )
    profiler.profile_all_tables()

    assert output_path.exists()
    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert "table_key" in payload
    assert "profile" in payload
    assert payload["profile"]["table"] == "users"
    assert isinstance(payload["profile"]["columns"], list)
