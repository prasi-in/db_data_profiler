"""Behavior-focused tests for performance, configurability, resilience, and output."""

import json
import sqlite3
from pathlib import Path

from src.data_profiler.adapters.sqlite_adapter import SQLiteAdapter
from src.data_profiler.config import ProfilerConfig
from src.data_profiler.profiler import DataProfiler


def _create_multi_table_db(db_path: Path, table_count: int = 4, rows_per_table: int = 50) -> Path:
    """Create a synthetic multi-table SQLite database for behavior tests."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    for i in range(table_count):
        table = f"events_{i}"
        cur.execute(f"CREATE TABLE {table} (id INTEGER, metric INTEGER, amount REAL, label TEXT)")
        rows = []
        for j in range(rows_per_table):
            metric = None if j % 10 == 0 else j
            amount = float(j) * 1.5
            label = f"label_{j % 3}"
            rows.append((j, metric, amount, label))
        cur.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return db_path


def test_performance_sampling_changes_sample_size_used(sqlite_db, tmp_path):
    """Verify that smaller sample sizes affect the recorded sample size used."""
    output_path = tmp_path / "profiles_sampling.jsonl"
    profiler = DataProfiler(
        SQLiteAdapter(str(sqlite_db)),
        ProfilerConfig(output_path=str(output_path), sample_rows=2, max_workers=1),
    )
    profiler.profile_all_tables()
    payload = json.loads(output_path.read_text(encoding="utf-8").strip().splitlines()[0])
    columns = payload["profile"]["columns"]
    assert all(col["sample_size_used"] == 2 for col in columns)


def test_performance_parallel_workers_profile_all_tables(tmp_path):
    """Verify that multiple tables can be profiled successfully with concurrency."""
    db_path = _create_multi_table_db(tmp_path / "parallel.db", table_count=6, rows_per_table=20)
    output_path = tmp_path / "profiles_parallel.jsonl"
    profiler = DataProfiler(
        SQLiteAdapter(str(db_path)),
        ProfilerConfig(output_path=str(output_path), max_workers=4),
    )
    summary = profiler.profile_all_tables()
    assert summary["table_count_discovered"] == 6
    assert summary["table_count_profiled"] == 6
    assert summary["failures"] == []
    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 6


def test_configurability_histograms_and_bins_are_reflected(sqlite_db, tmp_path):
    """Verify that histogram-related config changes the emitted output."""
    output_path = tmp_path / "profiles_hist.jsonl"
    profiler = DataProfiler(
        SQLiteAdapter(str(sqlite_db)),
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


def test_resilience_resume_skips_already_profiled_tables(tmp_path):
    """Verify that rerunning with the same output file skips completed tables."""
    db_path = _create_multi_table_db(tmp_path / "resume.db", table_count=3, rows_per_table=10)
    output_path = tmp_path / "profiles_resume.jsonl"

    profiler_first = DataProfiler(
        SQLiteAdapter(str(db_path)),
        ProfilerConfig(output_path=str(output_path), max_workers=2, resume=True),
    )
    first_summary = profiler_first.profile_all_tables()
    assert first_summary["table_count_profiled"] == 3
    assert first_summary["table_count_skipped"] == 0

    profiler_second = DataProfiler(
        SQLiteAdapter(str(db_path)),
        ProfilerConfig(output_path=str(output_path), max_workers=2, resume=True),
    )
    second_summary = profiler_second.profile_all_tables()
    assert second_summary["table_count_profiled"] == 0
    assert second_summary["table_count_skipped"] == 3

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3


def test_output_jsonl_is_persistent_and_downstream_readable(sqlite_db, tmp_path):
    """Verify that JSONL output persists and can be parsed downstream."""
    output_path = tmp_path / "profiles_output.jsonl"
    profiler = DataProfiler(
        SQLiteAdapter(str(sqlite_db)),
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
