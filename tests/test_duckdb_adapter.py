"""Unit tests for the DuckDB adapter."""

import pytest

pytest.importorskip("duckdb")

from src.data_profiler.adapters.duckdb_adapter import DuckDBAdapter
from src.data_profiler.config import ProfilerConfig


def test_duckdb_lists_tables(duckdb_db):
    """Verify that the DuckDB adapter discovers the users table."""
    adapter = DuckDBAdapter(str(duckdb_db))
    tables = adapter.list_tables()
    assert len(tables) == 1
    assert tables[0].table == "users"


def test_duckdb_column_stats(duckdb_db):
    """Verify that DuckDB column statistics are computed correctly."""
    adapter = DuckDBAdapter(str(duckdb_db))
    table = adapter.list_tables()[0]
    columns = adapter.get_columns(table)
    age_col = [c for c in columns if c.name == "age"][0]
    profile = adapter.get_column_stats(table, age_col, ProfilerConfig(), 4)
    assert profile.min_value == 25
    assert profile.max_value == 40
    assert profile.null_count == 1
