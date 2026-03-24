"""Unit tests for the SQLite adapter."""

from src.data_profiler.adapters.sqlite_adapter import SQLiteAdapter
from src.data_profiler.config import ProfilerConfig


def test_sqlite_lists_tables(sqlite_db):
    """Verify that the SQLite adapter discovers the users table."""
    adapter = SQLiteAdapter(str(sqlite_db))
    tables = adapter.list_tables()
    assert len(tables) == 1
    assert tables[0].table == "users"


def test_sqlite_column_stats(sqlite_db):
    """Verify that SQLite column statistics are computed correctly."""
    adapter = SQLiteAdapter(str(sqlite_db))
    table = adapter.list_tables()[0]
    columns = adapter.get_columns(table)
    age_col = [c for c in columns if c.name == "age"][0]
    profile = adapter.get_column_stats(table, age_col, ProfilerConfig(), 4)
    assert profile.min_value == 25
    assert profile.max_value == 40
    assert profile.null_count == 1
