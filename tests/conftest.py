"""Shared pytest fixtures for the profiler test suite."""

import sys
import sqlite3
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

@pytest.fixture
def sqlite_db(tmp_path: Path) -> Path:
    """Create a small SQLite database used by multiple tests."""
    db_path = tmp_path / "sample.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER, age INTEGER, name TEXT, salary REAL)")
    cur.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?)",
        [
            (1, 25, "A", 1000.0),
            (2, 30, "B", 1200.5),
            (3, None, "C", 1200.5),
            (4, 40, None, 1400.0),
        ],
    )
    conn.commit()
    conn.close()
    return db_path

@pytest.fixture
def duckdb_db(tmp_path: Path) -> Path:
    """Create a small DuckDB database used by multiple tests.

    The fixture skips automatically when the duckdb package is not installed.
    """
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / "sample.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE users (id INTEGER, age INTEGER, name VARCHAR, salary DOUBLE)")
    con.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?)",
        [
            (1, 25, "A", 1000.0),
            (2, 30, "B", 1200.5),
            (3, None, "C", 1200.5),
            (4, 40, None, 1400.0),
        ],
    )
    con.close()
    return db_path
