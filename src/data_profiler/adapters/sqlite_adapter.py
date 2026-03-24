"""SQLite adapter used for local testing and lightweight execution."""

from __future__ import annotations

import sqlite3

from ..models import ColumnMeta, TableRef
from ..utils import quote_ident
from .base import SqlAdapter


class SQLiteAdapter(SqlAdapter):
    """Profiler adapter for SQLite databases."""

    engine_name = "sqlite"

    def __init__(self, path: str):
        """Connect to the SQLite database file."""
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def fetch_all(self, sql: str) -> list[tuple]:
        """Execute a query and return all rows."""
        cur = self.conn.cursor()
        cur.execute(sql)
        return [tuple(row) for row in cur.fetchall()]

    def list_tables(self) -> list[TableRef]:
        """List non-system tables in the SQLite database."""
        rows = self.fetch_all(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1"
        )
        return [TableRef(database=None, schema=None, table=row[0]) for row in rows]

    def get_columns(self, table: TableRef) -> list[ColumnMeta]:
        """Return column metadata for a SQLite table."""
        rows = self.fetch_all(f"PRAGMA table_info({quote_ident(table.table)})")
        return [
            ColumnMeta(name=row[1], raw_type=row[2] or "TEXT", nullable=not bool(row[3]))
            for row in rows
        ]

    def get_table_row_count(self, table: TableRef) -> int | None:
        """Return the row count for a SQLite table."""
        row = self.fetch_one(f"SELECT COUNT(*) FROM {quote_ident(table.table)}")
        return int(row[0]) if row else None
