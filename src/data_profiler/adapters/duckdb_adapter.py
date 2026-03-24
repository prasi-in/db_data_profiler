"""DuckDB adapter used for local analytics-style profiling."""

from __future__ import annotations

from ..models import ColumnMeta, TableRef
from ..utils import qualify_table
from .base import SqlAdapter

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None


class DuckDBAdapter(SqlAdapter):
    """Profiler adapter for DuckDB."""

    engine_name = "duckdb"

    def __init__(self, path: str = ":memory:"):
        """Store the DuckDB database path.

        A fresh connection is opened per query to avoid sharing one connection
        across multiple worker threads.
        """
        if duckdb is None:
            raise RuntimeError("duckdb package is not installed")
        self.path = path

    def fetch_all(self, sql: str) -> list[tuple]:
        """Execute a query and return all rows."""
        conn = duckdb.connect(self.path)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    def approx_count_distinct_expr(self, column_name: str) -> str:
        """Use DuckDB's approximate distinct-count function."""
        from ..utils import quote_ident
        return f"APPROX_COUNT_DISTINCT({quote_ident(column_name)})"

    def list_tables(self) -> list[TableRef]:
        """List base tables visible through DuckDB information_schema."""
        sql = """
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY 1, 2, 3
        """
        rows = self.fetch_all(sql)
        return [TableRef(database=r[0], schema=r[1], table=r[2]) for r in rows]

    def get_columns(self, table: TableRef) -> list[ColumnMeta]:
        """Return column metadata for a DuckDB table."""
        sql = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_catalog = '{table.database}'
              AND table_schema = '{table.schema}'
              AND table_name = '{table.table}'
            ORDER BY ordinal_position
        """
        rows = self.fetch_all(sql)
        return [
            ColumnMeta(name=r[0], raw_type=r[1], nullable=(str(r[2]).upper() == "YES"))
            for r in rows
        ]

    def get_table_row_count(self, table: TableRef) -> int | None:
        """Return the row count for a DuckDB table."""
        fq = qualify_table(table.database, table.schema, table.table)
        row = self.fetch_one(f"SELECT COUNT(*) FROM {fq}")
        return int(row[0]) if row else None