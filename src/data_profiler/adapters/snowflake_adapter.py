"""Snowflake adapter for warehouse-backed profiling."""

from __future__ import annotations

from ..models import ColumnMeta, TableRef
from ..utils import qualify_table, quote_ident
from .base import SqlAdapter

try:
    import snowflake.connector  # type: ignore
except Exception:  # pragma: no cover
    snowflake = None
else:  # pragma: no cover
    snowflake = snowflake


class SnowflakeAdapter(SqlAdapter):
    """Profiler adapter for Snowflake."""

    engine_name = "snowflake"

    def __init__(
        self,
        *,
        user: str,
        password: str,
        account: str,
        warehouse: str,
        database: str,
        schema: str,
    ):
        """Connect to Snowflake using the provided credentials."""
        if not hasattr(globals().get("snowflake"), "connector"):
            raise RuntimeError(
                "snowflake-connector-python not installed. "
                "Run: pip install -r requirements-warehouse.txt"
            )
        self.conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
        )
        self.default_database = database
        self.default_schema = schema

    def fetch_all(self, sql: str) -> list[tuple]:
        """Execute a query and return all rows."""
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    def approx_count_distinct_expr(self, column_name: str) -> str:
        """Use Snowflake's approximate distinct-count function."""
        return f"APPROX_COUNT_DISTINCT({quote_ident(column_name)})"

    def list_tables(self) -> list[TableRef]:
        """List base tables from Snowflake information_schema."""
        sql = f"""
            SELECT table_catalog, table_schema, table_name
            FROM {quote_ident(self.default_database)}.information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema != 'INFORMATION_SCHEMA'
            ORDER BY 1, 2, 3
        """
        rows = self.fetch_all(sql)
        return [TableRef(database=r[0], schema=r[1], table=r[2]) for r in rows]

    def get_columns(self, table: TableRef) -> list[ColumnMeta]:
        """Return column metadata for a Snowflake table."""
        sql = f"""
            SELECT column_name, data_type, is_nullable, comment
            FROM {quote_ident(table.database or self.default_database)}.information_schema.columns
            WHERE table_schema = '{table.schema}'
              AND table_name = '{table.table}'
            ORDER BY ordinal_position
        """
        rows = self.fetch_all(sql)
        return [
            ColumnMeta(
                name=r[0],
                raw_type=r[1],
                nullable=(str(r[2]).upper() == "YES"),
                comment=r[3],
            )
            for r in rows
        ]

    def get_table_row_count(self, table: TableRef) -> int | None:
        """Return the row count for a Snowflake table."""
        fq = qualify_table(table.database, table.schema, table.table)
        row = self.fetch_one(f"SELECT COUNT(*) FROM {fq}")
        return int(row[0]) if row else None
