"""Databricks SQL Warehouse adapter."""

from __future__ import annotations

from ..models import ColumnMeta, TableRef
from ..utils import qualify_table, quote_ident
from .base import SqlAdapter

try:
    from databricks import sql as databricks_sql  # type: ignore
except Exception:  # pragma: no cover
    databricks_sql = None


class DatabricksAdapter(SqlAdapter):
    """Profiler adapter for Databricks SQL warehouses."""

    engine_name = "databricks"

    def __init__(
        self,
        *,
        server_hostname: str,
        http_path: str,
        access_token: str,
        catalog: str | None = None,
        schema: str | None = None,
    ):
        """Connect to a Databricks SQL warehouse."""
        if databricks_sql is None:
            raise RuntimeError(
                "databricks-sql-connector not installed. "
                "Run: pip install -r requirements-warehouse.txt"
            )
        self.conn = databricks_sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema=schema,
        )

    def fetch_all(self, sql: str) -> list[tuple]:
        """Execute a query and return all rows."""
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def approx_count_distinct_expr(self, column_name: str) -> str:
        """Use Databricks' approximate distinct-count function."""
        return f"APPROX_COUNT_DISTINCT({quote_ident(column_name)})"

    def list_tables(self) -> list[TableRef]:
        """List base tables from Databricks information_schema."""
        sql = """
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            ORDER BY 1, 2, 3
        """
        rows = self.fetch_all(sql)
        return [TableRef(database=r[0], schema=r[1], table=r[2]) for r in rows]

    def get_columns(self, table: TableRef) -> list[ColumnMeta]:
        """Return column metadata for a Databricks table."""
        sql = f"""
            SELECT column_name, full_data_type, is_nullable
            FROM information_schema.columns
            WHERE table_catalog = '{table.database}'
              AND table_schema = '{table.schema}'
              AND table_name = '{table.table}'
            ORDER BY ordinal_position
        """
        rows = self.fetch_all(sql)
        return [
            ColumnMeta(
                name=r[0],
                raw_type=r[1],
                nullable=(str(r[2]).upper() == "YES"),
            )
            for r in rows
        ]

    def get_table_row_count(self, table: TableRef) -> int | None:
        """Return the row count for a Databricks table."""
        fq = qualify_table(table.database, table.schema, table.table)
        row = self.fetch_one(f"SELECT COUNT(*) FROM {fq}")
        return int(row[0]) if row else None
