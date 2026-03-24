"""Base adapter abstractions and shared SQL adapter logic."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..config import ProfilerConfig
from ..models import ColumnMeta, ColumnProfile, TableRef
from ..utils import NUMERIC_CATEGORIES, normalize_type, qualify_table, quote_ident


class DatabaseAdapter(ABC):
    """Abstract adapter contract implemented by each database backend."""
    engine_name: str

    @abstractmethod
    def list_tables(self) -> list[TableRef]:
        """Return the tables available for profiling."""
        raise NotImplementedError

    @abstractmethod
    def get_columns(self, table: TableRef) -> list[ColumnMeta]:
        """Return column metadata for a table."""
        raise NotImplementedError

    @abstractmethod
    def get_table_row_count(self, table: TableRef) -> int | None:
        """Return the row count for a table."""
        raise NotImplementedError

    @abstractmethod
    def get_column_stats(
        self,
        table: TableRef,
        column: ColumnMeta,
        config: ProfilerConfig,
        table_row_count: int | None,
    ) -> ColumnProfile:
        """Return computed statistics for one column."""
        raise NotImplementedError

    def get_table_comment(self, table: TableRef) -> str | None:
        """Return an optional table comment if the backend supports it."""
        return None


class SqlAdapter(DatabaseAdapter):
    """Shared SQL-based implementation for most relational backends."""

    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return all rows."""
        raise NotImplementedError

    def fetch_one(self, sql: str) -> tuple[Any, ...] | None:
        """Execute a SQL statement and return the first row if present."""
        rows = self.fetch_all(sql)
        return rows[0] if rows else None

    def approx_count_distinct_expr(self, column_name: str) -> str:
        """Return the distinct-count SQL expression for a backend."""
        return f"COUNT(DISTINCT {quote_ident(column_name)})"

    def sample_clause(self, config: ProfilerConfig, table_row_count: int | None) -> str:
        """Return a portable sampling clause."""
        if table_row_count is None or table_row_count <= 0:
            return ""
        if table_row_count <= config.sample_rows:
            return ""
        return f" LIMIT {config.sample_rows}"

    def build_sample_subquery(
        self,
        table: TableRef,
        config: ProfilerConfig,
        table_row_count: int | None,
    ) -> str:
        """Build a sampling subquery for a table."""
        fq = qualify_table(table.database, table.schema, table.table)
        return f"SELECT * FROM {fq}{self.sample_clause(config, table_row_count)}"

    def get_column_stats(
        self,
        table: TableRef,
        column: ColumnMeta,
        config: ProfilerConfig,
        table_row_count: int | None,
    ) -> ColumnProfile:
        """Compute min/max/distinct/null statistics for a column."""
        portable = normalize_type(column.raw_type, column.nullable)
        sample_sql = self.build_sample_subquery(table, config, table_row_count)
        col = quote_ident(column.name)

        stats_sql = f"""
            SELECT
                MIN({col}) AS min_value,
                MAX({col}) AS max_value,
                {self.approx_count_distinct_expr(column.name)} AS distinct_count_estimate,
                COUNT(*) AS sample_size,
                SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count,
                SUM(CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count
            FROM ({sample_sql}) AS sampled
        """
        row = self.fetch_one(stats_sql)
        min_value, max_value, distinct_est, sample_size, null_count, non_null_count = row or (
            None, None, None, None, None, None
        )

        histogram = None
        if config.include_histograms and portable.category in NUMERIC_CATEGORIES and non_null_count:
            histogram = self._build_numeric_histogram(table, column, config, table_row_count)

        return ColumnProfile(
            name=column.name,
            portable_type=portable,
            min_value=min_value,
            max_value=max_value,
            distinct_count_estimate=int(distinct_est) if distinct_est is not None else None,
            null_count=int(null_count) if null_count is not None else None,
            non_null_count=int(non_null_count) if non_null_count is not None else None,
            sample_size_used=int(sample_size) if sample_size is not None else None,
            histogram=histogram,
            comment=column.comment,
        )

    def _build_numeric_histogram(
        self,
        table: TableRef,
        column: ColumnMeta,
        config: ProfilerConfig,
        table_row_count: int | None,
    ) -> dict[str, int] | None:
        """Build a simple equal-width histogram for a numeric column."""
        sample_sql = self.build_sample_subquery(table, config, table_row_count)
        col = quote_ident(column.name)
        bounds_sql = f"SELECT MIN({col}), MAX({col}) FROM ({sample_sql}) t WHERE {col} IS NOT NULL"
        bounds = self.fetch_one(bounds_sql)
        if not bounds or bounds[0] is None or bounds[1] is None:
            return None

        min_v, max_v = float(bounds[0]), float(bounds[1])
        if min_v == max_v:
            return {f"[{min_v}, {max_v}]": 1}

        width = (max_v - min_v) / config.histogram_bins
        buckets: dict[str, int] = {}
        for i in range(config.histogram_bins):
            low = min_v + i * width
            high = max_v if i == config.histogram_bins - 1 else min_v + (i + 1) * width
            if i == config.histogram_bins - 1:
                predicate = f"{col} >= {low} AND {col} <= {high}"
                label = f"[{round(low, 4)}, {round(high, 4)}]"
            else:
                predicate = f"{col} >= {low} AND {col} < {high}"
                label = f"[{round(low, 4)}, {round(high, 4)})"
            sql = f"SELECT COUNT(*) FROM ({sample_sql}) t WHERE {col} IS NOT NULL AND {predicate}"
            count_row = self.fetch_one(sql)
            buckets[label] = int(count_row[0]) if count_row else 0
        return buckets
