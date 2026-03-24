"""Core profiler orchestration logic."""

from __future__ import annotations

from concurrent import futures
from typing import Any

from .adapters.base import DatabaseAdapter
from .config import ProfilerConfig
from .models import ColumnProfile, TableProfile
from .persistence import JsonlStateStore
from .utils import normalize_type, stable_table_key, utc_now_iso


class DataProfiler:
    """Orchestrates profiling across tables for a given adapter."""

    def __init__(self, adapter: DatabaseAdapter, config: ProfilerConfig):
        """Create a profiler for a specific adapter and runtime config."""
        self.adapter = adapter
        self.config = config
        self.state_store = JsonlStateStore(config.output_path)

    def profile_all_tables(self) -> dict[str, Any]:
        """Profile all discovered tables and return a run summary."""
        tables = self.adapter.list_tables()
        results: dict[str, Any] = {
            "engine": self.adapter.engine_name,
            "started_at_utc": utc_now_iso(),
            "table_count_discovered": len(tables),
            "table_count_profiled": 0,
            "table_count_skipped": 0,
            "failures": [],
            "output_path": self.config.output_path,
        }

        with futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            work = {}
            for table in tables:
                key = stable_table_key(self.adapter.engine_name, table.database, table.schema, table.table)
                if self.config.resume and self.state_store.is_complete(key):
                    results["table_count_skipped"] += 1
                    continue
                work[executor.submit(self._profile_single_table, table)] = key

            for fut in futures.as_completed(work):
                key = work[fut]
                try:
                    profile = fut.result()
                    self.state_store.append(key, profile)
                    results["table_count_profiled"] += 1
                except Exception as exc:
                    results["failures"].append({"table_key": key, "error": str(exc)})
                    if self.config.fail_fast:
                        raise

        results["finished_at_utc"] = utc_now_iso()
        return results

    def _profile_single_table(self, table) -> TableProfile:
        """Profile one table and return its table profile."""
        warnings: list[str] = []
        row_count = self.adapter.get_table_row_count(table)
        columns_meta = self.adapter.get_columns(table)

        if row_count == 0 and not self.config.profile_empty_tables:
            warnings.append("Skipped column stats because table is empty and profile_empty_tables=false")
            columns = [
                ColumnProfile(
                    name=c.name,
                    portable_type=normalize_type(c.raw_type, c.nullable),
                    min_value=None,
                    max_value=None,
                    distinct_count_estimate=None,
                    null_count=None,
                    non_null_count=None,
                    sample_size_used=0,
                    histogram=None,
                    comment=c.comment,
                )
                for c in columns_meta
            ]
        else:
            columns = [self.adapter.get_column_stats(table, c, self.config, row_count) for c in columns_meta]

        return TableProfile(
            database=table.database,
            schema=table.schema,
            table=table.table,
            engine=self.adapter.engine_name,
            row_count=row_count,
            profiled_at_utc=utc_now_iso(),
            columns=columns,
            comment=self.adapter.get_table_comment(table),
            profile_warnings=warnings or None,
        )
