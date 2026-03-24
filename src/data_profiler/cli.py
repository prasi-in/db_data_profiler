"""Command-line entry point for the data profiler."""

from __future__ import annotations

import argparse
import json
import os

from .adapters import DatabricksAdapter, DuckDBAdapter, SnowflakeAdapter, SQLiteAdapter
from .config import ProfilerConfig
from .profiler import DataProfiler
from .utils import json_default


def build_adapter(args: argparse.Namespace):
    """Build the correct adapter from parsed CLI arguments."""
    engine = args.engine.lower()
    if engine == "sqlite":
        return SQLiteAdapter(args.sqlite_path)
    if engine == "duckdb":
        return DuckDBAdapter(args.duckdb_path)
    if engine == "snowflake":
        return SnowflakeAdapter(
            user=args.sf_user,
            password=args.sf_password,
            account=args.sf_account,
            warehouse=args.sf_warehouse,
            database=args.sf_database,
            schema=args.sf_schema,
        )
    if engine == "databricks":
        return DatabricksAdapter(
            server_hostname=args.dbx_server_hostname,
            http_path=args.dbx_http_path,
            access_token=args.dbx_access_token,
            catalog=args.dbx_catalog,
            schema=args.dbx_schema,
        )
    raise ValueError(f"Unsupported engine: {args.engine}")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for a profiling run."""
    p = argparse.ArgumentParser(description="Configurable multi-database data profiler")
    p.add_argument("--engine", required=True, choices=["sqlite", "duckdb", "snowflake", "databricks"])
    p.add_argument("--output-path", default="profiles.jsonl")
    p.add_argument("--sample-rows", type=int, default=10000)
    p.add_argument("--max-workers", type=int, default=8)
    p.add_argument("--include-histograms", action="store_true")
    p.add_argument("--histogram-bins", type=int, default=10)
    p.add_argument("--no-null-counts", action="store_true")
    p.add_argument("--no-resume", action="store_true")
    p.add_argument("--fail-fast", action="store_true")
    p.add_argument("--skip-empty-tables", action="store_true")

    p.add_argument("--sqlite-path", default=":memory:")
    p.add_argument("--duckdb-path", default=":memory:")

    p.add_argument("--sf-user", default=os.getenv("SF_USER"))
    p.add_argument("--sf-password", default=os.getenv("SF_PASSWORD"))
    p.add_argument("--sf-account", default=os.getenv("SF_ACCOUNT"))
    p.add_argument("--sf-warehouse", default=os.getenv("SF_WAREHOUSE"))
    p.add_argument("--sf-database", default=os.getenv("SF_DATABASE"))
    p.add_argument("--sf-schema", default=os.getenv("SF_SCHEMA"))

    p.add_argument("--dbx-server-hostname", default=os.getenv("DBX_SERVER_HOSTNAME"))
    p.add_argument("--dbx-http-path", default=os.getenv("DBX_HTTP_PATH"))
    p.add_argument("--dbx-access-token", default=os.getenv("DBX_ACCESS_TOKEN"))
    p.add_argument("--dbx-catalog", default=os.getenv("DBX_CATALOG"))
    p.add_argument("--dbx-schema", default=os.getenv("DBX_SCHEMA"))
    return p.parse_args()


def main() -> None:
    """Run the CLI entry point and print the profiling summary."""
    args = parse_args()
    config = ProfilerConfig(
        sample_rows=args.sample_rows,
        max_workers=args.max_workers,
        include_histograms=args.include_histograms,
        histogram_bins=args.histogram_bins,
        include_null_counts=not args.no_null_counts,
        profile_empty_tables=not args.skip_empty_tables,
        fail_fast=args.fail_fast,
        resume=not args.no_resume,
        output_path=args.output_path,
    )
    adapter = build_adapter(args)
    profiler = DataProfiler(adapter, config)
    summary = profiler.profile_all_tables()
    print(json.dumps(summary, indent=2, default=json_default))


if __name__ == "__main__":
    main()
