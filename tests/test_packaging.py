"""Tests for optional-connector error messages."""

import pytest

from src.data_profiler.adapters.databricks_adapter import DatabricksAdapter
from src.data_profiler.adapters.snowflake_adapter import SnowflakeAdapter


def test_snowflake_missing_dependency_message(monkeypatch):
    """Snowflake adapter should fail with a helpful install message."""
    import src.data_profiler.adapters.snowflake_adapter as mod

    monkeypatch.setattr(mod, "snowflake", None)
    with pytest.raises(RuntimeError, match="requirements-warehouse.txt"):
        SnowflakeAdapter(
            user="u",
            password="p",
            account="a",
            warehouse="w",
            database="d",
            schema="s",
        )


def test_databricks_missing_dependency_message(monkeypatch):
    """Databricks adapter should fail with a helpful install message."""
    import src.data_profiler.adapters.databricks_adapter as mod

    monkeypatch.setattr(mod, "databricks_sql", None)
    with pytest.raises(RuntimeError, match="requirements-warehouse.txt"):
        DatabricksAdapter(
            server_hostname="host",
            http_path="path",
            access_token="token",
        )
