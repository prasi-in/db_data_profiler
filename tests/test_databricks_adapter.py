"""Unit tests for the Databricks adapter using mocked connector objects."""

from types import SimpleNamespace

from src.data_profiler.adapters.databricks_adapter import DatabricksAdapter


class FakeCursor:
    def __init__(self, responses):
        self.responses = responses
        self.sql = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql):
        self.sql = sql

    def fetchall(self):
        for pattern, rows in self.responses:
            if pattern in self.sql:
                return rows
        return []


class FakeConnection:
    def __init__(self, responses):
        self.responses = responses

    def cursor(self):
        return FakeCursor(self.responses)


def test_databricks_lists_tables(monkeypatch):
    responses = [
        ("information_schema.tables", [("main", "default", "users")]),
    ]
    fake_connector = SimpleNamespace(connect=lambda **kwargs: FakeConnection(responses))
    import src.data_profiler.adapters.databricks_adapter as mod

    monkeypatch.setattr(mod, "databricks_sql", fake_connector)
    adapter = DatabricksAdapter(
        server_hostname="host",
        http_path="path",
        access_token="token",
        catalog="main",
        schema="default",
    )
    tables = adapter.list_tables()
    assert len(tables) == 1
    assert tables[0].table == "users"


def test_databricks_get_columns(monkeypatch):
    responses = [
        ("information_schema.columns", [("id", "BIGINT", "NO")]),
    ]
    fake_connector = SimpleNamespace(connect=lambda **kwargs: FakeConnection(responses))
    import src.data_profiler.adapters.databricks_adapter as mod

    monkeypatch.setattr(mod, "databricks_sql", fake_connector)
    adapter = DatabricksAdapter(
        server_hostname="host",
        http_path="path",
        access_token="token",
        catalog="main",
        schema="default",
    )
    table = SimpleNamespace(database="main", schema="default", table="users")
    cols = adapter.get_columns(table)
    assert len(cols) == 1
    assert cols[0].name == "id"
    assert cols[0].raw_type == "BIGINT"
    assert cols[0].nullable is False


def test_databricks_row_count(monkeypatch):
    responses = [
        ('SELECT COUNT(*) FROM "main"."default"."users"', [(7,)]),
    ]
    fake_connector = SimpleNamespace(connect=lambda **kwargs: FakeConnection(responses))
    import src.data_profiler.adapters.databricks_adapter as mod

    monkeypatch.setattr(mod, "databricks_sql", fake_connector)
    adapter = DatabricksAdapter(
        server_hostname="host",
        http_path="path",
        access_token="token",
        catalog="main",
        schema="default",
    )
    table = SimpleNamespace(database="main", schema="default", table="users")
    assert adapter.get_table_row_count(table) == 7
