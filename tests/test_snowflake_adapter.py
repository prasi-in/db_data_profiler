"""Unit tests for the Snowflake adapter using mocked connector objects."""

from types import SimpleNamespace

from src.data_profiler.adapters.snowflake_adapter import SnowflakeAdapter


class FakeCursor:
    def __init__(self, responses):
        self.responses = responses
        self.sql = None

    def execute(self, sql):
        self.sql = sql

    def fetchall(self):
        for pattern, rows in self.responses:
            if pattern in self.sql:
                return rows
        return []

    def close(self):
        return None


class FakeConnection:
    def __init__(self, responses):
        self.responses = responses

    def cursor(self):
        return FakeCursor(self.responses)


def test_snowflake_lists_tables(monkeypatch):
    responses = [
        ("information_schema.tables", [("DB1", "PUBLIC", "USERS")]),
    ]
    fake_module = SimpleNamespace(
        connector=SimpleNamespace(connect=lambda **kwargs: FakeConnection(responses))
    )
    import src.data_profiler.adapters.snowflake_adapter as mod

    monkeypatch.setattr(mod, "snowflake", fake_module)
    adapter = SnowflakeAdapter(
        user="u", password="p", account="a", warehouse="w", database="DB1", schema="PUBLIC"
    )
    tables = adapter.list_tables()
    assert len(tables) == 1
    assert tables[0].table == "USERS"


def test_snowflake_get_columns(monkeypatch):
    responses = [
        ("information_schema.columns", [("ID", "NUMBER", "NO", "primary key")]),
    ]
    fake_module = SimpleNamespace(
        connector=SimpleNamespace(connect=lambda **kwargs: FakeConnection(responses))
    )
    import src.data_profiler.adapters.snowflake_adapter as mod

    monkeypatch.setattr(mod, "snowflake", fake_module)
    adapter = SnowflakeAdapter(
        user="u", password="p", account="a", warehouse="w", database="DB1", schema="PUBLIC"
    )
    table = SimpleNamespace(database="DB1", schema="PUBLIC", table="USERS")
    cols = adapter.get_columns(table)
    assert len(cols) == 1
    assert cols[0].name == "ID"
    assert cols[0].raw_type == "NUMBER"
    assert cols[0].nullable is False
    assert cols[0].comment == "primary key"


def test_snowflake_row_count(monkeypatch):
    responses = [
        ('SELECT COUNT(*) FROM "DB1"."PUBLIC"."USERS"', [(42,)]),
    ]
    fake_module = SimpleNamespace(
        connector=SimpleNamespace(connect=lambda **kwargs: FakeConnection(responses))
    )
    import src.data_profiler.adapters.snowflake_adapter as mod

    monkeypatch.setattr(mod, "snowflake", fake_module)
    adapter = SnowflakeAdapter(
        user="u", password="p", account="a", warehouse="w", database="DB1", schema="PUBLIC"
    )
    table = SimpleNamespace(database="DB1", schema="PUBLIC", table="USERS")
    assert adapter.get_table_row_count(table) == 42
