"""Utility helpers shared across the profiler implementation."""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
from typing import Any

from .models import PortableType


NUMERIC_CATEGORIES = {"integer", "float", "decimal"}


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def stable_table_key(engine: str, database: str | None, schema: str | None, table: str) -> str:
    """Return a stable hash key for a table."""
    raw = f"{engine}|{database or ''}|{schema or ''}|{table}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def quote_ident(name: str) -> str:
    """Quote an identifier using ANSI-style double quotes."""
    return '"' + name.replace('"', '""') + '"'


def qualify_table(database: str | None, schema: str | None, table: str) -> str:
    """Build a fully-qualified table name from available parts."""
    parts = [p for p in [database, schema, table] if p]
    return ".".join(quote_ident(p) for p in parts)


def normalize_type(raw_type: str | None, nullable: bool | None) -> PortableType:
    """Normalize a native engine type to a portable type representation."""
    raw = (raw_type or "UNKNOWN").strip()
    upper = raw.upper()

    category = "string"
    if any(x in upper for x in ["INT", "LONG", "BIGINT", "SMALLINT", "TINYINT"]):
        category = "integer"
    elif any(x in upper for x in ["DECIMAL", "NUMERIC"]):
        category = "decimal"
    elif any(x in upper for x in ["FLOAT", "DOUBLE", "REAL"]):
        category = "float"
    elif "BOOL" in upper:
        category = "boolean"
    elif "DATE" in upper and "TIMESTAMP" not in upper:
        category = "date"
    elif any(x in upper for x in ["TIMESTAMP", "DATETIME"]):
        category = "timestamp"
    elif any(x in upper for x in ["BINARY", "BLOB", "VARBINARY"]):
        category = "binary"
    elif any(x in upper for x in ["ARRAY", "OBJECT", "VARIANT", "JSON", "MAP", "STRUCT"]):
        category = "semi_structured"

    precision = scale = length = None
    if "(" in upper and ")" in upper:
        inner = upper.split("(", 1)[1].rsplit(")", 1)[0]
        parts = [p.strip() for p in inner.split(",")]
        try:
            if len(parts) >= 1 and parts[0].isdigit():
                precision = int(parts[0])
            if len(parts) >= 2 and parts[1].isdigit():
                scale = int(parts[1])
            if len(parts) == 1 and category == "string" and parts[0].isdigit():
                length = int(parts[0])
        except Exception:
            pass

    return PortableType(
        raw_type=raw,
        category=category,
        nullable=nullable,
        precision=precision,
        scale=scale,
        length=length,
    )


def json_default(value: Any) -> Any:
    """JSON serializer fallback for dataclasses and datetime values."""
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return str(value)
