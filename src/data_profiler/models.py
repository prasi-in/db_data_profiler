"""Portable data model definitions used across adapters and persistence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class PortableType:
    """Cross-engine type representation for a column."""
    raw_type: str
    category: str
    nullable: Optional[bool] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None


@dataclass
class ColumnProfile:
    """Profile summary for a single column."""
    name: str
    portable_type: PortableType
    min_value: Optional[Any]
    max_value: Optional[Any]
    distinct_count_estimate: Optional[int]
    null_count: Optional[int] = None
    non_null_count: Optional[int] = None
    sample_size_used: Optional[int] = None
    histogram: Optional[dict[str, int]] = None
    comment: Optional[str] = None


@dataclass
class TableProfile:
    """Profile summary for a single table."""
    database: Optional[str]
    schema: Optional[str]
    table: str
    engine: str
    row_count: Optional[int]
    profiled_at_utc: str
    columns: list[ColumnProfile]
    comment: Optional[str] = None
    profile_warnings: Optional[list[str]] = None


@dataclass
class ColumnMeta:
    """Column metadata returned by an adapter before statistics are computed."""
    name: str
    raw_type: str
    nullable: Optional[bool]
    comment: Optional[str] = None


@dataclass
class TableRef:
    """Logical reference to a table inside a database or warehouse."""
    database: Optional[str]
    schema: Optional[str]
    table: str
