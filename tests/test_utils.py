"""Unit tests for utility helpers and type normalization."""

from src.data_profiler.utils import normalize_type


def test_normalize_integer():
    """Verify that integer-like types normalize to the integer category."""
    pt = normalize_type("BIGINT", True)
    assert pt.category == "integer"
    assert pt.nullable is True


def test_normalize_decimal():
    """Verify that decimal precision and scale are extracted correctly."""
    pt = normalize_type("DECIMAL(12,2)", False)
    assert pt.category == "decimal"
    assert pt.precision == 12
    assert pt.scale == 2


def test_normalize_string_length():
    """Verify that string length is captured for VARCHAR-like types."""
    pt = normalize_type("VARCHAR(255)", True)
    assert pt.category == "string"
    assert pt.length == 255
