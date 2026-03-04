"""Shared utility helpers for parsing and lightweight type coercion."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_iso_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp into UTC.

    Args:
        value: Candidate timestamp value, usually a string like
            ``2025-12-03T00:06:00.000Z``.

    Returns:
        A timezone-aware UTC ``datetime`` if parsing succeeds; otherwise ``None``.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def to_int(value: Any) -> int | None:
    """Best-effort conversion to ``int``.

    Args:
        value: Input value that may be numeric, string, or null-like.

    Returns:
        Parsed integer value, or ``None`` when conversion fails.
    """
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    """Best-effort conversion to ``float``.

    Args:
        value: Input value that may be numeric, string, or null-like.

    Returns:
        Parsed float value, or ``None`` when conversion fails.
    """
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def to_bool_int(value: Any) -> int | None:
    """Convert textual booleans to integer flags.

    Args:
        value: Input value, expected to contain ``true`` or ``false``.

    Returns:
        ``1`` for true, ``0`` for false, otherwise ``None``.
    """
    if value is None:
        return None

    text = str(value).strip().lower()
    if text == "true":
        return 1
    if text == "false":
        return 0
    return None


def rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    """Convert database row objects to plain dictionaries.

    This helper supports SQLite ``Row`` instances and other row types that can
    be directly wrapped by ``dict(...)``.

    Args:
        rows: Sequence of row-like objects returned by DB query methods.

    Returns:
        List of dictionaries preserving column names as keys.
    """
    return [dict(row) for row in rows]


def load_sql(sql_dir: Path, filename: str) -> str:
    """Load a SQL statement from a given SQL directory.

    Args:
        sql_dir: Directory containing SQL files.
        filename: SQL file name (for example ``overview.sql``).

    Returns:
        SQL text content as a UTF-8 string.
    """
    return (sql_dir / filename).read_text(encoding="utf-8")
