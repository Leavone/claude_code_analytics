"""Reusable analytics queries built on top of the SQLite warehouse.

This module centralizes read-oriented KPI queries so both CLI and future
dashboard/API layers can share one analytics implementation.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from analytics_platform.db import connect, init_schema
from analytics_platform.utils import rows_to_dicts

SQL_DIR = Path(__file__).with_name("sql")


def _load_sql(filename: str) -> str:
    """Load a SQL statement from the package's sql directory."""
    return (SQL_DIR / filename).read_text(encoding="utf-8")


def get_overview(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return top-level platform KPIs from the full event table.

    The output is intended for high-level summary cards and includes event
    volume, user/session counts, token totals, overall cost, and data range.

    Args:
        conn: Open SQLite connection.

    Returns:
        Dictionary with aggregate metrics such as ``total_events``,
        ``total_sessions``, ``total_users``, ``total_cost_usd``,
        ``total_input_tokens``, ``total_output_tokens``, and first/last event
        timestamps.
    """
    row = conn.execute(_load_sql("overview.sql")).fetchone()
    return dict(row) if row else {}


def get_daily_tokens_by_practice(conn: sqlite3.Connection, days: int = 30) -> list[dict[str, Any]]:
    """Return daily token consumption grouped by engineering practice.

    Query scope is bounded to a rolling ``days`` window relative to the latest
    ``event_date`` present in the warehouse.

    Args:
        conn: Open SQLite connection.
        days: Number of trailing days to include. Values below ``1`` are
            normalized to ``1``.

    Returns:
        List of dictionaries with:
        - ``event_date``
        - ``practice`` (employee practice, fallback to resource practice)
        - ``input_tokens``
        - ``output_tokens``
        - ``total_tokens`` (input + output)
    """
    rows = conn.execute(_load_sql("daily_tokens_by_practice.sql"), (max(days, 1),)).fetchall()
    return rows_to_dicts(rows)


def get_peak_usage_hours(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return usage distribution by hour-of-day.

    This metric is useful for detecting peak activity windows and comparing
    operational load across the 24-hour cycle.

    Args:
        conn: Open SQLite connection.

    Returns:
        List of dictionaries with ``event_hour``, ``event_count``, ``sessions``,
        and ``users`` sorted by highest event volume first.
    """
    rows = conn.execute(_load_sql("peak_usage_hours.sql")).fetchall()
    return rows_to_dicts(rows)


def get_tool_performance(conn: sqlite3.Connection, min_runs: int = 20) -> list[dict[str, Any]]:
    """Return tool usage volume and reliability/latency metrics.

    Metrics are computed from ``claude_code.tool_result`` events only.

    Args:
        conn: Open SQLite connection.
        min_runs: Minimum number of tool executions required for a tool to be
            included. Values below ``1`` are normalized to ``1``.

    Returns:
        List of dictionaries with:
        - ``tool_name``
        - ``runs``
        - ``success_rate`` (0..1)
        - ``avg_duration_ms``
        - ``max_duration_ms``
        ordered by descending run count.
    """
    rows = conn.execute(_load_sql("tool_performance.sql"), (max(min_runs, 1),)).fetchall()
    return rows_to_dicts(rows)


def get_model_usage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return model-level request, token, and cost usage.

    Metrics are computed from ``claude_code.api_request`` events only.

    Args:
        conn: Open SQLite connection.

    Returns:
        List of dictionaries with model identifier, request count, total cost,
        input tokens, and output tokens ordered by total cost descending.
    """
    rows = conn.execute(_load_sql("model_usage.sql")).fetchall()
    return rows_to_dicts(rows)


def get_seniority_usage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return usage and cost aggregates grouped by employee level.

    Args:
        conn: Open SQLite connection.

    Returns:
        List of level aggregates with event/session/user volume, token totals,
        total cost, and average tokens per session.
    """
    rows = conn.execute(_load_sql("seniority_usage.sql")).fetchall()
    return rows_to_dicts(rows)


def get_seniority_model_usage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return model usage split by employee level.

    Args:
        conn: Open SQLite connection.

    Returns:
        List of records with ``level``, ``model``, request count, token totals,
        and cost totals.
    """
    rows = conn.execute(_load_sql("seniority_model_usage.sql")).fetchall()
    return rows_to_dicts(rows)


def build_insights_report(
    db_path: Path,
    *,
    days: int = 30,
    min_tool_runs: int = 20,
) -> dict[str, Any]:
    """Build a complete structured insights report from a database path.

    This is the main orchestration function used by the CLI and intended for
    dashboard/API reuse. It opens a DB connection, ensures schema availability,
    executes all KPI query blocks, and returns one JSON-serializable payload.

    Args:
        db_path: Path to the SQLite analytics database.
        days: Trailing window (in days) for daily trend outputs.
        min_tool_runs: Minimum run threshold for tool performance table.

    Returns:
        Dictionary with sections:
        - ``overview``
        - ``daily_tokens_by_practice``
        - ``peak_usage_hours``
        - ``tool_performance``
        - ``model_usage``
        - ``seniority_usage``
        - ``seniority_model_usage``
    """
    conn = connect(db_path)
    try:
        init_schema(conn)
        return {
            "overview": get_overview(conn),
            "daily_tokens_by_practice": get_daily_tokens_by_practice(conn, days=days),
            "peak_usage_hours": get_peak_usage_hours(conn),
            "tool_performance": get_tool_performance(conn, min_runs=min_tool_runs),
            "model_usage": get_model_usage(conn),
            "seniority_usage": get_seniority_usage(conn),
            "seniority_model_usage": get_seniority_model_usage(conn),
        }
    finally:
        conn.close()
