"""Reusable analytics queries built on top of the SQLite warehouse.

This module centralizes read-oriented KPI queries so both CLI and future
dashboard/API layers can share one analytics implementation.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from analytics_platform.db import connect, init_schema


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    """Convert SQLite row objects into plain Python dictionaries.

    Args:
        rows: Rows returned by ``sqlite3`` queries with ``Row`` factory enabled.

    Returns:
        A list of dicts, preserving column names as keys.
    """
    return [dict(row) for row in rows]


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
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT session_id) AS total_sessions,
            COUNT(DISTINCT user_email) AS total_users,
            ROUND(COALESCE(SUM(cost_usd), 0), 6) AS total_cost_usd,
            COALESCE(SUM(input_tokens), 0) AS total_input_tokens,
            COALESCE(SUM(output_tokens), 0) AS total_output_tokens,
            MIN(event_timestamp) AS first_event_at,
            MAX(event_timestamp) AS last_event_at
        FROM events
        """
    ).fetchone()
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
    rows = conn.execute(
        """
        WITH bounded AS (
            SELECT
                e.event_date,
                e.input_tokens,
                e.output_tokens,
                emp.practice AS employee_practice,
                e.resource_practice
            FROM events e
            LEFT JOIN employees emp ON emp.email = e.user_email
            WHERE e.event_date >= DATE((SELECT MAX(event_date) FROM events), '-' || ? || ' days')
        )
        SELECT
            event_date,
            COALESCE(employee_practice, resource_practice, 'Unknown') AS practice,
            COALESCE(SUM(input_tokens), 0) AS input_tokens,
            COALESCE(SUM(output_tokens), 0) AS output_tokens,
            COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0) AS total_tokens
        FROM bounded
        GROUP BY event_date, practice
        ORDER BY event_date ASC, total_tokens DESC
        """,
        (max(days, 1),),
    ).fetchall()
    return _rows_to_dicts(rows)


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
    rows = conn.execute(
        """
        SELECT
            event_hour,
            COUNT(*) AS event_count,
            COUNT(DISTINCT session_id) AS sessions,
            COUNT(DISTINCT user_email) AS users
        FROM events
        GROUP BY event_hour
        ORDER BY event_count DESC, event_hour ASC
        """
    ).fetchall()
    return _rows_to_dicts(rows)


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
    rows = conn.execute(
        """
        SELECT
            tool_name,
            COUNT(*) AS runs,
            ROUND(AVG(CASE
                WHEN success IS NULL THEN NULL
                WHEN success = 1 THEN 1.0
                ELSE 0.0
            END), 4) AS success_rate,
            ROUND(AVG(duration_ms), 2) AS avg_duration_ms,
            MAX(duration_ms) AS max_duration_ms
        FROM events
        WHERE event_body = 'claude_code.tool_result' AND tool_name IS NOT NULL
        GROUP BY tool_name
        HAVING COUNT(*) >= ?
        ORDER BY runs DESC
        """,
        (max(min_runs, 1),),
    ).fetchall()
    return _rows_to_dicts(rows)


def get_model_usage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return model-level request, token, and cost usage.

    Metrics are computed from ``claude_code.api_request`` events only.

    Args:
        conn: Open SQLite connection.

    Returns:
        List of dictionaries with model identifier, request count, total cost,
        input tokens, and output tokens ordered by total cost descending.
    """
    rows = conn.execute(
        """
        SELECT
            model,
            COUNT(*) AS requests,
            ROUND(COALESCE(SUM(cost_usd), 0), 6) AS total_cost_usd,
            COALESCE(SUM(input_tokens), 0) AS input_tokens,
            COALESCE(SUM(output_tokens), 0) AS output_tokens
        FROM events
        WHERE event_body = 'claude_code.api_request' AND model IS NOT NULL
        GROUP BY model
        ORDER BY total_cost_usd DESC, requests DESC
        """
    ).fetchall()
    return _rows_to_dicts(rows)


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
        }
    finally:
        conn.close()
