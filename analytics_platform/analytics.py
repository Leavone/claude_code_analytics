"""Reusable analytics queries built on top of the SQLite warehouse.

This module centralizes read-oriented KPI queries so both CLI and future
dashboard/API layers can share one analytics implementation.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from analytics_platform.advanced_stats import build_advanced_statistics_payload
from analytics_platform.db import connect, init_schema
from analytics_platform.predictive import build_predictive_payload
from analytics_platform.utils import load_sql, rows_to_dicts

SQL_DIR = Path(__file__).with_name("sql") / "insights"


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
    row = conn.execute(load_sql(SQL_DIR, "overview.sql")).fetchone()
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
    rows = conn.execute(load_sql(SQL_DIR, "daily_tokens_by_practice.sql"), (max(days, 1),)).fetchall()
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
    rows = conn.execute(load_sql(SQL_DIR, "peak_usage_hours.sql")).fetchall()
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
    rows = conn.execute(load_sql(SQL_DIR, "tool_performance.sql"), (max(min_runs, 1),)).fetchall()
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
    rows = conn.execute(load_sql(SQL_DIR, "model_usage.sql")).fetchall()
    return rows_to_dicts(rows)


def get_seniority_usage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return usage and cost aggregates grouped by employee level.

    Args:
        conn: Open SQLite connection.

    Returns:
        List of level aggregates with event/session/user volume, token totals,
        total cost, and average tokens per session.
    """
    rows = conn.execute(load_sql(SQL_DIR, "seniority_usage.sql")).fetchall()
    return rows_to_dicts(rows)


def get_seniority_model_usage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return model usage split by employee level.

    Args:
        conn: Open SQLite connection.

    Returns:
        List of records with ``level``, ``model``, request count, token totals,
        and cost totals.
    """
    rows = conn.execute(load_sql(SQL_DIR, "seniority_model_usage.sql")).fetchall()
    return rows_to_dicts(rows)


def get_advanced_statistics(conn: sqlite3.Connection, days: int = 30) -> dict[str, Any]:
    """Compute advanced statistical sections over historical API usage.

    This function provides deeper analytical signals than base aggregates. It
    currently includes:
    - daily token anomaly detection via z-score;
    - distribution metrics across session token totals;
    - per-practice variability using standard deviation and coefficient of
      variation;
    - high-token session outlier list (above the 95th percentile).
    - correlation analysis across token/request/cost relationships.

    Args:
        conn: Open SQLite connection.
        days: Number of trailing days to evaluate for daily anomaly detection.
            Values below ``1`` are normalized to ``1``.

    Returns:
        Dictionary with the following keys:
        - ``daily_token_anomalies``
        - ``session_token_distribution``
        - ``practice_variability``
        - ``high_token_sessions``
        - ``correlation_analysis``
    """
    day_rows = rows_to_dicts(
        conn.execute(load_sql(SQL_DIR, "advanced_daily_tokens.sql"), (max(days, 1),)).fetchall()
    )
    session_rows = rows_to_dicts(conn.execute(load_sql(SQL_DIR, "advanced_session_totals.sql")).fetchall())
    return build_advanced_statistics_payload(
        day_rows,
        session_rows,
        window_days=max(days, 1),
    )


def get_predictive_analytics(
    conn: sqlite3.Connection,
    *,
    days: int = 90,
    forecast_days: int = 7,
    target_metric: str = "total_tokens",
    target_label: str | None = None,
) -> dict[str, Any]:
    """Build ML-style forecasting output over daily token usage.

    Args:
        conn: Open SQLite connection.
        days: Number of trailing days to include in training data.
        forecast_days: Number of future days to forecast.
        target_metric: Daily metric column to forecast.
        target_label: Optional human-readable metric label.

    Returns:
        Predictive analytics payload with in-sample fit quality and future
        forecasts.
    """
    daily_rows = rows_to_dicts(
        conn.execute(load_sql(SQL_DIR, "advanced_daily_tokens.sql"), (max(days, 1),)).fetchall()
    )
    return build_predictive_payload(
        daily_rows,
        forecast_days=forecast_days,
        target_metric=target_metric,
        target_label=target_label,
    )


def build_insights_report(
    db_path: Path,
    *,
    days: int = 30,
    min_tool_runs: int = 20,
    forecast_days: int = 7,
) -> dict[str, Any]:
    """Build a complete structured insights report from a database path.

    This is the main orchestration function used by the CLI and intended for
    dashboard/API reuse. It opens a DB connection, ensures schema availability,
    executes all KPI query blocks, and returns one JSON-serializable payload.

    Args:
        db_path: Path to the SQLite analytics database.
        days: Trailing window (in days) for daily trend outputs.
        min_tool_runs: Minimum run threshold for tool performance table.
        forecast_days: Forecast horizon in days for predictive analytics block.

    Returns:
        Dictionary with sections:
        - ``overview``
        - ``daily_tokens_by_practice``
        - ``peak_usage_hours``
        - ``tool_performance``
        - ``model_usage``
        - ``seniority_usage``
        - ``seniority_model_usage``
        - ``advanced_statistics``
        - ``predictive_analytics``
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
            "advanced_statistics": get_advanced_statistics(conn, days=days),
            "predictive_analytics": get_predictive_analytics(
                conn,
                days=max(days, 30),
                forecast_days=forecast_days,
            ),
        }
    finally:
        conn.close()
