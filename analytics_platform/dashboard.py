"""Dashboard data access layer with filter-aware analytics queries.

This module provides read-only query helpers used by ``streamlit_app.py``.
The functions here apply consistent filtering and return JSON-serializable
records that are easy to pass into DataFrame/chart components.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from analytics_platform.utils import rows_to_dicts


@dataclass
class DashboardFilters:
    """Filter set shared across all dashboard widgets.

    Attributes:
        date_from: Inclusive lower date bound in ``YYYY-MM-DD`` format.
        date_to: Inclusive upper date bound in ``YYYY-MM-DD`` format.
        practices: Optional list of engineering practices to include.
        models: Optional list of model names to include.
        users: Optional list of user emails to include.
    """

    date_from: str | None = None
    date_to: str | None = None
    practices: list[str] | None = None
    models: list[str] | None = None
    users: list[str] | None = None


def _where_clause(filters: DashboardFilters, alias: str = "e") -> tuple[str, list[Any]]:
    """Build a parameterized SQL ``WHERE`` clause from dashboard filters.

    Args:
        filters: Active dashboard filter values.
        alias: Table alias to prefix field names in generated predicates.

    Returns:
        Tuple ``(where_sql, params)`` where:
        - ``where_sql`` is either an empty string or a ``WHERE ...`` fragment.
        - ``params`` is the list of query parameters matching placeholders.
    """
    clauses: list[str] = []
    params: list[Any] = []

    if filters.date_from:
        clauses.append(f"{alias}.event_date >= ?")
        params.append(filters.date_from)
    if filters.date_to:
        clauses.append(f"{alias}.event_date <= ?")
        params.append(filters.date_to)

    if filters.practices:
        placeholders = ",".join("?" for _ in filters.practices)
        clauses.append(f"COALESCE(emp.practice, {alias}.resource_practice, 'Unknown') IN ({placeholders})")
        params.extend(filters.practices)

    if filters.models:
        placeholders = ",".join("?" for _ in filters.models)
        clauses.append(f"{alias}.model IN ({placeholders})")
        params.extend(filters.models)

    if filters.users:
        placeholders = ",".join("?" for _ in filters.users)
        clauses.append(f"{alias}.user_email IN ({placeholders})")
        params.extend(filters.users)

    if not clauses:
        return "", params

    return "WHERE " + " AND ".join(clauses), params


def get_filter_options(conn: sqlite3.Connection) -> dict[str, Any]:
    """Load dashboard filter options from current warehouse contents.

    Args:
        conn: Open SQLite connection.

    Returns:
        Dictionary with:
        - ``min_date`` and ``max_date`` over all events,
        - sorted ``practices``, ``models``, and ``users`` lists.
    """
    date_row = conn.execute(
        "SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date FROM events"
    ).fetchone()

    practices = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT COALESCE(emp.practice, e.resource_practice, 'Unknown') AS practice
            FROM events e
            LEFT JOIN employees emp ON emp.email = e.user_email
            ORDER BY practice ASC
            """
        ).fetchall()
    ]

    models = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT model FROM events WHERE model IS NOT NULL ORDER BY model ASC"
        ).fetchall()
    ]

    users = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT user_email FROM events ORDER BY user_email ASC"
        ).fetchall()
    ]

    return {
        "min_date": date_row["min_date"],
        "max_date": date_row["max_date"],
        "practices": practices,
        "models": models,
        "users": users,
    }


def get_kpis(conn: sqlite3.Connection, filters: DashboardFilters) -> dict[str, Any]:
    """Return top-level KPI cards for the selected filter scope.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        Dictionary containing event/session/user counts, token total, and
        aggregated cost in USD.
    """
    where, params = _where_clause(filters)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS events,
            COUNT(DISTINCT e.session_id) AS sessions,
            COUNT(DISTINCT e.user_email) AS users,
            ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd,
            COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens
        FROM events e
        LEFT JOIN employees emp ON emp.email = e.user_email
        {where}
        """,
        params,
    ).fetchone()
    return dict(row) if row else {}


def get_daily_tokens(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return daily token trend split by engineering practice.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        List of records with ``event_date``, ``practice``, and ``total_tokens``.
    """
    where, params = _where_clause(filters)
    rows = conn.execute(
        f"""
        SELECT
            e.event_date,
            COALESCE(emp.practice, e.resource_practice, 'Unknown') AS practice,
            COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens
        FROM events e
        LEFT JOIN employees emp ON emp.email = e.user_email
        {where}
        GROUP BY e.event_date, practice
        ORDER BY e.event_date ASC, total_tokens DESC
        """,
        params,
    ).fetchall()
    return rows_to_dicts(rows)


def get_hourly_usage(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return usage distribution by hour for selected scope.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        List of records with ``event_hour``, ``event_count``, and ``sessions``.
    """
    where, params = _where_clause(filters)
    rows = conn.execute(
        f"""
        SELECT
            e.event_hour,
            COUNT(*) AS event_count,
            COUNT(DISTINCT e.session_id) AS sessions
        FROM events e
        LEFT JOIN employees emp ON emp.email = e.user_email
        {where}
        GROUP BY e.event_hour
        ORDER BY event_count DESC
        """,
        params,
    ).fetchall()
    return rows_to_dicts(rows)


def get_model_usage(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return model-level request/cost/token breakdown.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        List of model usage records ordered by total cost and request volume.
    """
    where, params = _where_clause(filters)
    rows = conn.execute(
        f"""
        SELECT
            e.model,
            COUNT(*) AS requests,
            ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd,
            COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
            COALESCE(SUM(e.output_tokens), 0) AS output_tokens
        FROM events e
        LEFT JOIN employees emp ON emp.email = e.user_email
        {where}
          AND e.event_body = 'claude_code.api_request'
          AND e.model IS NOT NULL
        GROUP BY e.model
        ORDER BY total_cost_usd DESC, requests DESC
        """,
        params,
    ).fetchall()
    return rows_to_dicts(rows)


def get_tool_usage(conn: sqlite3.Connection, filters: DashboardFilters, limit: int = 20) -> list[dict[str, Any]]:
    """Return top tools by run count with reliability and latency metrics.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.
        limit: Maximum number of tool rows to return.

    Returns:
        List of records containing ``tool_name``, ``runs``, ``success_rate``,
        and ``avg_duration_ms``.
    """
    where, params = _where_clause(filters)
    rows = conn.execute(
        f"""
        SELECT
            e.tool_name,
            COUNT(*) AS runs,
            ROUND(AVG(CASE
                WHEN e.success IS NULL THEN NULL
                WHEN e.success = 1 THEN 1.0
                ELSE 0.0
            END), 4) AS success_rate,
            ROUND(AVG(e.duration_ms), 2) AS avg_duration_ms
        FROM events e
        LEFT JOIN employees emp ON emp.email = e.user_email
        {where}
          AND e.event_body = 'claude_code.tool_result'
          AND e.tool_name IS NOT NULL
        GROUP BY e.tool_name
        ORDER BY runs DESC
        LIMIT ?
        """,
        [*params, max(limit, 1)],
    ).fetchall()
    return rows_to_dicts(rows)


def get_top_users_by_tokens(conn: sqlite3.Connection, filters: DashboardFilters, limit: int = 15) -> list[dict[str, Any]]:
    """Return users with highest token consumption in the selected scope.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.
        limit: Maximum number of users to return.

    Returns:
        List of user records including email, practice, token totals, and cost.
    """
    where, params = _where_clause(filters)
    rows = conn.execute(
        f"""
        SELECT
            e.user_email,
            COALESCE(emp.practice, e.resource_practice, 'Unknown') AS practice,
            COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens,
            ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd
        FROM events e
        LEFT JOIN employees emp ON emp.email = e.user_email
        {where}
        GROUP BY e.user_email, practice
        ORDER BY total_tokens DESC
        LIMIT ?
        """,
        [*params, max(limit, 1)],
    ).fetchall()
    return rows_to_dicts(rows)
