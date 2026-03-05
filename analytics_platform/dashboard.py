"""Dashboard data access layer with filter-aware analytics queries.

This module provides read-only query helpers used by ``streamlit_app.py``.
The functions here apply consistent filtering and return JSON-serializable
records that are easy to pass into DataFrame/chart components.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from analytics_platform.advanced_stats import build_advanced_statistics_payload
from analytics_platform.utils import load_sql, rows_to_dicts

SQL_DIR = Path(__file__).with_name("sql") / "dashboard"


@dataclass
class DashboardFilters:
    """Filter set shared across all dashboard widgets.

    Attributes:
        date_from: Inclusive lower date bound in ``YYYY-MM-DD`` format.
        date_to: Inclusive upper date bound in ``YYYY-MM-DD`` format.
        practices: Optional list of engineering practices to include.
        levels: Optional list of employee levels (for example ``L4``) to include.
        models: Optional list of model names to include.
        users: Optional list of user emails to include.
    """

    date_from: str | None = None
    date_to: str | None = None
    practices: list[str] | None = None
    levels: list[str] | None = None
    models: list[str] | None = None
    users: list[str] | None = None


def _render_where_clause(
    filters: DashboardFilters,
    alias: str = "e",
    *,
    base_conditions: list[str] | None = None,
) -> tuple[str, list[Any]]:
    """Build a SQL ``WHERE`` fragment and matching parameters.

    Args:
        filters: Active dashboard filter values.
        alias: Table alias to prefix field names in generated predicates.
        base_conditions: Extra fixed SQL predicates to always apply.

    Returns:
        Tuple ``(where_clause, params)`` where:
        - ``where_clause`` is either an empty string or ``WHERE ...``.
        - ``params`` contains placeholder-bound parameter values.
    """
    clauses: list[str] = list(base_conditions or [])
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

    if filters.levels:
        placeholders = ",".join("?" for _ in filters.levels)
        clauses.append(f"COALESCE(emp.level, 'Unknown') IN ({placeholders})")
        params.extend(filters.levels)

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
        - sorted ``practices``, ``levels``, ``models``, and ``users`` lists.
    """
    date_row = conn.execute(load_sql(SQL_DIR, "filter_options_date_range.sql")).fetchone()

    practices = [
        row[0]
        for row in conn.execute(load_sql(SQL_DIR, "filter_options_practices.sql")).fetchall()
    ]
    levels = [
        row[0]
        for row in conn.execute(load_sql(SQL_DIR, "filter_options_levels.sql")).fetchall()
    ]
    models = [
        row[0]
        for row in conn.execute(load_sql(SQL_DIR, "filter_options_models.sql")).fetchall()
    ]
    users = [
        row[0]
        for row in conn.execute(load_sql(SQL_DIR, "filter_options_users.sql")).fetchall()
    ]

    return {
        "min_date": date_row["min_date"],
        "max_date": date_row["max_date"],
        "practices": practices,
        "levels": levels,
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
    where_clause, params = _render_where_clause(filters)
    sql = load_sql(SQL_DIR, "kpis.sql").format(where_clause=where_clause)
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else {}


def get_daily_tokens(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return daily token trend split by engineering practice.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        List of records with ``event_date``, ``practice``, and ``total_tokens``.
    """
    where_clause, params = _render_where_clause(filters)
    sql = load_sql(SQL_DIR, "daily_tokens.sql").format(where_clause=where_clause)
    rows = conn.execute(sql, params).fetchall()
    return rows_to_dicts(rows)


def get_hourly_usage(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return usage distribution by hour for selected scope.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        List of records with ``event_hour``, ``event_count``, and ``sessions``.
    """
    where_clause, params = _render_where_clause(filters)
    sql = load_sql(SQL_DIR, "hourly_usage.sql").format(where_clause=where_clause)
    rows = conn.execute(sql, params).fetchall()
    return rows_to_dicts(rows)


def get_model_usage(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return model-level request/cost/token breakdown.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        List of model usage records ordered by total cost and request volume.
    """
    where_clause, params = _render_where_clause(
        filters,
        base_conditions=["e.event_body = 'claude_code.api_request'", "e.model IS NOT NULL"],
    )
    sql = load_sql(SQL_DIR, "model_usage.sql").format(where_clause=where_clause)
    rows = conn.execute(sql, params).fetchall()
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
    where_clause, params = _render_where_clause(
        filters,
        base_conditions=["e.event_body = 'claude_code.tool_result'", "e.tool_name IS NOT NULL"],
    )
    sql = load_sql(SQL_DIR, "tool_usage.sql").format(where_clause=where_clause)
    rows = conn.execute(sql, [*params, max(limit, 1)]).fetchall()
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
    where_clause, params = _render_where_clause(filters)
    sql = load_sql(SQL_DIR, "top_users_by_tokens.sql").format(where_clause=where_clause)
    rows = conn.execute(sql, [*params, max(limit, 1)]).fetchall()
    return rows_to_dicts(rows)


def get_seniority_usage(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return usage and cost metrics grouped by employee level.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        List of records with ``level``, ``events``, ``sessions``,
        ``total_tokens``, and ``total_cost_usd``.
    """
    where_clause, params = _render_where_clause(filters)
    sql = load_sql(SQL_DIR, "seniority_usage.sql").format(where_clause=where_clause)
    rows = conn.execute(sql, params).fetchall()
    return rows_to_dicts(rows)


def get_advanced_statistics(conn: sqlite3.Connection, filters: DashboardFilters) -> dict[str, Any]:
    """Return advanced statistical analysis for the selected dashboard scope.

    Unlike global insights-level advanced statistics, this function applies
    current dashboard filters before computing metrics. It helps users inspect
    variability and anomalies inside an actively selected segment
    (for example one practice, level, or model).

    The returned payload contains:
    - ``daily_token_anomalies``: per-day totals and z-score outliers;
    - ``session_token_distribution``: distribution summary over session totals;
    - ``practice_variability``: standard deviation and coefficient of variation
      by practice;
    - ``high_token_sessions``: top sessions above p95 threshold.
    - ``correlation_analysis``: Pearson correlations for key metric pairs.

    Args:
        conn: Open SQLite connection.
        filters: Dashboard filter set.

    Returns:
        Dictionary with advanced-statistics sections suitable for tables/charts.
    """
    daily_where_clause, daily_params = _render_where_clause(
        filters,
        base_conditions=["e.event_body = 'claude_code.api_request'"],
    )
    session_where_clause, session_params = _render_where_clause(filters)
    daily_sql = load_sql(SQL_DIR, "advanced_daily_tokens.sql").format(where_clause=daily_where_clause)
    session_sql = load_sql(SQL_DIR, "advanced_session_totals.sql").format(where_clause=session_where_clause)

    daily_rows = rows_to_dicts(conn.execute(daily_sql, daily_params).fetchall())
    session_rows = rows_to_dicts(conn.execute(session_sql, session_params).fetchall())
    return build_advanced_statistics_payload(daily_rows, session_rows)
