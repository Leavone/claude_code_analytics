"""Dashboard data access layer with filter-aware analytics queries."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any


@dataclass
class DashboardFilters:
    """Filter set used across dashboard widgets."""

    date_from: str | None = None
    date_to: str | None = None
    practices: list[str] | None = None
    models: list[str] | None = None
    users: list[str] | None = None


def _where_clause(filters: DashboardFilters, alias: str = "e") -> tuple[str, list[Any]]:
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


def _dict_rows(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]


def get_filter_options(conn: sqlite3.Connection) -> dict[str, Any]:
    """Load filter options and available date range from the warehouse."""
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
    """Return top KPI values for the selected filter scope."""
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
    """Return daily token trend split by practice."""
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
    return _dict_rows(rows)


def get_hourly_usage(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return hourly activity distribution."""
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
    return _dict_rows(rows)


def get_model_usage(conn: sqlite3.Connection, filters: DashboardFilters) -> list[dict[str, Any]]:
    """Return model-level request/cost/token breakdown."""
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
    return _dict_rows(rows)


def get_tool_usage(conn: sqlite3.Connection, filters: DashboardFilters, limit: int = 20) -> list[dict[str, Any]]:
    """Return top tools by run count with reliability metrics."""
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
    return _dict_rows(rows)


def get_top_users_by_tokens(conn: sqlite3.Connection, filters: DashboardFilters, limit: int = 15) -> list[dict[str, Any]]:
    """Return users with highest token consumption."""
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
    return _dict_rows(rows)
