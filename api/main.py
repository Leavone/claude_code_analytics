"""FastAPI application exposing analytics data over HTTP."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from analytics_platform.analytics import (
    build_insights_report,
    get_overview,
    get_seniority_model_usage,
    get_seniority_usage,
)
from analytics_platform.dashboard import DashboardFilters, get_kpis
from analytics_platform.db import connect, init_schema

app = FastAPI(title="Claude Code Analytics API", version="1.0.0")


def _split_csv(value: str | None) -> list[str] | None:
    """Split comma-separated query values into clean string lists."""
    if not value:
        return None
    values = [part.strip() for part in value.split(",") if part.strip()]
    return values or None


def _assert_db_exists(db_path: Path) -> None:
    """Validate database path before opening connection."""
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"database not found: {db_path}")


@app.get("/health")
def health() -> dict[str, str]:
    """Lightweight health endpoint for operational checks."""
    return {"status": "ok"}


@app.get("/api/v1/overview")
def api_overview(db: str = Query(default="artifacts/analytics.db")) -> dict:
    """Return top-level analytics KPI snapshot."""
    db_path = Path(db)
    _assert_db_exists(db_path)
    conn = connect(db_path)
    try:
        init_schema(conn)
        return get_overview(conn)
    finally:
        conn.close()


@app.get("/api/v1/insights")
def api_insights(
    db: str = Query(default="artifacts/analytics.db"),
    days: int = Query(default=30, ge=1, le=365),
    min_tool_runs: int = Query(default=20, ge=1, le=10000),
) -> dict:
    """Return full insights payload (same structure as CLI insights command)."""
    db_path = Path(db)
    _assert_db_exists(db_path)
    return build_insights_report(db_path=db_path, days=days, min_tool_runs=min_tool_runs)


@app.get("/api/v1/dashboard/kpis")
def api_dashboard_kpis(
    db: str = Query(default="artifacts/analytics.db"),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    practices: str | None = Query(default=None, description="Comma-separated practice values"),
    levels: str | None = Query(default=None, description="Comma-separated level values"),
    models: str | None = Query(default=None, description="Comma-separated model values"),
    users: str | None = Query(default=None, description="Comma-separated user emails"),
) -> dict:
    """Return dashboard KPI cards using optional filter query params."""
    db_path = Path(db)
    _assert_db_exists(db_path)

    filters = DashboardFilters(
        date_from=date_from,
        date_to=date_to,
        practices=_split_csv(practices),
        levels=_split_csv(levels),
        models=_split_csv(models),
        users=_split_csv(users),
    )

    conn = connect(db_path)
    try:
        init_schema(conn)
        return get_kpis(conn, filters)
    finally:
        conn.close()


@app.get("/api/v1/seniority")
def api_seniority(db: str = Query(default="artifacts/analytics.db")) -> dict[str, list[dict]]:
    """Return level-based usage and model preference sections."""
    db_path = Path(db)
    _assert_db_exists(db_path)

    conn = connect(db_path)
    try:
        init_schema(conn)
        return {
            "seniority_usage": get_seniority_usage(conn),
            "seniority_model_usage": get_seniority_model_usage(conn),
        }
    finally:
        conn.close()
