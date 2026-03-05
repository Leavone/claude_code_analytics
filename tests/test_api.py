from __future__ import annotations

import json

import pytest

pytest.importorskip("fastapi")
from fastapi import HTTPException

from analytics_platform.ingestion import ingest_telemetry
from api.main import api_advanced_statistics, api_dashboard_kpis, api_insights, api_overview, health


def _make_event(event_id: str, ts_ms: int, payload: dict) -> dict:
    return {"id": event_id, "timestamp": ts_ms, "message": json.dumps(payload)}


def _make_batch(events: list[dict], day: int) -> str:
    return json.dumps(
        {
            "owner": "owner-1",
            "logGroup": "/claude-code/telemetry",
            "logStream": "otel-collector",
            "year": 2025,
            "month": 12,
            "day": day,
            "logEvents": events,
        }
    )


@pytest.fixture
def api_dataset(tmp_path):
    telemetry_path = tmp_path / "telemetry.jsonl"
    employees_path = tmp_path / "employees.csv"
    db_path = tmp_path / "analytics.db"

    employees_path.write_text(
        "email,full_name,practice,level,location\n"
        "a@example.com,User A,Backend Engineering,L5,Poland\n"
        "b@example.com,User B,Data Engineering,L4,Germany\n",
        encoding="utf-8",
    )

    api_a = {
        "body": "claude_code.api_request",
        "attributes": {
            "event.timestamp": "2025-12-03T10:00:00.000Z",
            "event.name": "api_request",
            "organization.id": "org-1",
            "session.id": "session-a",
            "terminal.type": "vscode",
            "user.account_uuid": "acct-a",
            "user.email": "a@example.com",
            "user.id": "user-a",
            "model": "claude-opus-4-6",
            "input_tokens": "120",
            "output_tokens": "30",
            "cache_creation_tokens": "0",
            "cache_read_tokens": "10",
            "cost_usd": "1.10",
            "duration_ms": "1000",
        },
        "scope": {"name": "scope", "version": "v1"},
        "resource": {"user.practice": "Backend Engineering"},
    }

    api_b = {
        "body": "claude_code.api_request",
        "attributes": {
            "event.timestamp": "2025-12-04T11:00:00.000Z",
            "event.name": "api_request",
            "organization.id": "org-1",
            "session.id": "session-b",
            "terminal.type": "vscode",
            "user.account_uuid": "acct-b",
            "user.email": "b@example.com",
            "user.id": "user-b",
            "model": "claude-haiku-4-5-20251001",
            "input_tokens": "60",
            "output_tokens": "20",
            "cache_creation_tokens": "5",
            "cache_read_tokens": "15",
            "cost_usd": "0.40",
            "duration_ms": "800",
        },
        "scope": {"name": "scope", "version": "v1"},
        "resource": {"user.practice": "Data Engineering"},
    }

    telemetry_path.write_text(
        _make_batch([_make_event("e1", 1764756000000, api_a)], 3)
        + "\n"
        + _make_batch([_make_event("e2", 1764846000000, api_b)], 4)
        + "\n",
        encoding="utf-8",
    )

    ingest_telemetry(
        db_path=db_path,
        telemetry_path=telemetry_path,
        employees_path=employees_path,
        replace=True,
        commit_every=10,
    )
    return db_path


def test_health() -> None:
    assert health() == {"status": "ok"}


def test_overview_and_insights(api_dataset) -> None:
    overview = api_overview(db=str(api_dataset))
    assert overview["total_events"] == 2

    payload = api_insights(db=str(api_dataset), days=7, min_tool_runs=1, forecast_days=7)
    assert "overview" in payload
    assert "seniority_usage" in payload
    assert "advanced_statistics" in payload
    assert "correlation_analysis" in payload["advanced_statistics"]
    assert "predictive_analytics" in payload


def test_dashboard_kpis_with_filters(api_dataset) -> None:
    body = api_dashboard_kpis(
        db=str(api_dataset),
        date_from=None,
        date_to=None,
        practices="Backend Engineering",
        levels="L5",
        models=None,
        users=None,
    )
    assert body["events"] == 1
    assert body["users"] == 1
    assert body["total_tokens"] == 150


def test_advanced_statistics_endpoint(api_dataset) -> None:
    """Return advanced-statistics payload and key expected sections."""
    body = api_advanced_statistics(
        db=str(api_dataset),
        days=30,
    )
    assert "daily_token_anomalies" in body
    assert "session_token_distribution" in body
    assert "practice_variability" in body
    assert "correlation_analysis" in body


def test_predictive_endpoint(api_dataset) -> None:
    """Return predictive payload with requested forecast horizon."""
    from api.main import api_predictive

    body = api_predictive(db=str(api_dataset), days=30, forecast_days=5, target_metric="total_tokens")
    assert body["forecast_days"] == 5
    assert "history" in body
    assert "forecast" in body


def test_missing_db_returns_404(tmp_path) -> None:
    missing = tmp_path / "missing.db"
    with pytest.raises(HTTPException) as exc_info:
        api_overview(db=str(missing))
    assert exc_info.value.status_code == 404
    assert "database not found" in str(exc_info.value.detail)
