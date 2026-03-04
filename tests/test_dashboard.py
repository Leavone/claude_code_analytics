from __future__ import annotations

import json

from analytics_platform.dashboard import (
    DashboardFilters,
    get_daily_tokens,
    get_filter_options,
    get_hourly_usage,
    get_kpis,
    get_model_usage,
    get_tool_usage,
    get_top_users_by_tokens,
)
from analytics_platform.db import connect, init_schema
from analytics_platform.ingestion import ingest_telemetry


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


def test_dashboard_queries_with_filters(tmp_path) -> None:
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

    tool_a = {
        "body": "claude_code.tool_result",
        "attributes": {
            "event.timestamp": "2025-12-03T10:00:01.000Z",
            "event.name": "tool_result",
            "organization.id": "org-1",
            "session.id": "session-a",
            "terminal.type": "vscode",
            "user.account_uuid": "acct-a",
            "user.email": "a@example.com",
            "user.id": "user-a",
            "decision_source": "config",
            "decision_type": "accept",
            "duration_ms": "200",
            "success": "true",
            "tool_name": "Read",
        },
        "scope": {"name": "scope", "version": "v1"},
        "resource": {"user.practice": "Backend Engineering"},
    }

    tool_b = {
        "body": "claude_code.tool_result",
        "attributes": {
            "event.timestamp": "2025-12-04T11:00:02.000Z",
            "event.name": "tool_result",
            "organization.id": "org-1",
            "session.id": "session-b",
            "terminal.type": "vscode",
            "user.account_uuid": "acct-b",
            "user.email": "b@example.com",
            "user.id": "user-b",
            "decision_source": "config",
            "decision_type": "accept",
            "duration_ms": "600",
            "success": "false",
            "tool_name": "Read",
        },
        "scope": {"name": "scope", "version": "v1"},
        "resource": {"user.practice": "Data Engineering"},
    }

    telemetry_path.write_text(
        _make_batch([_make_event("e1", 1764756000000, api_a), _make_event("e2", 1764756001000, tool_a)], 3)
        + "\n"
        + _make_batch([_make_event("e3", 1764846000000, api_b), _make_event("e4", 1764846002000, tool_b)], 4)
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

    conn = connect(db_path)
    init_schema(conn)
    try:
        options = get_filter_options(conn)
        assert options["min_date"] == "2025-12-03"
        assert options["max_date"] == "2025-12-04"
        assert "Backend Engineering" in options["practices"]
        assert "claude-opus-4-6" in options["models"]

        filters = DashboardFilters(
            date_from="2025-12-03",
            date_to="2025-12-04",
            practices=["Backend Engineering"],
        )
        kpis = get_kpis(conn, filters)
        assert kpis["events"] == 2
        assert kpis["sessions"] == 1
        assert kpis["users"] == 1
        assert kpis["total_tokens"] == 150

        daily = get_daily_tokens(conn, filters)
        assert len(daily) == 1
        assert daily[0]["practice"] == "Backend Engineering"
        assert daily[0]["total_tokens"] == 150

        hourly = get_hourly_usage(conn, filters)
        assert len(hourly) == 1
        assert {row["event_hour"] for row in hourly} == {10}

        model_usage = get_model_usage(conn, filters)
        assert len(model_usage) == 1
        assert model_usage[0]["model"] == "claude-opus-4-6"
        assert model_usage[0]["requests"] == 1

        tool_usage = get_tool_usage(conn, filters, limit=5)
        assert len(tool_usage) == 1
        assert tool_usage[0]["tool_name"] == "Read"
        assert tool_usage[0]["runs"] == 1
        assert tool_usage[0]["success_rate"] == 1.0

        top_users = get_top_users_by_tokens(conn, filters, limit=5)
        assert len(top_users) == 1
        assert top_users[0]["user_email"] == "a@example.com"
        assert top_users[0]["total_tokens"] == 150
    finally:
        conn.close()
