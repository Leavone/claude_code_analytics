from __future__ import annotations

import json
import sqlite3

from analytics_platform.ingestion import ingest_telemetry
from analytics_platform.utils import parse_iso_timestamp, to_bool_int


def test_parse_iso_timestamp() -> None:
    parsed = parse_iso_timestamp("2025-12-03T00:06:00.000Z")
    assert parsed is not None
    assert parsed.year == 2025
    assert parsed.month == 12


def test_to_bool_int() -> None:
    assert to_bool_int("true") == 1
    assert to_bool_int("false") == 0
    assert to_bool_int("unknown") is None


def test_ingestion_end_to_end(tmp_path) -> None:
    telemetry_path = tmp_path / "telemetry.jsonl"
    employees_path = tmp_path / "employees.csv"
    db_path = tmp_path / "analytics.db"

    employees_path.write_text(
        "email,full_name,practice,level,location\n"
        "blake.patel@example.com,Blake Patel,Frontend Engineering,L5,Poland\n",
        encoding="utf-8",
    )

    event_payload = {
        "body": "claude_code.api_request",
        "attributes": {
            "event.timestamp": "2025-12-03T00:06:00.000Z",
            "event.name": "api_request",
            "organization.id": "org-1",
            "session.id": "session-1",
            "terminal.type": "vscode",
            "user.account_uuid": "acct-1",
            "user.email": "blake.patel@example.com",
            "user.id": "user-1",
            "model": "claude-opus-4-6",
            "input_tokens": "10",
            "output_tokens": "20",
            "cache_creation_tokens": "30",
            "cache_read_tokens": "40",
            "cost_usd": "1.23",
            "duration_ms": "99",
        },
        "scope": {"name": "scope", "version": "v1"},
        "resource": {
            "host.arch": "arm64",
            "host.name": "host-1",
            "os.type": "darwin",
            "os.version": "24.6.0",
            "service.name": "claude-code",
            "service.version": "2.1.45",
            "user.practice": "Frontend Engineering",
            "user.profile": "patel",
            "user.serial": "SER123",
        },
    }

    batch = {
        "owner": "owner-1",
        "logGroup": "/claude-code/telemetry",
        "logStream": "otel-collector",
        "year": 2025,
        "month": 12,
        "day": 3,
        "logEvents": [
            {
                "id": "event-1",
                "timestamp": 1764720360000,
                "message": json.dumps(event_payload),
            }
        ],
    }

    telemetry_path.write_text(json.dumps(batch) + "\n", encoding="utf-8")

    stats = ingest_telemetry(
        db_path=db_path,
        telemetry_path=telemetry_path,
        employees_path=employees_path,
        replace=True,
        commit_every=1,
    )

    assert stats.batches_seen == 1
    assert stats.events_seen == 1
    assert stats.events_inserted == 1
    assert stats.bad_batch_json == 0
    assert stats.bad_event_json == 0
    assert stats.missing_required == 0
    assert stats.employees_loaded == 1

    conn = sqlite3.connect(db_path)
    events_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    practice = conn.execute(
        "SELECT employee_practice FROM events_enriched LIMIT 1"
    ).fetchone()[0]
    conn.close()

    assert events_count == 1
    assert practice == "Frontend Engineering"
