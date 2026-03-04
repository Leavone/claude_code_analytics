from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analytics_platform.analytics import build_insights_report
from analytics_platform.ingestion import ingest_telemetry


def _make_batch_line(events: list[dict], year: int, month: int, day: int) -> str:
    return json.dumps(
        {
            "owner": "owner-1",
            "logGroup": "/claude-code/telemetry",
            "logStream": "otel-collector",
            "year": year,
            "month": month,
            "day": day,
            "logEvents": events,
        }
    )


def _make_event(event_id: str, ts_ms: int, payload: dict) -> dict:
    return {"id": event_id, "timestamp": ts_ms, "message": json.dumps(payload)}


class AnalyticsTests(unittest.TestCase):
    def test_build_insights_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            telemetry_path = base / "telemetry.jsonl"
            employees_path = base / "employees.csv"
            db_path = base / "analytics.db"

            employees_path.write_text(
                "email,full_name,practice,level,location\n"
                "a@example.com,User A,Backend Engineering,L5,Poland\n"
                "b@example.com,User B,Data Engineering,L4,Germany\n",
                encoding="utf-8",
            )

            api_payload_a = {
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
                    "input_tokens": "100",
                    "output_tokens": "40",
                    "cache_creation_tokens": "0",
                    "cache_read_tokens": "50",
                    "cost_usd": "1.50",
                    "duration_ms": "1200",
                },
                "scope": {"name": "scope", "version": "v1"},
                "resource": {
                    "host.arch": "arm64",
                    "host.name": "host-a",
                    "os.type": "darwin",
                    "os.version": "24.6.0",
                    "service.name": "claude-code",
                    "service.version": "2.1.45",
                    "user.practice": "Backend Engineering",
                    "user.profile": "a",
                    "user.serial": "SERA",
                },
            }

            api_payload_b = {
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
                    "cache_read_tokens": "10",
                    "cost_usd": "0.50",
                    "duration_ms": "800",
                },
                "scope": {"name": "scope", "version": "v1"},
                "resource": {
                    "host.arch": "x86_64",
                    "host.name": "host-b",
                    "os.type": "linux",
                    "os.version": "6.1.0",
                    "service.name": "claude-code",
                    "service.version": "2.1.45",
                    "user.practice": "Data Engineering",
                    "user.profile": "b",
                    "user.serial": "SERB",
                },
            }

            tool_payload_ok = {
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

            tool_payload_fail = {
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
                    "duration_ms": "500",
                    "success": "false",
                    "tool_name": "Read",
                },
                "scope": {"name": "scope", "version": "v1"},
                "resource": {"user.practice": "Data Engineering"},
            }

            line1 = _make_batch_line(
                [
                    _make_event("e1", 1764756000000, api_payload_a),
                    _make_event("e2", 1764756001000, tool_payload_ok),
                ],
                year=2025,
                month=12,
                day=3,
            )
            line2 = _make_batch_line(
                [
                    _make_event("e3", 1764846000000, api_payload_b),
                    _make_event("e4", 1764846002000, tool_payload_fail),
                ],
                year=2025,
                month=12,
                day=4,
            )

            telemetry_path.write_text(line1 + "\n" + line2 + "\n", encoding="utf-8")

            ingest_telemetry(
                db_path=db_path,
                telemetry_path=telemetry_path,
                employees_path=employees_path,
                replace=True,
                commit_every=10,
            )

            report = build_insights_report(db_path, days=7, min_tool_runs=1)

            self.assertEqual(report["overview"]["total_events"], 4)
            self.assertEqual(report["overview"]["total_sessions"], 2)
            self.assertAlmostEqual(report["overview"]["total_cost_usd"], 2.0, places=4)

            self.assertGreaterEqual(len(report["daily_tokens_by_practice"]), 2)
            self.assertGreaterEqual(len(report["peak_usage_hours"]), 2)
            self.assertEqual(report["tool_performance"][0]["tool_name"], "Read")
            self.assertEqual(report["tool_performance"][0]["runs"], 2)
            self.assertAlmostEqual(report["tool_performance"][0]["success_rate"], 0.5, places=4)
            self.assertEqual(len(report["model_usage"]), 2)


if __name__ == "__main__":
    unittest.main()
