"""Command-line interface for the analytics platform.

Commands currently exposed:
- ``ingest``: load raw telemetry + employee metadata into SQLite;
- ``stats``: print a compact database health and volume summary.
- ``insights``: output a reusable analytics report in JSON format.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analytics_platform.analytics import build_insights_report
from analytics_platform.db import connect, init_schema
from analytics_platform.ingestion import ingest_telemetry, stats_to_dict


def _build_parser() -> argparse.ArgumentParser:
    """Create the root argument parser and register subcommands.

    Returns:
        Configured ``argparse.ArgumentParser`` instance.
    """
    parser = argparse.ArgumentParser(description="Claude Code analytics platform")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="ingest telemetry into sqlite")
    ingest_parser.add_argument("--telemetry", type=Path, default=Path("telemetry_logs.jsonl"))
    ingest_parser.add_argument("--employees", type=Path, default=Path("employees.csv"))
    ingest_parser.add_argument("--db", type=Path, default=Path("artifacts/analytics.db"))
    ingest_parser.add_argument("--replace", action="store_true", help="replace existing facts/dimensions")
    ingest_parser.add_argument("--commit-every", type=int, default=5000)

    stats_parser = subparsers.add_parser("stats", help="print basic db summary")
    stats_parser.add_argument("--db", type=Path, default=Path("artifacts/analytics.db"))

    insights_parser = subparsers.add_parser("insights", help="print analytics report as json")
    insights_parser.add_argument("--db", type=Path, default=Path("artifacts/analytics.db"))
    insights_parser.add_argument("--days", type=int, default=30)
    insights_parser.add_argument("--min-tool-runs", type=int, default=20)

    return parser


def run_ingest(args: argparse.Namespace) -> int:
    """Execute the ``ingest`` command flow.

    Steps:
    - run ingestion and capture counters;
    - write a run audit row into ``ingestion_runs``;
    - print stats as JSON to stdout.

    Args:
        args: Parsed CLI namespace from ``_build_parser``.

    Returns:
        Process-style exit code (``0`` on success).
    """
    started = datetime.now(tz=timezone.utc)
    stats = ingest_telemetry(
        db_path=args.db,
        telemetry_path=args.telemetry,
        employees_path=args.employees,
        replace=args.replace,
        commit_every=args.commit_every,
    )
    finished = datetime.now(tz=timezone.utc)

    conn = connect(args.db)
    try:
        init_schema(conn)
        conn.execute(
            """
            INSERT INTO ingestion_runs (
                started_at, finished_at, telemetry_path, employees_path, replace_mode,
                batches_seen, events_seen, events_inserted, bad_batch_json, bad_event_json,
                missing_required, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                started.isoformat(),
                finished.isoformat(),
                str(args.telemetry),
                str(args.employees),
                int(bool(args.replace)),
                stats.batches_seen,
                stats.events_seen,
                stats.events_inserted,
                stats.bad_batch_json,
                stats.bad_event_json,
                stats.missing_required,
                f"employees_loaded={stats.employees_loaded}",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    print(json.dumps(stats_to_dict(stats), indent=2))
    return 0


def run_stats(args: argparse.Namespace) -> int:
    """Execute the ``stats`` command to summarize database content.

    Args:
        args: Parsed CLI namespace containing the ``--db`` path.

    Returns:
        Process-style exit code (``0`` on success).

    Raises:
        SystemExit: If SQL queries fail due to missing/invalid schema.
    """
    conn = connect(args.db)
    try:
        init_schema(conn)
        summary = {
            "employees": conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0],
            "events": conn.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "sessions": conn.execute("SELECT COUNT(DISTINCT session_id) FROM events").fetchone()[0],
            "users": conn.execute("SELECT COUNT(DISTINCT user_email) FROM events").fetchone()[0],
        }
        by_type = conn.execute(
            """
            SELECT event_body, COUNT(*) AS cnt
            FROM events
            GROUP BY event_body
            ORDER BY cnt DESC
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise SystemExit(f"failed to query db: {exc}") from exc
    finally:
        conn.close()

    print(json.dumps(summary, indent=2))
    print("event_counts:")
    for row in by_type:
        print(f"  {row['event_body']}: {row['cnt']}")
    return 0


def run_insights(args: argparse.Namespace) -> int:
    """Execute the ``insights`` command and print a structured JSON report."""
    report = build_insights_report(
        db_path=args.db,
        days=args.days,
        min_tool_runs=args.min_tool_runs,
    )
    print(json.dumps(report, indent=2))
    return 0


def main() -> int:
    """CLI entrypoint used by ``python -m analytics_platform.cli``.

    Returns:
        Exit code from the selected command handler.
    """
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "ingest":
        return run_ingest(args)
    if args.command == "stats":
        return run_stats(args)
    if args.command == "insights":
        return run_insights(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
