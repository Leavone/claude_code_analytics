"""Database schema and connection helpers for the analytics platform.

This module owns SQLite-specific setup for:
- connection pragmas tuned for local analytics workloads;
- idempotent schema creation (tables, indexes, and convenience views);
- reset helpers used by full-refresh ingestion runs.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def connect(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection configured for this project.

    Args:
        db_path: Filesystem location for the database file.

    Returns:
        An open ``sqlite3.Connection`` with:
        - ``sqlite3.Row`` row factory for dict-like access,
        - WAL journal mode for safer concurrent reads,
        - relaxed sync mode (NORMAL) for faster ingestion,
        - foreign key checks enabled,
        - in-memory temp storage to avoid filesystem temp-db issues.

    Side effects:
        Creates parent directories for ``db_path`` when needed.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Create all database objects required by the analytics platform.

    The function is idempotent and can be called safely before every query
    command. It creates:
    - ``employees``: user metadata dimension table;
    - ``events``: flattened telemetry fact table;
    - ``ingestion_runs``: audit log for ingestion executions;
    - several indexes for time/user/session/model filters;
    - ``events_enriched`` view that joins ``events`` with ``employees``.

    Args:
        conn: Open SQLite connection to initialize.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS employees (
            email TEXT PRIMARY KEY,
            full_name TEXT,
            practice TEXT,
            level TEXT,
            location TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            batch_line INTEGER NOT NULL,
            batch_timestamp_ms INTEGER,
            batch_owner TEXT,
            batch_log_group TEXT,
            batch_log_stream TEXT,
            batch_year INTEGER,
            batch_month INTEGER,
            batch_day INTEGER,
            event_timestamp TEXT NOT NULL,
            event_date TEXT NOT NULL,
            event_hour INTEGER NOT NULL,
            event_body TEXT NOT NULL,
            event_name TEXT,
            organization_id TEXT,
            session_id TEXT NOT NULL,
            terminal_type TEXT,
            user_account_uuid TEXT,
            user_email TEXT NOT NULL,
            user_id TEXT,
            model TEXT,
            input_tokens INTEGER,
            output_tokens INTEGER,
            cache_creation_tokens INTEGER,
            cache_read_tokens INTEGER,
            cost_usd REAL,
            duration_ms INTEGER,
            prompt_length INTEGER,
            tool_name TEXT,
            decision TEXT,
            source TEXT,
            decision_source TEXT,
            decision_type TEXT,
            success INTEGER,
            status_code TEXT,
            error_message TEXT,
            attempt INTEGER,
            tool_result_size_bytes INTEGER,
            scope_name TEXT,
            scope_version TEXT,
            host_arch TEXT,
            host_name TEXT,
            os_type TEXT,
            os_version TEXT,
            service_name TEXT,
            service_version TEXT,
            resource_practice TEXT,
            resource_profile TEXT,
            resource_serial TEXT
        );

        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            telemetry_path TEXT NOT NULL,
            employees_path TEXT NOT NULL,
            replace_mode INTEGER NOT NULL,
            batches_seen INTEGER NOT NULL DEFAULT 0,
            events_seen INTEGER NOT NULL DEFAULT 0,
            events_inserted INTEGER NOT NULL DEFAULT 0,
            bad_batch_json INTEGER NOT NULL DEFAULT 0,
            bad_event_json INTEGER NOT NULL DEFAULT 0,
            missing_required INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(event_timestamp);
        CREATE INDEX IF NOT EXISTS idx_events_date_hour ON events(event_date, event_hour);
        CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_email);
        CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
        CREATE INDEX IF NOT EXISTS idx_events_body ON events(event_body);
        CREATE INDEX IF NOT EXISTS idx_events_model ON events(model);

        CREATE VIEW IF NOT EXISTS events_enriched AS
        SELECT
            e.*,
            emp.full_name,
            COALESCE(emp.practice, e.resource_practice) AS employee_practice,
            emp.level AS employee_level,
            emp.location AS employee_location
        FROM events e
        LEFT JOIN employees emp ON emp.email = e.user_email;
        """
    )


def reset_data(conn: sqlite3.Connection) -> None:
    """Delete fact and dimension rows to prepare a full refresh ingestion.

    This helper clears ``events`` and ``employees`` and commits immediately.
    Schema objects (tables/indexes/views) are preserved.

    Args:
        conn: Open SQLite connection.
    """
    conn.execute("DELETE FROM events;")
    conn.execute("DELETE FROM employees;")
    conn.commit()
