"""Ingestion pipeline from raw telemetry files into SQLite.

The pipeline reads:
- telemetry batches from JSONL, where each line contains a ``logEvents`` list;
- employee metadata from CSV.

It then flattens nested telemetry payloads into the ``events`` fact table,
upserts ``employees``, and reports quality counters (bad JSON, missing required
fields, inserted rows).
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analytics_platform.db import connect, init_schema, reset_data
from analytics_platform.utils import parse_iso_timestamp, to_bool_int, to_float, to_int


@dataclass
class IngestionStats:
    """Counters summarizing one ingestion run.

    Attributes:
        batches_seen: Number of valid JSONL batch lines processed.
        events_seen: Number of raw events encountered across processed batches.
        events_inserted: Number of event rows successfully inserted.
        bad_batch_json: Number of JSONL lines that failed JSON parsing.
        bad_event_json: Number of event ``message`` payloads with invalid JSON.
        missing_required: Number of events skipped due to missing required fields.
        employees_loaded: Total row count in ``employees`` after upsert.
    """

    batches_seen: int = 0
    events_seen: int = 0
    events_inserted: int = 0
    bad_batch_json: int = 0
    bad_event_json: int = 0
    missing_required: int = 0
    employees_loaded: int = 0


def _build_event_row(batch_line: int, batch: dict[str, Any], event: dict[str, Any]) -> tuple[Any, ...] | None:
    """Transform one raw event envelope into the ``events`` table row tuple.

    Args:
        batch_line: 1-based line number from telemetry JSONL file.
        batch: Parsed JSON object for a telemetry batch.
        event: One item from ``batch['logEvents']``.

    Returns:
        Ordered tuple matching the ``INSERT INTO events (...)`` statement, or
        ``None`` when required fields are missing.

    Raises:
        json.JSONDecodeError: If ``event['message']`` is not valid JSON.
    """
    msg_raw = event.get("message")
    if not isinstance(msg_raw, str):
        return None

    payload = json.loads(msg_raw)
    attrs = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
    scope = payload.get("scope") if isinstance(payload.get("scope"), dict) else {}
    resource = payload.get("resource") if isinstance(payload.get("resource"), dict) else {}

    event_ts = parse_iso_timestamp(attrs.get("event.timestamp"))
    if event_ts is None:
        batch_ts_ms = to_int(event.get("timestamp"))
        if batch_ts_ms is not None:
            event_ts = datetime.fromtimestamp(batch_ts_ms / 1000, tz=timezone.utc)

    user_email = attrs.get("user.email")
    session_id = attrs.get("session.id")
    if event_ts is None or not session_id or not user_email:
        return None

    return (
        event.get("id"),
        batch_line,
        to_int(event.get("timestamp")),
        batch.get("owner"),
        batch.get("logGroup"),
        batch.get("logStream"),
        to_int(batch.get("year")),
        to_int(batch.get("month")),
        to_int(batch.get("day")),
        event_ts.isoformat(),
        event_ts.date().isoformat(),
        event_ts.hour,
        payload.get("body"),
        attrs.get("event.name"),
        attrs.get("organization.id"),
        session_id,
        attrs.get("terminal.type"),
        attrs.get("user.account_uuid"),
        user_email,
        attrs.get("user.id"),
        attrs.get("model"),
        to_int(attrs.get("input_tokens")),
        to_int(attrs.get("output_tokens")),
        to_int(attrs.get("cache_creation_tokens")),
        to_int(attrs.get("cache_read_tokens")),
        to_float(attrs.get("cost_usd")),
        to_int(attrs.get("duration_ms")),
        to_int(attrs.get("prompt_length")),
        attrs.get("tool_name"),
        attrs.get("decision"),
        attrs.get("source"),
        attrs.get("decision_source"),
        attrs.get("decision_type"),
        to_bool_int(attrs.get("success")),
        attrs.get("status_code"),
        attrs.get("error"),
        to_int(attrs.get("attempt")),
        to_int(attrs.get("tool_result_size_bytes")),
        scope.get("name"),
        scope.get("version"),
        resource.get("host.arch"),
        resource.get("host.name"),
        resource.get("os.type"),
        resource.get("os.version"),
        resource.get("service.name"),
        resource.get("service.version"),
        resource.get("user.practice"),
        resource.get("user.profile"),
        resource.get("user.serial"),
    )


def _iter_employee_rows(path: Path) -> Iterable[tuple[str, str | None, str | None, str | None, str | None]]:
    """Yield normalized employee rows from CSV in database insert order.

    Args:
        path: Location of the ``employees.csv`` file.

    Yields:
        Tuples in the order ``(email, full_name, practice, level, location)``.
        Empty optional values are normalized to ``None``.

    Raises:
        ValueError: If required CSV columns are missing.
    """
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"email", "full_name", "practice", "level", "location"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            missing_cols = ", ".join(sorted(missing))
            raise ValueError(f"employees csv is missing required columns: {missing_cols}")

        for row in reader:
            email = (row.get("email") or "").strip()
            if not email:
                continue
            yield (
                email,
                (row.get("full_name") or "").strip() or None,
                (row.get("practice") or "").strip() or None,
                (row.get("level") or "").strip() or None,
                (row.get("location") or "").strip() or None,
            )


def ingest_telemetry(
    db_path: Path,
    telemetry_path: Path,
    employees_path: Path,
    *,
    replace: bool = False,
    commit_every: int = 5000,
) -> IngestionStats:
    """Load telemetry and employee metadata into SQLite.

    Args:
        db_path: Target SQLite database path.
        telemetry_path: JSONL telemetry input path.
        employees_path: Employee metadata CSV input path.
        replace: When ``True``, clears existing ``events`` and ``employees``
            rows before loading.
        commit_every: Buffer size for batched ``events`` inserts.

    Returns:
        ``IngestionStats`` with counters for throughput and data quality.

    Raises:
        FileNotFoundError: If telemetry or employee files are missing.
        ValueError: If employee CSV schema is invalid.
    """
    stats = IngestionStats()

    if not telemetry_path.exists():
        raise FileNotFoundError(f"telemetry file not found: {telemetry_path}")
    if not employees_path.exists():
        raise FileNotFoundError(f"employees file not found: {employees_path}")

    conn = connect(db_path)
    try:
        init_schema(conn)
        if replace:
            reset_data(conn)

        conn.executemany(
            """
            INSERT INTO employees(email, full_name, practice, level, location)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(email) DO UPDATE SET
                full_name=excluded.full_name,
                practice=excluded.practice,
                level=excluded.level,
                location=excluded.location
            """,
            _iter_employee_rows(employees_path),
        )
        stats.employees_loaded = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]

        rows_buffer: list[tuple[Any, ...]] = []
        with telemetry_path.open("r", encoding="utf-8") as handle:
            for batch_line, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    batch = json.loads(line)
                except json.JSONDecodeError:
                    stats.bad_batch_json += 1
                    continue

                stats.batches_seen += 1
                log_events = batch.get("logEvents")
                if not isinstance(log_events, list):
                    continue

                for event in log_events:
                    stats.events_seen += 1
                    try:
                        row = _build_event_row(batch_line, batch, event)
                    except json.JSONDecodeError:
                        stats.bad_event_json += 1
                        continue

                    if row is None:
                        stats.missing_required += 1
                        continue

                    rows_buffer.append(row)
                    if len(rows_buffer) >= commit_every:
                        _flush_event_rows(conn, rows_buffer)
                        stats.events_inserted += len(rows_buffer)
                        rows_buffer.clear()

        if rows_buffer:
            _flush_event_rows(conn, rows_buffer)
            stats.events_inserted += len(rows_buffer)
            rows_buffer.clear()

        conn.commit()
        return stats
    finally:
        conn.close()


def _flush_event_rows(conn, rows: list[tuple[Any, ...]]) -> None:
    """Insert buffered event rows with one batched ``executemany`` call.

    Args:
        conn: Open SQLite connection.
        rows: Pre-flattened event tuples in ``events`` table column order.
    """
    conn.executemany(
        """
        INSERT INTO events (
            event_id, batch_line, batch_timestamp_ms, batch_owner, batch_log_group, batch_log_stream,
            batch_year, batch_month, batch_day, event_timestamp, event_date, event_hour,
            event_body, event_name, organization_id, session_id, terminal_type, user_account_uuid,
            user_email, user_id, model, input_tokens, output_tokens, cache_creation_tokens,
            cache_read_tokens, cost_usd, duration_ms, prompt_length, tool_name, decision,
            source, decision_source, decision_type, success, status_code, error_message,
            attempt, tool_result_size_bytes, scope_name, scope_version, host_arch, host_name,
            os_type, os_version, service_name, service_version, resource_practice,
            resource_profile, resource_serial
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        rows,
    )


def stats_to_dict(stats: IngestionStats) -> dict[str, int]:
    """Convert ``IngestionStats`` dataclass into a plain integer dict.

    Args:
        stats: Ingestion counters object.

    Returns:
        Dictionary representation suitable for JSON serialization.
    """
    return {k: int(v) for k, v in asdict(stats).items()}
