# Claude Code Usage Analytics Platform

End-to-end analytics project for Claude Code telemetry data.

## Current status

This repository now includes **Step 1** of the implementation:
- SQLite schema for telemetry facts + employee dimension
- Streaming ingestion pipeline for JSONL telemetry and CSV metadata
- Basic validation and ingestion quality counters
- CLI commands to ingest data and print summary stats
- Unit tests for parser and ingestion path

## Project plan

1. Data foundation: schema + ingestion + validation + CLI
2. Analytics layer: reusable SQL queries and KPI computation module
3. Dashboard: Streamlit app with role/time/model filters and visualizations
4. Delivery polish: final README, LLM usage log, and presentation outline

## Quickstart

### 1) Run tests (stdlib unittest)

```bash
cd claude_code_analytics
python3 -m unittest discover -s tests -p 'test_*.py'
```

### 2) Ingest telemetry into SQLite

```bash
cd claude_code_analytics
python3 -m analytics_platform.cli ingest \
  --telemetry telemetry_logs.jsonl \
  --employees employees.csv \
  --db artifacts/analytics.db \
  --replace
```

### 3) Print basic stats

```bash
cd claude_code_analytics
python3 -m analytics_platform.cli stats --db artifacts/analytics.db
```

## Data model

### `employees`
- `email` (PK)
- `full_name`
- `practice`
- `level`
- `location`

### `events`
Flattened telemetry events, including:
- event identity and time dimensions (`event_id`, `event_timestamp`, `event_date`, `event_hour`)
- user/session metadata (`session_id`, `user_email`, `terminal_type`, `organization_id`)
- token/cost fields (`input_tokens`, `output_tokens`, `cache_*`, `cost_usd`)
- tool/API behavior fields (`tool_name`, `decision`, `success`, `status_code`, `duration_ms`)
- host/resource/scope metadata

### `events_enriched` (view)
`events` left-joined with `employees` for analytics by role, level, and location.

## Notes

- Ingestion is chunked (`--commit-every`, default `5000`) to keep memory predictable.
- Failed parse cases are counted in ingestion stats for transparency.
