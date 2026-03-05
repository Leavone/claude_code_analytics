# Claude Code Usage Analytics Platform

End-to-end analytics project for Claude Code telemetry data.

## Current status

This repository now includes **Step 1** of the implementation:
- SQLite schema for telemetry facts + employee dimension
- Streaming ingestion pipeline for JSONL telemetry and CSV metadata
- Basic validation and ingestion quality counters
- CLI commands to ingest data and print summary stats
- Unit tests for parser and ingestion path
- Reusable analytics report queries + `insights` CLI command
- Streamlit dashboard with interactive filters and KPI visualizations
- FastAPI endpoints for programmatic analytics access
- Advanced statistical analysis (anomaly detection and distribution/variability metrics)
- Dashboard panel for advanced statistics (anomalies, variability, high-token sessions)

## Project plan

1. Data foundation: schema + ingestion + validation + CLI (done)
2. Analytics layer: reusable SQL queries and KPI computation module (done)
3. Dashboard: Streamlit app with role/time/model/level filters and visualizations (done)
4. Delivery polish: final README, LLM usage log, and presentation outline

## Quickstart

### 1) Install dependencies

```bash
cd claude_code_analytics
python3 -m pip install -r requirements.txt
```

### 2) Run tests (pytest)

```bash
cd claude_code_analytics
python3 -m pytest -q
```

### 3) Ingest telemetry into SQLite

```bash
cd claude_code_analytics
python3 -m analytics_platform.cli ingest \
  --telemetry telemetry_logs.jsonl \
  --employees employees.csv \
  --db artifacts/analytics.db \
  --replace
```

### 4) Print basic stats

```bash
cd claude_code_analytics
python3 -m analytics_platform.cli stats --db artifacts/analytics.db
```

### 5) Generate analytics report JSON

```bash
cd claude_code_analytics
python3 -m analytics_platform.cli insights --db artifacts/analytics.db --days 30 --min-tool-runs 20
```

### 6) Run interactive dashboard

```bash
cd claude_code_analytics
streamlit run streamlit_app.py
```

### 7) Run API service

```bash
cd claude_code_analytics
uvicorn api.main:app --reload
```

Then open:
- Swagger UI: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/health`

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
- Dashboard filters support date range, practice, model, and user scoping.
- Seniority-level analytics are available in both CLI insights and dashboard tables/charts.

## API Endpoints

- `GET /health`
  - Basic service liveness response.
- `GET /api/v1/overview?db=artifacts/analytics.db`
  - Returns top-level KPI snapshot.
- `GET /api/v1/insights?db=artifacts/analytics.db&days=30&min_tool_runs=20`
  - Returns full structured insights payload.
- `GET /api/v1/dashboard/kpis?...`
  - Returns filtered KPI cards.
  - Supports `date_from`, `date_to`, `practices`, `levels`, `models`, `users` as query params (comma-separated for lists).
- `GET /api/v1/seniority?db=artifacts/analytics.db`
  - Returns seniority usage and seniority model usage sections.
- `GET /api/v1/advanced-statistics?db=artifacts/analytics.db&days=30`
  - Returns deeper statistics: daily token z-score anomalies, session token distribution percentiles, practice-level variability, and high-token sessions.

## Assignment Documentation

- [LLM usage log](./LLM_USAGE_LOG.md)
