<p align="center">
  <img src="./assets/project-logo.png" alt="Claude Code Analytics logo" width="250" />
</p>

<p align="center">
  <a href="https://www.linkedin.com/in/levon-avetisyan-41a680204/">LinkedIn</a> |
  <a href="mailto:lev.avyan17@gmail.com">Gmail</a>
</p>

# Claude Code Usage Analytics Platform

**By Levon Avetisyan**

End-to-end analytics platform for Claude Code telemetry: ingestion, storage, analytics SQL, dashboard visualization, and API access.

## What This Project Delivers

- Streaming-style ingestion from JSONL telemetry + employee CSV metadata.
- SQLite analytical store with normalized schema and enriched view.
- Reusable analytics layer (SQL files + Python query wrappers).
- Streamlit dashboard with interactive filters, advanced statistics, and ML-style forecasting.
- FastAPI service exposing analytics endpoints for programmatic use.
- Pytest coverage for ingestion, analytics, dashboard helpers, and API behavior.

## Detailed Setup Instructions

### 1. Prerequisites

- Python `3.11+` (tested with `3.13`)
- `pip`

### 2. Install dependencies

```bash
cd claude_code_analytics
python3 -m pip install -r requirements.txt
```

### 3. Build or refresh the analytics database

```bash
cd claude_code_analytics
python3 -m analytics_platform.cli ingest \
  --telemetry telemetry_logs.jsonl \
  --employees employees.csv \
  --db artifacts/analytics.db \
  --replace
```

Useful ingestion flags:
- `--replace`: recreate `events` and `employees`.
- `--commit-every 5000`: chunk commit size (default `5000`).

### 4. Validate data load

```bash
cd claude_code_analytics
python3 -m analytics_platform.cli stats --db artifacts/analytics.db
```

### 5. Run analytics from CLI

```bash
cd claude_code_analytics
python3 -m analytics_platform.cli insights \
  --db artifacts/analytics.db \
  --days 30 \
  --min-tool-runs 20
```

### 6. Run dashboard

```bash
cd claude_code_analytics
streamlit run streamlit_app.py
```

### 7. Run API

```bash
cd claude_code_analytics
uvicorn api.main:app --reload
```

API docs:
- Swagger UI: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/health`

### 8. Run tests

```bash
cd claude_code_analytics
python3 -m pytest -q
```

## Architecture Overview

### High-level flow

1. Raw telemetry events (`telemetry_logs.jsonl`) and user metadata (`employees.csv`) are ingested.
2. Ingestion parses nested event payloads, validates/coerces fields, and writes into SQLite tables.
3. SQL query files in `analytics_platform/sql/` produce reusable analytical datasets.
4. Two presentation layers consume the same query modules:
   - Streamlit dashboard (`streamlit_app.py`)
   - FastAPI service (`api/main.py`)

### Main modules

- `analytics_platform/ingestion.py`: parsing, validation, coercion, batch inserts.
- `analytics_platform/db.py`: database connection and schema initialization.
- `analytics_platform/analytics.py`: insights-oriented analytics queries.
- `analytics_platform/dashboard.py`: filter-aware dashboard queries.
- `analytics_platform/advanced_stats.py`: anomaly/variability/correlation computations.
- `analytics_platform/predictive.py`: forecasting and residual anomaly logic.
- `analytics_platform/sql/insights/*.sql`: SQL used by CLI/API insights.
- `analytics_platform/sql/dashboard/*.sql`: SQL used by dashboard filters/widgets.
- `api/main.py`: HTTP API endpoints over analytics functions.
- `streamlit_app.py`: dashboard UI composition and interaction logic.

### Data model

`employees` dimension:
- `email` (PK)
- `full_name`
- `practice`
- `level`
- `location`

`events` fact table:
- Event identity/time: `event_id`, `event_timestamp`, `event_date`, `event_hour`
- User/session: `session_id`, `user_email`, `organization_id`, `terminal_type`
- Usage/cost: `input_tokens`, `output_tokens`, `cache_creation_tokens`, `cache_read_tokens`, `cost_usd`
- Tool/API behavior: `tool_name`, `decision`, `success`, `status_code`, `duration_ms`
- Host/scope/resource metadata fields

`events_enriched` view:
- Left join `events` to `employees` to enable segmentation by practice/level/location.

## Dependencies

From `requirements.txt`:

- `pytest>=8.0,<9.0` for test execution
- `numpy>=2.0,<3.0` for numeric/statistical operations
- `pandas>=2.2,<3.0` for tabular transformations
- `streamlit>=1.40,<2.0` for dashboard UI
- `fastapi>=0.115,<1.0` for API layer
- `uvicorn>=0.30,<1.0` ASGI server for running API
- `httpx>=0.27,<1.0` required by `fastapi.testclient`/API tests

## API Endpoints

- `GET /health`
- `GET /api/v1/overview?db=artifacts/analytics.db`
- `GET /api/v1/insights?db=artifacts/analytics.db&days=30&min_tool_runs=20`
- `GET /api/v1/dashboard/kpis?...`
- `GET /api/v1/seniority?db=artifacts/analytics.db`
- `GET /api/v1/advanced-statistics?db=artifacts/analytics.db&days=30`
- `GET /api/v1/predictive?db=artifacts/analytics.db&days=90&forecast_days=7&target_metric=total_tokens`
