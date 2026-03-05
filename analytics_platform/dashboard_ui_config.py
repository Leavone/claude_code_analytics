"""UI configuration constants for Streamlit dashboard controls."""

from __future__ import annotations

from analytics_platform.predictive import FORECAST_TARGET_LABELS

DAILY_TREND_Y_AXIS_OPTIONS: dict[str, str] = {
    "Total tokens": "total_tokens",
    "Input tokens": "input_tokens",
    "Output tokens": "output_tokens",
    "Event count": "event_count",
    "Cost (USD)": "total_cost_usd",
}

DAILY_TREND_SPLIT_OPTIONS: dict[str, str] = {
    "Overall": "overall",
    "Practice": "practice",
    "Seniority level": "level",
    "Model": "model",
    "Event name": "event_name",
    "Terminal type": "terminal_type",
}

PREDICTIVE_TARGET_OPTIONS: dict[str, str] = {
    label: metric for metric, label in FORECAST_TARGET_LABELS.items()
}
