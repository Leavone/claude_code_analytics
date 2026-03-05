from __future__ import annotations

from analytics_platform.predictive import build_predictive_payload


def test_build_predictive_payload_linear_trend() -> None:
    daily_rows = [
        {"event_date": "2025-12-01", "total_tokens": 100},
        {"event_date": "2025-12-02", "total_tokens": 120},
        {"event_date": "2025-12-03", "total_tokens": 140},
        {"event_date": "2025-12-04", "total_tokens": 160},
        {"event_date": "2025-12-05", "total_tokens": 180},
    ]
    payload = build_predictive_payload(daily_rows, forecast_days=3)

    assert payload["status"] == "ok"
    assert payload["training_points"] == 5
    assert payload["forecast_days"] == 3
    assert len(payload["history"]) == 5
    assert len(payload["forecast"]) == 3

    predicted = [row["predicted_tokens"] for row in payload["forecast"]]
    assert predicted[0] <= predicted[1] <= predicted[2]
