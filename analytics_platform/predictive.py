"""Lightweight predictive analytics helpers for daily token forecasting.

This module implements a compact ML-style component using linear regression on
daily token totals. The goal is to provide an interpretable forecast baseline
without introducing heavy dependencies or complex training pipelines.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np

FORECAST_TARGET_LABELS: dict[str, str] = {
    "total_tokens": "Total tokens",
    "input_tokens": "Input tokens",
    "output_tokens": "Output tokens",
    "event_count": "Event count",
    "total_cost_usd": "Cost (USD)",
}


def _resolve_target_label(target_metric: str, target_label: str | None) -> str:
    """Validate target metric and return display label.

    Args:
        target_metric: Metric key requested by caller.
        target_label: Optional explicit label override.

    Returns:
        Display label for the selected metric.

    Raises:
        ValueError: If ``target_metric`` is unsupported.
    """
    if target_metric not in FORECAST_TARGET_LABELS:
        valid = ", ".join(sorted(FORECAST_TARGET_LABELS))
        raise ValueError(f"unsupported target_metric '{target_metric}'. choose one of: {valid}")
    return target_label or FORECAST_TARGET_LABELS[target_metric]


def _safe_r2(y_true: np.ndarray, y_pred: np.ndarray) -> float | None:
    """Compute R-squared while handling degenerate variance cases.

    Args:
        y_true: Ground-truth target values.
        y_pred: Predicted target values.

    Returns:
        R-squared value, or ``None`` when it is undefined.
    """
    if y_true.size < 2:
        return None
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    if ss_tot == 0.0:
        return None
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    return 1.0 - (ss_res / ss_tot)


def build_predictive_payload(
    daily_rows: list[dict[str, Any]],
    *,
    forecast_days: int = 7,
    target_metric: str = "total_tokens",
    target_label: str | None = None,
) -> dict[str, Any]:
    """Build predictive analytics output from daily token aggregates.

    Model:
    - simple univariate linear regression over time index;
    - in-sample residual-based anomaly flags (|z| >= 2);
    - future forecast interval using residual standard deviation.

    Args:
        daily_rows: Rows containing at least ``event_date`` (YYYY-MM-DD) and
            the selected target metric.
        forecast_days: Number of future dates to forecast. Values below ``1``
            are normalized to ``1``.
        target_metric: Numeric daily metric column to forecast.
        target_label: Optional human-readable label for UI.

    Returns:
        Dictionary with:
        - ``model_name``: baseline model identifier;
        - ``training_points``: number of historical daily observations;
        - ``metrics``: MAE/RMSE/R2 over in-sample fit;
        - ``history``: actual vs fitted values with anomaly annotations;
        - ``forecast``: point forecast and simple confidence band;
        - ``status``: ``ok`` or ``insufficient_data``.
    """
    resolved_label = _resolve_target_label(target_metric, target_label)
    horizon = max(forecast_days, 1)
    if not daily_rows:
        return {
            "model_name": "Linear trend (OLS)",
            "training_points": 0,
            "forecast_days": horizon,
            "metrics": {"mae": None, "rmse": None, "r2": None},
            "history": [],
            "forecast": [],
            "target_metric": target_metric,
            "target_label": resolved_label,
            "status": "insufficient_data",
        }

    rows = sorted(daily_rows, key=lambda row: str(row.get("event_date") or ""))
    y = np.array([float(row.get(target_metric) or 0.0) for row in rows], dtype=float)
    x = np.arange(y.size, dtype=float)

    if y.size < 2:
        base_date = date.fromisoformat(str(rows[-1]["event_date"]))
        point = max(float(y[0]), 0.0)
        return {
            "model_name": "Linear trend (OLS)",
            "training_points": int(y.size),
            "forecast_days": horizon,
            "metrics": {"mae": 0.0, "rmse": 0.0, "r2": None},
            "target_metric": target_metric,
            "target_label": resolved_label,
            "history": [
                {
                    "event_date": rows[0]["event_date"],
                    "actual_tokens": round(float(y[0]), 3),
                    "fitted_tokens": round(float(y[0]), 3),
                    "residual": 0.0,
                    "residual_zscore": None,
                    "is_anomaly": False,
                }
            ],
            "forecast": [
                {
                    "event_date": (base_date + timedelta(days=i)).isoformat(),
                    "predicted_tokens": round(point, 3),
                    "lower_bound": round(point, 3),
                    "upper_bound": round(point, 3),
                }
                for i in range(1, horizon + 1)
            ],
            "status": "insufficient_data",
        }

    slope, intercept = np.polyfit(x, y, deg=1)
    fitted = slope * x + intercept
    residuals = y - fitted
    resid_std = float(np.std(residuals, ddof=1)) if residuals.size > 1 else 0.0
    resid_z = residuals / resid_std if resid_std > 0 else np.array([np.nan] * residuals.size, dtype=float)

    mae = float(np.mean(np.abs(residuals)))
    rmse = float(np.sqrt(np.mean(residuals**2)))
    r2 = _safe_r2(y, fitted)

    history = []
    for idx, row in enumerate(rows):
        z_val = float(resid_z[idx]) if np.isfinite(resid_z[idx]) else None
        history.append(
            {
                "event_date": row["event_date"],
                "actual_tokens": round(float(y[idx]), 3),
                "fitted_tokens": round(float(fitted[idx]), 3),
                "residual": round(float(residuals[idx]), 3),
                "residual_zscore": round(z_val, 3) if z_val is not None else None,
                "is_anomaly": bool(abs(z_val) >= 2.0) if z_val is not None else False,
            }
        )

    last_date = date.fromisoformat(str(rows[-1]["event_date"]))
    x_future = np.arange(y.size, y.size + horizon, dtype=float)
    y_future = slope * x_future + intercept
    band = 1.96 * resid_std
    forecast = [
        {
            "event_date": (last_date + timedelta(days=idx + 1)).isoformat(),
            "predicted_tokens": round(max(float(pred), 0.0), 3),
            "lower_bound": round(max(float(pred - band), 0.0), 3),
            "upper_bound": round(max(float(pred + band), 0.0), 3),
        }
        for idx, pred in enumerate(y_future)
    ]

    return {
        "model_name": "Linear trend (OLS)",
        "training_points": int(y.size),
        "forecast_days": horizon,
        "target_metric": target_metric,
        "target_label": resolved_label,
        "metrics": {
            "mae": round(mae, 3),
            "rmse": round(rmse, 3),
            "r2": round(float(r2), 4) if r2 is not None else None,
        },
        "history": history,
        "forecast": forecast,
        "status": "ok",
    }
