"""Reusable advanced-statistics computations for analytics outputs.

This module contains pure, data-oriented helper functions that transform
already-fetched rows into advanced statistical sections. Query execution
remains in caller modules (for example ``analytics.py`` and ``dashboard.py``),
while this module focuses on metric computation and result shaping.
"""

from __future__ import annotations

from typing import Any

import numpy as np


def _compute_daily_token_anomalies(
    daily_rows: list[dict[str, Any]],
    *,
    window_days: int | None = None,
) -> dict[str, Any]:
    """Compute daily token z-score anomaly summary.

    Args:
        daily_rows: Daily aggregate rows with ``event_date``, ``total_tokens``,
            ``api_requests``, and ``sessions``.
        window_days: Optional trailing window value to include in output.

    Returns:
        Dictionary with mean/std metrics and list of anomaly records where
        absolute z-score is at least ``2.0``.
    """
    daily_values = np.array([float(row.get("total_tokens") or 0.0) for row in daily_rows], dtype=float)
    mean_daily = float(np.mean(daily_values)) if daily_values.size > 0 else 0.0
    std_daily = float(np.std(daily_values, ddof=1)) if daily_values.size > 1 else 0.0

    anomalies: list[dict[str, Any]] = []
    if std_daily > 0:
        for row in daily_rows:
            total_tokens = float(row.get("total_tokens") or 0.0)
            z_score = (total_tokens - mean_daily) / std_daily
            if abs(z_score) >= 2.0:
                anomalies.append(
                    {
                        "event_date": row.get("event_date"),
                        "total_tokens": int(total_tokens),
                        "z_score": round(z_score, 3),
                        "api_requests": row.get("api_requests"),
                        "sessions": row.get("sessions"),
                    }
                )

    payload = {
        "mean_daily_tokens": round(mean_daily, 3),
        "std_daily_tokens": round(std_daily, 3),
        "anomaly_threshold_abs_z": 2.0,
        "anomalies": anomalies,
    }
    if window_days is not None:
        payload["window_days"] = max(window_days, 1)
    return payload


def _compute_session_distribution(session_rows: list[dict[str, Any]]) -> tuple[dict[str, Any], float]:
    """Compute session-token distribution metrics and p95 threshold.

    Args:
        session_rows: Session aggregate rows with at least ``session_tokens``.

    Returns:
        Tuple ``(distribution, p95_threshold)`` where ``distribution`` contains
        count/centrality/quantile metrics.
    """
    session_values = np.array(
        sorted(float(row.get("session_tokens") or 0.0) for row in session_rows),
        dtype=float,
    )
    p95_threshold = 0.0
    if session_values.size == 0:
        return (
            {
                "session_count": 0,
                "min": 0.0,
                "mean": 0.0,
                "median": 0.0,
                "p90": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "max": 0.0,
                "iqr": 0.0,
            },
            p95_threshold,
        )

    p25 = float(np.percentile(session_values, 25))
    p95_threshold = float(np.percentile(session_values, 95))
    distribution = {
        "session_count": int(session_values.size),
        "min": round(float(session_values[0]), 3),
        "mean": round(float(np.mean(session_values)), 3),
        "median": round(float(np.percentile(session_values, 50)), 3),
        "p90": round(float(np.percentile(session_values, 90)), 3),
        "p95": round(p95_threshold, 3),
        "p99": round(float(np.percentile(session_values, 99)), 3),
        "max": round(float(session_values[-1]), 3),
        "iqr": round(float(np.percentile(session_values, 75)) - p25, 3),
    }
    return distribution, p95_threshold


def _compute_practice_variability(session_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compute per-practice variability over session token totals.

    Args:
        session_rows: Session aggregate rows containing ``practice`` and
            ``session_tokens``.

    Returns:
        Descending-sorted list of practice variability records.
    """
    practice_groups: dict[str, list[float]] = {}
    for row in session_rows:
        practice = row.get("practice") or "Unknown"
        practice_groups.setdefault(practice, []).append(float(row.get("session_tokens") or 0.0))

    practice_variability: list[dict[str, Any]] = []
    for practice, values in practice_groups.items():
        if not values:
            continue
        values_arr = np.array(values, dtype=float)
        mean_val = float(np.mean(values_arr))
        std_val = float(np.std(values_arr, ddof=1)) if values_arr.size > 1 else 0.0
        cv = (std_val / mean_val) if mean_val > 0 else 0.0
        practice_variability.append(
            {
                "practice": practice,
                "session_count": len(values),
                "mean_session_tokens": round(mean_val, 3),
                "std_session_tokens": round(std_val, 3),
                "coefficient_of_variation": round(cv, 4),
            }
        )

    practice_variability.sort(
        key=lambda item: (
            item["coefficient_of_variation"],
            item["mean_session_tokens"],
        ),
        reverse=True,
    )
    return practice_variability


def _select_high_token_sessions(
    session_rows: list[dict[str, Any]],
    *,
    p95_threshold: float,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Select high-token sessions using p95 threshold.

    Args:
        session_rows: Session aggregate rows.
        p95_threshold: Numeric p95 token threshold.
        limit: Maximum number of output rows.

    Returns:
        List of high-token session records.
    """
    if p95_threshold <= 0.0:
        return []
    return [
        {
            "session_id": row.get("session_id"),
            "practice": row.get("practice") or "Unknown",
            "session_tokens": int(row.get("session_tokens") or 0),
            "api_requests": row.get("api_requests"),
            "total_cost_usd": round(float(row.get("total_cost_usd") or 0.0), 4),
        }
        for row in session_rows
        if float(row.get("session_tokens") or 0.0) >= p95_threshold
    ][: max(limit, 1)]


def build_advanced_statistics_payload(
    daily_rows: list[dict[str, Any]],
    session_rows: list[dict[str, Any]],
    *,
    window_days: int | None = None,
) -> dict[str, Any]:
    """Build full advanced-statistics payload from query output rows.

    Args:
        daily_rows: Day-level aggregate rows.
        session_rows: Session-level aggregate rows.
        window_days: Optional trailing window value to include in anomaly
            section (used by insights report).

    Returns:
        Dictionary with sections:
        - ``daily_token_anomalies``
        - ``session_token_distribution``
        - ``practice_variability``
        - ``high_token_sessions``
    """
    distribution, p95_threshold = _compute_session_distribution(session_rows)
    return {
        "daily_token_anomalies": _compute_daily_token_anomalies(
            daily_rows,
            window_days=window_days,
        ),
        "session_token_distribution": distribution,
        "practice_variability": _compute_practice_variability(session_rows),
        "high_token_sessions": _select_high_token_sessions(
            session_rows,
            p95_threshold=p95_threshold,
        ),
    }
