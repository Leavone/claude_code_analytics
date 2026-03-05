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


def _correlation_strength(abs_r: float) -> str:
    """Map absolute Pearson coefficient to an interpretation label."""
    if abs_r >= 0.8:
        return "very strong"
    if abs_r >= 0.6:
        return "strong"
    if abs_r >= 0.4:
        return "moderate"
    if abs_r >= 0.2:
        return "weak"
    return "very weak"


def _pearson_correlation(x_values: list[float], y_values: list[float]) -> float | None:
    """Compute Pearson correlation for two equally-sized numeric vectors.

    Returns ``None`` when there are too few samples or one vector has no
    variance.
    """
    x_arr = np.array(x_values, dtype=float)
    y_arr = np.array(y_values, dtype=float)
    if x_arr.size < 3 or y_arr.size < 3 or x_arr.size != y_arr.size:
        return None
    if float(np.std(x_arr)) == 0.0 or float(np.std(y_arr)) == 0.0:
        return None
    return float(np.corrcoef(x_arr, y_arr)[0, 1])


def _valid_numeric_pairs(x_values: list[float], y_values: list[float]) -> tuple[list[float], list[float]]:
    """Filter paired vectors to finite numeric values only."""
    x_arr = np.array(x_values, dtype=float)
    y_arr = np.array(y_values, dtype=float)
    mask = np.isfinite(x_arr) & np.isfinite(y_arr)
    return x_arr[mask].tolist(), y_arr[mask].tolist()


def _compute_correlation_analysis(
    daily_rows: list[dict[str, Any]],
    session_rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Compute less-obvious correlation signals for usage efficiency patterns.

    Returns two sections:
    - ``global``: relationships computed over all selected rows.
    - ``by_practice``: per-practice correlations for session-shape metrics.
    """
    session_tokens = [float(row.get("session_tokens") or 0.0) for row in session_rows]
    session_requests = [float(row.get("api_requests") or 0.0) for row in session_rows]
    session_cost = [float(row.get("total_cost_usd") or 0.0) for row in session_rows]
    session_tool_runs = [float(row.get("tool_runs") or 0.0) for row in session_rows]
    session_tool_success_rate = [
        (float(row.get("successful_tool_runs") or 0.0) / tool_runs) if tool_runs > 0 else float("nan")
        for row, tool_runs in zip(session_rows, session_tool_runs)
    ]
    session_cache_read_ratio = [
        (
            float(row.get("cache_read_tokens") or 0.0)
            / (float(row.get("cache_read_tokens") or 0.0) + float(row.get("cache_creation_tokens") or 0.0))
        )
        if (float(row.get("cache_read_tokens") or 0.0) + float(row.get("cache_creation_tokens") or 0.0)) > 0
        else float("nan")
        for row in session_rows
    ]

    daily_tokens = [float(row.get("total_tokens") or 0.0) for row in daily_rows]
    daily_requests = [float(row.get("api_requests") or 0.0) for row in daily_rows]
    daily_sessions = [float(row.get("sessions") or 0.0) for row in daily_rows]

    session_avg_tokens_per_request = [
        (tokens / requests) if requests > 0 else float("nan")
        for tokens, requests in zip(session_tokens, session_requests)
    ]
    session_cost_per_request = [
        (cost / requests) if requests > 0 else float("nan")
        for cost, requests in zip(session_cost, session_requests)
    ]
    session_cost_per_1k_tokens = [
        (cost / tokens * 1000.0) if tokens > 0 else float("nan")
        for cost, tokens in zip(session_cost, session_tokens)
    ]

    daily_avg_tokens_per_request = [
        (tokens / requests) if requests > 0 else float("nan")
        for tokens, requests in zip(daily_tokens, daily_requests)
    ]
    daily_avg_tokens_per_session = [
        (tokens / sessions) if sessions > 0 else float("nan")
        for tokens, sessions in zip(daily_tokens, daily_sessions)
    ]

    global_pairs: list[tuple[str, str, list[float], list[float]]] = [
        (
            "session_api_requests",
            "session_avg_tokens_per_request",
            session_requests,
            session_avg_tokens_per_request,
        ),
        (
            "session_api_requests",
            "session_cost_per_request_usd",
            session_requests,
            session_cost_per_request,
        ),
        (
            "session_tokens",
            "session_cost_per_1k_tokens_usd",
            session_tokens,
            session_cost_per_1k_tokens,
        ),
        (
            "session_tool_runs",
            "session_cost_per_request_usd",
            session_tool_runs,
            session_cost_per_request,
        ),
        (
            "session_tool_success_rate",
            "session_tokens",
            session_tool_success_rate,
            session_tokens,
        ),
        (
            "session_cache_read_ratio",
            "session_cost_per_1k_tokens_usd",
            session_cache_read_ratio,
            session_cost_per_1k_tokens,
        ),
        (
            "daily_api_requests",
            "daily_avg_tokens_per_request",
            daily_requests,
            daily_avg_tokens_per_request,
        ),
        (
            "daily_sessions",
            "daily_avg_tokens_per_session",
            daily_sessions,
            daily_avg_tokens_per_session,
        ),
    ]

    global_output: list[dict[str, Any]] = []
    for metric_x, metric_y, x_values, y_values in global_pairs:
        clean_x, clean_y = _valid_numeric_pairs(x_values, y_values)
        coefficient = _pearson_correlation(clean_x, clean_y)
        if coefficient is None:
            continue
        abs_r = abs(coefficient)
        global_output.append(
            {
                "metric_x": metric_x,
                "metric_y": metric_y,
                "pearson_r": round(coefficient, 4),
                "direction": "positive" if coefficient > 0 else "negative",
                "strength": _correlation_strength(abs_r),
                "sample_size": len(clean_x),
            }
        )
    global_output.sort(key=lambda item: abs(item["pearson_r"]), reverse=True)

    practice_groups: dict[str, list[dict[str, Any]]] = {}
    for row in session_rows:
        practice = row.get("practice") or "Unknown"
        practice_groups.setdefault(practice, []).append(row)

    by_practice_output: list[dict[str, Any]] = []
    practice_pairs = [
        ("session_api_requests", "session_avg_tokens_per_request"),
        ("session_cache_read_ratio", "session_cost_per_1k_tokens_usd"),
    ]
    for practice, rows in practice_groups.items():
        p_requests = [float(row.get("api_requests") or 0.0) for row in rows]
        p_tokens = [float(row.get("session_tokens") or 0.0) for row in rows]
        p_cost = [float(row.get("total_cost_usd") or 0.0) for row in rows]
        p_cache_read = [float(row.get("cache_read_tokens") or 0.0) for row in rows]
        p_cache_create = [float(row.get("cache_creation_tokens") or 0.0) for row in rows]
        p_avg_tokens_per_request = [
            (tokens / requests) if requests > 0 else float("nan")
            for tokens, requests in zip(p_tokens, p_requests)
        ]
        p_cost_per_1k_tokens = [
            (cost / tokens * 1000.0) if tokens > 0 else float("nan")
            for cost, tokens in zip(p_cost, p_tokens)
        ]
        p_cache_read_ratio = [
            (read / (read + create)) if (read + create) > 0 else float("nan")
            for read, create in zip(p_cache_read, p_cache_create)
        ]

        vectors = {
            "session_api_requests": p_requests,
            "session_avg_tokens_per_request": p_avg_tokens_per_request,
            "session_cache_read_ratio": p_cache_read_ratio,
            "session_cost_per_1k_tokens_usd": p_cost_per_1k_tokens,
        }
        for metric_x, metric_y in practice_pairs:
            clean_x, clean_y = _valid_numeric_pairs(vectors[metric_x], vectors[metric_y])
            coefficient = _pearson_correlation(clean_x, clean_y)
            if coefficient is None:
                continue
            by_practice_output.append(
                {
                    "practice": practice,
                    "metric_x": metric_x,
                    "metric_y": metric_y,
                    "pearson_r": round(coefficient, 4),
                    "direction": "positive" if coefficient > 0 else "negative",
                    "strength": _correlation_strength(abs(coefficient)),
                    "sample_size": len(clean_x),
                }
            )
    by_practice_output.sort(key=lambda item: abs(item["pearson_r"]), reverse=True)
    return {"global": global_output, "by_practice": by_practice_output}


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
        - ``correlation_analysis`` (global and by-practice)
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
        "correlation_analysis": _compute_correlation_analysis(
            daily_rows,
            session_rows,
        ),
    }
