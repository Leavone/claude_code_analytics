"""Streamlit dashboard for Claude Code usage analytics.

The app provides an interactive UI for exploring telemetry by date, practice,
model, and user. It relies on ``analytics_platform.dashboard`` for filtered
query logic and uses SQLite as the source of truth.
"""

from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from analytics_platform.dashboard import (
    DashboardFilters,
    get_advanced_statistics,
    get_daily_trend,
    get_filter_options,
    get_hourly_usage,
    get_kpis,
    get_model_usage,
    get_predictive_analytics,
    get_seniority_usage,
    get_tool_usage,
    get_top_users_by_tokens,
)
from analytics_platform.db import connect, init_schema
from analytics_platform.dashboard_ui_config import (
    DAILY_TREND_SPLIT_OPTIONS,
    DAILY_TREND_Y_AXIS_OPTIONS,
    PREDICTIVE_TARGET_OPTIONS,
)


st.set_page_config(page_title="Claude Code Analytics", page_icon="📊", layout="wide")
st.title("Claude Code Usage Analytics")
st.caption("Interactive telemetry dashboard for usage, cost, and behavior patterns")


def _safe_df(rows: list[dict]) -> pd.DataFrame:
    """Convert row dictionaries to a DataFrame while handling empty results.

    Args:
        rows: Query output records.

    Returns:
        A pandas DataFrame. Empty DataFrame when ``rows`` is empty.
    """
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _normalize_date_range(date_range) -> tuple[str, str]:
    """Normalize Streamlit date input into inclusive ISO date boundaries.

    Args:
        date_range: Value returned by ``st.date_input``. It can be one date or
            a tuple of two dates.

    Returns:
        Tuple ``(date_from, date_to)`` formatted as ``YYYY-MM-DD``.
    """
    if isinstance(date_range, tuple):
        if len(date_range) == 0:
            raise ValueError("date range cannot be empty")
        if len(date_range) == 1:
            single = date_range[0].isoformat()
            return single, single
        start = date_range[0].isoformat()
        end = date_range[1].isoformat()
        return (start, end) if start <= end else (end, start)
    single = date_range.isoformat()
    return single, single


def _reset_filters(min_date, max_date) -> None:
    """Reset all sidebar filter widgets to their default values.

    Args:
        min_date: Earliest available event date.
        max_date: Latest available event date.
    """
    st.session_state["date_range"] = (
        pd.to_datetime(min_date).date(),
        pd.to_datetime(max_date).date(),
    )
    st.session_state["selected_practices"] = []
    st.session_state["selected_levels"] = []
    st.session_state["selected_models"] = []
    st.session_state["selected_users"] = []


def _render_daily_trend(conn, filters: DashboardFilters) -> None:
    """Render daily trend chart section with metric/split controls."""
    st.subheader("Daily Trend")
    trend_controls_left, trend_controls_right = st.columns(2)
    with trend_controls_left:
        selected_y_axis_label = st.selectbox(
            "Y-axis metric",
            options=list(DAILY_TREND_Y_AXIS_OPTIONS.keys()),
            index=0,
            key="daily_trend_y_axis",
        )
    with trend_controls_right:
        selected_split_label = st.selectbox(
            "Split lines by",
            options=list(DAILY_TREND_SPLIT_OPTIONS.keys()),
            index=1,
            key="daily_trend_split_by",
        )
    selected_split = DAILY_TREND_SPLIT_OPTIONS[selected_split_label]

    daily_df = _safe_df(
        get_daily_trend(
            conn,
            filters,
            group_by=selected_split,
            max_groups=None,
        )
    )
    if daily_df.empty:
        st.info("No data for selected filters.")
        return

    selected_y_axis = DAILY_TREND_Y_AXIS_OPTIONS[selected_y_axis_label]
    pivot = daily_df.pivot(index="event_date", columns="group_value", values=selected_y_axis).fillna(0)
    st.line_chart(pivot)


def _render_seniority_breakdown(conn, filters: DashboardFilters) -> None:
    """Render seniority chart and table with natural level ordering."""
    st.subheader("Seniority Level Breakdown")
    level_df = _safe_df(get_seniority_usage(conn, filters))
    if level_df.empty:
        st.info("No data for selected filters.")
        return

    level_num = pd.to_numeric(
        level_df["level"].astype(str).str.strip().str.upper().str.extract(r"(\d+)")[0],
        errors="coerce",
    )
    level_df = (
        level_df.assign(_level_num=level_num.fillna(10**9))
        .sort_values(by=["_level_num", "level"], kind="stable")
    )
    level_order = level_df["level"].tolist()
    chart_col, table_col = st.columns((1, 2))
    with chart_col:
        level_chart = (
            alt.Chart(level_df)
            .mark_bar()
            .encode(
                x=alt.X("level:N", sort=level_order, title="Level"),
                y=alt.Y("total_tokens:Q", title="Total Tokens"),
                tooltip=["level", "events", "sessions", "total_tokens", "total_cost_usd"],
            )
        )
        st.altair_chart(level_chart, width="stretch")
    with table_col:
        st.dataframe(level_df.drop(columns=["_level_num"]), width="stretch")


def _render_advanced_statistics(conn, filters: DashboardFilters) -> None:
    """Render advanced statistics cards, tables, and correlation notes."""
    st.subheader("Advanced Statistical Analysis")
    advanced_stats = get_advanced_statistics(conn, filters)
    dist = advanced_stats["session_token_distribution"]
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Median Session Tokens", f"{dist.get('median', 0):,.0f}")
    metric_col2.metric("P95 Session Tokens", f"{dist.get('p95', 0):,.0f}")
    metric_col3.metric("Session IQR", f"{dist.get('iqr', 0):,.0f}")
    metric_col4.metric("Session Count", f"{int(dist.get('session_count', 0)):,}")
    with st.expander("How to read these metrics", expanded=False):
        st.markdown(
            "- **Median Session Tokens**: typical session size.\n"
            "- **P95 Session Tokens**: top-5% high-usage threshold.\n"
            "- **Session IQR**: spread of the middle 50% of sessions.\n"
            "- **Daily anomalies**: days with |z-score| >= 2.\n"
            "- **Variability (CV)**: standard deviation / mean; higher means less predictable usage.\n"
            "- **Correlation analysis**: focuses on efficiency-style relationships (for example requests vs avg tokens/request), "
            "not only obvious volume-to-volume links."
        )

    anomalies_tab, variability_tab, high_sessions_tab, correlations_tab = st.tabs(
        ["Daily Anomalies", "Practice Variability", "High-Token Sessions", "Correlations"]
    )

    with anomalies_tab:
        anomalies_df = _safe_df(advanced_stats["daily_token_anomalies"]["anomalies"])
        if anomalies_df.empty:
            st.info("No token anomalies detected for current filters.")
        else:
            st.dataframe(anomalies_df, width="stretch")

    with variability_tab:
        variability_df = _safe_df(advanced_stats["practice_variability"])
        if variability_df.empty:
            st.info("Not enough session data for variability metrics.")
        else:
            st.bar_chart(variability_df.set_index("practice")["coefficient_of_variation"])
            st.dataframe(variability_df, width="stretch")

    with high_sessions_tab:
        high_sessions_df = _safe_df(advanced_stats["high_token_sessions"])
        if high_sessions_df.empty:
            st.info("No high-token sessions for current filters.")
        else:
            st.dataframe(high_sessions_df, width="stretch")

    with correlations_tab:
        st.markdown(
            "**What these correlations mean**\n"
            "- `session_api_requests` vs `session_avg_tokens_per_request`: do sessions with more requests also have bigger requests on average?\n"
            "- `session_api_requests` vs `session_cost_per_request_usd`: as sessions include more requests, does average cost per request change?\n"
            "- `session_tokens` vs `session_cost_per_1k_tokens_usd`: do larger sessions use a cheaper or more expensive model mix per 1K tokens?\n"
            "- `session_tool_runs` vs `session_cost_per_request_usd`: when tools are used more, does each request become cheaper or more expensive?\n"
            "- `session_tool_success_rate` vs `session_tokens`: do more successful tool runs happen in bigger or smaller sessions?\n"
            "- `session_cache_read_ratio` vs `session_cost_per_1k_tokens_usd`: when cache reuse is higher, does cost per 1K tokens go down?\n"
            "- `daily_api_requests` vs `daily_avg_tokens_per_request`: on high-traffic days, are requests shorter or longer on average?\n"
            "- `daily_sessions` vs `daily_avg_tokens_per_session`: on days with more sessions, are sessions lighter or heavier on average?\n"
        )
        corr_global_df = _safe_df(advanced_stats["correlation_analysis"]["global"])
        corr_practice_df = _safe_df(advanced_stats["correlation_analysis"]["by_practice"])
        if corr_global_df.empty and corr_practice_df.empty:
            st.info("Not enough data points to compute stable correlations.")
        else:
            st.caption("Global efficiency correlations")
            if corr_global_df.empty:
                st.info("No global correlation metrics available for current filters.")
            else:
                st.dataframe(corr_global_df, width="stretch")

            st.caption("By-practice correlations: request depth and cache-efficiency signals")
            if corr_practice_df.empty:
                st.info("No per-practice correlation metrics available for current filters.")
            else:
                st.dataframe(corr_practice_df, width="stretch")


def _render_predictive_analytics(conn, filters: DashboardFilters) -> None:
    """Render ML forecasting controls, fit metrics, and forecast chart."""
    st.subheader("Predictive Analytics (ML)")
    pred_col1, pred_col2 = st.columns(2)
    with pred_col1:
        forecast_target_label = st.selectbox(
            "Forecast target",
            options=list(PREDICTIVE_TARGET_OPTIONS.keys()),
            index=0,
            key="predictive_target_metric",
        )
    with pred_col2:
        forecast_days = st.slider("Forecast horizon (days)", min_value=3, max_value=30, value=7, step=1)

    predictive = get_predictive_analytics(
        conn,
        filters,
        forecast_days=forecast_days,
        target_metric=PREDICTIVE_TARGET_OPTIONS[forecast_target_label],
        target_label=forecast_target_label,
    )

    metrics = predictive.get("metrics", {})
    mcol1, mcol2, mcol3 = st.columns(3)
    mcol1.metric("Forecast model", str(predictive.get("model_name", "n/a")))
    mcol2.metric("Training points", f"{int(predictive.get('training_points', 0)):,}")
    mcol3.metric("R²", "n/a" if metrics.get("r2") is None else f"{float(metrics['r2']):.3f}")
    st.caption(f"Target series: {predictive.get('target_label', forecast_target_label)}")

    history_df = _safe_df(predictive.get("history", []))
    forecast_df = _safe_df(predictive.get("forecast", []))
    if history_df.empty:
        st.info("Not enough historical data for forecasting in current filters.")
        return

    hist_plot = history_df[["event_date", "actual_tokens", "fitted_tokens"]].rename(
        columns={"actual_tokens": "actual", "fitted_tokens": "fitted"}
    )
    if forecast_df.empty:
        chart_df = hist_plot
    else:
        pred_plot = forecast_df[["event_date", "predicted_tokens"]].rename(columns={"predicted_tokens": "forecast"})
        chart_df = hist_plot.merge(pred_plot, on="event_date", how="outer")
    chart_df = chart_df.set_index("event_date")
    st.line_chart(chart_df)

    anomaly_df = history_df[history_df["is_anomaly"] == True]  # noqa: E712
    if anomaly_df.empty:
        st.caption("No model-residual anomalies detected in training window.")
    else:
        st.caption("Residual anomalies (|z| >= 2) from fitted trend:")
        st.dataframe(anomaly_df, width="stretch")


with st.sidebar:
    st.header("Filters")
    db_path = st.text_input("Database path", value="artifacts/analytics.db")

if not Path(db_path).exists():
    st.error(f"Database not found: {db_path}")
    st.stop()

conn = connect(Path(db_path))
init_schema(conn)

options = get_filter_options(conn)
min_date = options["min_date"]
max_date = options["max_date"]

if min_date is None or max_date is None:
    st.warning("No data available yet. Run ingestion first.")
    st.stop()

with st.sidebar:
    if "date_range" not in st.session_state:
        st.session_state["date_range"] = (
            pd.to_datetime(min_date).date(),
            pd.to_datetime(max_date).date(),
        )
    if "selected_practices" not in st.session_state:
        st.session_state["selected_practices"] = []
    if "selected_levels" not in st.session_state:
        st.session_state["selected_levels"] = []
    if "selected_models" not in st.session_state:
        st.session_state["selected_models"] = []
    if "selected_users" not in st.session_state:
        st.session_state["selected_users"] = []

    if st.button("Reset filters"):
        _reset_filters(min_date, max_date)
        st.rerun()

    date_range = st.date_input(
        "Date range",
        min_value=pd.to_datetime(min_date).date(),
        max_value=pd.to_datetime(max_date).date(),
        key="date_range",
    )

    selected_practices = st.multiselect(
        "Practices",
        options["practices"],
        key="selected_practices",
    )
    selected_levels = st.multiselect(
        "Seniority levels",
        options["levels"],
        key="selected_levels",
    )
    selected_models = st.multiselect(
        "Models",
        options["models"],
        key="selected_models",
    )

    top_users = options["users"][:30]
    selected_users = st.multiselect(
        "Users (top 30 listed)",
        top_users,
        key="selected_users",
    )

date_from, date_to = _normalize_date_range(date_range)

filters = DashboardFilters(
    date_from=date_from,
    date_to=date_to,
    practices=selected_practices or None,
    levels=selected_levels or None,
    models=selected_models or None,
    users=selected_users or None,
)

kpis = get_kpis(conn, filters)
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Events", f"{int(kpis.get('events', 0)):,}")
col2.metric("Sessions", f"{int(kpis.get('sessions', 0)):,}")
col3.metric("Users", f"{int(kpis.get('users', 0)):,}")
col4.metric("Tokens", f"{int(kpis.get('total_tokens', 0)):,}")
col5.metric("Cost (USD)", f"${float(kpis.get('total_cost_usd', 0)):.2f}")

st.divider()

left, right = st.columns((2, 1))

with left:
    _render_daily_trend(conn, filters)

with right:
    st.subheader("Peak Usage Hours")
    hourly_df = _safe_df(get_hourly_usage(conn, filters))
    if hourly_df.empty:
        st.info("No data for selected filters.")
    else:
        hourly_df = hourly_df.sort_values("event_hour")
        st.bar_chart(hourly_df.set_index("event_hour")["event_count"])

row2_left, row2_right = st.columns(2)

with row2_left:
    st.subheader("Model Usage")
    model_df = _safe_df(get_model_usage(conn, filters))
    st.dataframe(model_df, width="stretch")

with row2_right:
    st.subheader("Tool Performance")
    tool_df = _safe_df(get_tool_usage(conn, filters))
    st.dataframe(tool_df, width="stretch")

st.subheader("Top Users by Tokens")
users_df = _safe_df(get_top_users_by_tokens(conn, filters))
st.dataframe(users_df, width="stretch")

_render_seniority_breakdown(conn, filters)
_render_advanced_statistics(conn, filters)
_render_predictive_analytics(conn, filters)

conn.close()
