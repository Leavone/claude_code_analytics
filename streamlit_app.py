"""Streamlit dashboard for Claude Code usage analytics.

The app provides an interactive UI for exploring telemetry by date, practice,
model, and user. It relies on ``analytics_platform.dashboard`` for filtered
query logic and uses SQLite as the source of truth.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from analytics_platform.dashboard import (
    DashboardFilters,
    get_daily_tokens,
    get_filter_options,
    get_hourly_usage,
    get_kpis,
    get_model_usage,
    get_seniority_usage,
    get_tool_usage,
    get_top_users_by_tokens,
)
from analytics_platform.db import connect, init_schema


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
    st.subheader("Daily Token Trend by Practice")
    daily_df = _safe_df(get_daily_tokens(conn, filters))
    if daily_df.empty:
        st.info("No data for selected filters.")
    else:
        pivot = daily_df.pivot(index="event_date", columns="practice", values="total_tokens").fillna(0)
        st.line_chart(pivot)

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
    st.dataframe(model_df, use_container_width=True)

with row2_right:
    st.subheader("Tool Performance")
    tool_df = _safe_df(get_tool_usage(conn, filters))
    st.dataframe(tool_df, use_container_width=True)

st.subheader("Top Users by Tokens")
users_df = _safe_df(get_top_users_by_tokens(conn, filters))
st.dataframe(users_df, use_container_width=True)

st.subheader("Seniority Level Breakdown")
level_df = _safe_df(get_seniority_usage(conn, filters))
if level_df.empty:
    st.info("No data for selected filters.")
else:
    chart_col, table_col = st.columns((1, 2))
    with chart_col:
        st.bar_chart(level_df.set_index("level")["total_tokens"])
    with table_col:
        st.dataframe(level_df, use_container_width=True)

conn.close()
