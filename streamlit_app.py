"""Streamlit dashboard for Claude Code usage analytics."""

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
    get_tool_usage,
    get_top_users_by_tokens,
)
from analytics_platform.db import connect, init_schema


st.set_page_config(page_title="Claude Code Analytics", page_icon="📊", layout="wide")
st.title("Claude Code Usage Analytics")
st.caption("Interactive telemetry dashboard for usage, cost, and behavior patterns")


def _safe_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows) if rows else pd.DataFrame()


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
    date_range = st.date_input(
        "Date range",
        value=(pd.to_datetime(min_date).date(), pd.to_datetime(max_date).date()),
        min_value=pd.to_datetime(min_date).date(),
        max_value=pd.to_datetime(max_date).date(),
    )

    selected_practices = st.multiselect("Practices", options["practices"], default=[])
    selected_models = st.multiselect("Models", options["models"], default=[])

    top_users = options["users"][:30]
    selected_users = st.multiselect("Users (top 30 listed)", top_users, default=[])

if isinstance(date_range, tuple):
    date_from = date_range[0].isoformat()
    date_to = date_range[1].isoformat()
else:
    date_from = date_range.isoformat()
    date_to = date_range.isoformat()

filters = DashboardFilters(
    date_from=date_from,
    date_to=date_to,
    practices=selected_practices or None,
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

conn.close()
