"""User Behavior page — pre-liquidation analysis for positions >= 1 BTC."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from src.queries import ASSETS
from src.ui_helpers import asset_selector
from src.data_processor import load_data
from src.behavior_analyzer import (
    load_actions,
    classify_users,
    get_behavior_summary,
    get_behavior_by_size,
    get_deposit_asset_breakdown,
)

# --- Asset Selector (persisted via session_state) ---
asset_key, asset_config = asset_selector()
symbol = asset_config["symbol"]

st.header(f"Pre-Liquidation User Behavior — {symbol}")
st.caption(f"Analysis of deposits & repays made by users in the 48 hours before their {symbol} liquidation (positions >= 1 BTC)")

liq_df = load_data(asset_key)
actions_df = load_actions(asset_key)

if liq_df.empty:
    st.warning(f"No liquidation data for {symbol}. Run `python scripts/update_data.py --asset {asset_key}` first.")
    st.stop()

if actions_df.empty:
    st.warning(
        f"No user behavior data for {symbol} yet. Run:\n\n"
        f"```\npython scripts/fetch_user_behavior.py --asset {asset_key}\n```"
    )
    st.stop()

# --- Sidebar ---
st.sidebar.header("Settings")
min_wbtc = st.sidebar.selectbox("Min liquidation size", [1.0, 5.0, 10.0, 50.0], index=0)
window_hours = st.sidebar.slider("Window before liquidation (hours)", 6, 72, 48)

# --- Classify ---
classified = classify_users(liq_df, actions_df, asset_key=asset_key, min_wbtc=min_wbtc, window_hours=window_hours)

if classified.empty:
    st.info("No liquidations matching criteria.")
    st.stop()

summary = get_behavior_summary(classified)

# --- KPI Metrics ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Liquidations Analyzed", f"{summary['total']:,}")
with col2:
    st.metric("Tried to Save Position", f"{summary['pct_tried_save']:.1f}%")
with col3:
    st.metric("Passive (No Actions)", f"{summary['passive']:,}")
with col4:
    st.metric(f"Deposited {symbol}", f"{summary.get('deposited_collateral_count', 0):,.0f}")

st.divider()

# --- Behavior Distribution ---
st.subheader("Behavior Distribution")

col_pie, col_bar = st.columns(2)

with col_pie:
    behavior_counts = classified["behavior"].value_counts()
    colors = {
        "Passive": "#95a5a6",
        "Deposit Only": "#4ECDC4",
        "Repay Only": "#FF6B6B",
        "Deposit + Repay": "#FFA500",
    }

    fig1 = go.Figure(data=[go.Pie(
        labels=behavior_counts.index,
        values=behavior_counts.values,
        hole=0.4,
        marker_colors=[colors.get(b, "#999") for b in behavior_counts.index],
        textinfo="label+percent",
        textfont_size=12,
    )])
    fig1.update_layout(
        title="By Liquidation Count",
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        showlegend=False,
    )
    st.plotly_chart(fig1, use_container_width=True)

with col_bar:
    # Behavior by USD volume
    behavior_usd = classified.groupby("behavior")["collateral_amount_usd"].sum().reset_index()
    behavior_usd.columns = ["behavior", "total_usd"]
    behavior_usd = behavior_usd.sort_values("total_usd", ascending=True)

    fig2 = go.Figure(data=[go.Bar(
        y=behavior_usd["behavior"],
        x=behavior_usd["total_usd"],
        orientation="h",
        marker_color=[colors.get(b, "#999") for b in behavior_usd["behavior"]],
        text=[f"${v:,.0f}" for v in behavior_usd["total_usd"]],
        textposition="auto",
    )])
    fig2.update_layout(
        title="By USD Volume Liquidated",
        template="plotly_dark",
        height=400,
        xaxis_tickformat="$,.0f",
        margin=dict(l=120, r=40, t=40, b=40),
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# --- Behavior by Liquidation Size Bracket ---
st.subheader("Behavior by Liquidation Size")

by_size = get_behavior_by_size(classified)
if not by_size.empty:
    fig3 = px.bar(
        by_size,
        x="size_bracket",
        y="count",
        color="behavior",
        barmode="stack",
        color_discrete_map=colors,
        labels={"size_bracket": "Liquidation Size", "count": "Count", "behavior": "Behavior"},
        text="count",
    )
    fig3.update_layout(
        template="plotly_dark",
        height=450,
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )
    st.plotly_chart(fig3, use_container_width=True)

    # Percentage view
    size_totals = by_size.groupby("size_bracket")["count"].sum().reset_index(name="total")
    by_size_pct = by_size.merge(size_totals, on="size_bracket")
    by_size_pct["pct"] = by_size_pct["count"] / by_size_pct["total"] * 100

    fig3b = px.bar(
        by_size_pct,
        x="size_bracket",
        y="pct",
        color="behavior",
        barmode="stack",
        color_discrete_map=colors,
        labels={"size_bracket": "Liquidation Size", "pct": "Percentage (%)", "behavior": "Behavior"},
        text=[f"{v:.0f}%" for v in by_size_pct["pct"]],
    )
    fig3b.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )
    st.plotly_chart(fig3b, use_container_width=True)

st.divider()

# --- What Assets Did Users Deposit? ---
st.subheader("Assets Deposited Before Liquidation")

asset_breakdown = get_deposit_asset_breakdown(classified, actions_df)
if not asset_breakdown.empty:
    col_a, col_b = st.columns(2)

    with col_a:
        fig4 = go.Figure(data=[go.Pie(
            labels=asset_breakdown["asset_symbol"],
            values=asset_breakdown["total_usd"],
            hole=0.4,
            textinfo="label+percent",
        )])
        fig4.update_layout(
            title="By USD Volume Deposited",
            template="plotly_dark",
            height=400,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig4, use_container_width=True)

    with col_b:
        fig5 = go.Figure(data=[go.Bar(
            x=asset_breakdown["asset_symbol"],
            y=asset_breakdown["unique_users"],
            marker_color="#4ECDC4",
            text=asset_breakdown["unique_users"],
            textposition="auto",
        )])
        fig5.update_layout(
            title="Unique Users by Deposit Asset",
            template="plotly_dark",
            height=400,
            xaxis_title="Asset",
            yaxis_title="Unique Users",
            margin=dict(l=40, r=40, t=40, b=40),
        )
        st.plotly_chart(fig5, use_container_width=True)

    with st.expander("Asset breakdown details"):
        st.dataframe(
            asset_breakdown.rename(columns={
                "asset_symbol": "Asset",
                "count": "Deposit Events",
                "total_usd": "Total USD Deposited",
                "unique_users": "Unique Users",
            }),
            use_container_width=True,
            hide_index=True,
        )
else:
    st.info("No deposit data found.")

st.divider()

# --- Time Before Liquidation ---
st.subheader("Time Between Last Action and Liquidation")

active_users = classified[classified["hours_before_last_action"].notna()].copy()
if not active_users.empty:
    fig6 = px.histogram(
        active_users,
        x="hours_before_last_action",
        nbins=40,
        color="behavior",
        color_discrete_map=colors,
        labels={"hours_before_last_action": "Hours Before Liquidation", "behavior": "Behavior"},
    )
    fig6.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=40, r=40, t=40, b=40),
    )
    st.plotly_chart(fig6, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Median Hours Before", f"{active_users['hours_before_last_action'].median():.1f}h")
    with col2:
        within_1h = len(active_users[active_users["hours_before_last_action"] <= 1])
        st.metric("Actions Within 1 Hour", f"{within_1h}")
    with col3:
        within_6h = len(active_users[active_users["hours_before_last_action"] <= 6])
        st.metric("Actions Within 6 Hours", f"{within_6h}")
