"""Dashboard page — KPIs, Overview & Analysis charts."""

import numpy as np
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from src.queries import ASSETS, get_asset_config
from src.ui_helpers import asset_selector
from src.data_processor import (
    load_data,
    filter_data,
    get_kpi_metrics,
    get_monthly_stats,
    get_monthly_by_version,
    get_size_distribution,
    get_monthly_by_size_bracket,
    get_top_liquidators,
    get_top_liquidatees,
)

# --- Custom CSS ---
st.markdown(
    """
    <style>
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #2d1f3d 100%);
        border: 1px solid #333;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #FF6B6B;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #888;
        margin-top: 4px;
    }
    .main-header {
        text-align: center;
        padding: 10px 0 30px 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Asset Selector (persisted via session_state) ---
asset_key, asset_config = asset_selector()
symbol = asset_config["symbol"]

# --- Header ---
st.markdown(
    f'<div class="main-header"><h1>Liquidations Analytics</h1>'
    f'<p>{symbol} | Aave {" & ".join(asset_config["subgraphs"])} | Ethereum Mainnet</p></div>',
    unsafe_allow_html=True,
)

# --- Load Data ---
df = load_data(asset_key)

if df.empty:
    st.warning(
        f"No data found for {symbol}. Run `python scripts/update_data.py --asset {asset_key}` to fetch liquidation data."
    )
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Filters")

min_date = df["datetime"].min().date()
max_date = df["datetime"].max().date()
date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

available_versions = asset_config["subgraphs"]
if len(available_versions) > 1:
    versions = st.sidebar.multiselect(
        "Protocol version",
        options=available_versions,
        default=available_versions,
    )
else:
    versions = available_versions
    st.sidebar.info(f"Only available on Aave {available_versions[0]}")

min_usd = st.sidebar.number_input("Min USD amount", min_value=0.0, value=0.0, step=1000.0)

top_n = st.sidebar.slider("Top N addresses", 5, 50, 20)

# Apply filters
filtered = filter_data(df, date_range=date_range, versions=versions, min_usd=min_usd if min_usd > 0 else None)

# =====================================================================
# SECTION 1 — KPI METRICS
# =====================================================================
kpi = get_kpi_metrics(filtered)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Liquidations", f"{kpi['total_liquidations']:,}")
with col2:
    st.metric(f"Total {symbol} Liquidated", f"{kpi['total_btc']:,.2f}")
with col3:
    st.metric("Total USD Value", f"${kpi['total_usd']:,.0f}")
with col4:
    st.metric("Unique Users Liquidated", f"{kpi['unique_liquidatees']:,}")

col5, col6, col7, col8 = st.columns(4)

with col5:
    if kpi["v2_count"] > 0:
        st.metric("Aave V2 Liquidations", f"{kpi['v2_count']:,}")
    else:
        st.metric("Aave V3 Liquidations", f"{kpi['v3_count']:,}")
with col6:
    if kpi["v2_count"] > 0:
        st.metric("Aave V3 Liquidations", f"{kpi['v3_count']:,}")
    else:
        st.metric("Unique Liquidators", f"{kpi['unique_liquidators']:,}")
with col7:
    st.metric("Avg Liquidation", f"${kpi['avg_liquidation_usd']:,.0f}")
with col8:
    st.metric("Max Liquidation", f"${kpi['max_liquidation_usd']:,.0f}")

st.divider()

# =====================================================================
# SECTION 2 — MONTHLY LIQUIDATION TIMELINE
# =====================================================================
st.subheader("Monthly Liquidation Timeline")

monthly = get_monthly_stats(filtered)
if not monthly.empty:
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=monthly["month"],
            y=monthly["count"],
            name="Liquidation Count",
            marker_color="#FF6B6B",
            yaxis="y",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=monthly["month"],
            y=monthly["total_usd"],
            name="Total USD",
            line=dict(color="#4ECDC4", width=2),
            yaxis="y2",
        )
    )

    # BTC price line
    if not filtered.empty:
        price_df = filtered[filtered["collateral_amount_btc"] > 0].copy()
        price_df["month"] = price_df["datetime"].dt.to_period("M").dt.to_timestamp()
        price_df["btc_price"] = price_df["collateral_amount_usd"] / price_df["collateral_amount_btc"]
        mp = price_df.groupby("month")["btc_price"].median().reset_index()
        if not mp.empty:
            fig.add_trace(
                go.Scatter(
                    x=mp["month"],
                    y=mp["btc_price"],
                    name="BTC Price",
                    line=dict(color="#FFA500", width=2, dash="dot"),
                    yaxis="y3",
                    hovertemplate="BTC: $%{y:,.0f}<extra></extra>",
                )
            )

    fig.update_layout(
        yaxis=dict(title="Count", side="left", showgrid=False),
        yaxis2=dict(
            title="USD Value",
            side="right",
            overlaying="y",
            showgrid=False,
            tickformat="$,.0f",
        ),
        yaxis3=dict(
            title="BTC Price",
            side="right",
            overlaying="y",
            showgrid=False,
            tickformat="$,.0f",
            position=0.95,
            anchor="free",
        ),
        template="plotly_dark",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=60, r=80, t=40, b=40),
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data for selected filters.")

# --- V2 vs V3 Split (only if both versions exist) ---
if len(available_versions) > 1:
    st.subheader("V2 vs V3 Split")
    col_pie1, col_pie2 = st.columns(2)

    with col_pie1:
        fig_count = go.Figure(
            data=[
                go.Pie(
                    labels=["Aave V2", "Aave V3"],
                    values=[kpi["v2_count"], kpi["v3_count"]],
                    hole=0.4,
                    marker_colors=["#FF6B6B", "#4ECDC4"],
                )
            ]
        )
        fig_count.update_layout(
            title="By Count",
            template="plotly_dark",
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig_count, use_container_width=True)

    with col_pie2:
        v2_usd = filtered[filtered["version"] == "V2"]["collateral_amount_usd"].sum() if not filtered.empty else 0
        v3_usd = filtered[filtered["version"] == "V3"]["collateral_amount_usd"].sum() if not filtered.empty else 0

        fig_usd = go.Figure(
            data=[
                go.Pie(
                    labels=["Aave V2", "Aave V3"],
                    values=[v2_usd, v3_usd],
                    hole=0.4,
                    marker_colors=["#FF6B6B", "#4ECDC4"],
                )
            ]
        )
        fig_usd.update_layout(
            title="By USD Volume",
            template="plotly_dark",
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig_usd, use_container_width=True)

    st.divider()

# =====================================================================
# SECTION 3 — OVERVIEW CHARTS
# =====================================================================

# --- Helper: monthly BTC price from data ---
def _get_monthly_btc_price(data):
    if data.empty:
        return pd.DataFrame(columns=["month", "btc_price"])
    p = data[data["collateral_amount_btc"] > 0].copy()
    p["month"] = p["datetime"].dt.to_period("M").dt.to_timestamp()
    p["btc_price"] = p["collateral_amount_usd"] / p["collateral_amount_btc"]
    return p.groupby("month")["btc_price"].median().reset_index()

monthly_price = _get_monthly_btc_price(filtered)

# --- Monthly by Version ---
if len(available_versions) > 1:
    st.subheader("Monthly Liquidations by Protocol")
    monthly_v = get_monthly_by_version(filtered)

    if not monthly_v.empty:
        fig_mv = px.bar(
            monthly_v,
            x="month",
            y="count",
            color="version",
            barmode="stack",
            color_discrete_map={"V2": "#FF6B6B", "V3": "#4ECDC4"},
            labels={"month": "Month", "count": "Liquidations", "version": "Protocol"},
        )
        if not monthly_price.empty:
            fig_mv.add_trace(
                go.Scatter(
                    x=monthly_price["month"],
                    y=monthly_price["btc_price"],
                    name="BTC Price",
                    line=dict(color="#FFA500", width=2.5),
                    yaxis="y2",
                    hovertemplate="BTC: $%{y:,.0f}<extra></extra>",
                )
            )
        fig_mv.update_layout(
            template="plotly_dark",
            height=450,
            margin=dict(l=40, r=60, t=40, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            yaxis2=dict(
                title="BTC Price (USD)",
                side="right",
                overlaying="y",
                showgrid=False,
                tickformat="$,.0f",
            ),
        )
        st.plotly_chart(fig_mv, use_container_width=True)

    # --- Monthly USD Volume by Version ---
    st.subheader("Monthly USD Volume by Protocol")
    if not monthly_v.empty:
        fig_musd = px.bar(
            monthly_v,
            x="month",
            y="total_usd",
            color="version",
            barmode="stack",
            color_discrete_map={"V2": "#FF6B6B", "V3": "#4ECDC4"},
            labels={"month": "Month", "total_usd": "USD Volume", "version": "Protocol"},
        )
        if not monthly_price.empty:
            fig_musd.add_trace(
                go.Scatter(
                    x=monthly_price["month"],
                    y=monthly_price["btc_price"],
                    name="BTC Price",
                    line=dict(color="#FFA500", width=2.5),
                    yaxis="y2",
                    hovertemplate="BTC: $%{y:,.0f}<extra></extra>",
                )
            )
        fig_musd.update_layout(
            template="plotly_dark",
            height=450,
            yaxis_tickformat="$,.0f",
            margin=dict(l=40, r=60, t=40, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            yaxis2=dict(
                title="BTC Price (USD)",
                side="right",
                overlaying="y",
                showgrid=False,
                tickformat="$,.0f",
            ),
        )
        st.plotly_chart(fig_musd, use_container_width=True)

# --- Monthly Volume by Size Bracket ---
st.subheader(f"Monthly {symbol} Volume by Liquidation Size + BTC Price")

monthly_size = get_monthly_by_size_bracket(filtered)
if not monthly_size.empty:
    bracket_colors = {
        "<0.1 BTC": "#88CCE6",
        "0.1-0.5 BTC": "#55BF9A",
        "0.5-1 BTC": "#A8E6CF",
        "1-5 BTC": "#FFB3BA",
        "5-10 BTC": "#C49BFF",
        "10-50 BTC": "#FF6B6B",
        ">50 BTC": "#FF4444",
    }

    fig_vol = go.Figure()

    brackets = monthly_size["size_bracket"].cat.categories.tolist() if hasattr(monthly_size["size_bracket"], "cat") else monthly_size["size_bracket"].unique().tolist()

    for bracket in brackets:
        bracket_data = monthly_size[monthly_size["size_bracket"] == bracket]
        fig_vol.add_trace(
            go.Bar(
                x=bracket_data["month"],
                y=bracket_data["total_btc"],
                name=str(bracket),
                marker_color=bracket_colors.get(str(bracket), "#999"),
                text=bracket_data["count"],
                textposition="inside",
                textfont=dict(size=10),
                customdata=list(zip(
                    bracket_data["count"],
                    bracket_data["total_btc"],
                    bracket_data["total_usd"],
                )),
                hovertemplate=(
                    "<b>%{x|%b %Y}</b><br>"
                    "Bracket: " + str(bracket) + "<br>"
                    "Liquidations: %{customdata[0]}<br>"
                    f"Volume: %{{customdata[1]:.2f}} {symbol}<br>"
                    "USD: $%{customdata[2]:,.0f}"
                    "<extra></extra>"
                ),
            )
        )

    # BTC price
    if not filtered.empty:
        price_df2 = filtered.copy()
        price_df2["month"] = price_df2["datetime"].dt.to_period("M").dt.to_timestamp()
        price_df2 = price_df2[price_df2["collateral_amount_btc"] > 0]
        price_df2["btc_price"] = price_df2["collateral_amount_usd"] / price_df2["collateral_amount_btc"]
        mp2 = price_df2.groupby("month")["btc_price"].median().reset_index()

        fig_vol.add_trace(
            go.Scatter(
                x=mp2["month"],
                y=mp2["btc_price"],
                name="BTC Price (median)",
                line=dict(color="#FFA500", width=2.5),
                yaxis="y2",
                hovertemplate="<b>%{x|%b %Y}</b><br>BTC Price: $%{y:,.0f}<extra></extra>",
            )
        )

    fig_vol.update_layout(
        barmode="stack",
        template="plotly_dark",
        height=500,
        yaxis=dict(title=f"{symbol} Volume", side="left", showgrid=False),
        yaxis2=dict(
            title="BTC Price (USD)",
            side="right",
            overlaying="y",
            showgrid=False,
            tickformat="$,.0f",
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(l=60, r=60, t=60, b=40),
        hovermode="x unified",
    )
    st.plotly_chart(fig_vol, use_container_width=True)

# --- Size Distribution ---
st.subheader(f"Liquidation Size Distribution ({symbol})")

size_dist = get_size_distribution(filtered)
if not size_dist.empty:
    col_sd1, col_sd2 = st.columns(2)

    with col_sd1:
        fig_sd1 = go.Figure(
            data=[
                go.Bar(
                    x=size_dist["size_bracket"].astype(str),
                    y=size_dist["count"],
                    marker_color="#FF6B6B",
                    text=size_dist["count"],
                    textposition="auto",
                )
            ]
        )
        fig_sd1.update_layout(
            title="By Count",
            xaxis_title=f"Size Bracket ({symbol})",
            yaxis_title="Count",
            template="plotly_dark",
            height=400,
            margin=dict(l=40, r=40, t=60, b=40),
        )
        st.plotly_chart(fig_sd1, use_container_width=True)

    with col_sd2:
        fig_sd2 = go.Figure(
            data=[
                go.Bar(
                    x=size_dist["size_bracket"].astype(str),
                    y=size_dist["total_usd"],
                    marker_color="#4ECDC4",
                    text=[f"${v:,.0f}" for v in size_dist["total_usd"]],
                    textposition="auto",
                )
            ]
        )
        fig_sd2.update_layout(
            title="By USD Volume",
            xaxis_title=f"Size Bracket ({symbol})",
            yaxis_title="USD Volume",
            yaxis_tickformat="$,.0f",
            template="plotly_dark",
            height=400,
            margin=dict(l=40, r=40, t=60, b=40),
        )
        st.plotly_chart(fig_sd2, use_container_width=True)

# --- Bubble Scatter ---
st.subheader("Bubble Scatter by Size")

if not filtered.empty:
    scatter_df = filtered[filtered["collateral_amount_btc"] > 0].copy()

    bins = [0, 0.1, 0.5, 1, 5, 10, 50, float("inf")]
    labels = ["<0.1 BTC", "0.1-0.5 BTC", "0.5-1 BTC", "1-5 BTC", "5-10 BTC", "10-50 BTC", ">50 BTC"]
    scatter_df["size_bracket"] = pd.cut(
        scatter_df["collateral_amount_btc"], bins=bins, labels=labels
    )

    bracket_colors_scatter = {
        "<0.1 BTC": "#88CCE6",
        "0.1-0.5 BTC": "#55BF9A",
        "0.5-1 BTC": "#A8E6CF",
        "1-5 BTC": "#FFB3BA",
        "5-10 BTC": "#C49BFF",
        "10-50 BTC": "#FF6B6B",
        ">50 BTC": "#FF4444",
    }

    fig_bubble = go.Figure()

    for bracket in labels:
        bd = scatter_df[scatter_df["size_bracket"] == bracket]
        if bd.empty:
            continue
        fig_bubble.add_trace(
            go.Scatter(
                x=bd["datetime"],
                y=bd["collateral_amount_btc"],
                mode="markers",
                name=bracket,
                marker=dict(
                    size=np.clip(bd["collateral_amount_btc"] * 3, 4, 40),
                    color=bracket_colors_scatter.get(bracket, "#999"),
                    opacity=0.6,
                    line=dict(width=0.5, color="#333"),
                ),
                customdata=list(zip(
                    bd["collateral_amount_usd"],
                    bd["tx_hash"],
                    bd["market_name"],
                    bd["version"],
                )),
                hovertemplate=(
                    "<b>%{x|%b %d, %Y}</b><br>"
                    f"{symbol}: %{{y:.4f}}<br>"
                    "USD: $%{customdata[0]:,.0f}<br>"
                    "Market: %{customdata[2]}<br>"
                    "Protocol: %{customdata[3]}<br>"
                    "Tx: %{customdata[1]}"
                    "<extra>" + bracket + "</extra>"
                ),
            )
        )

    fig_bubble.update_layout(
        template="plotly_dark",
        height=500,
        xaxis_title="Date",
        yaxis_title=f"{symbol} Amount (log)",
        yaxis_type="log",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(l=60, r=40, t=60, b=40),
    )
    st.plotly_chart(fig_bubble, use_container_width=True)

st.divider()

# =====================================================================
# SECTION 4 — ANALYSIS (Top Liquidators & Liquidatees)
# =====================================================================

# --- Top Liquidators ---
st.subheader(f"Top {top_n} Liquidators by USD Volume")

top_liq = get_top_liquidators(filtered, top_n=top_n)
if not top_liq.empty:
    top_liq["short_address"] = top_liq["liquidator"].apply(
        lambda x: f"{x[:6]}...{x[-4:]}"
    )

    fig_tl = go.Figure()
    fig_tl.add_trace(
        go.Bar(
            y=top_liq["short_address"],
            x=top_liq["total_usd"],
            orientation="h",
            marker_color="#FF6B6B",
            text=[f"${v:,.0f}" for v in top_liq["total_usd"]],
            textposition="auto",
            customdata=top_liq[["liquidator", "count", "total_btc"]].values.tolist(),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Liquidations: %{customdata[1]}<br>"
                f"{symbol}: %{{customdata[2]:.4f}}<br>"
                "Volume: $%{x:,.2f}"
                "<extra></extra>"
            ),
        )
    )
    fig_tl.update_layout(
        template="plotly_dark",
        height=max(400, top_n * 28),
        xaxis_tickformat="$,.0f",
        yaxis=dict(autorange="reversed"),
        margin=dict(l=120, r=40, t=20, b=40),
    )
    st.plotly_chart(fig_tl, use_container_width=True)

    with st.expander("Liquidator details"):
        st.dataframe(
            top_liq[["liquidator", "count", "total_btc", "total_usd"]].rename(
                columns={
                    "liquidator": "Address",
                    "count": "Liquidations",
                    "total_btc": f"Total {symbol}",
                    "total_usd": "Total USD",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

st.divider()

# --- Top Liquidatees ---
st.subheader(f"Top {top_n} Most Liquidated Users")

top_liqee = get_top_liquidatees(filtered, top_n=top_n)
if not top_liqee.empty:
    top_liqee["short_address"] = top_liqee["liquidatee"].apply(
        lambda x: f"{x[:6]}...{x[-4:]}"
    )

    fig_tle = go.Figure()
    fig_tle.add_trace(
        go.Bar(
            y=top_liqee["short_address"],
            x=top_liqee["total_usd"],
            orientation="h",
            marker_color="#4ECDC4",
            text=[f"${v:,.0f}" for v in top_liqee["total_usd"]],
            textposition="auto",
            customdata=top_liqee[["liquidatee", "count", "total_btc"]].values.tolist(),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Liquidations: %{customdata[1]}<br>"
                f"{symbol}: %{{customdata[2]:.4f}}<br>"
                "Volume: $%{x:,.2f}"
                "<extra></extra>"
            ),
        )
    )
    fig_tle.update_layout(
        template="plotly_dark",
        height=max(400, top_n * 28),
        xaxis_tickformat="$,.0f",
        yaxis=dict(autorange="reversed"),
        margin=dict(l=120, r=40, t=20, b=40),
    )
    st.plotly_chart(fig_tle, use_container_width=True)

    with st.expander("Liquidatee details"):
        st.dataframe(
            top_liqee[["liquidatee", "count", "total_btc", "total_usd"]].rename(
                columns={
                    "liquidatee": "Address",
                    "count": "Liquidations",
                    "total_btc": f"Total {symbol}",
                    "total_usd": "Total USD",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

# --- Footer ---
st.markdown("---")
st.caption("Liquidations Analytics \u2022 Data from The Graph (Aave subgraphs)")
