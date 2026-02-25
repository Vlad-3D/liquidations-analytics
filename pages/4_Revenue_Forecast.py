"""Aave Revenue Forecast — 3-Year Projection (wBTC & cbBTC Collateral).

Methodology:
  1. Compute monthly realized protocol revenue from borrow/repay event deltas × per-asset
     reserve factor, plus liquidation protocol fees.
  2. Compute monthly average BTC price from price_history.parquet + borrow/repay USD/token
     implied prices for historical dates not covered by the price cache.
  3. Run OLS regression: monthly_protocol_revenue ~ btc_price_monthly_avg.
  4. Apply the regression to 3 BTC price scenarios (bearish/base/bull) to project revenue.

BTC Price Scenarios (fixed):
  Year 1 (Mar 2026 – Feb 2027): ~$60K  (sideways, minimal TVL growth)
  Year 2 (Mar 2027 – Feb 2028): ~$100K (moderate growth)
  Year 3 (Mar 2028 – Feb 2029): ~$200K (major bull run)

Key assumptions:
  - Borrow utilization: 50% of TVL (historical 40-60%)
  - Blended reserve factor: 13% (current, held constant)
  - BTC TVL in Aave scales proportionally with BTC price
  - cbBTC growth follows historical progression vs BTC price
  - wBTC V2 excluded from projections (deprecated)
  - Liquidation revenue: historical liq_rate × projected TVL
"""

from datetime import date

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from scipy.stats import linregress

from src.collateral_fetcher import (
    compute_open_position_accrued,
    compute_realized_interest,
    load_all,
)
from src.collateral_queries import MARKET_RESERVE_FACTORS, RESERVE_FACTOR, STABLECOINS
from src.price_cache import fetch_and_cache_history, get_price_lookup

# ── Style (matches pages/3_Aave_Revenue.py) ──────────────────────────────────
st.markdown("""
<style>
.rev-header { text-align:center; padding:10px 0 24px 0; }
.kpi-card {
    background: linear-gradient(135deg,#0d1117 0%,#161b27 100%);
    border:1px solid #30363d; border-radius:12px;
    padding:18px 20px; text-align:center;
}
.kpi-value { font-size:1.7rem; font-weight:700; color:#58a6ff; }
.kpi-sub   { font-size:0.82rem; color:#8b949e; margin-top:4px; }
.kpi-label { font-size:0.9rem; color:#c9d1d9; margin-top:2px; }
.note-box  { background:#161b27; border:1px solid #30363d; border-radius:8px;
             padding:12px 16px; font-size:0.82rem; color:#8b949e; margin:8px 0 16px 0; }
</style>
""", unsafe_allow_html=True)

C = {
    "wbtc":       "#f7931a",
    "cbbtc":      "#0052ff",
    "interest":   "#3fb950",
    "liq_fee":    "#ff6b6b",
    "btc_price":  "#f0b429",
    "forecast":   "#58a6ff",
    "historical": "#8b949e",
    "year1":      "#f0b429",
    "year2":      "#58a6ff",
    "year3":      "#3fb950",
    "grid":       "rgba(255,255,255,0.06)",
    "accrued":    "#bc8cff",
}

LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#c9d1d9", size=12),
    xaxis=dict(gridcolor=C["grid"], showgrid=True),
    yaxis=dict(gridcolor=C["grid"], showgrid=True),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#30363d", borderwidth=1),
    margin=dict(t=44, b=40, l=10, r=10),
    hovermode="x unified",
)


def fmt(v: float) -> str:
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.2f}M"
    if v >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:,.0f}"


def kpi(col, value, label, sub=""):
    col.markdown(
        f'<div class="kpi-card">'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-sub">{sub}</div>'
        f'</div>', unsafe_allow_html=True)


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="rev-header"><h1>Aave Revenue Forecast</h1>'
    '<p>3-Year Projection · wBTC & cbBTC Collateral · BTC Price as Regressor</p></div>',
    unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
dfs = load_all()

borrows_df = dfs.get("borrows", pd.DataFrame())
repays_df = dfs.get("repays", pd.DataFrame())
liqs_df = dfs.get("liquidations", pd.DataFrame())
open_df = dfs.get("open_positions", pd.DataFrame())

if borrows_df.empty and repays_df.empty:
    st.error("Data files not found. Please ensure the parquet files are in data/.")
    st.stop()

# ── Price data ────────────────────────────────────────────────────────────────
with st.spinner("Loading price history…"):
    from src.collateral_queries import COINGECKO_IDS
    non_stable = [s for s in COINGECKO_IDS if s not in STABLECOINS]
    price_df = fetch_and_cache_history(non_stable, days=365)
    price_lookup = get_price_lookup(price_df)

# ── Compute realized interest ─────────────────────────────────────────────────
interest_df = compute_realized_interest(borrows_df, repays_df, price_lookup)

# ── Compute unrealized (open positions) run-rate ──────────────────────────────
today = date.today()
accrued_df = (
    compute_open_position_accrued(open_df, price_lookup, today)
    if not open_df.empty else pd.DataFrame()
)
# Unrealized annual revenue — the money already being generated right now
unrealized_total = accrued_df["annual_protocol_usd"].sum() if not accrued_df.empty else 0
unrealized_v3 = accrued_df[accrued_df["version"] == "V3"]["annual_protocol_usd"].sum() if not accrued_df.empty else 0
unrealized_v2 = accrued_df[accrued_df["version"] == "V2"]["annual_protocol_usd"].sum() if not accrued_df.empty else 0

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: Build monthly realized protocol revenue time series
# ═══════════════════════════════════════════════════════════════════════════════

# Monthly interest revenue
monthly_interest = pd.DataFrame()
if not interest_df.empty:
    d = interest_df.copy()
    d["month"] = pd.to_datetime(d["last_repay_date"]).dt.to_period("M").dt.to_timestamp()
    monthly_interest = d.groupby("month")["protocol_revenue_usd"].sum().reset_index()
    monthly_interest.columns = ["month", "interest_rev"]

# Monthly liquidation fees
monthly_liq = pd.DataFrame()
if not liqs_df.empty:
    d = liqs_df.copy()
    d["month"] = pd.to_datetime(d["date"]).dt.to_period("M").dt.to_timestamp()
    monthly_liq = d.groupby("month").agg(
        liq_fee=("protocol_fee_usd", "sum"),
        liq_volume=("amount_usd", "sum"),
    ).reset_index()

# Merge into single monthly revenue series
if not monthly_interest.empty and not monthly_liq.empty:
    monthly_rev = pd.merge(monthly_interest, monthly_liq, on="month", how="outer").fillna(0)
elif not monthly_interest.empty:
    monthly_rev = monthly_interest.copy()
    monthly_rev["liq_fee"] = 0.0
    monthly_rev["liq_volume"] = 0.0
else:
    st.error("Cannot compute monthly revenue — no interest data.")
    st.stop()

monthly_rev["total_rev"] = monthly_rev["interest_rev"] + monthly_rev["liq_fee"]
monthly_rev = monthly_rev.sort_values("month").reset_index(drop=True)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: Build monthly average BTC price
# ═══════════════════════════════════════════════════════════════════════════════
# Strategy: use WBTC prices from price_history (covers last year),
# and for earlier months derive implied BTC price from borrow events
# (amount_usd_spot / amount_tokens for WBTC borrows).

# Method A: from price_history.parquet (accurate, last year)
btc_from_cache = price_df[price_df["symbol"].isin(["WBTC", "cbBTC"])].copy()
btc_from_cache["month"] = btc_from_cache["date"].dt.to_period("M").dt.to_timestamp()
btc_monthly_cache = btc_from_cache.groupby("month")["price_usd"].mean().reset_index()
btc_monthly_cache.columns = ["month", "btc_price"]

# Method B: from borrow events (WBTC borrows have amount_usd_spot and amount_tokens)
wbtc_borrows = borrows_df[borrows_df["asset_symbol"].isin(["WBTC", "wBTC"])].copy()
if not wbtc_borrows.empty:
    wbtc_borrows = wbtc_borrows[wbtc_borrows["amount_tokens"] > 0.001].copy()
    wbtc_borrows["implied_price"] = wbtc_borrows["amount_usd_spot"] / wbtc_borrows["amount_tokens"]
    # Filter out obvious outliers
    wbtc_borrows = wbtc_borrows[
        (wbtc_borrows["implied_price"] > 1000) & (wbtc_borrows["implied_price"] < 500000)
    ]
    wbtc_borrows["month"] = pd.to_datetime(wbtc_borrows["date"]).dt.to_period("M").dt.to_timestamp()
    btc_monthly_implied = wbtc_borrows.groupby("month")["implied_price"].median().reset_index()
    btc_monthly_implied.columns = ["month", "btc_price"]
else:
    btc_monthly_implied = pd.DataFrame(columns=["month", "btc_price"])

# Combine: prefer cache prices where available, fill with implied
btc_monthly = pd.concat([btc_monthly_implied, btc_monthly_cache], ignore_index=True)
btc_monthly = btc_monthly.drop_duplicates(subset="month", keep="last").sort_values("month")

# Merge BTC price with monthly revenue
monthly = pd.merge(monthly_rev, btc_monthly, on="month", how="inner")
# Drop months with zero revenue (very early months with only tiny amounts)
monthly = monthly[monthly["total_rev"] > 100].copy()

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: OLS Regression  monthly_revenue ~ btc_price
# ═══════════════════════════════════════════════════════════════════════════════

x = monthly["btc_price"].values
y = monthly["total_rev"].values

slope, intercept, r_value, p_value, std_err = linregress(x, y)
r_squared = r_value ** 2

# Revenue per $1K BTC
rev_per_1k_btc = slope * 1000

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: BTC Price Scenarios & Projections
# ═══════════════════════════════════════════════════════════════════════════════

BTC_SCENARIOS = {
    1: {"label": "Year 1 (Mar 2026 – Feb 2027)", "avg_btc": 60_000,
        "description": "Sideways ~$60K, minimal TVL growth"},
    2: {"label": "Year 2 (Mar 2027 – Feb 2028)", "avg_btc": 100_000,
        "description": "Moderate growth ~$100K"},
    3: {"label": "Year 3 (Mar 2028 – Feb 2029)", "avg_btc": 200_000,
        "description": "Bull run toward ~$200K"},
}

# ── Compute historical liquidation rate as % of TVL proxy ─────────────────────
# TVL proxy: for each month, estimate BTC TVL from borrow volume and utilization
# Simple approach: liq_fee / total_rev ratio historically, applied to projected interest
total_hist_liq_fee = monthly["liq_fee"].sum()
total_hist_interest = monthly["interest_rev"].sum()
total_hist_rev = monthly["total_rev"].sum()
liq_to_interest_ratio = total_hist_liq_fee / total_hist_interest if total_hist_interest > 0 else 0.05

# ── Current TVL from open positions ───────────────────────────────────────────
if not open_df.empty:
    active = open_df[open_df["btc_collateral_tokens"] > 0].copy()

    # Deduplicate by account to get unique collateral tokens
    wbtc_tvl_tokens = active[active["collateral_key"] == "wbtc"].drop_duplicates(
        subset="account")["btc_collateral_tokens"].sum()
    cbbtc_tvl_tokens = active[active["collateral_key"] == "cbbtc"].drop_duplicates(
        subset="account")["btc_collateral_tokens"].sum()

    # Current BTC price (latest from price data)
    latest_btc = btc_monthly["btc_price"].iloc[-1] if not btc_monthly.empty else 95_000
    current_wbtc_tvl_usd = wbtc_tvl_tokens * latest_btc
    current_cbbtc_tvl_usd = cbbtc_tvl_tokens * latest_btc
    current_total_tvl_usd = current_wbtc_tvl_usd + current_cbbtc_tvl_usd
else:
    wbtc_tvl_tokens = cbbtc_tvl_tokens = 0
    current_wbtc_tvl_usd = current_cbbtc_tvl_usd = current_total_tvl_usd = 0
    latest_btc = 95_000

# ── Project TVL and revenue per year ──────────────────────────────────────────
# Assumption: BTC TVL (in tokens) stays roughly constant in Year 1 (sideways),
# but USD TVL scales with price. cbBTC grows in Year 2-3.
# wBTC V2 positions excluded (deprecated, winding down).

# Historical cbBTC share of total BTC TVL
cbbtc_share_current = cbbtc_tvl_tokens / (wbtc_tvl_tokens + cbbtc_tvl_tokens) if (wbtc_tvl_tokens + cbbtc_tvl_tokens) > 0 else 0.3

# ── Projection logic ──────────────────────────────────────────────────────────
# Year 1 FLOOR = unrealized run-rate from existing positions.
# These are real borrows happening right now — they don't depend on the regression.
# V2 portion decays as deprecated positions close; V3 stays.
# Adjust for BTC price change: if scenario BTC < current, borrow USD shrinks proportionally
# (stablecoin borrows stay, but BTC-denominated borrows scale with price).

# What fraction of unrealized revenue comes from BTC-denominated borrows (scales with price)
# vs stablecoin borrows (stays flat regardless of BTC price)?
if not accrued_df.empty:
    btc_denom_rev = accrued_df[
        accrued_df["borrowed_symbol"].isin(["WBTC", "wBTC", "cbBTC", "tBTC", "LBTC"])
    ]["annual_protocol_usd"].sum()
    stable_denom_rev = unrealized_total - btc_denom_rev
else:
    btc_denom_rev = 0
    stable_denom_rev = unrealized_total

yearly_projections = {}
for yr, scenario in BTC_SCENARIOS.items():
    avg_btc = scenario["avg_btc"]
    price_ratio = avg_btc / latest_btc if latest_btc > 0 else 1.0

    # ── Interest revenue ──────────────────────────────────────────────────
    if yr == 1:
        # Year 1: start from unrealized run-rate (real positions today)
        # V2 winds down ~50% over the year (deprecated, positions closing)
        v2_decay = 0.50
        base_interest = unrealized_v3 + unrealized_v2 * v2_decay
        # Adjust: stablecoin borrows stay flat, BTC-denom borrows scale with price
        annual_interest = stable_denom_rev * (1 - v2_decay * unrealized_v2 / unrealized_total if unrealized_total > 0 else 1) \
            + btc_denom_rev * price_ratio
        # Simpler: scale the V3 base by price ratio for BTC part, keep stable part
        annual_interest = (unrealized_v3 + unrealized_v2 * v2_decay)
        # If BTC drops from current → scenario, USD value of BTC collateral drops,
        # but stablecoin borrows (80%+ of revenue) are unaffected by BTC price.
        # Only adjust the BTC-denominated borrow portion.
        btc_share_of_base = btc_denom_rev / unrealized_total if unrealized_total > 0 else 0.1
        annual_interest = annual_interest * (
            (1 - btc_share_of_base) + btc_share_of_base * price_ratio
        )
        # No new position growth in sideways year
        token_growth = 1.0
    elif yr == 2:
        # Year 2: V3 unrealized base × price scaling + growth from new positions
        # V2 fully wound down, V3 base grows 15% in tokens
        token_growth = 1.15
        base_v3 = unrealized_v3 * token_growth
        annual_interest = base_v3 * (
            (1 - btc_share_of_base) + btc_share_of_base * price_ratio
        )
    else:
        # Year 3: aggressive growth
        token_growth = 1.40
        base_v3 = unrealized_v3 * token_growth
        annual_interest = base_v3 * (
            (1 - btc_share_of_base) + btc_share_of_base * price_ratio
        )

    # Also cross-check with regression (don't go below regression estimate)
    regression_annual = max(0, slope * avg_btc + intercept) * 12
    annual_interest = max(annual_interest, regression_annual)

    # ── Liq fees ──────────────────────────────────────────────────────────
    annual_liq = annual_interest * liq_to_interest_ratio

    annual_total = annual_interest + annual_liq
    monthly_interest_proj = annual_interest / 12
    monthly_liq_proj = annual_liq / 12
    monthly_total = annual_total / 12

    # ── TVL projection ────────────────────────────────────────────────────
    projected_btc_tvl = (wbtc_tvl_tokens + cbbtc_tvl_tokens) * token_growth * avg_btc
    wbtc_mult = {1: 1.0, 2: 0.9, 3: 0.8}[yr]  # wBTC slowly declining
    projected_wbtc_tvl = wbtc_tvl_tokens * wbtc_mult * avg_btc
    projected_cbbtc_tvl = projected_btc_tvl - projected_wbtc_tvl

    yearly_projections[yr] = {
        "avg_btc": avg_btc,
        "monthly_interest": monthly_interest_proj,
        "monthly_liq": monthly_liq_proj,
        "monthly_total": monthly_total,
        "annual_interest": annual_interest,
        "annual_liq": annual_liq,
        "annual_total": annual_total,
        "btc_tvl_usd": projected_btc_tvl,
        "wbtc_tvl_usd": projected_wbtc_tvl,
        "cbbtc_tvl_usd": projected_cbbtc_tvl,
    }

three_year_cumulative = sum(p["annual_total"] for p in yearly_projections.values())
total_historical = total_hist_rev

# ═══════════════════════════════════════════════════════════════════════════════
# BUILD THE PAGE
# ═══════════════════════════════════════════════════════════════════════════════

# ── KPI Row ───────────────────────────────────────────────────────────────────
st.markdown("### Key Projections")
c1, c2, c3, c4, c5 = st.columns(5)
kpi(c1, fmt(yearly_projections[1]["annual_total"]), "Year 1 Revenue",
    f"BTC ~${BTC_SCENARIOS[1]['avg_btc'] / 1000:.0f}K · sideways")
kpi(c2, fmt(yearly_projections[2]["annual_total"]), "Year 2 Revenue",
    f"BTC ~${BTC_SCENARIOS[2]['avg_btc'] / 1000:.0f}K · moderate")
kpi(c3, fmt(yearly_projections[3]["annual_total"]), "Year 3 Revenue",
    f"BTC ~${BTC_SCENARIOS[3]['avg_btc'] / 1000:.0f}K · bull")
kpi(c4, fmt(three_year_cumulative), "3-Year Cumulative",
    f"On top of {fmt(total_historical)} historical")
kpi(c5, fmt(unrealized_total), "Current Run-Rate / yr",
    f"V3: {fmt(unrealized_v3)} · V2: {fmt(unrealized_v2)} (winding down)")

st.markdown(
    '<div class="note-box">'
    f'<b>Year 1 base:</b> unrealized run-rate from existing positions = <b>{fmt(unrealized_total)}/yr</b> '
    f'({fmt(unrealized_v3)} V3 + {fmt(unrealized_v2)} V2). '
    'These are real borrows happening right now — stablecoin borrows (~85% of revenue) stay flat regardless of BTC price. '
    f'V2 decays ~50% (deprecated). '
    f'<b>Year 2-3:</b> V3 base × token growth (15-40%) × BTC price scaling, '
    f'cross-checked against regression (R²={r_squared:.2f}, ${rev_per_1k_btc:,.0f}/mo per $1K BTC). '
    f'<b>Liq fees:</b> {liq_to_interest_ratio:.1%} of interest (historical ratio). '
    'Reserve factors held constant (~13% blended).'
    '</div>',
    unsafe_allow_html=True)

# ── CHART 1: Historical Monthly Revenue + BTC Price (dual Y) ─────────────────
st.markdown("### Historical Monthly Revenue + BTC Price")

fig1 = go.Figure()
fig1.add_trace(go.Bar(
    x=monthly["month"], y=monthly["interest_rev"],
    name="Interest Revenue", marker_color=C["interest"],
    hovertemplate="%{x|%b %Y}<br>Interest: $%{y:,.0f}<extra></extra>",
    yaxis="y",
))
fig1.add_trace(go.Bar(
    x=monthly["month"], y=monthly["liq_fee"],
    name="Liq Fees", marker_color=C["liq_fee"],
    hovertemplate="%{x|%b %Y}<br>Liq Fees: $%{y:,.0f}<extra></extra>",
    yaxis="y",
))
fig1.add_trace(go.Scatter(
    x=monthly["month"], y=monthly["btc_price"],
    name="BTC Price (right)", line=dict(color=C["btc_price"], width=2.5),
    hovertemplate="%{x|%b %Y}<br>BTC: $%{y:,.0f}<extra></extra>",
    yaxis="y2",
))
layout1 = {k: v for k, v in LAYOUT.items() if k not in ("yaxis", "xaxis")}
fig1.update_layout(
    **layout1,
    barmode="stack",
    title="Monthly Protocol Revenue (bars) vs BTC Price (line)",
    xaxis=dict(gridcolor=C["grid"], showgrid=True),
    yaxis=dict(title="Protocol Revenue (USD)", gridcolor=C["grid"], showgrid=True, side="left"),
    yaxis2=dict(title="BTC Price (USD)", overlaying="y", side="right", showgrid=False,
                gridcolor=C["grid"]),
    height=440,
)
st.plotly_chart(fig1, use_container_width=True)

# ── CHART 2: BTC Price — Historical + Scenario Assumptions ───────────────────
st.markdown("### BTC Price Assumption: Historical + 3-Year Scenario")

# Build monthly BTC price scenario timeline (same structure as forecast_months)
btc_scenario_months = []
# Year 1: flat at $60K
for m in range(3, 15):  # Mar 2026 – Feb 2027
    mo = m if m <= 12 else m - 12
    yr = 2026 if m <= 12 else 2027
    btc_scenario_months.append({
        "month": pd.Timestamp(year=yr, month=mo, day=1),
        "btc_price": 60_000,
        "year_label": "Year 1 · Sideways ~$60K",
    })
# Year 2: flat at $100K
for m in range(3, 15):  # Mar 2027 – Feb 2028
    mo = m if m <= 12 else m - 12
    yr = 2027 if m <= 12 else 2028
    btc_scenario_months.append({
        "month": pd.Timestamp(year=yr, month=mo, day=1),
        "btc_price": 100_000,
        "year_label": "Year 2 · Growth ~$100K",
    })
# Year 3: gradual climb from $100K to $200K
for i, m in enumerate(range(3, 15)):  # Mar 2028 – Feb 2029
    mo = m if m <= 12 else m - 12
    yr = 2028 if m <= 12 else 2029
    # Linear ramp from 100K to 200K over 12 months
    btc_p = 100_000 + (200_000 - 100_000) * (i / 11)
    btc_scenario_months.append({
        "month": pd.Timestamp(year=yr, month=mo, day=1),
        "btc_price": btc_p,
        "year_label": "Year 3 · Bull → $200K",
    })

btc_scenario_df = pd.DataFrame(btc_scenario_months)

fig_btc = go.Figure()

# Historical BTC price (from btc_monthly — the merged implied + cache series)
fig_btc.add_trace(go.Scatter(
    x=btc_monthly["month"], y=btc_monthly["btc_price"],
    mode="lines", name="Historical BTC Price",
    line=dict(color=C["btc_price"], width=2.5),
    hovertemplate="%{x|%b %Y}<br>BTC: $%{y:,.0f}<extra></extra>",
))

# Scenario lines per year
for yr_label, color in [
    ("Year 1 · Sideways ~$60K", C["year1"]),
    ("Year 2 · Growth ~$100K", C["year2"]),
    ("Year 3 · Bull → $200K", C["year3"]),
]:
    sub = btc_scenario_df[btc_scenario_df["year_label"] == yr_label]
    fig_btc.add_trace(go.Scatter(
        x=sub["month"], y=sub["btc_price"],
        mode="lines+markers", name=yr_label,
        line=dict(color=color, width=2.5, dash="dot"),
        marker=dict(size=4),
        hovertemplate=yr_label + "<br>%{x|%b %Y}<br>BTC: $%{y:,.0f}<extra></extra>",
    ))

# Background shading
for start, end, label, color in [
    ("2026-03-01", "2027-02-28", "Year 1 · $60K", C["year1"]),
    ("2027-03-01", "2028-02-29", "Year 2 · $100K", C["year2"]),
    ("2028-03-01", "2029-02-28", "Year 3 → $200K", C["year3"]),
]:
    fig_btc.add_vrect(
        x0=start, x1=end,
        fillcolor=color, opacity=0.06,
        line_width=0,
        annotation_text=label,
        annotation_position="top left",
        annotation_font_color=color,
    )

fig_btc.update_layout(
    **LAYOUT,
    title="BTC Price: Historical (Dec 2020 – Feb 2026) + Scenario Assumptions (3 Years)",
    yaxis_title="BTC Price (USD)",
    height=440,
)
st.plotly_chart(fig_btc, use_container_width=True)

st.markdown(
    '<div class="note-box">'
    '<b>Scenario rationale:</b> '
    '<b>Year 1</b> — post-halving consolidation, BTC stays around $60K (sideways, minimal TVL growth). '
    '<b>Year 2</b> — market recovers, BTC reaches ~$100K (moderate growth, new institutional inflows). '
    '<b>Year 3</b> — full bull run, BTC gradually climbs from $100K to $200K over 12 months. '
    'These are conservative-to-moderate assumptions — not predictions.'
    '</div>',
    unsafe_allow_html=True)

# ── CHART 3: 3-Year Revenue Forecast ─────────────────────────────────────────
st.markdown("### Three-Year Revenue Forecast")

# Build monthly projected timeline
forecast_months = []
for yr, proj in yearly_projections.items():
    start_year = 2025 + yr
    for m in range(1, 13):
        mo = m + 2  # start from March
        actual_year = start_year
        if mo > 12:
            mo -= 12
            actual_year += 1
        dt = pd.Timestamp(year=actual_year, month=mo, day=1)
        forecast_months.append({
            "month": dt,
            "year_label": f"Year {yr}",
            "monthly_rev": proj["monthly_total"],
            "monthly_interest": proj["monthly_interest"],
            "monthly_liq": proj["monthly_liq"],
            "btc_price": proj["avg_btc"],
        })

forecast_df = pd.DataFrame(forecast_months)

fig3 = go.Figure()

# Historical data (grey)
fig3.add_trace(go.Scatter(
    x=monthly["month"], y=monthly["total_rev"],
    mode="lines+markers", name="Historical",
    line=dict(color=C["historical"], width=2),
    marker=dict(size=3),
    hovertemplate="%{x|%b %Y}<br>Actual: $%{y:,.0f}<extra></extra>",
))

# Forecast by year
for yr in [1, 2, 3]:
    sub = forecast_df[forecast_df["year_label"] == f"Year {yr}"]
    fig3.add_trace(go.Scatter(
        x=sub["month"], y=sub["monthly_rev"],
        mode="lines+markers", name=f"Year {yr} (BTC ~${BTC_SCENARIOS[yr]['avg_btc'] / 1000:.0f}K)",
        line=dict(color=C[f"year{yr}"], width=2.5),
        marker=dict(size=5),
        hovertemplate=f"Year {yr}" + "<br>%{x|%b %Y}<br>Projected: $%{y:,.0f}/mo<extra></extra>",
    ))

# Background shading for each year
year_boundaries = [
    ("2026-03-01", "2027-02-28", "Year 1", C["year1"]),
    ("2027-03-01", "2028-02-29", "Year 2", C["year2"]),
    ("2028-03-01", "2029-02-28", "Year 3", C["year3"]),
]
for start, end, label, color in year_boundaries:
    fig3.add_vrect(
        x0=start, x1=end,
        fillcolor=color, opacity=0.06,
        line_width=0,
        annotation_text=label,
        annotation_position="top left",
        annotation_font_color=color,
    )

fig3.update_layout(
    **LAYOUT,
    title="Monthly Protocol Revenue: Historical + 3-Year Forecast",
    yaxis_title="Monthly Protocol Revenue (USD)",
    height=480,
)
st.plotly_chart(fig3, use_container_width=True)

# ── CHART 4: Cumulative Forecast vs Historical ───────────────────────────────
st.markdown("### Cumulative Revenue: Historical + Projected")

# Historical cumulative
hist_cum = monthly[["month", "total_rev"]].copy().sort_values("month")
hist_cum["cumulative"] = hist_cum["total_rev"].cumsum()

# Projected cumulative (starts from end of historical)
hist_total = hist_cum["cumulative"].iloc[-1] if not hist_cum.empty else 0
proj_cum = forecast_df[["month", "monthly_rev"]].copy().sort_values("month")
proj_cum["cumulative"] = proj_cum["monthly_rev"].cumsum() + hist_total

fig4 = go.Figure()
fig4.add_trace(go.Scatter(
    x=hist_cum["month"], y=hist_cum["cumulative"],
    name="Historical Realized", fill="tozeroy",
    line=dict(color=C["historical"], width=2),
    fillcolor="rgba(139,148,158,0.1)",
    hovertemplate="%{x|%b %Y}<br>Cumulative: $%{y:,.0f}<extra></extra>",
))
fig4.add_trace(go.Scatter(
    x=proj_cum["month"], y=proj_cum["cumulative"],
    name="Projected", fill="tozeroy",
    line=dict(color=C["forecast"], width=2.5),
    fillcolor="rgba(88,166,255,0.1)",
    hovertemplate="%{x|%b %Y}<br>Projected cumulative: $%{y:,.0f}<extra></extra>",
))

# Add annotation for final value
if not proj_cum.empty:
    final_val = proj_cum["cumulative"].iloc[-1]
    fig4.add_annotation(
        x=proj_cum["month"].iloc[-1], y=final_val,
        text=f"<b>{fmt(final_val)}</b>",
        showarrow=True, arrowhead=2,
        font=dict(size=14, color=C["forecast"]),
        ax=-60, ay=-30,
    )

fig4.update_layout(
    **LAYOUT,
    title=f"Cumulative Protocol Revenue (historical {fmt(hist_total)} + projected {fmt(three_year_cumulative)})",
    yaxis_title="Cumulative Revenue (USD)",
    height=440,
)
st.plotly_chart(fig4, use_container_width=True)

# ── CHART 5: Revenue Breakdown by Year ────────────────────────────────────────
st.markdown("### Revenue Breakdown by Year")

years = ["Year 1", "Year 2", "Year 3"]
interest_vals = [yearly_projections[yr]["annual_interest"] for yr in [1, 2, 3]]
liq_vals = [yearly_projections[yr]["annual_liq"] for yr in [1, 2, 3]]
total_vals = [yearly_projections[yr]["annual_total"] for yr in [1, 2, 3]]

fig5 = go.Figure()
fig5.add_trace(go.Bar(
    x=years, y=interest_vals,
    name="Interest Revenue", marker_color=C["interest"],
    text=[fmt(v) for v in interest_vals], textposition="inside",
    textfont=dict(size=13),
    hovertemplate="%{x}<br>Interest: $%{y:,.0f}<extra></extra>",
))
fig5.add_trace(go.Bar(
    x=years, y=liq_vals,
    name="Liquidation Fees", marker_color=C["liq_fee"],
    text=[fmt(v) for v in liq_vals], textposition="inside",
    textfont=dict(size=11),
    hovertemplate="%{x}<br>Liq Fees: $%{y:,.0f}<extra></extra>",
))

# Total annotations on top
for i, (yr_label, total) in enumerate(zip(years, total_vals)):
    fig5.add_annotation(
        x=yr_label, y=total,
        text=f"<b>{fmt(total)}</b>",
        showarrow=False, yshift=15,
        font=dict(size=14, color="#c9d1d9"),
    )

fig5.update_layout(
    **LAYOUT,
    barmode="stack",
    title="Projected Annual Revenue by Component",
    yaxis_title="Annual Revenue (USD)",
    height=400,
)
st.plotly_chart(fig5, use_container_width=True)

# ── CHART 6: BTC TVL Bridge ──────────────────────────────────────────────────
st.markdown("### BTC TVL Projection")

tvl_labels = ["Current\nwBTC", "Current\ncbBTC", "Year 1", "Year 2", "Year 3"]
tvl_wbtc = [
    current_wbtc_tvl_usd,
    0,
    yearly_projections[1]["wbtc_tvl_usd"],
    yearly_projections[2]["wbtc_tvl_usd"],
    yearly_projections[3]["wbtc_tvl_usd"],
]
tvl_cbbtc = [
    0,
    current_cbbtc_tvl_usd,
    yearly_projections[1]["cbbtc_tvl_usd"],
    yearly_projections[2]["cbbtc_tvl_usd"],
    yearly_projections[3]["cbbtc_tvl_usd"],
]
tvl_total = [
    current_wbtc_tvl_usd,
    current_cbbtc_tvl_usd,
    yearly_projections[1]["btc_tvl_usd"],
    yearly_projections[2]["btc_tvl_usd"],
    yearly_projections[3]["btc_tvl_usd"],
]

fig6 = go.Figure()

# Grouped bar: wBTC and cbBTC TVL
bar_labels = ["Current", "Year 1", "Year 2", "Year 3"]
wbtc_tvl_bars = [current_wbtc_tvl_usd] + [yearly_projections[yr]["wbtc_tvl_usd"] for yr in [1, 2, 3]]
cbbtc_tvl_bars = [current_cbbtc_tvl_usd] + [yearly_projections[yr]["cbbtc_tvl_usd"] for yr in [1, 2, 3]]
total_tvl_bars = [current_total_tvl_usd] + [yearly_projections[yr]["btc_tvl_usd"] for yr in [1, 2, 3]]

fig6.add_trace(go.Bar(
    x=bar_labels, y=wbtc_tvl_bars,
    name="wBTC TVL", marker_color=C["wbtc"],
    hovertemplate="%{x}<br>wBTC TVL: $%{y:,.0f}<extra></extra>",
))
fig6.add_trace(go.Bar(
    x=bar_labels, y=cbbtc_tvl_bars,
    name="cbBTC TVL", marker_color=C["cbbtc"],
    hovertemplate="%{x}<br>cbBTC TVL: $%{y:,.0f}<extra></extra>",
))

# Total annotations
for i, (lbl, total) in enumerate(zip(bar_labels, total_tvl_bars)):
    fig6.add_annotation(
        x=lbl, y=total,
        text=f"<b>{fmt(total)}</b>",
        showarrow=False, yshift=15,
        font=dict(size=13, color="#c9d1d9"),
    )

fig6.update_layout(
    **LAYOUT,
    barmode="stack",
    title="Projected BTC TVL in Aave (wBTC + cbBTC)",
    yaxis_title="TVL (USD)",
    height=400,
)
st.plotly_chart(fig6, use_container_width=True)

st.markdown(
    '<div class="note-box">'
    f'<b>Current BTC TVL:</b> {wbtc_tvl_tokens:,.1f} wBTC ({fmt(current_wbtc_tvl_usd)}) + '
    f'{cbbtc_tvl_tokens:,.1f} cbBTC ({fmt(current_cbbtc_tvl_usd)}) = {fmt(current_total_tvl_usd)} total. '
    f'Latest BTC price: {fmt(latest_btc)}. '
    'Year 1: tokens flat (sideways market). Year 2: +15% token growth, wBTC share declining. '
    'Year 3: +40% tokens, wBTC continues declining as cbBTC gains share.'
    '</div>',
    unsafe_allow_html=True)

# ── Assumptions & Methodology ─────────────────────────────────────────────────
with st.expander("Methodology & Assumptions", expanded=False):
    m_c1, m_c2 = st.columns(2)
    with m_c1:
        st.markdown(f"""**Model Design**
- **Year 1 base:** unrealized run-rate from existing open positions ({fmt(unrealized_total)}/yr).
  These are real borrows with real rates — not a prediction, but current reality.
  V2 portion ({fmt(unrealized_v2)}) decays 50% (deprecated markets closing).
- **Year 2-3:** V3 base × token growth × BTC price scaling for BTC-denominated borrows.
  Cross-checked against OLS regression (R²={r_squared:.2f}) as a floor — if regression says
  higher, we use regression.
- **Liq fees:** historical ratio of liq fees / interest revenue ({liq_to_interest_ratio:.1%})
- **BTC price for regression:** WBTC implied from borrow events (pre-2025) + CoinGecko (2025+)
""")
    with m_c2:
        st.markdown(f"""**Key Assumptions**
- **Reserve factors:** held constant at current levels (blended ~14.6% effective)
- **wBTC V2:** decays 50% Year 1, fully excluded Year 2-3 (deprecated)
- **BTC token count:** flat in Year 1, +15% Year 2, +40% Year 3
- **BTC-denom borrows** (~{btc_denom_rev / unrealized_total * 100 if unrealized_total > 0 else 10:.0f}% of revenue): scale with BTC price
- **Stablecoin borrows** (~{stable_denom_rev / unrealized_total * 100 if unrealized_total > 0 else 90:.0f}% of revenue): stay flat regardless of BTC price
- **BTC price scenarios:** Year 1 $60K, Year 2 $100K, Year 3 ramp $100K→$200K
- **New BTC tokens (LBTC, tBTC, eBTC):** not included
""")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    f"**Revenue Forecast Model:** Year 1 anchored to unrealized run-rate ({fmt(unrealized_total)}/yr from "
    f"existing positions, V2 decaying). Year 2-3: V3 base × token growth × BTC price scaling, "
    f"floored by OLS regression (R²={r_squared:.2f}). "
    f"Historical: {len(monthly)} months from {monthly['month'].min():%b %Y} to {monthly['month'].max():%b %Y}. "
    f"BTC scenarios: Year 1 $60K, Year 2 $100K, Year 3 $200K. "
    f"Liq fees: {liq_to_interest_ratio:.1%} of interest (historical ratio). "
    f"Reserve factors constant (~14.6% effective). Data: The Graph Gateway + CoinGecko."
)
