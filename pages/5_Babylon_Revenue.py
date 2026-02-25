"""Babylon TBV Revenue Forecast — vaultBTC Collateral on Aave v4.

Methodology:
  Models Babylon protocol revenue from 4 streams when vaultBTC is used as
  collateral on Aave v4:
    1. protocolLiquidityFee — share of borrower interest (dominant stream)
    2. coreSpokeLiquidationFeeBps — share of liquidator profit
    3. Vault Swap commission — from arbitrageur purchases of escrowed vaults
    4. peginFee — one-time fee per vault creation

  TVL projection:
    - Starting point: 58,000 BTC already staked in Babylon (not cold start!)
    - Year 1: activation_rate% of staked BTC onboarded as Aave collateral
    - Year 2-3: organic growth from new stakers + BTC price appreciation
    - Position size distribution modeled on real Aave v3 wBTC/cbBTC clusters

  Utilization, borrow rates, and liquidation ratios derived from real wBTC/cbBTC
  data in Aave V2/V3.

Revenue dominance:
  Interest fee is the primary revenue stream (~60-80% of total) because it accrues
  continuously on the entire borrowed amount. Liquidation-based streams only trigger
  during market dislocations.

BTC Price Scenarios (same as Revenue Forecast):
  Year 1 (Jun 2026 – May 2027): ~$60K  (sideways)
  Year 2 (Jun 2027 – May 2028): ~$100K (moderate)
  Year 3 (Jun 2028 – May 2029): ~$200K (bull)
"""

from datetime import date

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.collateral_fetcher import load_all
from src.collateral_queries import STABLECOINS
from src.price_cache import fetch_and_cache_history, get_price_lookup

# ── Style (identical to pages/3 & 4) ─────────────────────────────────────────
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

# ── Colors ────────────────────────────────────────────────────────────────────
C = {
    "grid":       "rgba(255,255,255,0.06)",
    "year1":      "#f0b429",
    "year2":      "#58a6ff",
    "year3":      "#3fb950",
    "historical": "#8b949e",
    "btc_price":  "#f0b429",
    "interest_fee":  "#e0a526",
    "liq_fee":       "#f97066",
    "vault_swap":    "#a78bfa",
    "pegin":         "#34d399",
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
    if abs(v) >= 1e9:
        return f"${v / 1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"${v / 1e6:.2f}M"
    if abs(v) >= 1e3:
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
    '<div class="rev-header"><h1>Babylon TBV Revenue Forecast</h1>'
    '<p>vaultBTC Collateral on Aave v4 · 3-Year Projection · 4 Revenue Streams</p></div>',
    unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
dfs = load_all()
borrows_df = dfs.get("borrows", pd.DataFrame())
repays_df = dfs.get("repays", pd.DataFrame())
liqs_df = dfs.get("liquidations", pd.DataFrame())
open_df = dfs.get("open_positions", pd.DataFrame())

if borrows_df.empty:
    st.error("Data files not found. Please ensure the parquet files are in data/.")
    st.stop()

# ── Price data ────────────────────────────────────────────────────────────────
with st.spinner("Loading price history…"):
    from src.collateral_queries import COINGECKO_IDS
    non_stable = [s for s in COINGECKO_IDS if s not in STABLECOINS]
    price_df = fetch_and_cache_history(non_stable, days=365)
    price_lookup = get_price_lookup(price_df)

# ═══════════════════════════════════════════════════════════════════════════════
# DERIVE HISTORICAL METRICS FROM REAL DATA
# ═══════════════════════════════════════════════════════════════════════════════

# ── BTC price ─────────────────────────────────────────────────────────────────
btc_monthly_price = price_df[price_df["symbol"].isin(["WBTC", "cbBTC"])].copy()
if not btc_monthly_price.empty:
    btc_monthly_price["month"] = btc_monthly_price["date"].dt.to_period("M").dt.to_timestamp()
    latest_btc_price = btc_monthly_price.groupby("month")["price_usd"].mean().iloc[-1]
else:
    latest_btc_price = 95_000

# ── Current TVL from open positions ───────────────────────────────────────────
if not open_df.empty:
    active = open_df[open_df["btc_collateral_tokens"] > 0].copy()
    cbbtc_tvl_tokens = active[active["collateral_key"] == "cbbtc"].drop_duplicates(
        subset="account")["btc_collateral_tokens"].sum()
    wbtc_tvl_tokens = active[active["collateral_key"] == "wbtc"].drop_duplicates(
        subset="account")["btc_collateral_tokens"].sum()
    total_btc_tvl_tokens = wbtc_tvl_tokens + cbbtc_tvl_tokens
else:
    cbbtc_tvl_tokens = wbtc_tvl_tokens = total_btc_tvl_tokens = 0

# ── Position size distribution (for vault estimation & methodology) ──────────
if not open_df.empty:
    positions_with_btc = open_df[open_df["btc_collateral_tokens"] > 0].drop_duplicates(
        subset=["account", "collateral_key"]
    )
    if not positions_with_btc.empty:
        avg_position_btc = positions_with_btc["btc_collateral_tokens"].mean()
        median_position_btc = positions_with_btc["btc_collateral_tokens"].median()
        n_positions = len(positions_with_btc)
        # Position size clusters (for display)
        bins = [0, 1, 5, 10, 25, 50, 100, 500, 1000, float("inf")]
        labels_b = ["<1", "1-5", "5-10", "10-25", "25-50", "50-100", "100-500", "500-1K", "1K+"]
        positions_with_btc = positions_with_btc.copy()
        positions_with_btc["bucket"] = pd.cut(
            positions_with_btc["btc_collateral_tokens"], bins=bins, labels=labels_b
        )
        bucket_stats = positions_with_btc.groupby("bucket", observed=True).agg(
            n_pos=("account", "count"),
            total_btc=("btc_collateral_tokens", "sum"),
        ).reset_index()
        bucket_stats["pct_pos"] = bucket_stats["n_pos"] / bucket_stats["n_pos"].sum() * 100
        bucket_stats["pct_btc"] = bucket_stats["total_btc"] / bucket_stats["total_btc"].sum() * 100
    else:
        avg_position_btc = 5.0
        median_position_btc = 2.0
        n_positions = 100
        bucket_stats = pd.DataFrame()
else:
    avg_position_btc = 5.0
    median_position_btc = 2.0
    n_positions = 100
    bucket_stats = pd.DataFrame()

# avg_vault_size_btc and monthly_churn_rate are now set via sidebar sliders

# ── Historical borrow rates ──────────────────────────────────────────────────
if not open_df.empty:
    active_stable = open_df[
        (open_df["btc_collateral_tokens"] > 0) &
        (open_df["borrowed_symbol"].isin(STABLECOINS))
    ].copy()
    if not active_stable.empty:
        active_stable["weighted_rate"] = active_stable["variable_borrow_rate"] * active_stable["borrow_balance_tokens"]
        total_borrow_tokens = active_stable["borrow_balance_tokens"].sum()
        avg_stable_borrow_rate = active_stable["weighted_rate"].sum() / total_borrow_tokens if total_borrow_tokens > 0 else 4.0
        avg_stable_borrow_rate = min(avg_stable_borrow_rate, 15.0)
    else:
        avg_stable_borrow_rate = 4.0
else:
    avg_stable_borrow_rate = 4.0

# ── Historical liquidation ratio (median to avoid spike distortion) ──────────
if not liqs_df.empty:
    liqs_monthly = liqs_df.copy()
    liqs_monthly["month"] = pd.to_datetime(liqs_monthly["date"]).dt.to_period("M").dt.to_timestamp()
    monthly_liq_vol = liqs_monthly.groupby("month")["amount_usd"].sum()
    if total_btc_tvl_tokens > 0:
        median_monthly_liq = monthly_liq_vol.median()
        estimated_tvl = total_btc_tvl_tokens * latest_btc_price
        annual_liq_ratio = (median_monthly_liq / estimated_tvl * 12) if estimated_tvl > 0 else 0.03
        annual_liq_ratio = max(0.01, min(annual_liq_ratio, 0.15))
    else:
        annual_liq_ratio = 0.03
else:
    annual_liq_ratio = 0.03


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — ALL PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════════

st.sidebar.header("Babylon TBV Parameters")

BABYLON_STAKED_BTC = 58_000  # Current Babylon staked BTC

st.sidebar.markdown("**Babylon Staked BTC**")
babylon_staked = st.sidebar.number_input(
    "BTC staked in Babylon", value=BABYLON_STAKED_BTC, min_value=10_000, max_value=200_000, step=5_000,
    help="Current BTC staked in Babylon protocol (source of collateral)")
activation_pct = st.sidebar.slider(
    "Year 1 activation rate (%)", 10, 80, 50, 5,
    help="% of staked BTC activated as Aave v4 collateral during Year 1")
yr2_growth = st.sidebar.slider(
    "Year 2 organic growth (%)", 10, 200, 60, 5,
    help="Additional TVL growth in Year 2 from new stakers")
yr3_growth = st.sidebar.slider(
    "Year 3 organic growth (%)", 10, 200, 120, 5,
    help="Additional TVL growth in Year 3")

st.sidebar.markdown("**Market Parameters**")
utilization_rate = st.sidebar.slider(
    "Utilization rate (%)", 20.0, 80.0, 50.0, 5.0,
    help="% of TVL that is borrowed against")
blended_borrow_rate = st.sidebar.slider(
    "Blended borrow rate (%)", 1.0, 10.0, round(avg_stable_borrow_rate, 1), 0.5,
    help=f"Average borrow rate for stablecoins (current real: {avg_stable_borrow_rate:.1f}%)")

st.sidebar.markdown("---")

# ── Protocol Fees (Interest Fee stream) ──────────────────────────────────────
enable_protocol_fees = st.sidebar.checkbox("**Protocol Fees**", value=True,
    help="protocolLiquidityFee — Babylon's share of borrower interest")
if enable_protocol_fees:
    protocol_liquidity_fee = st.sidebar.slider(
        "protocolLiquidityFee (%)", 0.5, 10.0, 2.5, 0.5,
        help="Babylon's share of borrower interest (separate from Aave's reserve factor ~10-25%)")
else:
    protocol_liquidity_fee = 0.0

# ── Liquidation Parameters ───────────────────────────────────────────────────
enable_liquidation = st.sidebar.checkbox("**Liquidation Parameters**", value=True,
    help="coreSpokeLiquidationFeeBps — Babylon's share of liquidator profit")
if enable_liquidation:
    core_spoke_liq_fee = st.sidebar.slider(
        "coreSpokeLiquidationFeeBps (%)", 0.5, 10.0, 2.5, 0.5,
        help="Babylon's % of liquidator profit (profit = seized collateral − repaid debt)")
    max_liq_bonus = st.sidebar.slider(
        "maxLiquidationBonus (%)", 3.0, 15.0, 5.0, 0.5,
        help="Flat liquidation bonus (105 = 5% extra collateral per $1 repaid)")
    collateral_factor = st.sidebar.slider(
        "collateralFactor (%)", 50.0, 85.0, 75.0, 5.0,
        help="Liquidation LTV for vaultBTC (reference only, not used in liq calc)")
else:
    core_spoke_liq_fee = 0.0
    max_liq_bonus = 0.0
    collateral_factor = 75.0

# ── Vault Swap (Vault Swap commission stream) ────────────────────────────────
enable_vault_swap = st.sidebar.checkbox("**Vault Swap**", value=True,
    help="Commission from arbitrageur purchases of escrowed vaults")
if enable_vault_swap:
    liquidator_fee_bps = st.sidebar.slider(
        "liquidatorFeeBps (%)", 0.5, 5.0, 1.1, 0.1,
        help="Discount when Vault Swap pays liquidator in WBTC (= sellDiscountBps)")
    arb_discount_bps = st.sidebar.slider(
        "arbitrageurDiscountBps (%)", 0.1, 3.0, 1.0, 0.1,
        help="Discount arbitrageur gets when purchasing escrowed vault")
    vault_swap_share = st.sidebar.slider(
        "Vault Swap share (%)", 50.0, 95.0, 75.0, 5.0,
        help="% of liquidations via Vault Swap (vs direct BTC redemption)")
else:
    liquidator_fee_bps = 0.0
    arb_discount_bps = 0.0
    vault_swap_share = 0.0

# ── Vault Creation (Peg-in Fee stream) ───────────────────────────────────────
enable_vault_creation = st.sidebar.checkbox("**Vault Creation**", value=True,
    help="Peg-in fee — one-time fee per vault creation")
if enable_vault_creation:
    avg_vault_size_btc = st.sidebar.slider(
        "Avg vault size (BTC)", 0.5, 10.0, round(max(1.0, median_position_btc / 2), 1), 0.5,
        help=f"Average BTC per vault UTXO (derived from Aave data: median position {median_position_btc:.1f} BTC / 2)")
    pegin_fee_usd = st.sidebar.slider(
        "peginFee ($)", 10, 200, 50, 10,
        help="One-time fee per vault creation in ETH equivalent")
else:
    avg_vault_size_btc = max(1.0, median_position_btc / 2)
    pegin_fee_usd = 0

# ═══════════════════════════════════════════════════════════════════════════════
# BTC PRICE SCENARIOS
# ═══════════════════════════════════════════════════════════════════════════════

LAUNCH_MONTH = 6   # June 2026
LAUNCH_YEAR  = 2026

BTC_SCENARIOS = {
    1: {"label": "Year 1 (Jun 2026 – May 2027)", "avg_btc": 60_000,
        "description": "Sideways ~$60K"},
    2: {"label": "Year 2 (Jun 2027 – May 2028)", "avg_btc": 100_000,
        "description": "Moderate ~$100K"},
    3: {"label": "Year 3 (Jun 2028 – May 2029)", "avg_btc": 200_000,
        "description": "Bull ~$200K"},
}

# ═══════════════════════════════════════════════════════════════════════════════
# TVL PROJECTION — Babylon staked BTC activation model
# ═══════════════════════════════════════════════════════════════════════════════
#
# Key difference from wBTC/cbBTC: Babylon is NOT a cold start.
# 58K BTC already staked → users just need to "activate" their vaults for Aave.
# This is more like a feature launch for existing users, not a new protocol.
#
# Year 1: S-curve activation of staked BTC (fast ramp, then plateau)
#   - Month 1-3: early adopters (10-20% of target)
#   - Month 4-8: main wave (60-80% of target)
#   - Month 9-12: plateau near target
# Year 2: organic growth (new BTC stakers + existing stakers increasing positions)
# Year 3: continued growth, slowing

yr1_target_btc = babylon_staked * (activation_pct / 100)
yr2_target_btc = yr1_target_btc * (1 + yr2_growth / 100)
yr3_target_btc = yr2_target_btc * (1 + yr3_growth / 100)

projection_months = []
for yr, scenario in BTC_SCENARIOS.items():
    btc_price = scenario["avg_btc"]
    for m_in_year in range(12):
        t = (m_in_year + 1) / 12  # 0.083 to 1.0

        if yr == 1:
            # S-curve activation: fast ramp in months 3-8
            # Logistic: y = target / (1 + exp(-k*(t - t0)))
            s = 1.0 / (1.0 + np.exp(-10 * (t - 0.4)))
            tvl_btc = yr1_target_btc * s
        elif yr == 2:
            # Linear growth from yr1 end to yr2 target
            tvl_btc = yr1_target_btc + (yr2_target_btc - yr1_target_btc) * t
        else:
            # Slowing growth from yr2 end to yr3 target
            tvl_btc = yr2_target_btc + (yr3_target_btc - yr2_target_btc) * t

        tvl_usd = tvl_btc * btc_price

        cal_month = LAUNCH_MONTH + m_in_year + (yr - 1) * 12
        cal_year = LAUNCH_YEAR + (cal_month - 1) // 12
        cal_month = (cal_month - 1) % 12 + 1

        projection_months.append({
            "month": pd.Timestamp(year=cal_year, month=cal_month, day=1),
            "year": yr,
            "year_label": f"Year {yr}",
            "month_in_year": m_in_year,
            "global_month_idx": (yr - 1) * 12 + m_in_year,
            "btc_price": btc_price,
            "tvl_btc_tokens": tvl_btc,
            "tvl_usd": tvl_usd,
        })

proj_df = pd.DataFrame(projection_months)

# ═══════════════════════════════════════════════════════════════════════════════
# REVENUE MODEL — 4 STREAMS
# ═══════════════════════════════════════════════════════════════════════════════

revenue_rows = []
for _, row in proj_df.iterrows():
    tvl_usd = row["tvl_usd"]
    tvl_btc = row["tvl_btc_tokens"]

    # ── Stream 1: protocolLiquidityFee (from borrower interest) ───────────
    # Borrowers take stablecoins against vaultBTC → pay interest → Babylon takes %
    borrow_volume_usd = tvl_usd * (utilization_rate / 100)
    monthly_interest_paid = borrow_volume_usd * (blended_borrow_rate / 100) / 12
    stream_interest_fee = monthly_interest_paid * (protocol_liquidity_fee / 100)

    # ── Stream 2: coreSpokeLiquidationFeeBps (from liquidator profit) ─────
    # monthly_liq_volume ≈ seized collateral value
    # liquidator profit = seized − repaid_debt = repaid_debt × (bonus − 1)
    # since seized = repaid_debt × bonus → repaid_debt = seized / bonus
    monthly_liq_volume = tvl_usd * (annual_liq_ratio / 12)
    bonus_multiplier = 1.0 + max_liq_bonus / 100          # e.g. 1.05
    repaid_debt = monthly_liq_volume / bonus_multiplier
    liquidator_profit = monthly_liq_volume - repaid_debt   # = repaid_debt × (bonus − 1)
    stream_liq_fee = liquidator_profit * (core_spoke_liq_fee / 100)

    # ── Stream 3: Vault Swap commission ───────────────────────────────────
    # Per Architecture doc Section 4.3:
    #   At escrow: liquidator gets vault_value × (1 − liquidatorFeeBps) in WBTC
    #   hub_debt = vault_value × (1 − liquidatorFeeBps)
    #   At purchase: arb pays hub_debt + commission
    #   commission = (wbtcEquivalentNow − hub_debt) × discountCommissionBps
    # TBV Parameters: TBV charges (liquidatorFeeBps − arbitrageurDiscountBps) × vault_value
    vault_swap_liq_volume = monthly_liq_volume * (vault_swap_share / 100)
    # Protocol's net revenue per vault = (liquidatorFeeBps − arbitrageurDiscountBps) × vault_value
    protocol_spread = max(0, liquidator_fee_bps - arb_discount_bps) / 100
    stream_vault_swap = vault_swap_liq_volume * protocol_spread

    # ── Stream 4: peginFee (one-time vault creation) ──────────────────────
    idx = row["global_month_idx"]
    if idx == 0:
        prev_tvl_btc = 0
    else:
        prev_row = proj_df[proj_df["global_month_idx"] == idx - 1]
        prev_tvl_btc = prev_row["tvl_btc_tokens"].iloc[0] if not prev_row.empty else 0

    new_btc = max(0, tvl_btc - prev_tvl_btc)
    new_vaults = new_btc / avg_vault_size_btc
    stream_pegin = new_vaults * pegin_fee_usd

    total_monthly = stream_interest_fee + stream_liq_fee + stream_vault_swap + stream_pegin

    revenue_rows.append({
        "month": row["month"],
        "year": row["year"],
        "year_label": row["year_label"],
        "tvl_usd": tvl_usd,
        "tvl_btc": tvl_btc,
        "btc_price": row["btc_price"],
        "interest_fee": stream_interest_fee,
        "liq_fee": stream_liq_fee,
        "vault_swap": stream_vault_swap,
        "pegin": stream_pegin,
        "total": total_monthly,
    })

rev_df = pd.DataFrame(revenue_rows)

# ── Yearly aggregates ─────────────────────────────────────────────────────────
yearly = {}
for yr in [1, 2, 3]:
    yr_data = rev_df[rev_df["year"] == yr]
    yearly[yr] = {
        "interest_fee": yr_data["interest_fee"].sum(),
        "liq_fee": yr_data["liq_fee"].sum(),
        "vault_swap": yr_data["vault_swap"].sum(),
        "pegin": yr_data["pegin"].sum(),
        "total": yr_data["total"].sum(),
        "avg_tvl": yr_data["tvl_usd"].mean(),
        "end_tvl_btc": yr_data["tvl_btc"].iloc[-1],
    }

three_year_total = sum(y["total"] for y in yearly.values())
interest_share = sum(y["interest_fee"] for y in yearly.values()) / three_year_total * 100 if three_year_total > 0 else 0

# ═══════════════════════════════════════════════════════════════════════════════
# BUILD THE PAGE
# ═══════════════════════════════════════════════════════════════════════════════

# ── KPI Row ───────────────────────────────────────────────────────────────────
st.markdown("### Key Projections")
c1, c2, c3, c4 = st.columns(4)
kpi(c1, fmt(yearly[1]["total"]), "Year 1 Revenue",
    f"BTC ~${BTC_SCENARIOS[1]['avg_btc'] / 1000:.0f}K · {yearly[1]['end_tvl_btc']:,.0f} BTC · {fmt(yearly[1]['end_tvl_btc'] * BTC_SCENARIOS[1]['avg_btc'])} TVL")
kpi(c2, fmt(yearly[2]["total"]), "Year 2 Revenue",
    f"BTC ~${BTC_SCENARIOS[2]['avg_btc'] / 1000:.0f}K · {yearly[2]['end_tvl_btc']:,.0f} BTC · {fmt(yearly[2]['end_tvl_btc'] * BTC_SCENARIOS[2]['avg_btc'])} TVL")
kpi(c3, fmt(yearly[3]["total"]), "Year 3 Revenue",
    f"BTC ~${BTC_SCENARIOS[3]['avg_btc'] / 1000:.0f}K · {yearly[3]['end_tvl_btc']:,.0f} BTC · {fmt(yearly[3]['end_tvl_btc'] * BTC_SCENARIOS[3]['avg_btc'])} TVL")
kpi(c4, fmt(three_year_total), "3-Year Cumulative",
    f"Interest fee = {interest_share:.0f}% of total")

st.markdown(
    '<div class="note-box">'
    f'<b>Starting point:</b> <b>{babylon_staked:,} BTC</b> already staked in Babylon — NOT cold start! '
    f'Year 1: <b>{activation_pct}%</b> activated as Aave collateral → {yr1_target_btc:,.0f} BTC '
    f'({fmt(yr1_target_btc * BTC_SCENARIOS[1]["avg_btc"])} at $60K). '
    f'Borrowers take stablecoins (USDC, USDT, DAI…) at ~{blended_borrow_rate:.1f}% rate, '
    f'Babylon takes <b>{protocol_liquidity_fee}%</b> of interest → '
    f'<b>interest fee = {interest_share:.0f}% of total revenue</b>. '
    f'For context: Aave has {wbtc_tvl_tokens:,.0f} wBTC + {cbbtc_tvl_tokens:,.0f} cbBTC = '
    f'{total_btc_tvl_tokens:,.0f} BTC as collateral today.'
    '</div>',
    unsafe_allow_html=True)

# ── CHART 1: vaultBTC TVL Projection ─────────────────────────────────────────
st.markdown("### vaultBTC TVL Projection")

fig_tvl = go.Figure()

# Projected vaultBTC TVL by year
for yr in [1, 2, 3]:
    sub = proj_df[proj_df["year"] == yr]
    fig_tvl.add_trace(go.Scatter(
        x=sub["month"], y=sub["tvl_usd"],
        mode="lines+markers",
        name=f"vaultBTC Year {yr} (BTC ~${BTC_SCENARIOS[yr]['avg_btc'] / 1000:.0f}K)",
        line=dict(color=C[f"year{yr}"], width=2.5),
        marker=dict(size=5),
        hovertemplate=f"Year {yr}" + "<br>%{x|%b %Y}<br>TVL: $%{y:,.0f}<br>BTC: %{customdata:,.0f}<extra></extra>",
        customdata=sub["tvl_btc_tokens"],
    ))

for start, end, label, color in [
    ("2026-06-01", "2027-05-31", "Year 1", C["year1"]),
    ("2027-06-01", "2028-05-31", "Year 2", C["year2"]),
    ("2028-06-01", "2029-05-31", "Year 3", C["year3"]),
]:
    fig_tvl.add_vrect(
        x0=start, x1=end, fillcolor=color, opacity=0.06, line_width=0,
        annotation_text=label, annotation_position="top left",
        annotation_font_color=color,
    )

fig_tvl.update_layout(
    **LAYOUT,
    title=f"vaultBTC TVL Projection ({babylon_staked:,} BTC staked → {activation_pct}% activation Year 1)",
    yaxis_title="TVL (USD)",
    height=440,
)
st.plotly_chart(fig_tvl, use_container_width=True)

st.markdown(
    '<div class="note-box">'
    f'<b>TVL model:</b> {babylon_staked:,} BTC already staked in Babylon. '
    f'Year 1: S-curve activation → {yr1_target_btc:,.0f} BTC as Aave collateral '
    f'(={fmt(yr1_target_btc * BTC_SCENARIOS[1]["avg_btc"])} at $60K). '
    f'Year 2: +{yr2_growth}% organic → {yr2_target_btc:,.0f} BTC '
    f'({fmt(yr2_target_btc * BTC_SCENARIOS[2]["avg_btc"])} at $100K). '
    f'Year 3: +{yr3_growth}% → {yr3_target_btc:,.0f} BTC '
    f'({fmt(yr3_target_btc * BTC_SCENARIOS[3]["avg_btc"])} at $200K). '
    f'Babylon starts with an existing user base — adoption expected to be faster '
    f'than wBTC/cbBTC which grew from zero.'
    '</div>',
    unsafe_allow_html=True)

# ── CHART 1b: TVL in BTC + USD + BTC Price ───────────────────────────────────
st.markdown("### TVL Forecast: BTC Tokens + USD Value")

fig_tvl_combo = go.Figure()

# TVL in USD — bars (left axis), single color
fig_tvl_combo.add_trace(go.Bar(
    x=proj_df["month"], y=proj_df["tvl_usd"],
    name="TVL (USD)",
    marker_color="rgba(240,180,41,0.55)",
    hovertemplate="%{x|%b %Y}<br>TVL: $%{y:,.0f}<extra></extra>",
    yaxis="y",
))

# BTC tokens — green line (right axis)
fig_tvl_combo.add_trace(go.Scatter(
    x=proj_df["month"], y=proj_df["tvl_btc_tokens"],
    mode="lines+markers", name="vaultBTC Collateral (BTC)",
    line=dict(color="#3fb950", width=2.5),
    marker=dict(size=4),
    hovertemplate="%{x|%b %Y}<br>%{y:,.0f} BTC<extra></extra>",
    yaxis="y2",
))

# BTC price — yellow dotted line (right axis)
fig_tvl_combo.add_trace(go.Scatter(
    x=proj_df["month"], y=proj_df["btc_price"],
    mode="lines", name="BTC Price (forecast)",
    line=dict(color=C["btc_price"], width=1.5, dash="dot"),
    hovertemplate="%{x|%b %Y}<br>BTC: $%{y:,.0f}<extra></extra>",
    yaxis="y2",
))

fig_tvl_combo.update_layout(
    **{k: v for k, v in LAYOUT.items() if k not in ("yaxis",)},
    title="TVL (USD bars) · vaultBTC Collateral & BTC Price (lines)",
    yaxis=dict(title="TVL (USD)", gridcolor=C["grid"], showgrid=True, side="left"),
    yaxis2=dict(title="BTC / BTC Price", overlaying="y", side="right", showgrid=False),
    height=460,
)
st.plotly_chart(fig_tvl_combo, use_container_width=True)

# ── CHART 1d: Outstanding Borrows (USD) under BTC collateral ──────────────────────
st.markdown("### Outstanding Borrows (USD) — Loans Against vaultBTC")

proj_df["borrow_usd"] = proj_df["tvl_usd"] * (utilization_rate / 100)
proj_df["monthly_interest_usd"] = proj_df["borrow_usd"] * (blended_borrow_rate / 100) / 12

fig_borrow = go.Figure()

# Borrow volume bars
for yr in [1, 2, 3]:
    sub = proj_df[proj_df["year"] == yr]
    fig_borrow.add_trace(go.Bar(
        x=sub["month"], y=sub["borrow_usd"],
        name=f"Outstanding Borrows Year {yr}",
        marker_color=C[f"year{yr}"], opacity=0.75,
        hovertemplate=f"Year {yr}" + "<br>%{x|%b %Y}<br>Borrowed: $%{y:,.0f}<extra></extra>",
    ))

# Monthly interest line (secondary axis)
fig_borrow.add_trace(go.Scatter(
    x=proj_df["month"], y=proj_df["monthly_interest_usd"],
    mode="lines+markers", name="Monthly Interest Paid",
    line=dict(color=C["interest_fee"], width=2.5),
    marker=dict(size=4),
    hovertemplate="%{x|%b %Y}<br>Interest/mo: $%{y:,.0f}<extra></extra>",
    yaxis="y2",
))

for start, end, label, color in [
    ("2026-06-01", "2027-05-31", "Year 1", C["year1"]),
    ("2027-06-01", "2028-05-31", "Year 2", C["year2"]),
    ("2028-06-01", "2029-05-31", "Year 3", C["year3"]),
]:
    fig_borrow.add_vrect(
        x0=start, x1=end, fillcolor=color, opacity=0.04, line_width=0,
        annotation_text=label, annotation_position="top left",
        annotation_font_color=color,
    )

fig_borrow.update_layout(
    **{k: v for k, v in LAYOUT.items() if k not in ("yaxis",)},
    title=f"Outstanding Borrows (utilization {utilization_rate:.0f}%) + Monthly Interest ({blended_borrow_rate:.1f}% APR)",
    yaxis=dict(title="Outstanding Borrows (USD)", gridcolor=C["grid"], showgrid=True, side="left"),
    yaxis2=dict(title="Monthly Interest (USD)", overlaying="y", side="right", showgrid=False),
    height=440,
)
st.plotly_chart(fig_borrow, use_container_width=True)

st.markdown(
    '<div class="note-box">'
    f'<b>Outstanding Borrows</b> = TVL × {utilization_rate:.0f}% utilization. '
    f'Borrowers take stablecoins (USDC, USDT, DAI) against vaultBTC collateral. '
    f'<b>Monthly Interest</b> = Borrowed × {blended_borrow_rate:.1f}% / 12 — '
    f'Babylon takes {protocol_liquidity_fee}% of this (= primary revenue stream).'
    '</div>',
    unsafe_allow_html=True)

# ── CHART 2: Revenue Breakdown — stacked bar by year ─────────────────────────
st.markdown("### Revenue Breakdown by Year")

years_labels = ["Year 1", "Year 2", "Year 3"]
streams = [
    ("interest_fee", "Interest Fee (protocolLiquidityFee)", C["interest_fee"]),
    ("liq_fee", "Liquidation Fee (coreSpokeLiqFee)", C["liq_fee"]),
    ("vault_swap", "Vault Swap Commission", C["vault_swap"]),
    ("pegin", "Peg-in Fee", C["pegin"]),
]

fig_breakdown = go.Figure()
for stream_key, stream_name, color in streams:
    vals = [yearly[yr][stream_key] for yr in [1, 2, 3]]
    fig_breakdown.add_trace(go.Bar(
        x=years_labels, y=vals,
        name=stream_name, marker_color=color,
        text=[fmt(v) for v in vals], textposition="inside",
        textfont=dict(size=11),
        hovertemplate="%{x}<br>" + stream_name + ": $%{y:,.0f}<extra></extra>",
    ))

for i, yr in enumerate([1, 2, 3]):
    total = yearly[yr]["total"]
    fig_breakdown.add_annotation(
        x=years_labels[i], y=total,
        text=f"<b>{fmt(total)}</b>",
        showarrow=False, yshift=15,
        font=dict(size=14, color="#c9d1d9"),
    )

fig_breakdown.update_layout(
    **LAYOUT, barmode="stack",
    title="Projected Annual Babylon Revenue by Stream",
    yaxis_title="Annual Revenue (USD)", height=440,
)
st.plotly_chart(fig_breakdown, use_container_width=True)

# ── CHART 3: Monthly Revenue Timeline ────────────────────────────────────────
st.markdown("### Monthly Revenue Timeline")

fig_monthly = go.Figure()
for stream_key, stream_name, color in streams:
    fig_monthly.add_trace(go.Bar(
        x=rev_df["month"], y=rev_df[stream_key],
        name=stream_name, marker_color=color,
        hovertemplate="%{x|%b %Y}<br>" + stream_name + ": $%{y:,.0f}<extra></extra>",
    ))

for start, end, label, color in [
    ("2026-06-01", "2027-05-31", "Year 1", C["year1"]),
    ("2027-06-01", "2028-05-31", "Year 2", C["year2"]),
    ("2028-06-01", "2029-05-31", "Year 3", C["year3"]),
]:
    fig_monthly.add_vrect(
        x0=start, x1=end, fillcolor=color, opacity=0.06, line_width=0,
        annotation_text=label, annotation_position="top left",
        annotation_font_color=color,
    )

fig_monthly.update_layout(
    **LAYOUT, barmode="stack",
    title="Monthly Babylon Revenue (4 streams stacked)",
    yaxis_title="Monthly Revenue (USD)", height=480,
)
st.plotly_chart(fig_monthly, use_container_width=True)

# ── CHART 4: Cumulative Revenue ──────────────────────────────────────────────
st.markdown("### Cumulative Revenue")

cum_df = rev_df[["month", "interest_fee", "liq_fee", "vault_swap", "pegin", "total"]].copy().sort_values("month")
cum_df["cumulative"] = cum_df["total"].cumsum()
for sk, _, _ in streams:
    cum_df[f"cum_{sk}"] = cum_df[sk].cumsum()

fig_cum = go.Figure()
fig_cum.add_trace(go.Scatter(
    x=cum_df["month"], y=cum_df["cum_interest_fee"],
    name="Interest Fee", fill="tozeroy",
    line=dict(color=C["interest_fee"], width=0), fillcolor="rgba(224,165,38,0.3)",
    hovertemplate="%{x|%b %Y}<br>Interest: $%{y:,.0f}<extra></extra>",
))
fig_cum.add_trace(go.Scatter(
    x=cum_df["month"], y=cum_df["cum_interest_fee"] + cum_df["cum_liq_fee"],
    name="+ Liq Fee", fill="tonexty",
    line=dict(color=C["liq_fee"], width=0), fillcolor="rgba(249,112,102,0.3)",
    hovertemplate="%{x|%b %Y}<br>+ Liq Fee: $%{y:,.0f}<extra></extra>",
))
fig_cum.add_trace(go.Scatter(
    x=cum_df["month"],
    y=cum_df["cum_interest_fee"] + cum_df["cum_liq_fee"] + cum_df["cum_vault_swap"],
    name="+ Vault Swap", fill="tonexty",
    line=dict(color=C["vault_swap"], width=0), fillcolor="rgba(167,139,250,0.3)",
    hovertemplate="%{x|%b %Y}<br>+ Vault Swap: $%{y:,.0f}<extra></extra>",
))
fig_cum.add_trace(go.Scatter(
    x=cum_df["month"], y=cum_df["cumulative"],
    name="Total", fill="tonexty",
    line=dict(color=C["pegin"], width=2), fillcolor="rgba(52,211,153,0.3)",
    hovertemplate="%{x|%b %Y}<br>Total: $%{y:,.0f}<extra></extra>",
))

if not cum_df.empty:
    fig_cum.add_annotation(
        x=cum_df["month"].iloc[-1], y=cum_df["cumulative"].iloc[-1],
        text=f"<b>{fmt(cum_df['cumulative'].iloc[-1])}</b>",
        showarrow=True, arrowhead=2, font=dict(size=14, color=C["pegin"]), ax=-60, ay=-30,
    )

fig_cum.update_layout(
    **LAYOUT,
    title=f"Cumulative Babylon Revenue — 3-Year Total: {fmt(three_year_total)}",
    yaxis_title="Cumulative Revenue (USD)", height=440,
)
st.plotly_chart(fig_cum, use_container_width=True)

# ── Parameters Table ──────────────────────────────────────────────────────────
st.markdown("### Current Parameter Values")

params_data = [
    ["Babylon staked BTC", f"{babylon_staked:,}", "BTC currently staked in Babylon protocol"],
    ["Year 1 activation", f"{activation_pct}%", f"→ {yr1_target_btc:,.0f} BTC as Aave collateral"],
    ["Year 2 growth", f"+{yr2_growth}%", f"→ {yr2_target_btc:,.0f} BTC"],
    ["Year 3 growth", f"+{yr3_growth}%", f"→ {yr3_target_btc:,.0f} BTC"],
    ["protocolLiquidityFee", f"{protocol_liquidity_fee}%", "Babylon's share of borrower interest (separate from Aave reserve factor)"],
    ["coreSpokeLiquidationFeeBps", f"{core_spoke_liq_fee}%", "Babylon's % of liquidator profit"],
    ["maxLiquidationBonus", f"{max_liq_bonus}%", "Flat bonus: 105 = liquidator gets 5% extra collateral per $1 repaid"],
    ["collateralFactor", f"{collateral_factor}%", "Liquidation LTV for vaultBTC (reference only)"],
    ["liquidatorFeeBps", f"{liquidator_fee_bps}%", "Discount when Vault Swap pays liquidator in WBTC"],
    ["arbitrageurDiscountBps", f"{arb_discount_bps}%", "Discount arbitrageur gets purchasing escrowed vault"],
    ["Vault Swap spread", f"{max(0, liquidator_fee_bps - arb_discount_bps):.1f}%", "Protocol revenue = liquidatorFee − arbDiscount"],
    ["Vault Swap share", f"{vault_swap_share}%", "% of liquidations via Vault Swap (vs direct BTC redemption)"],
    ["Avg vault size", f"{avg_vault_size_btc:.1f} BTC", f"From Aave data: median {median_position_btc:.1f} BTC / 2"],
    ["peginFee", f"${pegin_fee_usd}", f"Per vault creation (~{avg_vault_size_btc:.1f} BTC/vault)"],
    ["Utilization", f"{utilization_rate}%", "% of TVL borrowed against"],
    ["Borrow rate", f"{blended_borrow_rate}%", f"Real weighted avg: {avg_stable_borrow_rate:.1f}%"],
    ["Annual liq ratio", f"{annual_liq_ratio:.1%}", "Median historical (wBTC/cbBTC) — flat rate, not volatility-adjusted"],
]

st.dataframe(pd.DataFrame(params_data, columns=["Parameter", "Value", "Description"]),
             use_container_width=True, hide_index=True)

# ── Methodology Expander ─────────────────────────────────────────────────────
with st.expander("Methodology & Assumptions", expanded=False):
    m_c1, m_c2 = st.columns(2)
    with m_c1:
        st.markdown(f"""**Revenue Model — 4 streams**

**Stream 1 — Interest Fee — DOMINANT ({interest_share:.0f}% of total):**
```
borrowed = TVL × utilization ({utilization_rate:.0f}%)
interest/mo = borrowed × rate ({blended_borrow_rate:.1f}%) / 12
babylon = interest × fee ({protocol_liquidity_fee}%)
```
Borrowers take stablecoins against vaultBTC. Babylon takes a
separate cut of borrower interest (not from Aave's reserve factor).
Dominant because it accrues on ALL borrowed volume continuously.

**Stream 2 — Liquidation Fee (coreSpokeLiquidationFeeBps):**
```
seized/mo = TVL × liq_ratio ({annual_liq_ratio:.1%}/yr) / 12
repaid_debt = seized / bonus (1 + {max_liq_bonus}%)
liquidator_profit = seized − repaid_debt
babylon = profit × {core_spoke_liq_fee}%
```
⚠️ `annual_liq_ratio` = median from real wBTC/cbBTC data ({annual_liq_ratio:.1%}/yr),
applied as a flat rate across all years. Does not model BTC price
volatility or liquidation threshold — actual liquidations would spike
during sharp drawdowns and be near-zero in calm markets.

**Stream 3 — Vault Swap Commission:**
```
swap_volume = seized × swap_share ({vault_swap_share:.0f}%)
spread = liquidatorFee ({liquidator_fee_bps}%)
         − arbDiscount ({arb_discount_bps}%)
         = {max(0, liquidator_fee_bps - arb_discount_bps):.1f}%
babylon = swap_volume × spread
```
Per Architecture doc: at escrow liquidator gets WBTC at
(1 − liquidatorFeeBps) discount; arb buys at arbDiscount.
Protocol keeps the spread between the two.

**Stream 4 — Peg-in Fee:**
```
new_vaults = new_btc / {avg_vault_size_btc:.1f}
babylon = vaults × ${pegin_fee_usd}
```
""")
    with m_c2:
        st.markdown(f"""**TVL Model — Babylon activation**

**Not cold start:** {babylon_staked:,} BTC already staked.
Users activate vaults for Aave — like enabling a feature, not
starting from zero.

**Year 1:** S-curve activation
- {activation_pct}% of staked → {yr1_target_btc:,.0f} BTC
- Fast ramp months 3-8 (whale-driven)
- {fmt(yr1_target_btc * BTC_SCENARIOS[1]["avg_btc"])} TVL at $60K

**Year 2:** +{yr2_growth}% organic growth
- New stakers + increased positions
- {yr2_target_btc:,.0f} BTC, {fmt(yr2_target_btc * BTC_SCENARIOS[2]["avg_btc"])} at $100K

**Year 3:** +{yr3_growth}% continued growth
- {yr3_target_btc:,.0f} BTC, {fmt(yr3_target_btc * BTC_SCENARIOS[3]["avg_btc"])} at $200K

**Position distribution (from Aave v3):**
- 62% positions < 5 BTC (3.7% of TVL)
- 4% positions > 100 BTC (71.5% of TVL)
- Whale-heavy → early whale activation drives fast TVL

**Data sources:**
- {n_positions} Aave positions, {len(liqs_df) if not liqs_df.empty else 0} liquidations
- Borrow rates: real weighted avg {avg_stable_borrow_rate:.1f}%
- Liq ratio: {annual_liq_ratio:.1%}/yr (median from 63 months)
""")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    f"**Babylon TBV Revenue Forecast:** {babylon_staked:,} BTC staked → {activation_pct}% Year 1 activation. "
    f"4 revenue streams, interest fee = {interest_share:.0f}% of total. "
    f"BTC: Year 1 $60K, Year 2 $100K, Year 3 $200K. "
    f"Position distribution from {n_positions} real Aave positions. "
    f"Data: The Graph + CoinGecko. Architecture: Trustless Bitcoin Vaults (Babylon Labs)."
)
