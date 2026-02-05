"""Raw Data page — full table with Etherscan links, filters and CSV export."""

import streamlit as st
import pandas as pd
from src.queries import ASSETS
from src.ui_helpers import asset_selector
from src.data_processor import load_data, filter_data

ETHERSCAN_TX = "https://etherscan.io/tx/"
ETHERSCAN_ADDR = "https://etherscan.io/address/"

# --- Asset Selector (persisted via session_state) ---
asset_key, asset_config = asset_selector()
symbol = asset_config["symbol"]

st.header(f"Raw Data — {symbol}")

df = load_data(asset_key)
if df.empty:
    st.warning(f"No data for {symbol}. Run `python scripts/update_data.py --asset {asset_key}` first.")
    st.stop()

# --- Sidebar ---
st.sidebar.header("Filters")
min_date = df["datetime"].min().date()
max_date = df["datetime"].max().date()
date_range = st.sidebar.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

available_versions = asset_config["subgraphs"]
if len(available_versions) > 1:
    versions = st.sidebar.multiselect("Protocol version", options=available_versions, default=available_versions)
else:
    versions = available_versions

min_usd = st.sidebar.number_input("Min USD amount", min_value=0.0, value=0.0, step=1000.0)
max_usd = st.sidebar.number_input("Max USD amount", min_value=0.0, value=0.0, step=1000.0)

filtered = filter_data(
    df,
    date_range=date_range,
    versions=versions,
    min_usd=min_usd if min_usd > 0 else None,
    max_usd=max_usd if max_usd > 0 else None,
)

# --- Search ---
search = st.text_input("Search by tx hash or address", "")
if search:
    mask = (
        filtered["tx_hash"].str.contains(search, case=False, na=False)
        | filtered["liquidator"].str.contains(search, case=False, na=False)
        | filtered["liquidatee"].str.contains(search, case=False, na=False)
    )
    filtered = filtered[mask]

# --- Stats ---
st.caption(f"Showing {len(filtered):,} of {len(df):,} records")

# --- Build display DataFrame with Etherscan links ---
display_cols = [
    "datetime",
    "version",
    "collateral_amount_btc",
    "collateral_amount_usd",
    "liquidator",
    "liquidatee",
    "market_name",
    "tx_hash",
]

existing_cols = [c for c in display_cols if c in filtered.columns]
display_df = filtered[existing_cols].copy()

# Convert hashes and addresses to Etherscan links
if "tx_hash" in display_df.columns:
    display_df["tx_hash"] = display_df["tx_hash"].apply(
        lambda h: f"{ETHERSCAN_TX}{h}" if pd.notna(h) and h else h
    )

if "liquidator" in display_df.columns:
    display_df["liquidator"] = display_df["liquidator"].apply(
        lambda a: f"{ETHERSCAN_ADDR}{a}" if pd.notna(a) and a else a
    )

if "liquidatee" in display_df.columns:
    display_df["liquidatee"] = display_df["liquidatee"].apply(
        lambda a: f"{ETHERSCAN_ADDR}{a}" if pd.notna(a) and a else a
    )

display_df = display_df.rename(
    columns={
        "datetime": "Date",
        "version": "Protocol",
        "collateral_amount_btc": f"{symbol} Amount",
        "collateral_amount_usd": "USD Value",
        "liquidator": "Liquidator",
        "liquidatee": "Liquidatee",
        "market_name": "Market",
        "tx_hash": "Tx Hash",
    }
)

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    height=600,
    column_config={
        "Tx Hash": st.column_config.LinkColumn(
            "Tx Hash",
            display_text=r"https://etherscan\.io/tx/(0x[a-f0-9]{6}).*",
        ),
        "Liquidator": st.column_config.LinkColumn(
            "Liquidator",
            display_text=r"https://etherscan\.io/address/(0x[a-f0-9]{6}).*",
        ),
        "Liquidatee": st.column_config.LinkColumn(
            "Liquidatee",
            display_text=r"https://etherscan\.io/address/(0x[a-f0-9]{6}).*",
        ),
    },
)

# --- CSV Export ---
st.divider()

csv_data = filtered[existing_cols].to_csv(index=False)
st.download_button(
    label="Download CSV",
    data=csv_data,
    file_name=f"{asset_key}_liquidations.csv",
    mime="text/csv",
)
