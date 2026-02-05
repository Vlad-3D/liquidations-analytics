"""Shared UI helpers for Streamlit pages."""

import streamlit as st
from src.queries import ASSETS


def asset_selector():
    """Render asset radio in sidebar, persist selection in session_state.

    Returns:
        tuple: (asset_key, asset_config) — e.g. ("wbtc", {"symbol": "wBTC", ...})
    """
    if "asset_key" not in st.session_state:
        st.session_state["asset_key"] = "wbtc"

    asset_options = {v["symbol"]: k for k, v in ASSETS.items()}
    symbols = list(asset_options.keys())

    current_key = st.session_state["asset_key"]
    current_symbol = ASSETS.get(current_key, ASSETS["wbtc"])["symbol"]
    current_index = symbols.index(current_symbol) if current_symbol in symbols else 0

    st.sidebar.header("Asset")
    selected = st.sidebar.radio(
        "Select asset", symbols, index=current_index, horizontal=True
    )
    st.session_state["asset_key"] = asset_options[selected]

    asset_key = st.session_state["asset_key"]
    return asset_key, ASSETS[asset_key]
