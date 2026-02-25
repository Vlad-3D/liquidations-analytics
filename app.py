"""Liquidations Analytics — Entrypoint / Router."""

import streamlit as st

st.set_page_config(
    page_title="Liquidations Analytics",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Simple password gate ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown("## 🔒 Dashboard")
    pwd = st.text_input("Enter password to continue", type="password")
    if pwd == "BabeRevenue2028":
        st.session_state.authenticated = True
        st.rerun()
    elif pwd:
        st.error("Wrong password")
    st.stop()

# --- Define pages with st.Page + st.navigation ---
pages = {
    "Liquidation Analytics (old)": [
        st.Page("pages/0_Dashboard.py", title="Liquidations Analytics"),
        st.Page("pages/1_User_Behavior.py", title="User Behavior"),
        st.Page("pages/2_Raw_Data.py", title="Raw Data"),
    ],
    "AAVE + BABE Revenue (new)": [
        st.Page("pages/3_Aave_Revenue.py", title="Aave Revenue"),
        st.Page("pages/4_Revenue_Forecast.py", title="Revenue Forecast"),
        st.Page("pages/5_Babylon_Revenue.py", title="Babylon Revenue", default=True),
    ],
}

pg = st.navigation(pages)
pg.run()
