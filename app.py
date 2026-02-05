"""Liquidations Analytics — Entrypoint / Router."""

import streamlit as st

st.set_page_config(
    page_title="Liquidations Analytics",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Define pages with st.Page + st.navigation ---
pages = [
    st.Page("pages/0_Dashboard.py", title="Liquidations Analytics", default=True),
    st.Page("pages/1_User_Behavior.py", title="User Behavior"),
    st.Page("pages/2_Raw_Data.py", title="Raw Data"),
]

pg = st.navigation(pages)
pg.run()
