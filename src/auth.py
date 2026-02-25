"""Simple password gate for public deployment."""

import streamlit as st

PASSWORD = "BabeRevenue2028"


def check_password():
    """Block page if not authenticated. Call at the top of every page."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if not st.session_state.authenticated:
        st.markdown("## 🔒 Dashboard")
        pwd = st.text_input("Enter password to continue", type="password")
        if pwd == PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        elif pwd:
            st.error("Wrong password")
        st.stop()
