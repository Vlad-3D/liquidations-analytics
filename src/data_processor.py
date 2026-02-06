"""Data processing and aggregation for the Streamlit dashboard."""

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).parent.parent / "data"


def _data_path(asset_key: str) -> Path:
    return DATA_DIR / f"{asset_key}_liquidations.parquet"


@st.cache_data(ttl=600, show_spinner="Loading data...")
def load_data(asset_key: str = "wbtc") -> pd.DataFrame:
    """Load liquidation data from parquet with caching."""
    path = _data_path(asset_key)

    # Migration fallback for wbtc
    if not path.exists() and asset_key == "wbtc":
        old_path = DATA_DIR / "liquidations.parquet"
        if old_path.exists():
            path = old_path

    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path, engine="pyarrow")
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
    # Column migration for old data
    if "collateral_amount_wbtc" in df.columns:
        df = df.rename(columns={"collateral_amount_wbtc": "collateral_amount_btc"})
    return df


def filter_data(
    df: pd.DataFrame,
    date_range: Optional[Tuple] = None,
    versions: Optional[List[str]] = None,
    min_usd: Optional[float] = None,
    max_usd: Optional[float] = None,
) -> pd.DataFrame:
    """Apply filters to liquidation DataFrame."""
    if df.empty:
        return df

    filtered = df.copy()

    if date_range and len(date_range) == 2:
        start = pd.to_datetime(date_range[0])
        end = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        filtered = filtered[
            (filtered["datetime"] >= start) & (filtered["datetime"] <= end)
        ]

    if versions:
        filtered = filtered[filtered["version"].isin(versions)]

    if min_usd is not None:
        filtered = filtered[filtered["collateral_amount_usd"] >= min_usd]

    if max_usd is not None:
        filtered = filtered[filtered["collateral_amount_usd"] <= max_usd]

    return filtered


def get_kpi_metrics(df: pd.DataFrame) -> dict:
    """Calculate KPI metrics from liquidation data."""
    if df.empty:
        return {
            "total_liquidations": 0,
            "total_btc": 0.0,
            "total_usd": 0.0,
            "unique_liquidators": 0,
            "unique_liquidatees": 0,
            "avg_liquidation_usd": 0.0,
            "max_liquidation_usd": 0.0,
            "v2_count": 0,
            "v3_count": 0,
        }

    return {
        "total_liquidations": len(df),
        "total_btc": df["collateral_amount_btc"].sum(),
        "total_usd": df["collateral_amount_usd"].sum(),
        "unique_liquidators": df["liquidator"].nunique(),
        "unique_liquidatees": df["liquidatee"].nunique(),
        "avg_liquidation_usd": df["collateral_amount_usd"].mean(),
        "max_liquidation_usd": df["collateral_amount_usd"].max(),
        "v2_count": len(df[df["version"] == "V2"]),
        "v3_count": len(df[df["version"] == "V3"]),
    }


def get_monthly_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate liquidations by month."""
    if df.empty:
        return pd.DataFrame()

    monthly = df.copy()
    monthly["month"] = monthly["datetime"].dt.to_period("M").dt.to_timestamp()
    agg = (
        monthly.groupby("month")
        .agg(
            count=("tx_hash", "count"),
            total_btc=("collateral_amount_btc", "sum"),
            total_usd=("collateral_amount_usd", "sum"),
            avg_usd=("collateral_amount_usd", "mean"),
        )
        .reset_index()
    )
    return agg


def get_monthly_by_version(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate liquidations by month and version."""
    if df.empty:
        return pd.DataFrame()

    monthly = df.copy()
    monthly["month"] = monthly["datetime"].dt.to_period("M").dt.to_timestamp()
    agg = (
        monthly.groupby(["month", "version"])
        .agg(
            count=("tx_hash", "count"),
            total_btc=("collateral_amount_btc", "sum"),
            total_usd=("collateral_amount_usd", "sum"),
        )
        .reset_index()
    )
    return agg


def get_size_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Categorize liquidations by size brackets."""
    if df.empty:
        return pd.DataFrame()

    bins = [0, 0.1, 0.5, 1, 5, 10, 50, float("inf")]
    labels = ["<0.1", "0.1-0.5", "0.5-1", "1-5", "5-10", "10-50", ">50"]

    result = df.copy()
    result["size_bracket"] = pd.cut(
        result["collateral_amount_btc"], bins=bins, labels=labels
    )

    agg = (
        result.groupby("size_bracket", observed=True)
        .agg(
            count=("tx_hash", "count"),
            total_btc=("collateral_amount_btc", "sum"),
            total_usd=("collateral_amount_usd", "sum"),
        )
        .reset_index()
    )
    return agg


def get_monthly_by_size_bracket(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate liquidations by month and size bracket (for stacked bar chart)."""
    if df.empty:
        return pd.DataFrame()

    bins = [0, 0.1, 0.5, 1, 5, 10, 50, float("inf")]
    labels = ["<0.1 BTC", "0.1-0.5 BTC", "0.5-1 BTC", "1-5 BTC", "5-10 BTC", "10-50 BTC", ">50 BTC"]

    result = df.copy()
    result["month"] = result["datetime"].dt.to_period("M").dt.to_timestamp()
    result["size_bracket"] = pd.cut(
        result["collateral_amount_btc"], bins=bins, labels=labels
    )

    agg = (
        result.groupby(["month", "size_bracket"], observed=True)
        .agg(
            count=("tx_hash", "count"),
            total_btc=("collateral_amount_btc", "sum"),
            total_usd=("collateral_amount_usd", "sum"),
        )
        .reset_index()
    )
    return agg


def get_top_liquidators(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Get top liquidators by total USD volume."""
    if df.empty:
        return pd.DataFrame()

    agg = (
        df.groupby("liquidator")
        .agg(
            count=("tx_hash", "count"),
            total_btc=("collateral_amount_btc", "sum"),
            total_usd=("collateral_amount_usd", "sum"),
        )
        .sort_values("total_usd", ascending=False)
        .head(top_n)
        .reset_index()
    )
    return agg


def get_top_liquidatees(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Get most liquidated users by total USD volume."""
    if df.empty:
        return pd.DataFrame()

    agg = (
        df.groupby("liquidatee")
        .agg(
            count=("tx_hash", "count"),
            total_btc=("collateral_amount_btc", "sum"),
            total_usd=("collateral_amount_usd", "sum"),
        )
        .sort_values("total_usd", ascending=False)
        .head(top_n)
        .reset_index()
    )
    return agg
