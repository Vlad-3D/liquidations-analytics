"""Classify liquidated user behavior based on pre-liquidation deposits & repays."""

from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from src.queries import get_asset_config

DATA_DIR = Path(__file__).parent.parent / "data"


def _actions_path(asset_key: str) -> Path:
    return DATA_DIR / f"{asset_key}_user_actions.parquet"


@st.cache_data(ttl=3600)
def load_actions(asset_key: str = "wbtc") -> pd.DataFrame:
    """Load user actions from parquet."""
    path = _actions_path(asset_key)

    # Migration fallback for wbtc
    if not path.exists() and asset_key == "wbtc":
        old_path = DATA_DIR / "user_actions.parquet"
        if old_path.exists():
            path = old_path

    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path, engine="pyarrow")
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def classify_users(
    liq_df: pd.DataFrame,
    actions_df: pd.DataFrame,
    asset_key: str = "wbtc",
    min_wbtc: float = 1.0,
    window_hours: int = 48,
) -> pd.DataFrame:
    """Classify each liquidation event by user pre-liquidation behavior.

    Returns DataFrame with one row per liquidation (>= min_wbtc), with columns:
    - All original liquidation columns
    - behavior: Passive / Deposit Only / Repay Only / Deposit + Repay
    - deposit_count, repay_count
    - deposit_usd, repay_usd
    - deposit_assets: list of asset symbols deposited
    - deposited_collateral: bool - whether user deposited the same collateral asset
    - hours_before_last_action: time between last action and liquidation
    """
    config = get_asset_config(asset_key)
    target_address = config["address"]

    big = liq_df[liq_df["collateral_amount_btc"] >= min_wbtc].copy()

    if actions_df.empty:
        big["behavior"] = "Passive"
        big["deposit_count"] = 0
        big["repay_count"] = 0
        big["deposit_usd"] = 0.0
        big["repay_usd"] = 0.0
        big["deposit_assets"] = ""
        big["deposited_collateral"] = False
        big["hours_before_last_action"] = None
        return big

    window_seconds = window_hours * 3600
    results = []

    for _, liq in big.iterrows():
        account = liq["liquidatee"]
        liq_ts = liq["timestamp"]
        ts_from = liq_ts - window_seconds

        # Find actions for this user in the window before this liquidation
        user_actions = actions_df[
            (actions_df["account"] == account)
            & (actions_df["timestamp"] >= ts_from)
            & (actions_df["timestamp"] <= liq_ts)
        ]

        deposits = user_actions[user_actions["action_type"] == "deposit"]
        repays = user_actions[user_actions["action_type"] == "repay"]

        row = liq.to_dict()
        row["deposit_count"] = len(deposits)
        row["repay_count"] = len(repays)
        row["deposit_usd"] = deposits["amount_usd"].sum() if len(deposits) > 0 else 0.0
        row["repay_usd"] = repays["amount_usd"].sum() if len(repays) > 0 else 0.0

        # Classify behavior
        has_deposit = len(deposits) > 0
        has_repay = len(repays) > 0

        if has_deposit and has_repay:
            row["behavior"] = "Deposit + Repay"
        elif has_deposit:
            row["behavior"] = "Deposit Only"
        elif has_repay:
            row["behavior"] = "Repay Only"
        else:
            row["behavior"] = "Passive"

        # Asset analysis for deposits
        if has_deposit:
            row["deposit_assets"] = ", ".join(sorted(deposits["asset_symbol"].unique()))
            row["deposited_collateral"] = target_address in deposits["asset_address"].values
        else:
            row["deposit_assets"] = ""
            row["deposited_collateral"] = False

        # Time between last action and liquidation
        if len(user_actions) > 0:
            last_action_ts = user_actions["timestamp"].max()
            row["hours_before_last_action"] = (liq_ts - last_action_ts) / 3600
        else:
            row["hours_before_last_action"] = None

        results.append(row)

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df["datetime"] = pd.to_datetime(result_df["datetime"])
    return result_df


def get_behavior_summary(classified_df: pd.DataFrame) -> dict:
    """Get summary stats from classified data."""
    if classified_df.empty:
        return {}

    total = len(classified_df)
    return {
        "total": total,
        "passive": len(classified_df[classified_df["behavior"] == "Passive"]),
        "deposit_only": len(classified_df[classified_df["behavior"] == "Deposit Only"]),
        "repay_only": len(classified_df[classified_df["behavior"] == "Repay Only"]),
        "both": len(classified_df[classified_df["behavior"] == "Deposit + Repay"]),
        "pct_tried_save": (total - len(classified_df[classified_df["behavior"] == "Passive"])) / total * 100,
        "deposited_collateral_count": classified_df["deposited_collateral"].sum() if "deposited_collateral" in classified_df.columns else (classified_df["deposited_wbtc"].sum() if "deposited_wbtc" in classified_df.columns else 0),
        "avg_deposit_usd": classified_df[classified_df["deposit_usd"] > 0]["deposit_usd"].mean(),
        "avg_repay_usd": classified_df[classified_df["repay_usd"] > 0]["repay_usd"].mean(),
    }


def get_behavior_by_size(classified_df: pd.DataFrame) -> pd.DataFrame:
    """Behavior breakdown by liquidation size bracket."""
    if classified_df.empty:
        return pd.DataFrame()

    bins = [1, 5, 10, 50, float("inf")]
    labels = ["1-5 BTC", "5-10 BTC", "10-50 BTC", ">50 BTC"]

    df = classified_df.copy()
    df["size_bracket"] = pd.cut(df["collateral_amount_btc"], bins=bins, labels=labels)

    agg = (
        df.groupby(["size_bracket", "behavior"], observed=True)
        .size()
        .reset_index(name="count")
    )
    return agg


def get_deposit_asset_breakdown(classified_df: pd.DataFrame, actions_df: pd.DataFrame) -> pd.DataFrame:
    """What assets did users deposit to try to save their position."""
    if actions_df.empty:
        return pd.DataFrame()

    deposits = actions_df[actions_df["action_type"] == "deposit"]
    if deposits.empty:
        return pd.DataFrame()

    # Only deposits from users who were liquidated >= 1 BTC
    big_users = classified_df["liquidatee"].unique()
    user_deposits = deposits[deposits["account"].isin(big_users)]

    agg = (
        user_deposits.groupby("asset_symbol")
        .agg(
            count=("id", "count"),
            total_usd=("amount_usd", "sum"),
            unique_users=("account", "nunique"),
        )
        .sort_values("total_usd", ascending=False)
        .reset_index()
    )
    return agg
