"""Fetch deposits & repays for liquidated users (>=1 BTC) from Aave V2/V3."""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

from src.queries import (
    AAVE_V2_SUBGRAPH_ID,
    AAVE_V3_SUBGRAPH_ID,
    USER_DEPOSITS_QUERY,
    USER_REPAYS_QUERY,
    get_asset_config,
    get_endpoint,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000
RATE_LIMIT_DELAY = 0.3
MAX_RETRIES = 3
DATA_DIR = Path(__file__).parent.parent / "data"

# Time window: 48 hours before liquidation
PRE_LIQUIDATION_WINDOW = 48 * 3600


def _actions_parquet_path(asset_key: str) -> Path:
    return DATA_DIR / f"{asset_key}_user_actions.parquet"


def _fetch_user_events(
    endpoint: str, query: str, account: str,
    ts_from: int, ts_to: int
) -> List[dict]:
    """Fetch events (deposits or repays) for a single account in time window."""
    all_events = []
    skip = 0

    while True:
        variables = {
            "first": BATCH_SIZE,
            "skip": skip,
            "account": account,
            "timestampFrom": ts_from,
            "timestampTo": ts_to,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(
                    endpoint,
                    json={"query": query, "variables": variables},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                if "errors" in data:
                    logger.warning(f"GraphQL errors for {account[:10]}: {data['errors']}")
                    return all_events
                break
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    logger.error(f"Failed for {account[:10]}: {e}")
                    return all_events
                time.sleep(RATE_LIMIT_DELAY * attempt)

        # Extract events - key name is either 'deposits' or 'repays'
        events = []
        for key in ("deposits", "repays"):
            if key in data.get("data", {}):
                events = data["data"][key]
                break

        if not events:
            break

        all_events.extend(events)
        if len(events) < BATCH_SIZE:
            break

        skip += BATCH_SIZE
        time.sleep(RATE_LIMIT_DELAY)

    return all_events


def _parse_events(raw_events: List[dict], action_type: str, version: str) -> List[dict]:
    """Parse raw deposit/repay events into flat dicts."""
    rows = []
    for ev in raw_events:
        decimals = int(ev["asset"]["decimals"])
        amount = float(ev["amount"]) / (10 ** decimals)
        ts = int(ev["timestamp"])

        rows.append({
            "id": ev["id"],
            "action_type": action_type,
            "version": version,
            "timestamp": ts,
            "datetime": datetime.utcfromtimestamp(ts),
            "tx_hash": ev["hash"],
            "account": ev["account"]["id"],
            "asset_symbol": ev["asset"]["symbol"],
            "asset_address": ev["asset"]["id"],
            "amount": amount,
            "amount_usd": float(ev.get("amountUSD", 0)),
            "market_name": ev["market"]["name"],
        })
    return rows


def fetch_user_actions(
    api_key: str,
    liquidations_df: pd.DataFrame,
    asset_key: str = "wbtc",
    min_wbtc: float = 1.0,
    window_hours: int = 48,
) -> pd.DataFrame:
    """Fetch deposits & repays for liquidated users before their liquidation.

    Args:
        api_key: The Graph API key.
        liquidations_df: DataFrame with liquidation data.
        asset_key: Asset identifier (e.g. "wbtc", "cbbtc").
        min_wbtc: Minimum BTC amount to filter liquidations.
        window_hours: Hours before liquidation to look for actions.

    Returns:
        DataFrame with user deposit/repay actions.
    """
    config = get_asset_config(asset_key)
    window_seconds = window_hours * 3600

    # Filter to big liquidations
    big = liquidations_df[liquidations_df["collateral_amount_btc"] >= min_wbtc].copy()
    logger.info(f"Processing {len(big)} liquidations (>= {min_wbtc} BTC) for {big['liquidatee'].nunique()} users")

    # Build endpoints based on asset config
    v2_endpoint = None
    v3_endpoint = None
    if "V2" in config["subgraphs"]:
        v2_endpoint = get_endpoint(api_key, AAVE_V2_SUBGRAPH_ID)
    if "V3" in config["subgraphs"]:
        v3_endpoint = get_endpoint(api_key, AAVE_V3_SUBGRAPH_ID)

    all_rows = []
    processed = 0

    # Group by liquidatee + version to avoid duplicate queries
    groups = big.groupby(["liquidatee", "version"]).agg(
        min_ts=("timestamp", "min"),
        max_ts=("timestamp", "max"),
    ).reset_index()

    total = len(groups)
    logger.info(f"Total user-version pairs to query: {total}")

    for _, row in groups.iterrows():
        account = row["liquidatee"]
        version = row["version"]
        ts_from = row["min_ts"] - window_seconds
        ts_to = row["max_ts"]

        # Pick endpoint based on version
        if version == "V3" and v3_endpoint:
            endpoint = v3_endpoint
        elif version == "V2" and v2_endpoint:
            endpoint = v2_endpoint
        else:
            continue  # skip if version not available for this asset

        # Fetch deposits
        raw_deposits = _fetch_user_events(
            endpoint, USER_DEPOSITS_QUERY, account, ts_from, ts_to
        )
        all_rows.extend(_parse_events(raw_deposits, "deposit", version))

        time.sleep(RATE_LIMIT_DELAY)

        # Fetch repays
        raw_repays = _fetch_user_events(
            endpoint, USER_REPAYS_QUERY, account, ts_from, ts_to
        )
        all_rows.extend(_parse_events(raw_repays, "repay", version))

        processed += 1
        if processed % 50 == 0:
            logger.info(f"  Processed {processed}/{total} user-version pairs, {len(all_rows)} events found")

        time.sleep(RATE_LIMIT_DELAY)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.drop_duplicates(subset=["id"], keep="last")
        df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)

    logger.info(f"Total user actions fetched: {len(df)}")
    return df


def save_user_actions(
    df: pd.DataFrame, asset_key: str = "wbtc", path: Optional[Path] = None
) -> Path:
    path = path or _actions_parquet_path(asset_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"Saved {len(df)} user actions to {path}")
    return path


def load_user_actions(asset_key: str = "wbtc", path: Optional[Path] = None) -> pd.DataFrame:
    path = path or _actions_parquet_path(asset_key)

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
