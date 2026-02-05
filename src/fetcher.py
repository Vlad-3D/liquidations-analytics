"""Fetcher module for BTC liquidation data from Aave V2/V3 via The Graph."""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from src.queries import (
    AAVE_V2_SUBGRAPH_ID,
    AAVE_V3_SUBGRAPH_ID,
    LIQUIDATIONS_QUERY,
    WBTC_ADDRESS,
    get_asset_config,
    get_endpoint,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000
RATE_LIMIT_DELAY = 0.5
MAX_RETRIES = 3
DATA_DIR = Path(__file__).parent.parent / "data"


def _parquet_path(asset_key: str) -> Path:
    """Return parquet path for a given asset."""
    return DATA_DIR / f"{asset_key}_liquidations.parquet"


def _fetch_liquidations_from_endpoint(
    endpoint: str, version: str, timestamp_from: int, asset_address: str
) -> List[dict]:
    """Fetch all liquidations for a given asset from a single Aave subgraph endpoint."""
    all_liquidations = []
    skip = 0

    logger.info(f"Fetching {version} liquidations from timestamp {timestamp_from}...")

    while True:
        variables = {
            "first": BATCH_SIZE,
            "skip": skip,
            "timestampFrom": timestamp_from,
            "asset": asset_address,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(
                    endpoint,
                    json={"query": LIQUIDATIONS_QUERY, "variables": variables},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                if "errors" in data:
                    logger.error(f"GraphQL errors: {data['errors']}")
                    return all_liquidations

                break
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")
                if attempt == MAX_RETRIES:
                    logger.error(f"All retries exhausted for {version}")
                    return all_liquidations
                time.sleep(RATE_LIMIT_DELAY * attempt)

        liquidations = data.get("data", {}).get("liquidates", [])
        if not liquidations:
            break

        for liq in liquidations:
            liq["version"] = version

        all_liquidations.extend(liquidations)
        logger.info(f"  {version}: {len(all_liquidations)} liquidations fetched")

        if len(liquidations) < BATCH_SIZE:
            break

        skip += BATCH_SIZE
        time.sleep(RATE_LIMIT_DELAY)

    return all_liquidations


def _raw_to_dataframe(raw_liquidations: List[dict]) -> pd.DataFrame:
    """Convert raw API response to a clean DataFrame."""
    rows = []
    for liq in raw_liquidations:
        decimals = int(liq["asset"]["decimals"])
        amount_btc = float(liq["amount"]) / (10**decimals)
        ts = int(liq["timestamp"])

        rows.append(
            {
                "id": liq["id"],
                "version": liq["version"],
                "timestamp": ts,
                "datetime": datetime.utcfromtimestamp(ts),
                "block_number": int(liq["blockNumber"]),
                "tx_hash": liq["hash"],
                "liquidator": liq["liquidator"]["id"],
                "liquidatee": liq["liquidatee"]["id"],
                "collateral_asset_symbol": liq["asset"]["symbol"],
                "collateral_asset_address": liq["asset"]["id"],
                "collateral_amount_raw": liq["amount"],
                "collateral_amount_btc": amount_btc,
                "collateral_amount_usd": float(liq.get("amountUSD", 0)),
                "market_name": liq["market"]["name"],
                "market_address": liq["market"]["id"],
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"])
        # Remove zero-amount records (empty liquidations)
        df = df[df["collateral_amount_btc"] > 0]
        df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    return df


def fetch_all_liquidations(
    api_key: str, asset_key: str = "wbtc", days_back: int = 365 * 4
) -> pd.DataFrame:
    """Fetch liquidations for a given asset from Aave subgraphs.

    Args:
        api_key: The Graph Gateway API key.
        asset_key: Asset identifier (e.g. "wbtc", "cbbtc").
        days_back: How many days of history to fetch.

    Returns:
        DataFrame with all liquidation records.
    """
    config = get_asset_config(asset_key)
    timestamp_from = int((datetime.now() - timedelta(days=days_back)).timestamp())

    all_raw = []

    if "V2" in config["subgraphs"]:
        v2_endpoint = get_endpoint(api_key, AAVE_V2_SUBGRAPH_ID)
        v2_raw = _fetch_liquidations_from_endpoint(
            v2_endpoint, "V2", timestamp_from, config["address"]
        )
        all_raw.extend(v2_raw)

    if "V3" in config["subgraphs"]:
        v3_endpoint = get_endpoint(api_key, AAVE_V3_SUBGRAPH_ID)
        v3_raw = _fetch_liquidations_from_endpoint(
            v3_endpoint, "V3", timestamp_from, config["address"]
        )
        all_raw.extend(v3_raw)

    logger.info(f"Total {config['symbol']}: {len(all_raw)}")
    return _raw_to_dataframe(all_raw)


def save_parquet(
    df: pd.DataFrame, asset_key: str = "wbtc", path: Optional[Path] = None
) -> Path:
    """Save DataFrame to parquet file."""
    path = path or _parquet_path(asset_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"Saved {len(df)} records to {path}")
    return path


def load_parquet(asset_key: str = "wbtc", path: Optional[Path] = None) -> pd.DataFrame:
    """Load liquidations from parquet file."""
    path = path or _parquet_path(asset_key)

    # Migration: old file -> new naming for wbtc
    if not path.exists() and asset_key == "wbtc":
        old_path = DATA_DIR / "liquidations.parquet"
        if old_path.exists():
            logger.info(f"Migrating {old_path} -> {path}")
            df = pd.read_parquet(old_path, engine="pyarrow")
            if "collateral_amount_wbtc" in df.columns:
                df = df.rename(columns={"collateral_amount_wbtc": "collateral_amount_btc"})
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"])
            df.to_parquet(path, index=False, engine="pyarrow")
            return df

    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path, engine="pyarrow")
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
    # Column migration for old data
    if "collateral_amount_wbtc" in df.columns:
        df = df.rename(columns={"collateral_amount_wbtc": "collateral_amount_btc"})
    return df


def update_data(
    api_key: str, asset_key: str = "wbtc", days_back: int = 365 * 4
) -> pd.DataFrame:
    """Fetch fresh data and save to parquet. Merges with existing data."""
    existing = load_parquet(asset_key)
    new_df = fetch_all_liquidations(api_key, asset_key, days_back)

    if existing.empty:
        combined = new_df
    elif new_df.empty:
        combined = existing
    else:
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["id"], keep="last")
        combined = combined.sort_values("timestamp", ascending=False).reset_index(
            drop=True
        )

    save_parquet(combined, asset_key)
    return combined
