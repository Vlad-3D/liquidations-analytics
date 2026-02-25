"""Fetcher for Aave V2/V3 revenue data (interest + liquidations) per BTC asset."""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests

from src.revenue_queries import (
    AAVE_V2_SUBGRAPH_ID,
    AAVE_V3_SUBGRAPH_ID,
    ASSETS,
    LIQUIDATION_EVENTS_QUERY,
    MARKET_DAILY_SNAPSHOTS_QUERY,
    MARKETS_CURRENT_QUERY,
    get_endpoint,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000
RETRY_DELAY = 1.0
MAX_RETRIES = 3

# Aave protocol fee share of total revenue (protocol side revenue = treasury fee)
# liquidation bonus goes entirely to liquidators; protocol earns via interest spread only
LIQUIDATION_PROTOCOL_FEE = 0.0  # tracked separately via dailyProtocolSideRevenueUSD


def _post(url: str, query: str, variables: dict) -> Optional[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(
                url,
                json={"query": query, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                return None
            return data
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt == MAX_RETRIES:
                return None
            time.sleep(RETRY_DELAY * attempt)
    return None


def fetch_market_daily_snapshots(
    api_key: str, subgraph_id: str, version: str, asset_key: str
) -> List[dict]:
    """Fetch all daily market snapshots for a given asset from one subgraph."""
    config = ASSETS[asset_key]
    url = get_endpoint(api_key, subgraph_id)
    all_snaps = []
    skip = 0

    while True:
        data = _post(
            url,
            MARKET_DAILY_SNAPSHOTS_QUERY,
            {"first": BATCH_SIZE, "skip": skip, "asset": config["address"]},
        )
        if not data:
            break

        snaps = data.get("data", {}).get("marketDailySnapshots", [])
        if not snaps:
            break

        for s in snaps:
            s["version"] = version
            s["asset_key"] = asset_key
            s["asset_symbol"] = config["symbol"]

        all_snaps.extend(snaps)

        if len(snaps) < BATCH_SIZE:
            break
        skip += BATCH_SIZE
        time.sleep(0.3)

    logger.info(f"  {version} {config['symbol']}: {len(all_snaps)} daily snapshots")
    return all_snaps


def fetch_all_daily_snapshots(api_key: str) -> pd.DataFrame:
    """Fetch daily revenue snapshots for all assets from V2 + V3."""
    all_raw = []

    # wBTC: V2 + V3
    all_raw.extend(fetch_market_daily_snapshots(api_key, AAVE_V2_SUBGRAPH_ID, "V2", "wbtc"))
    all_raw.extend(fetch_market_daily_snapshots(api_key, AAVE_V3_SUBGRAPH_ID, "V3", "wbtc"))

    # cbBTC: V3 only
    all_raw.extend(fetch_market_daily_snapshots(api_key, AAVE_V3_SUBGRAPH_ID, "V3", "cbbtc"))

    if not all_raw:
        return pd.DataFrame()

    rows = []
    for s in all_raw:
        ts = int(s["timestamp"])
        supply_rev = float(s.get("dailySupplySideRevenueUSD") or 0)
        # dailyProtocolSideRevenueUSD in V3 subgraph is bugged (contains cumulative spikes).
        # Derive protocol revenue from supply revenue using Aave reserve factor (20%):
        # protocol_rev = supply_rev * RF / (1 - RF) = supply_rev * 0.25
        raw_protocol = float(s.get("dailyProtocolSideRevenueUSD") or 0)
        # Use subgraph value only for V2 where it is reliable; derive for V3.
        if s["version"] == "V2":
            protocol_rev = raw_protocol
        else:
            protocol_rev = supply_rev * 0.25  # reserve factor 20%

        rows.append(
            {
                "date": datetime.utcfromtimestamp(ts).date(),
                "timestamp": ts,
                "version": s["version"],
                "asset_key": s["asset_key"],
                "asset_symbol": s["asset_symbol"],
                "market_id": s["market"]["id"],
                "market_name": s["market"]["name"],
                "daily_protocol_revenue_usd": protocol_rev,
                "daily_supply_revenue_usd": supply_rev,
                "daily_total_revenue_usd": float(s.get("dailyTotalRevenueUSD") or 0),
                "daily_liquidate_usd": float(s.get("dailyLiquidateUSD") or 0),
                "total_borrow_usd": float(s.get("totalBorrowBalanceUSD") or 0),
                "total_deposit_usd": float(s.get("totalDepositBalanceUSD") or 0),
            }
        )

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date", "asset_key"]).reset_index(drop=True)
    return df


def fetch_liquidation_events(
    api_key: str, subgraph_id: str, version: str, asset_key: str
) -> List[dict]:
    """Fetch all individual liquidation events for profit breakdown."""
    config = ASSETS[asset_key]
    url = get_endpoint(api_key, subgraph_id)
    all_events = []
    skip = 0

    while True:
        data = _post(
            url,
            LIQUIDATION_EVENTS_QUERY,
            {"first": BATCH_SIZE, "skip": skip, "asset": config["address"]},
        )
        if not data:
            break

        events = data.get("data", {}).get("liquidates", [])
        if not events:
            break

        for e in events:
            e["version"] = version
            e["asset_key"] = asset_key
            e["asset_symbol"] = config["symbol"]

        all_events.extend(events)

        if len(events) < BATCH_SIZE:
            break
        skip += BATCH_SIZE
        time.sleep(0.3)

    return all_events


def fetch_all_liquidation_events(api_key: str) -> pd.DataFrame:
    """Fetch all liquidation events across V2/V3 for wBTC and cbBTC."""
    all_raw = []
    all_raw.extend(fetch_liquidation_events(api_key, AAVE_V2_SUBGRAPH_ID, "V2", "wbtc"))
    all_raw.extend(fetch_liquidation_events(api_key, AAVE_V3_SUBGRAPH_ID, "V3", "wbtc"))
    all_raw.extend(fetch_liquidation_events(api_key, AAVE_V3_SUBGRAPH_ID, "V3", "cbbtc"))

    if not all_raw:
        return pd.DataFrame()

    rows = []
    for e in all_raw:
        ts = int(e["timestamp"])
        decimals = int(e["asset"]["decimals"])
        rows.append(
            {
                "date": datetime.utcfromtimestamp(ts).date(),
                "timestamp": ts,
                "version": e["version"],
                "asset_key": e["asset_key"],
                "asset_symbol": e["asset_symbol"],
                "amount_btc": float(e["amount"]) / (10**decimals),
                "amount_usd": float(e.get("amountUSD") or 0),
                "profit_usd": float(e.get("profitUSD") or 0),
                "market_name": e["market"]["name"],
            }
        )

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def fetch_current_markets(api_key: str) -> pd.DataFrame:
    """Fetch current open position snapshot from V2 + V3."""
    results = []

    for subgraph_id, version in [
        (AAVE_V2_SUBGRAPH_ID, "V2"),
        (AAVE_V3_SUBGRAPH_ID, "V3"),
    ]:
        url = get_endpoint(api_key, subgraph_id)
        data = _post(url, MARKETS_CURRENT_QUERY, {})
        if not data:
            continue

        markets = data.get("data", {}).get("markets", [])
        for m in markets:
            raw_sym = m["inputToken"]["symbol"]
            asset_key = "wbtc" if raw_sym.upper() == "WBTC" else "cbbtc"
            sym = "wBTC" if asset_key == "wbtc" else "cbBTC"

            variable_borrow_rate = 0.0
            for rate in m.get("rates", []):
                if rate["side"] == "BORROWER" and rate["type"] == "VARIABLE":
                    variable_borrow_rate = float(rate["rate"])

            results.append(
                {
                    "version": version,
                    "asset_key": asset_key,
                    "asset_symbol": sym,
                    "market_id": m["id"],
                    "market_name": m["name"],
                    "total_borrow_usd": float(m.get("totalBorrowBalanceUSD") or 0),
                    "total_deposit_usd": float(m.get("totalDepositBalanceUSD") or 0),
                    "total_tvl_usd": float(m.get("totalValueLockedUSD") or 0),
                    "cumulative_liquidate_usd": float(m.get("cumulativeLiquidateUSD") or 0),
                    "variable_borrow_rate": variable_borrow_rate,
                }
            )

    return pd.DataFrame(results)


def fetch_btc_price_history(days: int = 1825) -> pd.DataFrame:
    """Fetch BTC/USD price history from CoinGecko (free API, up to 365 days)."""
    url = f"https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {"vs_currency": "usd", "days": min(days, 365), "interval": "daily"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        prices = data.get("prices", [])
        rows = [
            {
                "date": datetime.utcfromtimestamp(ts / 1000).date(),
                "btc_price_usd": price,
            }
            for ts, price in prices
        ]
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        logger.error(f"CoinGecko fetch failed: {e}")
        return pd.DataFrame()


def fetch_btc_current_price() -> float:
    """Fetch current BTC price from CoinGecko."""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "bitcoin", "vs_currencies": "usd"},
            timeout=10,
        )
        return float(r.json()["bitcoin"]["usd"])
    except Exception:
        return 0.0
