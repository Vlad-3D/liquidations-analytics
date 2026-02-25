#!/usr/bin/env python3
"""Fetch and cache collateral-based Aave revenue data.

Usage:
    # Full refresh (15-20 min first run):
    python scripts/update_collateral_revenue.py

    # Only refresh price cache (3-4 min, no subgraph calls):
    python scripts/update_collateral_revenue.py --prices-only

    # Only refresh open positions (2-3 min, uses cached accounts):
    python scripts/update_collateral_revenue.py --open-only

    # Full subgraph refresh but skip re-scanning deposit events (uses cached accounts):
    python scripts/update_collateral_revenue.py --no-accounts

Set GRAPH_API_KEY in .env or as an environment variable.
"""

import argparse
import logging
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from src.collateral_fetcher import (
    PATHS,
    fetch_borrows_repays,
    fetch_liquidations,
    fetch_open_positions,
    collect_depositor_accounts,
    load_all,
    save_all,
)
from src.collateral_queries import (
    AAVE_V2_SUBGRAPH_ID,
    AAVE_V3_SUBGRAPH_ID,
    ASSETS,
    COINGECKO_IDS,
    STABLECOINS,
)
from src.price_cache import fetch_and_cache_history

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

JOBS = [
    (AAVE_V2_SUBGRAPH_ID, "V2", "wbtc"),
    (AAVE_V3_SUBGRAPH_ID, "V3", "wbtc"),
    (AAVE_V3_SUBGRAPH_ID, "V3", "cbbtc"),
]


def load_api_key() -> str:
    api_key = os.environ.get("GRAPH_API_KEY", "")
    if not api_key:
        env_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
        )
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GRAPH_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
    return api_key


def update_prices():
    logger.info("\n=== Refreshing price cache (CoinGecko) ===")
    all_symbols = [s for s in COINGECKO_IDS if s not in STABLECOINS]
    price_df = fetch_and_cache_history(all_symbols, days=365)
    logger.info(f"Price cache: {len(price_df)} rows, {price_df['symbol'].nunique()} symbols")
    return price_df


def load_cached_accounts() -> dict:
    """Load account sets per (version, asset_key) from cached parquet."""
    cached = load_all()
    accounts_df = cached.get("accounts", pd.DataFrame())
    result = {}
    for subgraph_id, version, asset_key in JOBS:
        if accounts_df.empty:
            result[(version, asset_key)] = set()
        else:
            sub = accounts_df[
                (accounts_df["collateral_key"] == asset_key) &
                (accounts_df["version"] == version)
            ]
            result[(version, asset_key)] = set(sub["account"].tolist())
            logger.info(f"  Loaded {len(result[(version, asset_key)])} cached {version} {asset_key} accounts")
    return result


def update_open_only(api_key: str, account_map: dict):
    """Re-fetch only open borrow positions (fastest update, ~2 min)."""
    logger.info("\n=== Updating open positions only ===")
    all_open = []
    for subgraph_id, version, asset_key in JOBS:
        accounts = account_map.get((version, asset_key), set())
        if not accounts:
            logger.warning(f"  {version} {asset_key}: no accounts in cache, skipping")
            continue
        sym = ASSETS[asset_key]["symbol"]
        logger.info(f"  {version} {sym}: fetching open positions ({len(accounts)} accounts)...")
        open_df = fetch_open_positions(api_key, subgraph_id, version, asset_key, accounts)
        all_open.append(open_df)

    dfs = load_all()  # keep existing borrows/repays/liqs/accounts
    non_empty = [d for d in all_open if d is not None and not d.empty]
    if non_empty:
        dfs["open_positions"] = pd.concat(non_empty, ignore_index=True)
        save_all({"open_positions": dfs["open_positions"]})
        logger.info(f"  Saved {len(dfs['open_positions']):,} open positions")
    else:
        logger.warning("  No open positions data returned")


def update_full(api_key: str, account_map: dict):
    """Full refresh of borrows, repays, liquidations, open positions."""
    logger.info("\n=== Full subgraph refresh ===")
    all_accounts_rows, all_borrows, all_repays, all_liqs, all_open = [], [], [], [], []

    for subgraph_id, version, asset_key in JOBS:
        sym = ASSETS[asset_key]["symbol"]
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing {version} {sym}...")

        accounts = account_map.get((version, asset_key), set())
        if not accounts:
            logger.info(f"  Collecting {sym} depositor accounts...")
            accounts = collect_depositor_accounts(api_key, subgraph_id, asset_key)

        for acc_id in accounts:
            all_accounts_rows.append({
                "account": acc_id,
                "collateral_key": asset_key,
                "collateral_symbol": sym,
                "version": version,
            })

        if not accounts:
            continue

        borrows_df, repays_df = fetch_borrows_repays(api_key, subgraph_id, version, asset_key, accounts)
        all_borrows.append(borrows_df)
        all_repays.append(repays_df)

        liq_df = fetch_liquidations(api_key, subgraph_id, version, asset_key)
        all_liqs.append(liq_df)

        open_df = fetch_open_positions(api_key, subgraph_id, version, asset_key, accounts)
        all_open.append(open_df)

    def safe_concat(dfs):
        dfs = [d for d in dfs if d is not None and not d.empty]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    dfs = {
        "accounts":       pd.DataFrame(all_accounts_rows),
        "borrows":        safe_concat(all_borrows),
        "repays":         safe_concat(all_repays),
        "liquidations":   safe_concat(all_liqs),
        "open_positions": safe_concat(all_open),
    }

    logger.info("\n=== Summary ===")
    for key, df in dfs.items():
        logger.info(f"  {key}: {len(df):,} rows")

    save_all(dfs)
    logger.info("Done! All parquet files saved.")
    for key in dfs:
        p = PATHS.get(key)
        if p and p.exists():
            logger.info(f"  {p.name:45s} {p.stat().st_size // 1024:>6} KB")


def main():
    parser = argparse.ArgumentParser(description="Update collateral revenue data")
    parser.add_argument("--prices-only", action="store_true",
                        help="Only refresh price cache (no subgraph calls, ~3 min)")
    parser.add_argument("--open-only", action="store_true",
                        help="Only refresh open positions using cached accounts (~2 min)")
    parser.add_argument("--no-accounts", action="store_true",
                        help="Skip deposit scan, reuse cached account list (saves ~2 min)")
    args = parser.parse_args()

    api_key = load_api_key()
    if not api_key and not args.prices_only:
        print("Error: GRAPH_API_KEY not set. Add it to .env or export as env variable.")
        sys.exit(1)

    # Always refresh prices
    update_prices()

    if args.prices_only:
        logger.info("--prices-only: done.")
        return

    # Load cached accounts if we're skipping deposit scan
    account_map = {}
    if args.open_only or args.no_accounts:
        if not PATHS["accounts"].exists():
            logger.error("No cached accounts found. Run without --open-only/--no-accounts first.")
            sys.exit(1)
        account_map = load_cached_accounts()

    if args.open_only:
        update_open_only(api_key, account_map)
    else:
        update_full(api_key, account_map)


if __name__ == "__main__":
    main()
