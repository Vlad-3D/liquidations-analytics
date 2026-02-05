#!/usr/bin/env python3
"""Fetch deposit/repay actions for liquidated users (>= 1 BTC).

Usage:
    python scripts/fetch_user_behavior.py
    python scripts/fetch_user_behavior.py --min-wbtc 5 --window 72
    python scripts/fetch_user_behavior.py --asset cbbtc
    python scripts/fetch_user_behavior.py --asset all
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.fetcher import load_parquet
from src.queries import ASSETS, get_asset_config
from src.user_behavior_fetcher import fetch_user_actions, save_user_actions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def main():
    parser = argparse.ArgumentParser(description="Fetch user behavior data")
    parser.add_argument("--min-wbtc", type=float, default=1.0, help="Min BTC threshold")
    parser.add_argument("--window", type=int, default=48, help="Hours before liquidation to scan")
    parser.add_argument(
        "--asset",
        choices=list(ASSETS.keys()) + ["all"],
        default="all",
        help="Which asset to fetch behavior for (default: all)",
    )
    args = parser.parse_args()

    # Load API key
    api_key = os.environ.get("GRAPH_API_KEY", "")
    if not api_key:
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GRAPH_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")

    if not api_key:
        print("Error: GRAPH_API_KEY not set.")
        sys.exit(1)

    assets_to_fetch = list(ASSETS.keys()) if args.asset == "all" else [args.asset]

    for asset_key in assets_to_fetch:
        config = get_asset_config(asset_key)
        print(f"\n--- {config['symbol']} ---")

        # Load existing liquidation data
        liq_df = load_parquet(asset_key)
        if liq_df.empty:
            print(f"No {config['symbol']} liquidation data. Run update_data.py --asset {asset_key} first.")
            continue

        big_count = len(liq_df[liq_df["collateral_amount_btc"] >= args.min_wbtc])
        unique_users = liq_df[liq_df["collateral_amount_btc"] >= args.min_wbtc]["liquidatee"].nunique()
        print(f"Fetching user actions for {unique_users} users ({big_count} liquidations >= {args.min_wbtc} BTC)")
        print(f"Window: {args.window} hours before liquidation")
        print()

        df = fetch_user_actions(
            api_key, liq_df, asset_key=asset_key,
            min_wbtc=args.min_wbtc, window_hours=args.window,
        )

        if df.empty:
            print(f"No user actions found for {config['symbol']}.")
        else:
            save_user_actions(df, asset_key=asset_key)
            print(f"\nDone! {config['symbol']} actions: {len(df)}")
            print(f"  Deposits: {len(df[df['action_type'] == 'deposit'])}")
            print(f"  Repays: {len(df[df['action_type'] == 'repay'])}")
            print(f"  Unique users with actions: {df['account'].nunique()}")
            print(f"  Assets used: {df['asset_symbol'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
