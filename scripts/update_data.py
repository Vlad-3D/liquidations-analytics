#!/usr/bin/env python3
"""Script to fetch/update liquidation data and save to parquet.

Usage:
    python scripts/update_data.py
    python scripts/update_data.py --days 730
    python scripts/update_data.py --asset cbbtc
    python scripts/update_data.py --asset all

Set GRAPH_API_KEY env variable or create .env file.
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.fetcher import update_data
from src.queries import ASSETS, get_asset_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def main():
    parser = argparse.ArgumentParser(description="Update BTC liquidation data")
    parser.add_argument(
        "--days", type=int, default=365 * 4, help="Days of history to fetch"
    )
    parser.add_argument(
        "--asset",
        choices=list(ASSETS.keys()) + ["all"],
        default="all",
        help="Which asset to fetch (default: all)",
    )
    args = parser.parse_args()

    api_key = os.environ.get("GRAPH_API_KEY", "")

    # Try loading from .env file
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

    if not api_key:
        print("Error: GRAPH_API_KEY not set. Set it in .env or as env variable.")
        sys.exit(1)

    assets_to_fetch = list(ASSETS.keys()) if args.asset == "all" else [args.asset]

    for asset_key in assets_to_fetch:
        config = get_asset_config(asset_key)
        print(f"\nFetching {config['symbol']} liquidations (last {args.days} days)...")
        print(f"  Subgraphs: {', '.join(config['subgraphs'])}")

        df = update_data(api_key, asset_key=asset_key, days_back=args.days)
        print(f"Done! {config['symbol']}: {len(df)} records")

        if not df.empty:
            total_btc = df["collateral_amount_btc"].sum()
            total_usd = df["collateral_amount_usd"].sum()
            print(f"  Total BTC: {total_btc:.4f}")
            print(f"  Total USD: ${total_usd:,.2f}")
            for v in config["subgraphs"]:
                print(f"  {v}: {len(df[df['version'] == v])}")


if __name__ == "__main__":
    main()
