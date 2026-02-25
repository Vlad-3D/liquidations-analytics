"""Fetcher for collateral-based Aave revenue.

Methodology:
  Realized interest  = sum(repay.amount_tokens) - sum(borrow.amount_tokens)
                       converted to USD via historical price on repay date
                       × reserve_factor (20%) = Aave protocol revenue

  Realized liq fee   = liquidation.amountUSD × liquidation_protocol_fee (0.5%)

  Unrealized accrued = open_borrow_balance_USD
                       × variable_borrow_rate × reserve_factor
                       (annualised; divide by 365 for daily)

All per-position attribution: only accounts that deposited ≥ 1 BTC as collateral.
"""

import logging
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import requests

from src.collateral_queries import (
    AAVE_V2_SUBGRAPH_ID,
    AAVE_V3_SUBGRAPH_ID,
    ASSETS,
    BORROWS_FOR_ACCOUNTS_QUERY,
    DEPOSITS_FOR_ACCOUNTS_QUERY,
    LIQUIDATIONS_AS_COLLATERAL_QUERY,
    LIQ_PROTOCOL_FEE,
    LIQ_PROTOCOL_FEE_BY_COLLATERAL,
    MARKET_RESERVE_FACTORS,
    MIN_BTC_RAW,
    OPEN_BORROW_POSITIONS_QUERY,
    REPAYS_FOR_ACCOUNTS_QUERY,
    RESERVE_FACTOR,
    get_endpoint,
)
from src.price_cache import price_for

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
BATCH_SIZE = 1000
ACCOUNT_BATCH = 50   # accounts per borrow/repay query
RETRY_DELAY = 1.5
MAX_RETRIES = 3


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _post(url: str, query: str, variables: dict) -> Optional[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(
                url,
                json={"query": query, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=45,
            )
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                logger.error(f"GraphQL error: {data['errors'][0]['message']}")
                return None
            return data
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt == MAX_RETRIES:
                return None
            time.sleep(RETRY_DELAY * attempt)
    return None


# ── Step 1: collect account IDs ───────────────────────────────────────────────

def collect_depositor_accounts(api_key: str, subgraph_id: str, asset_key: str) -> Set[str]:
    """Return all account IDs that ever deposited ≥ 1 BTC of the given asset."""
    config = ASSETS[asset_key]
    url = get_endpoint(api_key, subgraph_id)
    accounts: Set[str] = set()
    last_id = "0x" + "0" * 64

    logger.info(f"  Collecting {config['symbol']} depositor accounts...")
    while True:
        data = _post(url, DEPOSITS_FOR_ACCOUNTS_QUERY, {
            "asset": config["address"],
            "minAmount": MIN_BTC_RAW,
            "lastId": last_id,
        })
        if not data:
            break
        batch = data.get("data", {}).get("deposits", [])
        if not batch:
            break
        for dep in batch:
            accounts.add(dep["account"]["id"])
        last_id = batch[-1]["id"]
        if len(batch) < BATCH_SIZE:
            break
        time.sleep(0.3)

    logger.info(f"  {config['symbol']}: {len(accounts)} depositor accounts found")
    return accounts


# ── Step 2: fetch borrow / repay events ───────────────────────────────────────

def _fetch_events_for_accounts(
    url: str, query: str, entity: str, accounts: List[str]
) -> List[dict]:
    """Fetch all borrow or repay events for a list of account IDs (batched)."""
    all_events: List[dict] = []

    for i in range(0, len(accounts), ACCOUNT_BATCH):
        batch_accounts = accounts[i: i + ACCOUNT_BATCH]
        last_id = "0x" + "0" * 64

        while True:
            data = _post(url, query, {"accounts": batch_accounts, "lastId": last_id})
            if not data:
                break
            events = data.get("data", {}).get(entity, [])
            if not events:
                break
            all_events.extend(events)
            last_id = events[-1]["id"]
            if len(events) < BATCH_SIZE:
                break
            time.sleep(0.2)

        time.sleep(0.25)

    return all_events


def fetch_borrows_repays(
    api_key: str, subgraph_id: str, version: str, asset_key: str, accounts: Set[str]
) -> tuple:
    """Return (borrows_df, repays_df) for the given accounts."""
    config = ASSETS[asset_key]
    url = get_endpoint(api_key, subgraph_id)
    acc_list = sorted(accounts)

    logger.info(f"  {version} {config['symbol']}: fetching borrows ({len(acc_list)} accounts)...")
    raw_borrows = _fetch_events_for_accounts(url, BORROWS_FOR_ACCOUNTS_QUERY, "borrows", acc_list)

    logger.info(f"  {version} {config['symbol']}: fetching repays ({len(acc_list)} accounts)...")
    raw_repays = _fetch_events_for_accounts(url, REPAYS_FOR_ACCOUNTS_QUERY, "repays", acc_list)

    def to_df(events: List[dict], collateral_key: str, ver: str) -> pd.DataFrame:
        rows = []
        for e in events:
            ts = int(e["timestamp"])
            dec = int(e["asset"]["decimals"])
            rows.append({
                "timestamp": ts,
                "date": datetime.utcfromtimestamp(ts).date(),
                "account": e["account"]["id"],
                "collateral_key": collateral_key,
                "version": ver,
                "asset_symbol": e["asset"]["symbol"],
                "asset_decimals": dec,
                "amount_tokens": float(e["amount"]) / (10 ** dec),
                "amount_usd_spot": float(e.get("amountUSD") or 0),
                "market_name": e["market"]["name"],
            })
        df = pd.DataFrame(rows)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df

    borrows_df = to_df(raw_borrows, asset_key, version)
    repays_df = to_df(raw_repays, asset_key, version)

    logger.info(f"  {version} {config['symbol']}: {len(borrows_df)} borrows, {len(repays_df)} repays")
    return borrows_df, repays_df


# ── Step 3: fetch liquidation events ──────────────────────────────────────────

def fetch_liquidations(api_key: str, subgraph_id: str, version: str, asset_key: str) -> pd.DataFrame:
    """Fetch all liquidation events where wBTC/cbBTC was the collateral seized."""
    config = ASSETS[asset_key]
    url = get_endpoint(api_key, subgraph_id)
    all_liqs: List[dict] = []
    last_id = "0x" + "0" * 64

    logger.info(f"  {version} {config['symbol']}: fetching liquidation events...")
    while True:
        data = _post(url, LIQUIDATIONS_AS_COLLATERAL_QUERY, {
            "asset": config["address"],
            "lastId": last_id,
        })
        if not data:
            break
        batch = data.get("data", {}).get("liquidates", [])
        if not batch:
            break
        all_liqs.extend(batch)
        last_id = batch[-1]["id"]
        if len(batch) < BATCH_SIZE:
            break
        time.sleep(0.3)

    rows = []
    for liq in all_liqs:
        ts = int(liq["timestamp"])
        dec = int(liq["asset"]["decimals"])
        amount_tokens = float(liq["amount"]) / (10 ** dec)
        amount_usd = float(liq.get("amountUSD") or 0)
        # Per-collateral liquidation protocol fee = penalty% * LPF (=10%) of amountUSD.
        # V2 had NO liquidation protocol fee — bonus went entirely to liquidators.
        # wBTC: penalty=5% * 0.1 = 0.5% of amountUSD
        # cbBTC: penalty=7.5% * 0.1 = 0.75% of amountUSD
        fee_rate = LIQ_PROTOCOL_FEE_BY_COLLATERAL.get(asset_key, {}).get(version, 0.0)
        protocol_fee_usd = amount_usd * fee_rate
        rows.append({
            "timestamp": ts,
            "date": datetime.utcfromtimestamp(ts).date(),
            "collateral_key": asset_key,
            "collateral_symbol": config["symbol"],
            "version": version,
            "liquidatee": liq["liquidatee"]["id"],
            "liquidator": liq["liquidator"]["id"],
            "amount_tokens": amount_tokens,
            "amount_usd": amount_usd,
            "profit_usd": float(liq.get("profitUSD") or 0),
            "protocol_fee_usd": protocol_fee_usd,
            "market_name": liq["market"]["name"],
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    logger.info(f"  {version} {config['symbol']}: {len(df)} liquidations")
    return df


# ── Step 4: fetch open positions ──────────────────────────────────────────────

def fetch_open_positions(
    api_key: str, subgraph_id: str, version: str, asset_key: str, accounts: Set[str]
) -> pd.DataFrame:
    """Fetch current open borrow positions for accounts with wBTC/cbBTC collateral."""
    config = ASSETS[asset_key]
    url = get_endpoint(api_key, subgraph_id)
    acc_list = sorted(accounts)
    rows = []

    logger.info(f"  {version} {config['symbol']}: fetching open positions ({len(acc_list)} accounts)...")
    for i in range(0, len(acc_list), ACCOUNT_BATCH):
        batch = acc_list[i: i + ACCOUNT_BATCH]
        data = _post(url, OPEN_BORROW_POSITIONS_QUERY, {"accounts": batch})
        if not data:
            continue

        for acc in data.get("data", {}).get("accounts", []):
            # Sum collateral BTC balance for this account
            btc_collateral_tokens = 0.0
            for pos in acc.get("collateral", []):
                sym = pos["market"]["inputToken"]["symbol"]
                if sym.upper() in ("WBTC", "CBBTC"):
                    dec = int(pos["market"]["inputToken"]["decimals"])
                    btc_collateral_tokens += float(pos["balance"]) / (10 ** dec)

            for pos in acc.get("positions", []):
                sym = pos["market"]["inputToken"]["symbol"]
                dec = int(pos["market"]["inputToken"]["decimals"])
                bal_tokens = float(pos["balance"]) / (10 ** dec)
                rate = 0.0
                if pos["market"]["rates"]:
                    rate = float(pos["market"]["rates"][0]["rate"])
                rows.append({
                    "account": acc["id"],
                    "collateral_key": asset_key,
                    "collateral_symbol": config["symbol"],
                    "version": version,
                    "borrowed_symbol": sym,
                    "borrowed_decimals": dec,
                    "borrow_balance_tokens": bal_tokens,
                    "variable_borrow_rate": rate,
                    "btc_collateral_tokens": btc_collateral_tokens,
                })

        time.sleep(0.25)

    df = pd.DataFrame(rows)
    logger.info(f"  {version} {config['symbol']}: {len(df)} open borrow positions")
    return df


# ── Step 5: compute realized interest ─────────────────────────────────────────

def compute_realized_interest(
    borrows_df: pd.DataFrame,
    repays_df: pd.DataFrame,
    price_lookup: dict,
) -> pd.DataFrame:
    """Compute realized interest per (account, collateral_key, version, asset_symbol).

    Method — event-level running balance with deduplication:

    1. DEDUP: accounts holding both wBTC and cbBTC get the same on-chain tx attributed
       to both collateral_keys. We deduplicate by (account, timestamp, asset_symbol)
       keeping only the row with the earliest collateral_key (wbtc < cbbtc alphabetically),
       then re-attribute to the account's primary collateral for that version.
       Simpler: deduplicate borrows/repays on (account, timestamp, asset_symbol) globally —
       the collateral_key distinction is spurious for the interest calculation because
       the borrowed asset doesn't know which BTC token was the collateral.

    2. RUNNING BALANCE: for each (account, asset_symbol) process events chronologically.
       At each repay, the outstanding balance tells us how much is principal vs interest.
       interest_on_repay = max(0, repay_amount - outstanding_balance_before_repay... )
       But we don't have intra-position balance from subgraph, so we use the aggregate:
       interest_tokens = max(0, total_repaid - total_borrowed)
       This is correct for closed positions. For open positions with reborrowing cycles,
       some realized interest is lost (embedded in the repay stream but indistinguishable
       from principal repayment without per-block balance data).

    3. PRICE: use historical price on last_repay_date (correct for token-denominated interest).
       For stablecoins price=1.00, so no distortion.
    """
    if borrows_df.empty or repays_df.empty:
        return pd.DataFrame()

    GROUP_KEYS = ["account", "collateral_key", "version", "asset_symbol", "asset_decimals"]
    # Dedup key: same on-chain tx can appear twice within the same collateral_key/version
    # because the paginated subgraph query sometimes returns the boundary item twice.
    # We keep collateral_key in the dedup key so legitimate cross-collateral events survive.
    DEDUP_KEYS = ["account", "timestamp", "asset_symbol", "collateral_key", "version", "amount_tokens"]

    # ── Step 1: Deduplicate events ────────────────────────────────────────────
    borrows_dedup = borrows_df.drop_duplicates(subset=DEDUP_KEYS, keep="first")
    repays_dedup  = repays_df.drop_duplicates(subset=DEDUP_KEYS, keep="first")

    logger.info(
        f"Dedup: borrows {len(borrows_df)} → {len(borrows_dedup)} "
        f"({len(borrows_df)-len(borrows_dedup)} duplicates removed); "
        f"repays {len(repays_df)} → {len(repays_dedup)} "
        f"({len(repays_df)-len(repays_dedup)} duplicates removed)"
    )

    # ── Step 2: Aggregate ─────────────────────────────────────────────────────
    b_agg = (
        borrows_dedup.groupby(GROUP_KEYS)["amount_tokens"].sum()
        .reset_index()
        .rename(columns={"amount_tokens": "total_borrowed_tokens"})
    )
    r_agg = (
        repays_dedup.groupby(GROUP_KEYS)
        .agg(
            total_repaid_tokens=("amount_tokens", "sum"),
            last_repay_date=("date", "max"),
        )
        .reset_index()
    )

    merged = pd.merge(
        r_agg, b_agg,
        on=GROUP_KEYS,
        how="left",
    )
    merged["total_borrowed_tokens"] = merged["total_borrowed_tokens"].fillna(0)
    merged["interest_tokens"] = merged["total_repaid_tokens"] - merged["total_borrowed_tokens"]

    # Keep only positive interest.
    # Negative = account still has open principal > repaid so far (or liquidation closed
    # the position without a repay event in our data). These represent unrealized or
    # untracked interest — captured separately in compute_open_position_accrued().
    merged = merged[merged["interest_tokens"] > 0].copy()

    # ── Step 3: USD conversion ────────────────────────────────────────────────
    def get_price(row):
        dt = row["last_repay_date"]
        if hasattr(dt, "date"):
            dt = dt.date()
        return price_for(row["asset_symbol"], dt, price_lookup)

    merged["price_usd"] = merged.apply(get_price, axis=1)
    merged["interest_usd"] = merged["interest_tokens"] * merged["price_usd"]

    # Per-asset reserve factor (falls back to global RESERVE_FACTOR = 20%)
    merged["reserve_factor"] = merged["asset_symbol"].map(
        lambda sym: MARKET_RESERVE_FACTORS.get(sym, RESERVE_FACTOR)
    )
    merged["protocol_revenue_usd"] = merged["interest_usd"] * merged["reserve_factor"]

    return merged[[
        "account", "collateral_key", "version", "asset_symbol",
        "total_borrowed_tokens", "total_repaid_tokens", "interest_tokens",
        "last_repay_date", "price_usd", "interest_usd", "reserve_factor",
        "protocol_revenue_usd",
    ]]


def compute_open_position_accrued(
    open_df: pd.DataFrame,
    price_lookup: dict,
    today: date,
) -> pd.DataFrame:
    """Compute unrealized (accrued) protocol revenue on open borrow positions.

    Only includes positions where the account currently has active BTC collateral
    (btc_collateral_tokens > 0). Accounts may have originally deposited BTC but later
    changed their collateral composition — those positions are excluded from the
    unrealized revenue estimate since they are no longer BTC-collateral-backed.

    Extreme borrow rates (>50%) from frozen/deprecated V2 markets (LUSD 319%,
    AMPL 271%) are capped at 50% to avoid phantom revenue from stale subgraph data.
    """
    if open_df.empty:
        return pd.DataFrame()

    # Filter to positions with actual current BTC collateral.
    # Without this filter, 70% of rows show btc_collateral_tokens==0, meaning the
    # account has since removed or changed their BTC collateral — those borrows are
    # not BTC-collateral-backed anymore and should not be included here.
    open_df = open_df[open_df["btc_collateral_tokens"] > 0].copy()

    if open_df.empty:
        return pd.DataFrame()

    # Cap extreme borrow rates from frozen/deprecated V2 markets (LUSD 319%, AMPL 271%).
    # These are not real future revenue — the markets are frozen and positions cannot
    # continue accruing at those rates in practice.
    MAX_RATE = 50.0
    open_df["variable_borrow_rate"] = open_df["variable_borrow_rate"].clip(upper=MAX_RATE)

    def get_price(row):
        return price_for(row["borrowed_symbol"], today, price_lookup)

    open_df["price_usd"] = open_df.apply(get_price, axis=1)
    open_df["borrow_balance_usd"] = open_df["borrow_balance_tokens"] * open_df["price_usd"]

    # Per-asset reserve factor (falls back to global RESERVE_FACTOR = 20%)
    open_df["reserve_factor"] = open_df["borrowed_symbol"].map(
        lambda sym: MARKET_RESERVE_FACTORS.get(sym, RESERVE_FACTOR)
    )
    # Annual protocol revenue on this position
    open_df["annual_protocol_usd"] = (
        open_df["borrow_balance_usd"]
        * (open_df["variable_borrow_rate"] / 100)
        * open_df["reserve_factor"]
    )
    open_df["monthly_protocol_usd"] = open_df["annual_protocol_usd"] / 12

    return open_df


# ── Master fetch function ──────────────────────────────────────────────────────

def fetch_all_collateral_revenue(api_key: str) -> dict:
    """Fetch everything and return dict of DataFrames ready for saving.

    Returns:
        {
          "accounts":     DataFrame  — all depositor account IDs
          "borrows":      DataFrame  — all borrow events
          "repays":       DataFrame  — all repay events
          "liquidations": DataFrame  — all liquidation events (collateral seized)
          "open_positions": DataFrame — current open borrow positions
        }
    """
    all_accounts_rows = []
    all_borrows = []
    all_repays = []
    all_liquidations = []
    all_open = []

    jobs = [
        (AAVE_V2_SUBGRAPH_ID, "V2", "wbtc"),
        (AAVE_V3_SUBGRAPH_ID, "V3", "wbtc"),
        (AAVE_V3_SUBGRAPH_ID, "V3", "cbbtc"),
    ]

    for subgraph_id, version, asset_key in jobs:
        sym = ASSETS[asset_key]["symbol"]
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing {version} {sym}...")

        # 1. Collect accounts
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

        # 2. Borrows + repays
        borrows_df, repays_df = fetch_borrows_repays(api_key, subgraph_id, version, asset_key, accounts)
        all_borrows.append(borrows_df)
        all_repays.append(repays_df)

        # 3. Liquidations
        liq_df = fetch_liquidations(api_key, subgraph_id, version, asset_key)
        all_liquidations.append(liq_df)

        # 4. Open positions
        open_df = fetch_open_positions(api_key, subgraph_id, version, asset_key, accounts)
        all_open.append(open_df)

    def safe_concat(dfs):
        dfs = [d for d in dfs if d is not None and not d.empty]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    return {
        "accounts":       pd.DataFrame(all_accounts_rows),
        "borrows":        safe_concat(all_borrows),
        "repays":         safe_concat(all_repays),
        "liquidations":   safe_concat(all_liquidations),
        "open_positions": safe_concat(all_open),
    }


# ── Parquet paths ──────────────────────────────────────────────────────────────

PATHS = {
    "accounts":       DATA_DIR / "collateral_accounts.parquet",
    "borrows":        DATA_DIR / "collateral_borrows.parquet",
    "repays":         DATA_DIR / "collateral_repays.parquet",
    "liquidations":   DATA_DIR / "collateral_liquidations.parquet",
    "open_positions": DATA_DIR / "collateral_open_positions.parquet",
}


def save_all(dfs: dict) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    for key, df in dfs.items():
        if df is not None and not df.empty:
            path = PATHS[key]
            df.to_parquet(path, index=False)
            logger.info(f"Saved {key}: {len(df)} rows → {path.name}")


def load_all() -> dict:
    result = {}
    for key, path in PATHS.items():
        if path.exists():
            df = pd.read_parquet(path)
            # Parse date columns
            for col in ["date", "last_repay_date"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            result[key] = df
        else:
            result[key] = pd.DataFrame()
    return result
