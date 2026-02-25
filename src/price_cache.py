"""Historical and current price cache via CoinGecko free API.

Prices are stored in data/price_history.parquet:
  columns: date (datetime), symbol (str), price_usd (float)

Strategy:
- Stablecoins always return 1.0 (no API call)
- Non-stablecoins fetched once per symbol, cached locally
- CoinGecko free API: max 365 days per call, rate-limited
"""

import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

from src.collateral_queries import COINGECKO_IDS, STABLECOINS

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
PRICE_PATH = DATA_DIR / "price_history.parquet"

# CoinGecko free API base
CG_BASE = "https://api.coingecko.com/api/v3"


def _cg_get(url: str, params: dict, retries: int = 3) -> Optional[dict]:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 429:
                wait = 60 if attempt == 0 else 120
                logger.warning(f"CoinGecko rate limit, waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning(f"CoinGecko attempt {attempt+1} failed: {e}")
            time.sleep(5 * (attempt + 1))
    return None


def fetch_current_prices(symbols: list) -> Dict[str, float]:
    """Get current prices for a list of symbols.

    First tries CoinGecko API; if that fails, falls back to the latest
    cached price from price_history.parquet (offline mode).
    """
    prices = {s: 1.0 for s in symbols if s in STABLECOINS}
    non_stable = [s for s in symbols if s not in STABLECOINS and s in COINGECKO_IDS]

    if not non_stable:
        return prices

    # Try API first
    cg_ids = [COINGECKO_IDS[s] for s in non_stable]
    data = _cg_get(
        f"{CG_BASE}/simple/price",
        {"ids": ",".join(cg_ids), "vs_currencies": "usd"},
    )
    if data:
        for sym in non_stable:
            cg_id = COINGECKO_IDS[sym]
            if cg_id in data:
                prices[sym] = float(data[cg_id]["usd"])
        return prices

    # Offline fallback: use latest cached price
    logger.info("CoinGecko API unavailable, using cached prices")
    cached = _load_cached()
    if not cached.empty:
        for sym in non_stable:
            sym_data = cached[cached["symbol"] == sym]
            if not sym_data.empty:
                prices[sym] = float(sym_data.sort_values("date").iloc[-1]["price_usd"])

    return prices


def _load_cached() -> pd.DataFrame:
    if PRICE_PATH.exists():
        df = pd.read_parquet(PRICE_PATH)
        df["date"] = pd.to_datetime(df["date"])
        return df
    return pd.DataFrame(columns=["date", "symbol", "price_usd"])


def _save_cached(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    df.to_parquet(PRICE_PATH, index=False)


def fetch_and_cache_history(symbols: list, days: int = 1825) -> pd.DataFrame:
    """Return historical daily prices for symbols from cache.

    If cache is fresh (has data within last 2 days), return immediately.
    Otherwise try CoinGecko API to update, falling back to stale cache.
    """
    cached = _load_cached()
    today = pd.Timestamp(date.today())

    non_stable = [s for s in symbols if s not in STABLECOINS and s in COINGECKO_IDS]

    # Check if cache is fresh enough — skip API entirely if so
    all_fresh = True
    for sym in non_stable:
        sym_cached = cached[cached["symbol"] == sym]
        if sym_cached.empty or sym_cached["date"].max() < today - pd.Timedelta(days=2):
            all_fresh = False
            break

    if all_fresh:
        logger.info("Price cache is fresh, skipping API calls")
        return cached

    # Try to update stale symbols via API
    new_rows = []
    for sym in non_stable:
        cg_id = COINGECKO_IDS[sym]

        sym_cached = cached[cached["symbol"] == sym]
        if not sym_cached.empty:
            latest = sym_cached["date"].max()
            if latest >= today - pd.Timedelta(days=1):
                continue
            fetch_days = (today - latest).days + 1
        else:
            fetch_days = min(days, 365)

        fetch_days = min(fetch_days, 365)
        logger.info(f"  Fetching {sym} ({cg_id}): last {fetch_days} days...")

        data = _cg_get(
            f"{CG_BASE}/coins/{cg_id}/market_chart",
            {"vs_currency": "usd", "days": fetch_days, "interval": "daily"},
        )
        if not data:
            logger.warning(f"  {sym}: fetch failed, using cached data")
            continue

        for ts_ms, price in data.get("prices", []):
            dt = pd.Timestamp(datetime.utcfromtimestamp(ts_ms / 1000).date())
            new_rows.append({"date": dt, "symbol": sym, "price_usd": float(price)})

        time.sleep(1.5)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        combined = pd.concat([cached, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "symbol"], keep="last")
        combined = combined.sort_values(["symbol", "date"]).reset_index(drop=True)
        _save_cached(combined)
        logger.info(f"Price cache updated: {len(combined)} rows total")
        return combined

    return cached


def get_price_lookup(df: pd.DataFrame) -> Dict[tuple, float]:
    """Build a fast (symbol, date) → price dict from the cached DataFrame.

    Stablecoins always return 1.0. Missing entries fall back to the nearest
    available price for that symbol.
    """
    lookup: Dict[tuple, float] = {}
    for sym in df["symbol"].unique():
        sub = df[df["symbol"] == sym].set_index("date")["price_usd"]
        for dt, price in sub.items():
            lookup[(sym, dt.date())] = price

    return lookup


def price_for(symbol: str, dt: date, lookup: Dict[tuple, float]) -> float:
    """Get USD price for a symbol on a given date.

    Falls back: exact date → yesterday → nearest available → 0
    """
    if symbol in STABLECOINS:
        return 1.0

    for delta in range(0, 8):
        candidate = dt - timedelta(days=delta)
        if (symbol, candidate) in lookup:
            return lookup[(symbol, candidate)]

    # Last resort: find any price for this symbol
    sym_prices = {k[1]: v for k, v in lookup.items() if k[0] == symbol}
    if sym_prices:
        nearest_date = min(sym_prices.keys(), key=lambda d: abs((d - dt).days))
        return sym_prices[nearest_date]

    logger.warning(f"No price found for {symbol} on {dt}")
    return 0.0
