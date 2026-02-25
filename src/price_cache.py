"""Price cache — reads pre-saved data from data/price_history.parquet.

No API calls. All data served from local parquet files.
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict

import pandas as pd

from src.collateral_queries import COINGECKO_IDS, STABLECOINS

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
PRICE_PATH = DATA_DIR / "price_history.parquet"


def _load_cached() -> pd.DataFrame:
    if PRICE_PATH.exists():
        df = pd.read_parquet(PRICE_PATH)
        df["date"] = pd.to_datetime(df["date"])
        return df
    return pd.DataFrame(columns=["date", "symbol", "price_usd"])


def fetch_current_prices(symbols: list) -> Dict[str, float]:
    """Return latest cached price for each symbol. No API calls."""
    prices = {s: 1.0 for s in symbols if s in STABLECOINS}
    non_stable = [s for s in symbols if s not in STABLECOINS and s in COINGECKO_IDS]

    if not non_stable:
        return prices

    cached = _load_cached()
    if not cached.empty:
        for sym in non_stable:
            sym_data = cached[cached["symbol"] == sym]
            if not sym_data.empty:
                prices[sym] = float(sym_data.sort_values("date").iloc[-1]["price_usd"])

    return prices


def fetch_and_cache_history(symbols: list, days: int = 1825) -> pd.DataFrame:
    """Return historical daily prices from cached parquet. No API calls."""
    return _load_cached()


def get_price_lookup(df: pd.DataFrame) -> Dict[tuple, float]:
    """Build a fast (symbol, date) -> price dict from the cached DataFrame."""
    lookup: Dict[tuple, float] = {}
    for sym in df["symbol"].unique():
        sub = df[df["symbol"] == sym].set_index("date")["price_usd"]
        for dt, price in sub.items():
            lookup[(sym, dt.date())] = price
    return lookup


def price_for(symbol: str, dt: date, lookup: Dict[tuple, float]) -> float:
    """Get USD price for a symbol on a given date.

    Falls back: exact date -> yesterday -> nearest available -> 0
    """
    if symbol in STABLECOINS:
        return 1.0

    for delta in range(0, 8):
        candidate = dt - timedelta(days=delta)
        if (symbol, candidate) in lookup:
            return lookup[(symbol, candidate)]

    sym_prices = {k[1]: v for k, v in lookup.items() if k[0] == symbol}
    if sym_prices:
        nearest_date = min(sym_prices.keys(), key=lambda d: abs((d - dt).days))
        return sym_prices[nearest_date]

    logger.warning(f"No price found for {symbol} on {dt}")
    return 0.0
