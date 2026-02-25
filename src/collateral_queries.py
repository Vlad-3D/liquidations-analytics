"""GraphQL queries for collateral-based revenue analysis (wBTC/cbBTC as collateral)."""

AAVE_V2_SUBGRAPH_ID = "C2zniPn45RnLDGzVeGZCx2Sw3GXrbc9gL4ZfL8B8Em2j"
AAVE_V3_SUBGRAPH_ID = "JCNWRypm7FYwV8fx5HhzZPSFaMxgkPuw4TnR3Gpi81zk"

WBTC_ADDRESS = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
CBBTC_ADDRESS = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"

# Minimum collateral threshold: 1 BTC = 1e8 raw units (8 decimals)
MIN_BTC_RAW = "100000000"

RESERVE_FACTOR = 0.20          # Aave reserve factor fallback (used when asset not in MARKET_RESERVE_FACTORS)
LIQ_PROTOCOL_FEE = 0.005       # Fallback liquidation protocol fee (0.5% = wBTC default)

# Per-collateral liquidation protocol fee as a fraction of amountUSD.
# Formula: liquidationPenalty% * _liquidationProtocolFee (=0.1 for all V3 markets) / 100
# Source: V3 subgraph fields `liquidationPenalty` and `_liquidationProtocolFee`, verified 2026-02-24.
# V2 had NO liquidation protocol fee (goes entirely to liquidators) → 0.0
LIQ_PROTOCOL_FEE_BY_COLLATERAL: dict[str, dict[str, float]] = {
    # collateral_key → {"V2": fee, "V3": fee}
    # fee = fraction of amountUSD going to Aave treasury
    "wbtc":  {"V2": 0.000, "V3": 0.005},   # penalty=5%  * LPF=0.1 = 0.5%
    "cbbtc": {"V2": 0.000, "V3": 0.0075},  # penalty=7.5%* LPF=0.1 = 0.75%
}

# Per-asset Reserve Factors — source of truth:
#   V2: implied RF = cumulativeProtocolSideRevenueUSD / cumulativeTotalRevenueUSD
#       (the `reserveFactor` field in Messari V2 subgraph stores a different internal value)
#   V3: `reserveFactor` field directly from the Messari subgraph (on-chain parameter, 0–1 scale)
#       Verified 2026-02-24 via The Graph API query on markets(first:50, orderBy:totalValueLockedUSD)
# Fallback = 0.20 for any asset not in this map.
#
# NOTE: for assets that exist in both V2 and V3, V3 RF is used (more recent and authoritative).
# The key difference: V3 raised RF on BTC (50%), LSTs (35-45%), and some stables (25%).
MARKET_RESERVE_FACTORS: dict[str, float] = {
    # ── Stablecoins ──────────────────────────────────────────────────────────
    "USDC":   0.10,   # V3: 10% ✓
    "USDT":   0.10,   # V3: 10% ✓
    "DAI":    0.25,   # V3: 25% ✓ (was 5% — our previous guess was wrong)
    "GHO":    1.00,   # V3: 100% ✓ — all GHO interest goes to Aave treasury
    "FRAX":   0.20,   # V3: 20% ✓
    "PYUSD":  0.10,   # V3: 10% ✓
    "USDe":   0.25,   # V3: 25% ✓ (was 20%)
    "crvUSD": 0.20,   # V3: 20% ✓
    "LUSD":   0.20,   # V3: 20% ✓ (was 10%)
    "FDUSD":  0.20,   # fallback
    "USDS":   0.25,   # V3: 25% ✓ (was 15%)
    "RLUSD":  0.20,   # V3: 20% ✓
    "USDtb":  0.20,   # V3: 20% ✓
    "USDG":   0.20,   # V3: 20% ✓
    # ── BTC ──────────────────────────────────────────────────────────────────
    "WBTC":   0.50,   # V3: 50% ✓ (was 20% — major correction!)
    "wBTC":   0.50,   # alias
    "cbBTC":  0.50,   # V3: 50% ✓ (was 20% — major correction!)
    "tBTC":   0.50,   # V3: 50% ✓
    "LBTC":   0.50,   # V3: 50% ✓
    "eBTC":   0.50,   # V3: 50% ✓
    "FBTC":   0.50,   # V3: 50% ✓
    # ── ETH & LSTs ───────────────────────────────────────────────────────────
    "WETH":   0.15,   # V3: 15% ✓
    "ETH":    0.15,
    "wstETH": 0.35,   # V3: 35% ✓ (was 15% — major correction!)
    "weETH":  0.45,   # V3: 45% ✓ (was 15% — major correction!)
    "rETH":   0.15,   # V3: 15% ✓
    "cbETH":  0.15,   # V3: 15% ✓
    "rsETH":  0.15,   # V3: 15% ✓ (was 20%)
    "ezETH":  0.15,   # V3: 15% ✓ (was 20%)
    "osETH":  0.15,   # V3: 15% ✓
    "sUSDe":  0.20,   # V3: 20% ✓
    "ETHx":   0.15,   # V3: 15% ✓
    "tETH":   0.15,   # V3: 15% ✓
    # ── DeFi blue chips ──────────────────────────────────────────────────────
    "LINK":   0.20,   # V3: 20% ✓ (V2 implied ~8.7%, V3 raised it)
    "AAVE":   0.00,   # Non-borrowable collateral — no RF
    "UNI":    0.20,   # V3: 20% ✓
    "MKR":    0.20,   # V3: 20% ✓
    "CRV":    0.35,   # V3: 35% ✓ (was 20% — correction!)
    "BAL":    0.20,   # V2 implied 20% (not in V3 top markets)
    "SNX":    0.95,   # V3: 95% ✓ (was 5% — extreme correction! nearly frozen market)
    "LDO":    0.20,   # V3: 20% ✓
    "ENS":    0.20,   # V3: 20% ✓
    "RPL":    0.20,   # V3: 20% ✓
    "YFI":    0.20,   # V2 implied 20%
    "1INCH":  0.20,   # V3: 20% ✓
    "CVX":    0.20,   # V2 implied ~20%
    "XAUt":   0.20,   # V3: 20% ✓
    # ── Other ────────────────────────────────────────────────────────────────
    "sDAI":   0.20,   # V3: 20% ✓ (was 15%)
    "EURS":   0.20,
    "EURC":   0.10,   # V3: 10% ✓
    "syrupUSDT": 0.50, # V3: 50%
}

ASSETS = {
    "wbtc":  {"address": WBTC_ADDRESS,  "symbol": "wBTC",  "subgraphs": ["V2", "V3"]},
    "cbbtc": {"address": CBBTC_ADDRESS, "symbol": "cbBTC", "subgraphs": ["V3"]},
}

# CoinGecko IDs for price lookups
COINGECKO_IDS = {
    # Stablecoins (kept for completeness, priced at $1 via STABLECOINS set)
    "USDC":   "usd-coin",
    "USDT":   "tether",
    "DAI":    "dai",
    "GHO":    "gho",
    "FRAX":   "frax",
    "PYUSD":  "paypal-usd",
    "USDe":   "ethena-usde",
    "crvUSD": "crvusd",
    "LUSD":   "liquity-usd",
    # BTC
    "WBTC":   "wrapped-bitcoin",
    "cbBTC":  "coinbase-wrapped-btc",
    "tBTC":   "tbtc",
    "LBTC":   "lombard-staked-btc",
    # ETH & LSTs
    "WETH":   "ethereum",
    "ETH":    "ethereum",
    "wstETH": "wrapped-steth",
    "weETH":  "wrapped-eeth",
    "rETH":   "rocket-pool-eth",
    "cbETH":  "coinbase-wrapped-staked-eth",
    "rsETH":  "kelp-dao-restaked-eth",
    "ezETH":  "renzo-restaked-eth",
    # "osETH":  "staked-ether-stakewise",  # not available on CoinGecko free API
    # Yield-bearing
    "sDAI":   "savings-dai",
    # DeFi blue chips
    "LINK":   "chainlink",
    "AAVE":   "aave",
    "UNI":    "uniswap",
    "MKR":    "maker",
    "CRV":    "curve-dao-token",
    "BAL":    "balancer",
    "SNX":    "havven",
    "LDO":    "lido-dao",
    "ENS":    "ethereum-name-service",
    "RPL":    "rocket-pool",
    "YFI":    "yearn-finance",
    "1INCH":  "1inch",
    "CVX":    "convex-finance",
    # EUR-pegged (NOT USD stablecoins — priced via CoinGecko, ~$1.08)
    "EURS":   "stasis-eurs",
    "EURC":   "euro-coin",
}

# Stablecoins — always price = $1, skip CoinGecko
STABLECOINS = {
    # USD-pegged → priced at exactly $1.00
    "USDC", "USDT", "DAI", "GHO", "FRAX", "PYUSD", "USDe", "crvUSD",
    "LUSD", "FDUSD", "USDS", "RLUSD", "TUSD", "sUSD", "BUSD", "USDtb",
    "USDG", "mUSD",
    # Note: EURS and EURC are EUR-pegged (~$1.08) — NOT in this set, priced via CoinGecko
}


def get_endpoint(api_key: str, subgraph_id: str) -> str:
    return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"


# ── Account discovery ──────────────────────────────────────────────────────────

# All deposit events ≥ 1 BTC for a given asset (to find accounts)
DEPOSITS_FOR_ACCOUNTS_QUERY = """
query($asset: String!, $minAmount: String!, $lastId: String!) {
  deposits(
    first: 1000
    orderBy: id
    orderDirection: asc
    where: {
      asset: $asset
      amount_gte: $minAmount
      id_gt: $lastId
    }
  ) {
    id
    account { id }
    amount
    amountUSD
    timestamp
  }
}
"""

# ── Borrow / Repay events ──────────────────────────────────────────────────────

BORROWS_FOR_ACCOUNTS_QUERY = """
query($accounts: [String!]!, $lastId: String!) {
  borrows(
    first: 1000
    orderBy: id
    orderDirection: asc
    where: {
      account_in: $accounts
      id_gt: $lastId
    }
  ) {
    id
    timestamp
    account { id }
    asset { id symbol decimals }
    amount
    amountUSD
    market { id name }
  }
}
"""

REPAYS_FOR_ACCOUNTS_QUERY = """
query($accounts: [String!]!, $lastId: String!) {
  repays(
    first: 1000
    orderBy: id
    orderDirection: asc
    where: {
      account_in: $accounts
      id_gt: $lastId
    }
  ) {
    id
    timestamp
    account { id }
    asset { id symbol decimals }
    amount
    amountUSD
    market { id name }
  }
}
"""

# ── Liquidation events (wBTC/cbBTC as collateral seized) ──────────────────────

LIQUIDATIONS_AS_COLLATERAL_QUERY = """
query($asset: String!, $lastId: String!) {
  liquidates(
    first: 1000
    orderBy: id
    orderDirection: asc
    where: {
      asset: $asset
      id_gt: $lastId
    }
  ) {
    id
    timestamp
    liquidatee { id }
    liquidator { id }
    asset { id symbol decimals }
    amount
    amountUSD
    profitUSD
    market { id name }
  }
}
"""

# ── Open positions (current state) ────────────────────────────────────────────

OPEN_BORROW_POSITIONS_QUERY = """
query($accounts: [String!]!) {
  accounts(
    first: 1000
    where: { id_in: $accounts }
  ) {
    id
    positions(where: { side: BORROWER, balance_gt: 0 }) {
      balance
      market {
        id
        name
        inputToken { id symbol decimals }
        rates(where: { side: BORROWER, type: VARIABLE }) { rate }
      }
    }
    collateral: positions(where: { side: COLLATERAL, isCollateral: true, balance_gt: 0 }) {
      balance
      market {
        inputToken { id symbol decimals }
      }
    }
  }
}
"""
