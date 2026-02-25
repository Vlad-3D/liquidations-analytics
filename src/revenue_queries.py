"""GraphQL queries for Aave V2/V3 revenue analysis."""

AAVE_V2_SUBGRAPH_ID = "C2zniPn45RnLDGzVeGZCx2Sw3GXrbc9gL4ZfL8B8Em2j"
AAVE_V3_SUBGRAPH_ID = "JCNWRypm7FYwV8fx5HhzZPSFaMxgkPuw4TnR3Gpi81zk"

WBTC_ADDRESS = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
CBBTC_ADDRESS = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"

ASSETS = {
    "wbtc": {"address": WBTC_ADDRESS, "symbol": "wBTC", "subgraphs": ["V2", "V3"]},
    "cbbtc": {"address": CBBTC_ADDRESS, "symbol": "cbBTC", "subgraphs": ["V3"]},
}


def get_endpoint(api_key: str, subgraph_id: str) -> str:
    return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"


# Daily per-market revenue snapshots (interest + liquidation volume)
MARKET_DAILY_SNAPSHOTS_QUERY = """
query GetMarketDailySnapshots($first: Int!, $skip: Int!, $asset: String!) {
  marketDailySnapshots(
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
    where: { market_: { inputToken: $asset } }
  ) {
    id
    timestamp
    market {
      id
      name
      inputToken { id symbol decimals }
    }
    dailySupplySideRevenueUSD
    dailyProtocolSideRevenueUSD
    dailyTotalRevenueUSD
    dailyLiquidateUSD
    totalBorrowBalanceUSD
    totalDepositBalanceUSD
  }
}
"""

# Current market state for accrued revenue snapshot
MARKETS_CURRENT_QUERY = """
{
  markets(
    where: {
      inputToken_in: [
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
      ]
    }
  ) {
    id
    name
    inputToken { id symbol decimals }
    totalBorrowBalanceUSD
    totalDepositBalanceUSD
    totalValueLockedUSD
    cumulativeLiquidateUSD
    rates {
      rate
      side
      type
    }
  }
}
"""

# Individual liquidation events with profitUSD (liquidator bonus)
LIQUIDATION_EVENTS_QUERY = """
query GetLiquidationEvents($first: Int!, $skip: Int!, $asset: String!) {
  liquidates(
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
    where: { asset: $asset }
  ) {
    id
    timestamp
    amount
    amountUSD
    profitUSD
    asset { id symbol decimals }
    market { id name }
  }
}
"""
