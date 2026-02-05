"""GraphQL queries for The Graph / Messari Aave subgraphs."""

# Contract addresses on Ethereum mainnet
WBTC_ADDRESS = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
CBBTC_ADDRESS = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"

# Subgraph IDs (used with The Graph Gateway)
AAVE_V2_SUBGRAPH_ID = "C2zniPn45RnLDGzVeGZCx2Sw3GXrbc9gL4ZfL8B8Em2j"
AAVE_V3_SUBGRAPH_ID = "JCNWRypm7FYwV8fx5HhzZPSFaMxgkPuw4TnR3Gpi81zk"

# Asset registry — config for each supported BTC-wrapped token
ASSETS = {
    "wbtc": {
        "address": WBTC_ADDRESS,
        "symbol": "wBTC",
        "name": "Wrapped BTC",
        "decimals": 8,
        "subgraphs": ["V2", "V3"],
    },
    "cbbtc": {
        "address": CBBTC_ADDRESS,
        "symbol": "cbBTC",
        "name": "Coinbase Wrapped BTC",
        "decimals": 8,
        "subgraphs": ["V3"],
    },
}


def get_asset_config(asset_key: str) -> dict:
    """Get configuration for an asset by its key."""
    if asset_key not in ASSETS:
        raise KeyError(f"Unknown asset: {asset_key}. Available: {list(ASSETS.keys())}")
    return ASSETS[asset_key]


def get_endpoint(api_key: str, subgraph_id: str) -> str:
    return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"


LIQUIDATIONS_QUERY = """
query GetLiquidations($first: Int!, $skip: Int!, $timestampFrom: Int!, $asset: String!) {
  liquidates(
    first: $first
    skip: $skip
    where: {
      timestamp_gte: $timestampFrom
      asset: $asset
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    hash
    blockNumber
    timestamp
    liquidator {
      id
    }
    liquidatee {
      id
    }
    market {
      id
      name
      inputToken {
        id
        symbol
        decimals
      }
    }
    asset {
      id
      symbol
      name
      decimals
    }
    amount
    amountUSD
    profitUSD
  }
}
"""

# --- User behavior queries (deposits, repays) ---

USER_DEPOSITS_QUERY = """
query GetDeposits($first: Int!, $skip: Int!, $account: String!, $timestampFrom: Int!, $timestampTo: Int!) {
  deposits(
    first: $first
    skip: $skip
    where: {
      account: $account
      timestamp_gte: $timestampFrom
      timestamp_lte: $timestampTo
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    hash
    timestamp
    account { id }
    market { id name }
    asset { id symbol decimals }
    amount
    amountUSD
  }
}
"""

USER_REPAYS_QUERY = """
query GetRepays($first: Int!, $skip: Int!, $account: String!, $timestampFrom: Int!, $timestampTo: Int!) {
  repays(
    first: $first
    skip: $skip
    where: {
      account: $account
      timestamp_gte: $timestampFrom
      timestamp_lte: $timestampTo
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    hash
    timestamp
    account { id }
    market { id name }
    asset { id symbol decimals }
    amount
    amountUSD
  }
}
"""
