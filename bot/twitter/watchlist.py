# twitter/watchlist.py

# Tier 1: Major KOLs (100K+ followers) — priority for quote tweets
TIER1_USERNAMES = [
    "VitalikButerin",
    "APompliano",
    "RaoulGMI",
    "LynAldenContact",
    "CryptoHayes",
    "MacroAlf",
    "Ki_Young_Ju",
    "woonomic",
    "100trillionUSD",  # PlanB
    "NorthmanTrader",
    "cz_binance",      # CZ — massive reach, every post gets thousands of replies
]

# Tier 2: Mid-tier KOLs (10K-100K followers) — less competition in replies
TIER2_USERNAMES = [
    "WClementeIII",
    "glassnode",
    "dgt10011",
    "CryptoCred",
    "crypto_birb",
    "Route2FI",
    "DefiIgnas",
    "thedefiedge",
    "Pentosh1",
    "inversebrah",
    "MustStopMurad",
    "ZoomerOracle",
    "star_okx",        # OKX CEO — exchange ecosystem
    "CoinDesk",        # Top crypto news — reply to breaking news
    "HyperliquidX",    # Hot DeFi protocol — active reply section
]

# Polymarket / prediction market accounts — new track, higher engagement ROI
POLYMARKET_USERNAMES = [
    "Polymarket",
    "PolymarketWhale",
    "polyaboretum",         # Polymarket data/analytics
    "Kalshi",               # Competing prediction market (drives discussion)
    "NatePolymarket",
    "DustinMoskovitz",      # Prediction market advocate
]

# Tier 3: Mid-range accounts (1K-10K followers) — open replies, low competition
# These accounts are small enough that Twitter doesn't block replies from low-follower accounts.
# Focused on crypto data/analysis accounts that post about our brand pillars.
TIER3_USERNAMES = [
    "OnChainCollege",
    "crypto_nerd101",
    "btc_macro",
    "CryptoKaleo",
    "ColdBloodShill",
    "AltcoinSherpa",
    "CryptoGodJohn",
    "blaboratory",
    "CryptoTony__",
    "DeFiMinty",
    "raboratory",
    "TheCryptoLark",
    "CroissantEth",
    "lookonchain",
    "EmberCN",
]

# Backwards compatibility
WATCHLIST_USERNAMES = TIER1_USERNAMES + TIER2_USERNAMES + TIER3_USERNAMES
