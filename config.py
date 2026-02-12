"""
Centralized configuration for the data collection pipeline.

All date ranges, output paths, API constants, and stock lists live here.
Edit this file to change what data gets fetched.
"""

import os
from datetime import date

# ---------------------------------------------------------------------------
# Dhan API settings
# ---------------------------------------------------------------------------

# Base URL for Dhan v2 API.
DHAN_BASE_URL = "https://api.dhan.co/v2"

# Detailed instrument list CSV from Dhan.
INSTRUMENT_LIST_URL = (
    "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
)

# Delay between API calls (seconds) to respect rate limits.
API_CALL_DELAY = 0.5

# ---------------------------------------------------------------------------
# Output directory structure
# ---------------------------------------------------------------------------

DATA_DIR = "data"
SPOT_DIR = os.path.join(DATA_DIR, "spot")
OPTIONS_DIR = os.path.join(DATA_DIR, "options")
STOCKS_DIR = os.path.join(DATA_DIR, "stocks")

# ---------------------------------------------------------------------------
# Date ranges (inclusive start, exclusive end)
# ---------------------------------------------------------------------------

# Spot index data: 5 years.
SPOT_START_DATE = date(2021, 1, 1)

# Options data: 1 year.
OPTIONS_START_DATE = date(2025, 1, 1)

# Nifty 100 stocks: 3 years.
STOCKS_START_DATE = date(2023, 1, 1)

# ---------------------------------------------------------------------------
# Index configuration
# ---------------------------------------------------------------------------

# How to recognise NIFTY 50 and SENSEX in Dhan's instrument list.
INDEX_MATCH_RULES = {
    "NIFTY_50": {
        "preferred_keywords": ["NIFTY 50"],
        "fallback_keywords": ["NIFTY"],
        "exclude_keywords": ["BANK", "FIN", "MIDCAP", "NEXT 50", "IT"],
    },
    "SENSEX": {
        "preferred_keywords": ["S&P BSE SENSEX", "SENSEX"],
        "fallback_keywords": [],
        "exclude_keywords": ["MIDCAP", "SMALLCAP"],
    },
}

# Dhan enums for index historical data.
INDEX_EXCHANGE_SEGMENT = "IDX_I"
INDEX_INSTRUMENT_TYPE = "INDEX"

# ---------------------------------------------------------------------------
# Options configuration
# ---------------------------------------------------------------------------

# Exchange segments for options on indices.
NSE_FNO = "NSE_FNO"
BSE_FNO = "BSE_FNO"
OPTIDX = "OPTIDX"

# Expiry types and codes.
EXPIRY_FLAGS = ["WEEK", "MONTH"]
# Dhan docs say 0/1/2, but the rolling API actually uses 1/2/3.
# 1=near (current), 2=next, 3=far.
EXPIRY_CODES = [1, 2, 3]

# ATM +/- 10 strike buckets for rolling options.
STRIKE_BUCKETS = ["ATM"]
for _i in range(1, 11):
    STRIKE_BUCKETS.append(f"ATM+{_i}")
for _i in range(1, 11):
    STRIKE_BUCKETS.append(f"ATM-{_i}")

# Strike step per index (distance between consecutive strikes).
# NIFTY strikes: 14000, 14050, 14100 ... (step = 50)
# SENSEX strikes: 48000, 48100, 48200 ... (step = 100)
STRIKE_STEPS = {
    "NIFTY": 50,
    "SENSEX": 100,
}

# ---------------------------------------------------------------------------
# Nifty 100 stock symbols
#
# These are the constituents of the NIFTY 100 index as of early 2025.
# The list changes infrequently (semi-annual rebalance).
# Symbols must match Dhan's SYMBOL_NAME for NSE equity instruments.
# ---------------------------------------------------------------------------

NIFTY_100_SYMBOLS = [
    "ABB",
    "ADANIENT",
    "ADANIENSOL",  # Adani Energy Solutions
    "ADANIGREEN",
    "ADANIPORTS",
    "ADANIPOWER",
    "AMBUJACEM",
    "APOLLOHOSP",
    "ASIANPAINT",
    "ATGL",
    "AXISBANK",
    "BAJAJ-AUTO",
    "BAJAJFINSV",
    "BAJAJHLDNG",  # Bajaj Holdings
    "BAJFINANCE",
    "BANKBARODA",
    "BEL",
    "BHARTIARTL",
    "BHEL",
    "BOSCHLTD",
    "BPCL",
    "BRITANNIA",
    "CANBK",
    "CHOLAFIN",
    "CIPLA",
    "COALINDIA",
    "DABUR",
    "DIVISLAB",
    "DLF",
    "DMART",        # Avenue Supermarts
    "DRREDDY",
    "EICHERMOT",
    "GAIL",
    "GODREJCP",
    "GRASIM",
    "HAL",          # Hindustan Aeronautics
    "HAVELLS",
    "HCLTECH",
    "HDFCBANK",
    "HDFCLIFE",
    "HEROMOTOCO",
    "HINDALCO",
    "HINDUNILVR",
    "ICICIBANK",
    "ICICIGI",
    "ICICIPRULI",
    "INDHOTEL",
    "INDIGO",
    "INDUSINDBK",
    "INFY",
    "IOC",
    "IRCTC",
    "IRFC",         # Indian Railway Finance Corp
    "ITC",
    "JINDALSTEL",
    "JIOFIN",
    "JSWENERGY",    # JSW Energy
    "JSWSTEEL",
    "KOTAKBANK",
    "LT",
    "LICI",
    "LODHA",        # Macrotech Developers
    "LTIM",
    "LUPIN",
    "M&M",
    "MARICO",
    "MARUTI",
    "MOTHERSON",
    "NAUKRI",       # Info Edge India
    "NESTLEIND",
    "NHPC",
    "NTPC",
    "ONGC",
    "PAGEIND",
    "PFC",
    "PIDILITIND",
    "PNB",
    "POWERGRID",
    "RECLTD",
    "RELIANCE",
    "SAIL",
    "SBICARD",
    "SBILIFE",
    "SBIN",
    "SHREECEM",
    "SHRIRAMFIN",
    "SIEMENS",
    "SUNPHARMA",
    "TATACONSUM",
    "TATAMOTORS",
    "TATAPOWER",
    "TATASTEEL",
    "TCS",
    "TECHM",
    "TITAN",
    "TORNTPHARM",
    "TRENT",
    "TVSMOTOR",     # TVS Motor Company
    "ULTRACEMCO",
    "UNIONBANK",
    "UNITDSPR",
    "VEDL",
    "VBL",
    "WIPRO",
    "ZOMATO",
    "ZYDUSLIFE",
]
