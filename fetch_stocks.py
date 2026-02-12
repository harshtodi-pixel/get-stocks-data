#!/usr/bin/env python3

"""
Fetch 1-minute intraday data for all Nifty 100 stocks
from Dhan's Historical Data APIs and store locally as Parquet.

Date range: 2023-01-01 to today (IST), i.e. 3 years.
Output: data/stocks/{SYMBOL}/{SYMBOL}_1m.parquet (one file per stock)

Columns (standard backtesting schema, same as spot index data):
    ts        - epoch seconds (int64)
    datetime  - ISO 8601 with IST offset (string)
    open      - open price (float64)
    high      - high price (float64)
    low       - low price (float64)
    close     - close price (float64)
    volume    - volume traded (int64)

Stock symbol is stored as Parquet file metadata (not a repeated column),
since each file contains data for only one stock.

Supports incremental updates: skips already-fetched date ranges.

Usage:
    export DHAN_ACCESS_TOKEN='your-token'
    python fetch_stocks.py
"""

import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from config import (
    DHAN_BASE_URL,
    NIFTY_100_SYMBOLS,
    STOCKS_DIR,
    STOCKS_START_DATE,
)
from utils import (
    IST,
    build_auth_headers,
    ensure_dir,
    epoch_to_ist_iso,
    generate_date_windows,
    get_last_timestamp,
    load_instrument_list,
    load_parquet,
    merge_and_deduplicate,
    post_json,
    print_progress,
    save_parquet,
    throttle,
    today_ist,
)


# Dhan intraday endpoint (same as spot index data).
INTRADAY_URL = DHAN_BASE_URL + "/charts/intraday"

# Equity instruments on NSE.
NSE_EQ = "NSE_EQ"
EQUITY = "EQUITY"

# Dedup columns for stock data.
STOCK_DEDUP_COLS = ["ts"]


def resolve_stock_security_ids(
    instrument_rows: List[Dict[str, str]],
    symbols: List[str],
) -> Dict[str, str]:
    """
    Look up security IDs for a list of stock symbols from Dhan's instrument list.

    Filters by: EXCH_ID=NSE, INSTRUMENT_TYPE=EQUITY (or similar).
    Returns: dict of {symbol: security_id}.
    Prints warnings for symbols that can't be found.
    """
    # Build a lookup: symbol_name -> security_id for NSE equity instruments.
    symbol_to_id: Dict[str, str] = {}

    for row in instrument_rows:
        exch = (row.get("EXCH_ID") or "").upper()
        instr = (row.get("INSTRUMENT_TYPE") or "").upper()

        # Only look at NSE equity instruments.
        # INSTRUMENT_TYPE can be "EQUITY" or "ES" or other variants.
        if exch != "NSE":
            continue
        if instr not in ("EQUITY", "ES", "EQ"):
            continue

        sym = (row.get("SYMBOL_NAME") or "").strip()
        if not sym:
            continue

        sid = (
            row.get("SEM_SMST_SECURITY_ID")
            or row.get("SecurityID")
            or row.get("SECURITY_ID")
        )
        if not sid:
            continue

        # Some symbols may have multiple entries (e.g., different series).
        # Prefer EQ series over others.
        series = (row.get("SERIES") or row.get("SEM_SERIES") or "").upper()
        if sym in symbol_to_id and series != "EQ":
            continue  # Keep the EQ series entry.

        symbol_to_id[sym] = str(sid)

    # Match requested symbols.
    result: Dict[str, str] = {}
    missing: List[str] = []

    for sym in symbols:
        if sym in symbol_to_id:
            result[sym] = symbol_to_id[sym]
        else:
            missing.append(sym)

    if missing:
        print(f"  WARNING: Could not find security IDs for: {', '.join(missing)}")
        print(f"  These stocks will be skipped.")

    return result


def fetch_stock_data(
    symbol: str,
    security_id: str,
    headers: dict,
    start_date,
    end_date,
) -> pd.DataFrame:
    """
    Fetch 1-minute candles for a single stock.

    Returns DataFrame with standard columns.
    Uses 90-day windows as required by Dhan.
    """
    windows = generate_date_windows(start_date, end_date, max_days=90)
    all_rows = []

    for w_idx, (d_from, d_to) in enumerate(windows, start=1):
        from_str = datetime(
            d_from.year, d_from.month, d_from.day, 0, 0, 0, tzinfo=IST
        ).strftime("%Y-%m-%d %H:%M:%S")
        to_str = datetime(
            d_to.year, d_to.month, d_to.day, 23, 59, 59, tzinfo=IST
        ).strftime("%Y-%m-%d %H:%M:%S")

        payload = {
            "securityId": security_id,
            "exchangeSegment": NSE_EQ,
            "instrument": EQUITY,
            "interval": "1",
            "oi": False,
            "fromDate": from_str,
            "toDate": to_str,
        }

        try:
            data = post_json(INTRADAY_URL, payload, headers)
        except RuntimeError as e:
            # If HTTP 400 for a large window, try splitting.
            if "HTTP 400" in str(e) and (d_to - d_from).days > 1:
                mid = d_from + (d_to - d_from) // 2
                df_left = fetch_stock_data(symbol, security_id, headers, d_from, mid)
                df_right = fetch_stock_data(symbol, security_id, headers, mid, d_to)
                all_rows.append(df_left)
                all_rows.append(df_right)
                continue
            # For other errors, skip this window and continue.
            print(f"    Error for {symbol} window {d_from}->{d_to}: {e}")
            continue

        timestamps = data.get("timestamp") or []
        if not timestamps:
            continue

        opens = data.get("open") or []
        highs = data.get("high") or []
        lows = data.get("low") or []
        closes = data.get("close") or []
        volumes = data.get("volume") or []

        n = len(timestamps)
        if len(volumes) < n:
            volumes = volumes + [0] * (n - len(volumes))

        for i in range(n):
            ts = int(timestamps[i])
            all_rows.append({
                "ts": ts,
                "datetime": epoch_to_ist_iso(ts),
                "open": float(opens[i]) if i < len(opens) else 0.0,
                "high": float(highs[i]) if i < len(highs) else 0.0,
                "low": float(lows[i]) if i < len(lows) else 0.0,
                "close": float(closes[i]) if i < len(closes) else 0.0,
                "volume": int(volumes[i]),
            })

        throttle()

    # Combine dicts and any sub-DataFrames from recursive splits.
    dicts = [r for r in all_rows if isinstance(r, dict)]
    dfs = [r for r in all_rows if isinstance(r, pd.DataFrame)]

    df = pd.DataFrame(dicts) if dicts else pd.DataFrame()
    for sub_df in dfs:
        df = pd.concat([df, sub_df], ignore_index=True)

    return df


def run_stocks_fetch() -> None:
    """
    Main entry point: fetch 1-min data for all Nifty 100 stocks.

    Processes stocks one by one, saving each to its own Parquet file.
    Supports incremental updates per stock.
    """
    ensure_dir(STOCKS_DIR)

    print("Downloading Dhan instrument list...")
    instrument_rows = load_instrument_list()
    print(f"  Loaded {len(instrument_rows)} instrument rows.")

    # Resolve security IDs for all Nifty 100 symbols.
    print(f"  Resolving security IDs for {len(NIFTY_100_SYMBOLS)} stocks...")
    sym_to_id = resolve_stock_security_ids(instrument_rows, NIFTY_100_SYMBOLS)
    print(f"  Found {len(sym_to_id)} stocks with valid security IDs.")

    headers = build_auth_headers()
    total = len(sym_to_id)
    end = today_ist()

    for idx, (symbol, sec_id) in enumerate(sorted(sym_to_id.items()), start=1):
        out_dir = os.path.join(STOCKS_DIR, symbol)
        out_file = os.path.join(out_dir, f"{symbol}_1m.parquet")

        print(f"\n[{idx}/{total}] {symbol} (securityId={sec_id})")

        # Determine start date (incremental update).
        start = STOCKS_START_DATE
        existing_df = load_parquet(out_file)
        last_ts = get_last_timestamp(out_file)
        if last_ts is not None:
            last_dt = datetime.fromtimestamp(last_ts, tz=IST)
            start = last_dt.date()
            print(f"  Resuming from {start} (incremental)")

        if start >= end:
            print(f"  Already up to date.")
            continue

        new_df = fetch_stock_data(symbol, sec_id, headers, start, end)

        if new_df.empty:
            print(f"  No data returned for {symbol}.")
            continue

        # Merge and deduplicate.
        final_df = merge_and_deduplicate(existing_df, new_df, STOCK_DEDUP_COLS)

        # Enforce column types.
        final_df["ts"] = final_df["ts"].astype("int64")
        final_df["open"] = final_df["open"].astype("float64")
        final_df["high"] = final_df["high"].astype("float64")
        final_df["low"] = final_df["low"].astype("float64")
        final_df["close"] = final_df["close"].astype("float64")
        final_df["volume"] = final_df["volume"].astype("int64")

        save_parquet(final_df, out_file)

    print(f"\nStocks data fetch complete. Processed {total} stocks.")


if __name__ == "__main__":
    try:
        run_stocks_fetch()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
