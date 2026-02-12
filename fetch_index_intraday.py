#!/usr/bin/env python3

"""
Fetch 1-minute intraday data for NIFTY 50 and S&P BSE SENSEX
from Dhan's Historical Data APIs and store locally as Parquet.

Date range: 2021-01-01 to today (IST).
Output: data/spot/nifty/NIFTY_1m.parquet
        data/spot/sensex/SENSEX_1m.parquet

Columns (standard backtesting schema):
    ts        - epoch seconds (int64)
    datetime  - ISO 8601 with IST offset (string)
    open      - open price (float64)
    high      - high price (float64)
    low       - low price (float64)
    close     - close price (float64)
    volume    - volume traded (int64)

Supports incremental updates: if the Parquet file already exists,
only fetches data after the last timestamp.

Usage:
    export DHAN_ACCESS_TOKEN='your-token'
    python fetch_index_intraday.py
"""

import os
import sys
from datetime import datetime

import pandas as pd

from config import (
    DHAN_BASE_URL,
    INDEX_EXCHANGE_SEGMENT,
    INDEX_INSTRUMENT_TYPE,
    INDEX_MATCH_RULES,
    SPOT_DIR,
    SPOT_START_DATE,
)
from utils import (
    IST,
    build_auth_headers,
    ensure_dir,
    epoch_to_ist_iso,
    extract_security_id,
    generate_date_windows,
    get_last_timestamp,
    load_instrument_list,
    load_parquet,
    merge_and_deduplicate,
    post_json,
    print_progress,
    resolve_index_instrument,
    save_parquet,
    throttle,
    today_ist,
)


# Dhan intraday endpoint.
INTRADAY_URL = DHAN_BASE_URL + "/charts/intraday"

# Spot data dedup columns: timestamp is unique per candle.
SPOT_DEDUP_COLS = ["ts"]


def fetch_spot_for_index(
    name: str,
    security_id: str,
    headers: dict,
    start_date,
    end_date,
) -> pd.DataFrame:
    """
    Fetch all 1-minute candles for an index over a date range.

    Returns a DataFrame with standardized column names.
    Fetches in <= 90-day windows as required by Dhan API.
    """
    windows = generate_date_windows(start_date, end_date, max_days=90)
    all_rows = []

    for w_idx, (d_from, d_to) in enumerate(windows, start=1):
        # Dhan intraday API expects datetime strings.
        from_str = datetime(
            d_from.year, d_from.month, d_from.day, 0, 0, 0, tzinfo=IST
        ).strftime("%Y-%m-%d %H:%M:%S")
        to_str = datetime(
            d_to.year, d_to.month, d_to.day, 23, 59, 59, tzinfo=IST
        ).strftime("%Y-%m-%d %H:%M:%S")

        print_progress(w_idx, len(windows), f"{name} {d_from} -> {d_to}")

        payload = {
            "securityId": security_id,
            "exchangeSegment": INDEX_EXCHANGE_SEGMENT,
            "instrument": INDEX_INSTRUMENT_TYPE,
            "interval": "1",
            "oi": False,
            "fromDate": from_str,
            "toDate": to_str,
        }

        try:
            data = post_json(INTRADAY_URL, payload, headers)
        except RuntimeError as e:
            # If HTTP 400 for a large window, split into halves.
            if "HTTP 400" in str(e) and (d_to - d_from).days > 1:
                print(f"  HTTP 400; splitting window for {name}...")
                mid = d_from + (d_to - d_from) // 2
                # Recursively fetch each half.
                df_left = fetch_spot_for_index(
                    name, security_id, headers, d_from, mid
                )
                df_right = fetch_spot_for_index(
                    name, security_id, headers, mid, d_to
                )
                all_rows.append(df_left)
                all_rows.append(df_right)
                continue
            raise

        timestamps = data.get("timestamp") or []
        if not timestamps:
            continue

        opens = data.get("open") or []
        highs = data.get("high") or []
        lows = data.get("low") or []
        closes = data.get("close") or []
        volumes = data.get("volume") or []

        n = len(timestamps)
        if not (len(opens) == len(highs) == len(lows) == len(closes) == n):
            raise RuntimeError(
                f"{name}: Misaligned arrays for {d_from} -> {d_to}"
            )

        # Pad volumes if shorter (some indices return empty volume).
        if len(volumes) < n:
            volumes = volumes + [0] * (n - len(volumes))

        # Build rows for this window.
        for i in range(n):
            ts = int(timestamps[i])
            all_rows.append({
                "ts": ts,
                "datetime": epoch_to_ist_iso(ts),
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
                "volume": int(volumes[i]),
            })

        throttle()

    # Build DataFrame from collected rows (skip any sub-DataFrames from splits).
    dicts = [r for r in all_rows if isinstance(r, dict)]
    dfs = [r for r in all_rows if isinstance(r, pd.DataFrame)]

    df = pd.DataFrame(dicts) if dicts else pd.DataFrame()
    for sub_df in dfs:
        df = pd.concat([df, sub_df], ignore_index=True)

    return df


def run_spot_fetch() -> None:
    """
    Main entry point: fetch spot data for NIFTY and SENSEX.

    Supports incremental updates by checking existing Parquet files.
    """
    ensure_dir(SPOT_DIR)

    # Load Dhan instrument list to find security IDs.
    print("Downloading Dhan instrument list...")
    rows = load_instrument_list()
    print(f"  Loaded {len(rows)} instrument rows.")

    headers = build_auth_headers()

    # Define which indices to fetch and their output paths.
    indices = [
        {
            "name": "NIFTY_50",
            "short_name": "nifty",
            "display": "NIFTY 50",
        },
        {
            "name": "SENSEX",
            "short_name": "sensex",
            "display": "SENSEX",
        },
    ]

    for idx_info in indices:
        name = idx_info["name"]
        short = idx_info["short_name"]
        out_dir = os.path.join(SPOT_DIR, short)
        out_file = os.path.join(out_dir, f"{short.upper()}_1m.parquet")

        # Resolve the index instrument from Dhan's list.
        best_row = resolve_index_instrument(rows, name, INDEX_MATCH_RULES)
        sec_id = extract_security_id(best_row)
        print(f"  Resolved {name}: securityId={sec_id}")

        # Determine start date (incremental update support).
        start = SPOT_START_DATE
        existing_df = load_parquet(out_file)
        last_ts = get_last_timestamp(out_file)
        if last_ts is not None:
            # Resume from the day of the last timestamp.
            last_dt = datetime.fromtimestamp(last_ts, tz=IST)
            start = last_dt.date()
            print(f"  Resuming {name} from {start} (incremental update)")

        end = today_ist()
        if start >= end:
            print(f"  {name} is already up to date.")
            continue

        print(f"  Fetching {idx_info['display']} from {start} to {end}...")
        new_df = fetch_spot_for_index(name, sec_id, headers, start, end)

        if new_df.empty:
            print(f"  No new data for {name}.")
            continue

        # Merge with existing data and deduplicate.
        final_df = merge_and_deduplicate(existing_df, new_df, SPOT_DEDUP_COLS)

        # Enforce column types.
        final_df["ts"] = final_df["ts"].astype("int64")
        final_df["open"] = final_df["open"].astype("float64")
        final_df["high"] = final_df["high"].astype("float64")
        final_df["low"] = final_df["low"].astype("float64")
        final_df["close"] = final_df["close"].astype("float64")
        final_df["volume"] = final_df["volume"].astype("int64")

        save_parquet(final_df, out_file)

    print("Spot data fetch complete.")


if __name__ == "__main__":
    try:
        run_spot_fetch()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
