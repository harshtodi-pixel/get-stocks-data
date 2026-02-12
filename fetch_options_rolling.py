#!/usr/bin/env python3

"""
Fetch expired options data for NIFTY 50 and S&P BSE SENSEX
from Dhan's Rolling Options API and store locally as Parquet.

Date range: 2025-01-01 to today (IST).
Output: data/options/nifty/NIFTY_OPTIONS_1m.parquet
        data/options/sensex/SENSEX_OPTIONS_1m.parquet

Columns (standard backtesting schema):
    ts             - epoch seconds (int64)
    datetime       - ISO 8601 with IST offset (string)
    underlying     - "NIFTY" or "SENSEX" (string)
    option_type    - "CE" or "PE" (string)
    expiry_type    - "WEEK" or "MONTH" (string)
    expiry_code    - 1=near, 2=next, 3=far (int8)
    atm_strike     - ATM strike = spot rounded to nearest step (float64)
    strike_offset  - numeric offset from ATM: 0, +1, -1, ... (int8)
    moneyness      - "ITM", "ATM", or "OTM" (string)
    strike         - actual strike price (float64)
    spot           - underlying spot price (float64)
    open           - option open price (float64)
    high           - option high price (float64)
    low            - option low price (float64)
    close          - option close price (float64)
    volume         - volume traded (int64)
    oi             - open interest (int64)
    iv             - implied volatility (float64)

Supports incremental updates and deduplication.

Usage:
    export DHAN_ACCESS_TOKEN='your-token'
    python fetch_options_rolling.py
"""

import os
import sys
from datetime import datetime

import pandas as pd

from config import (
    BSE_FNO,
    DHAN_BASE_URL,
    EXPIRY_CODES,
    EXPIRY_FLAGS,
    INDEX_MATCH_RULES,
    NSE_FNO,
    OPTIDX,
    OPTIONS_DIR,
    OPTIONS_START_DATE,
    STRIKE_BUCKETS,
    STRIKE_STEPS,
)
from utils import (
    IST,
    build_auth_headers,
    compute_atm_strike,
    compute_moneyness,
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


# Rolling options endpoint.
ROLLING_URL = DHAN_BASE_URL + "/charts/rollingoption"

# Dedup key for options data.
# A unique option candle is identified by these columns.
OPTIONS_DEDUP_COLS = [
    "ts", "underlying", "option_type", "expiry_type", "expiry_code", "strike",
]


def _parse_strike_offset(bucket: str) -> int:
    """
    Parse numeric strike offset from bucket string.

    "ATM" -> 0, "ATM+3" -> 3, "ATM-7" -> -7
    """
    if bucket == "ATM":
        return 0
    return int(bucket.replace("ATM", ""))


def _count_total_api_calls(n_windows: int) -> int:
    """
    Calculate total API calls for progress reporting.

    Per window: expiry_flags * expiry_codes * strike_buckets * option_types
    = 2 * 3 * 21 * 2 = 252 calls per window.
    """
    calls_per_window = (
        len(EXPIRY_FLAGS) * len(EXPIRY_CODES) * len(STRIKE_BUCKETS) * 2
    )
    return n_windows * calls_per_window


def fetch_options_for_underlying(
    name: str,
    security_id: str,
    exch_segment: str,
    strike_step: int,
    headers: dict,
    start_date,
    end_date,
) -> pd.DataFrame:
    """
    Fetch rolling options data for one underlying.

    Iterates over: date windows x expiry types x expiry codes x strikes x CE/PE.
    Returns a DataFrame with standardized columns.
    """
    # Rolling options API uses 30-day windows.
    windows = generate_date_windows(start_date, end_date, max_days=30)
    total_calls = _count_total_api_calls(len(windows))
    call_count = 0
    all_rows = []

    for w_idx, (d_from, d_to) in enumerate(windows, start=1):
        from_str = d_from.strftime("%Y-%m-%d")
        to_str = d_to.strftime("%Y-%m-%d")

        print(f"\n  [{name}] Window {w_idx}/{len(windows)}: {from_str} -> {to_str}")

        for expiry_flag in EXPIRY_FLAGS:
            for expiry_code in EXPIRY_CODES:
                for bucket in STRIKE_BUCKETS:
                    for opt_side, opt_label in (("CALL", "CE"), ("PUT", "PE")):
                        call_count += 1

                        # Show progress every 50 calls.
                        if call_count % 50 == 0:
                            print_progress(
                                call_count, total_calls,
                                f"{name} {expiry_flag} code={expiry_code} "
                                f"{bucket} {opt_label}"
                            )

                        payload = {
                            "exchangeSegment": exch_segment,
                            "interval": "1",
                            "securityId": security_id,
                            "instrument": OPTIDX,
                            "expiryFlag": expiry_flag,
                            "expiryCode": expiry_code,
                            "strike": bucket,
                            "drvOptionType": opt_side,
                            "requiredData": [
                                "open", "high", "low", "close",
                                "volume", "oi", "iv",
                                "strike", "spot",
                            ],
                            "fromDate": from_str,
                            "toDate": to_str,
                        }

                        try:
                            data = post_json(ROLLING_URL, payload, headers)
                        except RuntimeError:
                            # Skip errors for individual combos (log and continue).
                            continue

                        if not data:
                            continue

                        # Extract the CE or PE side from the response.
                        side_key = "ce" if opt_side == "CALL" else "pe"
                        side = (data.get("data") or {}).get(side_key)
                        if not side:
                            continue

                        timestamps = side.get("timestamp") or []
                        if not timestamps:
                            continue

                        opens = side.get("open") or []
                        highs = side.get("high") or []
                        lows = side.get("low") or []
                        closes = side.get("close") or []
                        vols = side.get("volume") or []
                        ois = side.get("oi") or []
                        ivs = side.get("iv") or []
                        strikes = side.get("strike") or []
                        spots = side.get("spot") or []

                        n = len(timestamps)
                        offset = _parse_strike_offset(bucket)

                        # Build rows from this API response.
                        for i in range(n):
                            ts = int(timestamps[i])

                            # Safe access for arrays that might be shorter.
                            spot_val = spots[i] if i < len(spots) else None
                            strike_val = strikes[i] if i < len(strikes) else None

                            # Compute ATM strike from spot.
                            atm = None
                            if spot_val is not None and spot_val != "":
                                spot_f = float(spot_val)
                                atm = compute_atm_strike(spot_f, strike_step)
                            else:
                                spot_f = None

                            # Compute moneyness (ITM / ATM / OTM).
                            moneyness = ""
                            if (
                                strike_val is not None
                                and strike_val != ""
                                and spot_f is not None
                                and atm is not None
                            ):
                                moneyness = compute_moneyness(
                                    opt_label, float(strike_val), spot_f, atm
                                )

                            all_rows.append({
                                "ts": ts,
                                "datetime": epoch_to_ist_iso(ts),
                                "underlying": name,
                                "option_type": opt_label,
                                "expiry_type": expiry_flag,
                                "expiry_code": expiry_code,
                                "atm_strike": float(atm) if atm else None,
                                "strike_offset": offset,
                                "moneyness": moneyness,
                                "strike": float(strike_val) if strike_val not in (None, "") else None,
                                "spot": spot_f,
                                "open": float(opens[i]) if i < len(opens) else None,
                                "high": float(highs[i]) if i < len(highs) else None,
                                "low": float(lows[i]) if i < len(lows) else None,
                                "close": float(closes[i]) if i < len(closes) else None,
                                "volume": int(vols[i]) if i < len(vols) else 0,
                                "oi": int(ois[i]) if i < len(ois) else 0,
                                "iv": float(ivs[i]) if i < len(ivs) else None,
                            })

                        throttle()

    print(f"\n  [{name}] Completed {call_count} API calls, collected {len(all_rows)} rows.")
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


def run_options_fetch() -> None:
    """
    Main entry point: fetch options data for NIFTY and SENSEX.

    Supports incremental updates.
    """
    ensure_dir(OPTIONS_DIR)

    print("Downloading Dhan instrument list...")
    rows = load_instrument_list()
    print(f"  Loaded {len(rows)} instrument rows.")

    headers = build_auth_headers()

    # Define underlyings to fetch.
    underlyings = [
        {
            "name": "NIFTY",
            "match_name": "NIFTY_50",
            "exch_segment": NSE_FNO,
            "short_name": "nifty",
        },
        {
            "name": "SENSEX",
            "match_name": "SENSEX",
            "exch_segment": BSE_FNO,
            "short_name": "sensex",
        },
    ]

    for ul in underlyings:
        name = ul["name"]
        short = ul["short_name"]
        out_dir = os.path.join(OPTIONS_DIR, short)
        out_file = os.path.join(out_dir, f"{name}_OPTIONS_1m.parquet")
        strike_step = STRIKE_STEPS[name]

        # Resolve the underlying's security ID.
        best_row = resolve_index_instrument(
            rows, ul["match_name"], INDEX_MATCH_RULES
        )
        sec_id = extract_security_id(best_row)
        print(f"  Resolved {name}: securityId={sec_id}")

        # Determine start date (incremental update).
        start = OPTIONS_START_DATE
        existing_df = load_parquet(out_file)
        last_ts = get_last_timestamp(out_file)
        if last_ts is not None:
            last_dt = datetime.fromtimestamp(last_ts, tz=IST)
            start = last_dt.date()
            print(f"  Resuming {name} options from {start} (incremental)")

        end = today_ist()
        if start >= end:
            print(f"  {name} options are already up to date.")
            continue

        print(f"  Fetching {name} options from {start} to {end}...")
        new_df = fetch_options_for_underlying(
            name=name,
            security_id=sec_id,
            exch_segment=ul["exch_segment"],
            strike_step=strike_step,
            headers=headers,
            start_date=start,
            end_date=end,
        )

        if new_df.empty:
            print(f"  No new data for {name} options.")
            continue

        # Merge with existing and deduplicate.
        final_df = merge_and_deduplicate(
            existing_df, new_df, OPTIONS_DEDUP_COLS
        )

        # Enforce column types.
        final_df["ts"] = final_df["ts"].astype("int64")
        final_df["expiry_code"] = final_df["expiry_code"].astype("int8")
        final_df["strike_offset"] = final_df["strike_offset"].astype("int8")
        final_df["volume"] = final_df["volume"].fillna(0).astype("int64")
        final_df["oi"] = final_df["oi"].fillna(0).astype("int64")

        save_parquet(final_df, out_file)

    print("Options data fetch complete.")


if __name__ == "__main__":
    try:
        run_options_fetch()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
