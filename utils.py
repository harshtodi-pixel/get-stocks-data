"""
Shared utilities for the data collection pipeline.

Contains all reusable functions:
- IST timezone handling
- Directory creation
- Dhan API authentication and HTTP helpers
- Instrument list loading and resolution
- Parquet read/write with incremental update support
- Expiry date derivation (calendar logic)
- Moneyness computation
"""

import csv
import io
import json
import os
import ssl
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import API_CALL_DELAY, DHAN_BASE_URL, INSTRUMENT_LIST_URL


# ---------------------------------------------------------------------------
# Timezone helpers
# ---------------------------------------------------------------------------

# IST is UTC+5:30. We use a fixed offset so it works everywhere.
IST = timezone(timedelta(hours=5, minutes=30))


def now_ist() -> datetime:
    """Return current datetime in IST."""
    return datetime.now(IST)


def today_ist() -> date:
    """Return today's date in IST."""
    return now_ist().date()


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def ensure_dir(path: str) -> None:
    """Create directory (and parents) if it does not exist."""
    os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# Dhan API authentication
# ---------------------------------------------------------------------------

def build_auth_headers() -> Dict[str, str]:
    """
    Build HTTP headers with the Dhan access token.

    Token must be in the DHAN_ACCESS_TOKEN environment variable.
    """
    token = os.getenv("DHAN_ACCESS_TOKEN")
    if not token:
        raise RuntimeError(
            "DHAN_ACCESS_TOKEN is not set. "
            "Export it before running: export DHAN_ACCESS_TOKEN='your-token'"
        )
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": token,
    }


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def post_json(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 120,
) -> Dict[str, Any]:
    """
    POST JSON to Dhan API and return decoded JSON response.

    Handles Dhan's structured error responses:
    - DH-905 / DH-907 with "no data" -> returns empty dict (not fatal).

    NOTE: SSL verification is disabled due to cert issues in user's env.
    In production, fix system certificates instead.
    """
    body = json.dumps(payload).encode("utf-8")
    req_headers = {
        "Accept": headers.get("Accept", "application/json"),
        "Content-Type": headers.get("Content-Type", "application/json"),
        "access-token": headers["access-token"],
    }

    ctx = ssl._create_unverified_context()
    req = urlrequest.Request(url, data=body, headers=req_headers, method="POST")

    try:
        with urlrequest.urlopen(req, timeout=timeout, context=ctx) as resp:
            resp_bytes = resp.read()
    except HTTPError as e:
        # Read error body for debugging.
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = ""

        try:
            parsed = json.loads(err_body) if err_body else {}
        except Exception:
            parsed = {}

        error_code = str(parsed.get("errorCode") or "")
        error_message = str(parsed.get("errorMessage") or "").lower()

        # "No data" is not fatal (holidays, weekends, illiquid strikes).
        if error_code in ("DH-905", "DH-907") and "no data" in error_message:
            return {}

        raise RuntimeError(
            f"HTTP {e.code} from Dhan. Body={err_body.strip()}"
        ) from e
    except URLError as e:
        raise RuntimeError(f"Network error calling Dhan: {e}") from e

    try:
        return json.loads(resp_bytes.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to decode JSON from Dhan: {e}") from e


def throttle() -> None:
    """Sleep briefly between API calls to respect rate limits."""
    time.sleep(API_CALL_DELAY)


# ---------------------------------------------------------------------------
# Instrument list loading
# ---------------------------------------------------------------------------

def load_instrument_list(url: str = INSTRUMENT_LIST_URL) -> List[Dict[str, str]]:
    """
    Download and parse Dhan's detailed instrument list CSV.

    Returns a list of dicts, one per instrument row.
    Keys are stripped of whitespace.
    """
    ctx = ssl._create_unverified_context()
    try:
        with urlrequest.urlopen(url, timeout=120, context=ctx) as resp:
            text_stream = io.TextIOWrapper(resp, encoding="utf-8")
            reader = csv.DictReader(text_stream)
            rows = []
            for row in reader:
                cleaned = {
                    (k.strip() if isinstance(k, str) else k): (
                        v.strip() if isinstance(v, str) else v
                    )
                    for k, v in row.items()
                }
                rows.append(cleaned)
            return rows
    except (HTTPError, URLError) as e:
        raise RuntimeError(f"Failed to download instrument list: {e}") from e


# ---------------------------------------------------------------------------
# Index instrument resolution
# ---------------------------------------------------------------------------

def _matches_keywords(text: str, includes: List[str], excludes: List[str]) -> bool:
    """Check if text matches include keywords and none of the exclude keywords."""
    upper = text.upper()
    if includes and not any(inc.upper() in upper for inc in includes):
        return False
    if any(exc.upper() in upper for exc in excludes):
        return False
    return True


def resolve_index_instrument(
    rows: Iterable[Dict[str, str]],
    name: str,
    match_rules: Dict[str, Any],
) -> Dict[str, str]:
    """
    Find the best matching INDEX instrument row for a given index name.

    Returns the raw instrument dict with the highest match score.
    """
    rules = match_rules[name]
    preferred = rules["preferred_keywords"]
    fallback = rules["fallback_keywords"]
    excludes = rules["exclude_keywords"]

    # Preferred exchange for each index.
    preferred_exch = "NSE" if name == "NIFTY_50" else "BSE"

    candidates: List[Tuple[Dict[str, str], int]] = []

    for row in rows:
        instr_type = (
            row.get("INSTRUMENT_TYPE") or row.get("InstrumentType") or ""
        ).upper()
        if instr_type != "INDEX":
            continue

        symbol = row.get("SYMBOL_NAME") or row.get("SymbolName") or ""
        display = row.get("DISPLAY_NAME") or row.get("DisplayName") or ""
        combined = f"{symbol} {display}".strip()
        if not combined:
            continue

        # Score: 2 for preferred match, 1 for fallback, +1 for right exchange.
        score = -1
        if _matches_keywords(combined, preferred, excludes):
            score = 2
        elif fallback and _matches_keywords(combined, fallback, excludes):
            score = 1
        else:
            continue

        exch_id = (row.get("EXCH_ID") or row.get("ExchangeId") or "").upper()
        if exch_id == preferred_exch:
            score += 1

        candidates.append((row, score))

    if not candidates:
        raise RuntimeError(f"Could not find index instrument for {name}.")

    candidates.sort(key=lambda t: t[1], reverse=True)
    return candidates[0][0]


def extract_security_id(row: Dict[str, str]) -> str:
    """Extract the security ID from an instrument row."""
    sid = (
        row.get("SEM_SMST_SECURITY_ID")
        or row.get("SecurityID")
        or row.get("SECURITY_ID")
    )
    if not sid:
        raise RuntimeError("Missing security ID in instrument row.")
    return str(sid)


# ---------------------------------------------------------------------------
# Date windowing
# ---------------------------------------------------------------------------

def generate_date_windows(
    start: date,
    end: date,
    max_days: int = 90,
) -> List[Tuple[date, date]]:
    """
    Split [start, end) into (from_date, to_date) windows of up to max_days.

    Dhan recommends <= 90 days for intraday, <= 30 for rolling options.
    """
    windows: List[Tuple[date, date]] = []
    cur = start
    delta = timedelta(days=max_days)
    while cur < end:
        nxt = min(cur + delta, end)
        windows.append((cur, nxt))
        cur = nxt
    return windows


# ---------------------------------------------------------------------------
# Parquet I/O with incremental update support
# ---------------------------------------------------------------------------

def save_parquet(df: pd.DataFrame, filepath: str) -> None:
    """
    Save a DataFrame to Parquet with Snappy compression.

    Creates parent directories if needed.
    """
    ensure_dir(os.path.dirname(filepath))
    df.to_parquet(filepath, engine="pyarrow", compression="snappy", index=False)
    print(f"  Saved {len(df)} rows to {filepath}")


def load_parquet(filepath: str) -> Optional[pd.DataFrame]:
    """
    Load a Parquet file if it exists. Returns None if file doesn't exist.
    """
    if not os.path.exists(filepath):
        return None
    return pd.read_parquet(filepath, engine="pyarrow")


def get_last_timestamp(filepath: str) -> Optional[int]:
    """
    Read the last 'ts' value from an existing Parquet file.

    Returns None if the file doesn't exist or is empty.
    Used for incremental updates: resume fetching from this point.
    """
    df = load_parquet(filepath)
    if df is None or df.empty:
        return None
    return int(df["ts"].max())


def merge_and_deduplicate(
    existing_df: Optional[pd.DataFrame],
    new_df: pd.DataFrame,
    dedup_columns: List[str],
) -> pd.DataFrame:
    """
    Merge new data with existing data and remove duplicates.

    Keeps the last occurrence (new data wins over old).
    Sorts by 'ts' ascending.
    """
    if existing_df is not None and not existing_df.empty:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined = new_df

    # Drop duplicates, keeping last (new data preferred).
    combined = combined.drop_duplicates(subset=dedup_columns, keep="last")

    # Sort by timestamp.
    if "ts" in combined.columns:
        combined = combined.sort_values("ts").reset_index(drop=True)

    return combined


# ---------------------------------------------------------------------------
# Datetime formatting
# ---------------------------------------------------------------------------

def epoch_to_ist_iso(epoch_seconds: int) -> str:
    """
    Convert epoch seconds to ISO 8601 string with IST offset.

    Example: 1609472700 -> "2021-01-01T09:15:00+05:30"
    """
    dt = datetime.fromtimestamp(epoch_seconds, tz=IST)
    return dt.isoformat()


# ---------------------------------------------------------------------------
# Moneyness computation
# ---------------------------------------------------------------------------

def compute_moneyness(
    option_type: str,
    strike: float,
    spot: float,
    atm_strike: float,
) -> str:
    """
    Compute moneyness category: ITM, ATM, or OTM.

    Args:
        option_type: "CE" (call) or "PE" (put).
        strike: The option's strike price.
        spot: The current spot price of the underlying.
        atm_strike: The at-the-money strike (spot rounded to nearest step).

    Returns:
        "ATM" if strike == atm_strike.
        For CALL: "ITM" if strike < spot, "OTM" if strike > spot.
        For PUT:  "ITM" if strike > spot, "OTM" if strike < spot.
    """
    # ATM check: if this is the ATM strike bucket.
    if abs(strike - atm_strike) < 0.01:
        return "ATM"

    if option_type == "CE":
        return "ITM" if strike < spot else "OTM"
    else:  # PE
        return "ITM" if strike > spot else "OTM"


# ---------------------------------------------------------------------------
# ATM strike computation
# ---------------------------------------------------------------------------

def compute_atm_strike(spot: float, strike_step: int) -> int:
    """
    Round spot price to the nearest strike step to get ATM strike.

    NIFTY (step=50):   spot 14013.3 -> 14000
    SENSEX (step=100): spot 48237.5 -> 48200
    """
    return int(round(spot / strike_step) * strike_step)


# ---------------------------------------------------------------------------
# Progress display
# ---------------------------------------------------------------------------

def print_progress(current: int, total: int, prefix: str = "") -> None:
    """Print a simple progress indicator: [current/total] prefix."""
    pct = (current / total * 100) if total > 0 else 0
    print(f"  [{current}/{total}] ({pct:.0f}%) {prefix}")
