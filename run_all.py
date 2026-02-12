#!/usr/bin/env python3

"""
Run all data fetchers in sequence.

This is the main entry point for the data collection pipeline.
It runs:
    1. Spot index data (NIFTY + SENSEX, 5 years, ~2 min)
    2. Options data (NIFTY + SENSEX, 1 year, ~2-4 hours)
    3. Nifty 100 stocks (3 years, ~20-30 min)

Each fetcher supports incremental updates, so re-running is safe
and will only fetch new data since the last run.

Usage:
    export DHAN_ACCESS_TOKEN='your-token'
    python run_all.py              # Run all fetchers
    python run_all.py spot         # Run only spot fetcher
    python run_all.py options      # Run only options fetcher
    python run_all.py stocks       # Run only stocks fetcher
"""

import sys
import time


def run_spot():
    """Run the spot index data fetcher."""
    print("=" * 60)
    print("STEP 1: Fetching spot index data (NIFTY + SENSEX)")
    print("=" * 60)
    from fetch_index_intraday import run_spot_fetch
    run_spot_fetch()


def run_options():
    """Run the options data fetcher."""
    print("=" * 60)
    print("STEP 2: Fetching options data (NIFTY + SENSEX)")
    print("  This takes a long time (~2-4 hours) due to many API calls.")
    print("=" * 60)
    from fetch_options_rolling import run_options_fetch
    run_options_fetch()


def run_stocks():
    """Run the Nifty 100 stocks data fetcher."""
    print("=" * 60)
    print("STEP 3: Fetching Nifty 100 stocks data")
    print("=" * 60)
    from fetch_stocks import run_stocks_fetch
    run_stocks_fetch()


def main():
    """
    Run all fetchers, or a specific one if given as argument.

    Accepted arguments: spot, options, stocks.
    No argument = run all three in order.
    """
    start = time.time()

    # Parse which fetchers to run from command-line args.
    args = [a.lower() for a in sys.argv[1:]]
    run_all = len(args) == 0

    if run_all or "spot" in args:
        run_spot()

    if run_all or "options" in args:
        run_options()

    if run_all or "stocks" in args:
        run_stocks()

    elapsed = time.time() - start
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print()
    print("=" * 60)
    print(f"ALL DONE. Total time: {minutes}m {seconds}s")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
