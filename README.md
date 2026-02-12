# get-data

Data collection pipeline for Indian market backtesting. Fetches 1-minute OHLCV data from Dhan's API and stores it locally as Parquet files.

## What it fetches

| Dataset | Date Range | Output |
|---------|-----------|--------|
| NIFTY 50 spot | Jan 2021 - today | `data/spot/nifty/NIFTY_1m.parquet` |
| SENSEX spot | Jan 2021 - today | `data/spot/sensex/SENSEX_1m.parquet` |
| NIFTY options | Jan 2025 - today | `data/options/nifty/NIFTY_OPTIONS_1m.parquet` |
| SENSEX options | Jan 2025 - today | `data/options/sensex/SENSEX_OPTIONS_1m.parquet` |
| Nifty 100 stocks | Jan 2023 - today | `data/stocks/{SYMBOL}/{SYMBOL}_1m.parquet` |

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export DHAN_ACCESS_TOKEN='your-dhan-api-token'
```

## Usage

```bash
# Run everything
python run_all.py

# Run individual fetchers
python run_all.py spot       # ~2 min
python run_all.py options    # ~2-4 hours
python run_all.py stocks     # ~20-30 min
```

All fetchers support **incremental updates** -- re-running only fetches new data since the last run.

## Column Schema

**Spot & Stock data:** `ts`, `datetime`, `open`, `high`, `low`, `close`, `volume`

**Options data:** `ts`, `datetime`, `underlying`, `option_type`, `expiry_type`, `expiry_code`, `atm_strike`, `strike_offset`, `moneyness`, `strike`, `spot`, `open`, `high`, `low`, `close`, `volume`, `oi`, `iv`

## Reading the data

```python
import pandas as pd

# Spot data
nifty = pd.read_parquet("data/spot/nifty/NIFTY_1m.parquet")

# Options data
opts = pd.read_parquet("data/options/nifty/NIFTY_OPTIONS_1m.parquet")

# Single stock
reliance = pd.read_parquet("data/stocks/RELIANCE/RELIANCE_1m.parquet")
```
