"""
data_ingest.py
Historical data ingestion & local caching for the SEP Market-Profile Agent.

Features
- Fetch historical bars from Schwab API (via api_client)
- Normalize to canonical columns: ts, open, high, low, close, volume
- Basic QA: sort, dedupe, fixed-interval gap check (optional)
- Save/load Parquet cache under ./data/{symbol}/{interval}/
- Simple replay generator for downstream modules

NOTE: Schwab's exact price-history endpoint/params may differ.
Adjust `build_history_endpoint()` and `parse_history_payload()` to match
your account's API shape.

Usage:
    python -m src.data_ingest --symbol NWL --interval 30m --start 2024-06-01 --end 2024-07-01
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .api_client import get as api_get


# ---------- Config ----------

# Replace this:
# DATA_ROOT = Path.cwd() / "data"

# With this (repo root = one level above src/):
DATA_ROOT = Path(__file__).resolve().parents[1] / "data"


# Map friendly intervals to (freq, seconds) for QA/gap checks
_INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "60m": 3600,
    "1h": 3600,
    "1d": 86400,
    "day": 86400,
}


@dataclass(frozen=True)
class HistoryRequest:
    symbol: str
    interval: str  # e.g., "30m", "1d"
    start_utc: datetime
    end_utc: datetime


# ---------- Endpoint builder (adjust to your Schwab account) ----------

def build_history_endpoint(req: HistoryRequest) -> Tuple[str, Dict[str, Any]]:
    """
    Build the endpoint + params for Schwab price history.
    You may need to change this to match your exact REST shape.

    Common patterns (TD/Schwab style):
      /marketdata/v1/pricehistory
      params:
        symbol, periodType, frequencyType, frequency, startDate, endDate, needExtendedHoursData

    Here we emit a generic style you can tweak in one place.
    """
    # Example mapping: translate "30m" to frequencyType="minute", frequency=30
    if req.interval.endswith("m"):
        frequency_type = "minute"
        frequency = int(req.interval.replace("m", ""))
    elif req.interval in ("60m", "1h"):
        frequency_type = "minute"
        frequency = 60
    else:
        frequency_type = "daily"
        frequency = 1

    endpoint = "marketdata/v1/pricehistory"  # adjust if your API differs
    params = {
        "symbol": req.symbol.upper(),
        "frequencyType": frequency_type,
        "frequency": frequency,
        # milliseconds since epoch (commonly accepted); change if needed
        "startDate": int(req.start_utc.timestamp() * 1000),
        "endDate": int(req.end_utc.timestamp() * 1000),
        "needExtendedHoursData": "false",
    }
    return endpoint, params

def _to_utc_ts(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, unit="ms", utc=True, errors="coerce")
    except Exception:
        return pd.to_datetime(series, utc=True, errors="coerce")
# then use: "ts": _to_utc_ts(df[ts_col]),

# ---------- Response parsing (adjust to API payload) ----------

def parse_history_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert Schwab history payload -> DataFrame with:
    columns = ['ts','open','high','low','close','volume'] ; ts is UTC datetime64[ns]

    Adjust keys if your payload differs. Many APIs return something like:
    {
      "candles": [
        {"datetime": 1717521600000, "open": 9.5, "high": 9.7, "low": 9.4, "close": 9.6, "volume": 123456},
        ...
      ]
    }
    """
    candles = payload.get("candles") or payload.get("data") or []
    if not isinstance(candles, list):
        raise ValueError("Unexpected payload format: no 'candles' list")

    df = pd.DataFrame(candles)
    if df.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    # Normalize typical field names
    # Accept alternative keys if present
    ts_col = "datetime" if "datetime" in df.columns else "time"
    vol_col = "volume" if "volume" in df.columns else "vol"

    # Build final frame
    out = pd.DataFrame(
        {
            "ts": pd.to_datetime(df[ts_col], unit="ms", utc=True, errors="coerce"),
            "open": df.get("open"),
            "high": df.get("high"),
            "low": df.get("low"),
            "close": df.get("close"),
            "volume": df.get(vol_col),
        }
    )
    return out


# ---------- QA helpers ----------

def _basic_qa(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """
    - drop rows with NaT ts
    - sort by ts
    - drop duplicate ts
    - (optional) report gaps for fixed-interval series
    """
    if df.empty:
        return df

    df = df.dropna(subset=["ts"]).copy()
    df = df.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)

    # Fixed-interval gap check (report only; we don't fill here)
    step = _INTERVAL_SECONDS.get(interval)
    if step and len(df) > 2:
        # Expected deltas in seconds
        deltas = (df["ts"].astype("int64").diff() // 1_000_000_000).fillna(0).astype(int)
        gaps = df.loc[deltas.gt(step * 1.5), "ts"]
        if not gaps.empty:
            # Simple console note — later this will route to observability
            first_gap = gaps.iloc[0]
            print(f"[data_ingest] gap(s) detected for interval={interval}; first at {first_gap}")

    return df


# ---------- IO ----------

def cache_path(symbol: str, interval: str, start_utc: datetime, end_utc: datetime) -> Path:
    folder = DATA_ROOT / symbol.upper() / interval
    folder.mkdir(parents=True, exist_ok=True)
    name = f"{symbol.upper()}_{interval}_{start_utc.date()}_{end_utc.date()}.parquet"
    return folder / name


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    if df.empty:
        print(f"[data_ingest] nothing to save: {path.name}")
        return
    # Ensure dtypes are reasonable
    df = df.astype(
        {
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "volume": "int64",
        },
        errors="ignore",
    )
    df.to_parquet(path, index=False)
    print(f"[data_ingest] saved {len(df):,} rows -> {path}")


def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    return pd.read_parquet(path)


# ---------- Public API ----------

def fetch_historical_bars(symbol: str, interval: str, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
    """
    Fetch bars from Schwab API, normalize, QA, and return a DataFrame.
    """
    req = HistoryRequest(symbol=symbol, interval=interval, start_utc=start_utc, end_utc=end_utc)
    endpoint, params = build_history_endpoint(req)
    payload = api_get(endpoint, params=params)
    df = parse_history_payload(payload)
    df = _basic_qa(df, interval)
    return df


def fetch_and_cache(symbol: str, interval: str, start_utc: datetime, end_utc: datetime) -> Path:
    """
    Convenience: fetch, QA, and save to Parquet. Returns the file path.
    """
    df = fetch_historical_bars(symbol, interval, start_utc, end_utc)
    path = cache_path(symbol, interval, start_utc, end_utc)
    save_parquet(df, path)
    return path


def replay_rows(df: pd.DataFrame) -> Iterable[Dict[str, Any]]:
    """
    Yield rows as dicts in order (no sleeping). Downstream can pace if desired.
    """
    for row in df.itertuples(index=False):
        yield {
            "ts": row.ts,
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
            "volume": int(row.volume) if pd.notna(row.volume) else 0,
        }


# ---------- CLI ----------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch & cache historical bars")
    p.add_argument("--symbol", required=True, help="Ticker symbol, e.g., NWL")
    p.add_argument("--interval", default="30m", help="e.g., 1m, 5m, 15m, 30m, 1h, 1d")
    p.add_argument("--start", required=True, help="UTC start date YYYY-MM-DD")
    p.add_argument("--end", required=True, help="UTC end date YYYY-MM-DD")
    return p.parse_args()


def _parse_date(d: str) -> datetime:
    return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    args = _parse_args()
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if end <= start:
        raise SystemExit("--end must be after --start")

    path = fetch_and_cache(args.symbol, args.interval, start, end)
    print(f"[data_ingest] done: {path}")
