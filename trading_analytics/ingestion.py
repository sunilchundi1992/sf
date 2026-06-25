"""Data ingestion: Upstox fetchers + master builder + synthetic seed.

Refactored from the original standalone scripts:
  - get_intraday_maxpain_pcr.py  -> fetch_maxpain_pcr_candles()
  - get_OI_data.py               -> fetch_oi()
  - get_change_OI_data.py        -> fetch_change_oi()

The master builder merges the time-series on the `time` column and upserts
into SQLite. A synthetic generator lets the whole app run before a live
Upstox token is wired (USE_SYNTHETIC_DATA=true).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import requests

from config import settings
from storage import upsert_option_chain, upsert_snapshots


# ====================================================================
# Upstox client
# ====================================================================
def _read_token() -> str:
    with open(settings.upstox_token_file, "r") as fh:
        return fh.read().strip()


def _headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {_read_token()}",
    }


def _get(path: str, params: dict[str, Any] | None = None) -> dict:
    url = f"{settings.upstox_base}{path}"
    resp = requests.get(url, params=params, headers=_headers(), timeout=15)
    resp.raise_for_status()
    return resp.json()


# ====================================================================
# Fetchers (return DataFrames)
# ====================================================================
def fetch_maxpain_pcr_candles() -> pd.DataFrame:
    """Master time-series: time, max_pain, pcr, ohlc, volume, oi, spot_price."""
    common = {
        "instrument_key": settings.instrument_key,
        "expiry": settings.expiry_date,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "bucket_interval": settings.interval_minutes,
    }
    mp = _get("/v2/market/max-pain", common)
    pcr = _get("/v2/market/pcr", common)

    df_mp = pd.DataFrame(mp["data"]["insights"])
    df_pcr = pd.DataFrame(pcr["data"]["insights"])
    df_pcr.drop(columns=["spot_price"], inplace=True, errors="ignore")
    ts = pd.merge(df_mp, df_pcr, on="time", how="outer")[["time", "max_pain", "pcr"]]

    quote = requests.utils.quote(settings.candle_instrument_key)
    candle_url = (
        f"/v3/historical-candle/intraday/{quote}/minutes/"
        f"{settings.interval_minutes}"
    )
    candle = _get(candle_url)
    candles = candle["data"]["candles"]
    df_c = pd.DataFrame(
        candles,
        columns=["datetime", "open", "high", "low", "close", "volume", "oi"],
    )
    df_c["datetime"] = pd.to_datetime(df_c["datetime"])
    df_c["time"] = df_c["datetime"].dt.strftime("%H:%M")
    df_c = df_c[["time", "open", "high", "low", "close", "volume", "oi"]]

    df = pd.merge(ts, df_c, on="time", how="left")
    df["spot_price"] = df["close"]
    return df.sort_values("time").reset_index(drop=True)


def _fetch_chain_raw() -> tuple[pd.DataFrame, float | None]:
    """Strike-level absolute + change OI merged on strike_price."""
    today = datetime.now().strftime("%Y-%m-%d")
    oi = _get(
        "/v2/market/oi",
        {"instrument_key": settings.instrument_key,
         "expiry": settings.expiry_date, "date": today},
    )
    chg = _get(
        "/v2/market/change-oi",
        {"instrument_key": settings.instrument_key,
         "expiry": settings.expiry_date, "date": today, "interval": 2},
    )
    oi_data = oi.get("data") or {}
    chg_data = chg.get("data") or {}
    spot = oi_data.get("spot_closing_price")

    df_oi = pd.DataFrame(oi_data.get("call_put_oi_data_list", []))
    df_chg = pd.DataFrame(chg_data.get("call_put_oi_data_list", []))
    if df_oi.empty:
        return pd.DataFrame(), spot

    df_oi = df_oi[["strike_price", "call_oi", "put_oi"]]
    if not df_chg.empty:
        df_chg = df_chg.rename(
            columns={"call_change_oi": "call_chg_oi", "put_change_oi": "put_chg_oi"}
        )[["strike_price", "call_chg_oi", "put_chg_oi"]]
        merged = pd.merge(df_oi, df_chg, on="strike_price", how="left")
    else:
        merged = df_oi.assign(call_chg_oi=0.0, put_chg_oi=0.0)

    lo, hi = settings.strike_lower, settings.strike_upper
    merged = merged[
        (merged["strike_price"] >= lo) & (merged["strike_price"] <= hi)
    ].sort_values("strike_price").reset_index(drop=True)
    return merged, spot


# ====================================================================
# Master builder: fetch -> persist
# ====================================================================
def ingest_live() -> dict[str, int]:
    ts = fetch_maxpain_pcr_candles()
    chain, _ = _fetch_chain_raw()

    chain_total_chg = float(
        (chain["call_chg_oi"].fillna(0) + chain["put_chg_oi"].fillna(0)).sum()
    ) if not chain.empty else 0.0

    snap_rows = []
    for _, r in ts.iterrows():
        snap_rows.append(
            dict(
                time=r["time"],
                spot_price=_f(r.get("spot_price")),
                open=_f(r.get("open")),
                high=_f(r.get("high")),
                low=_f(r.get("low")),
                close=_f(r.get("close")),
                volume=_f(r.get("volume")),
                oi=_f(r.get("oi")),
                change_oi=chain_total_chg,
                pcr=_f(r.get("pcr")),
                max_pain=_f(r.get("max_pain")),
            )
        )
    n_snap = upsert_snapshots(snap_rows)

    n_chain = 0
    if not chain.empty:
        latest_time = ts["time"].max() if not ts.empty else datetime.now().strftime("%H:%M")
        chain_rows = [
            dict(
                time=latest_time,
                strike_price=_f(c["strike_price"]),
                call_oi=_f(c.get("call_oi")),
                put_oi=_f(c.get("put_oi")),
                call_chg_oi=_f(c.get("call_chg_oi")),
                put_chg_oi=_f(c.get("put_chg_oi")),
            )
            for _, c in chain.iterrows()
        ]
        n_chain = upsert_option_chain(chain_rows)

    return {"snapshots": n_snap, "option_chain": n_chain}


def _f(v) -> float | None:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ====================================================================
# Synthetic seed (realistic intraday session) for offline/dev use
# ====================================================================
def seed_synthetic(session_minutes: int = 375) -> dict[str, int]:
    """Generate one realistic NIFTY-like intraday session into the DB.

    375 min = 09:15 -> 15:30. Buckets at INTERVAL_MINUTES.
    """
    rng = np.random.default_rng(42)
    step = settings.interval_minutes
    start = datetime.strptime("09:15", "%H:%M")
    n = session_minutes // step

    base = 24300.0
    # Correlated random walk for price; OI ramps; PCR mean-reverting.
    price = base + np.cumsum(rng.normal(0, 12, n))
    oi = 9_000_000 + np.cumsum(rng.normal(15_000, 40_000, n)).clip(min=-2e6)
    pcr = np.clip(0.9 + np.cumsum(rng.normal(0, 0.015, n)), 0.5, 1.8)
    base_vol = 120_000

    snap_rows = []
    prev_close = base
    for i in range(n):
        t = (start + timedelta(minutes=i * step)).strftime("%H:%M")
        close = float(price[i])
        op = prev_close
        hi = max(op, close) + abs(rng.normal(0, 6))
        lo = min(op, close) - abs(rng.normal(0, 6))
        vol = float(max(1000, base_vol * (1 + rng.normal(0, 0.4))))
        chg_oi = float(rng.normal(20_000, 60_000))
        # max pain drifts slowly toward a round strike near price
        mp = round((base + (close - base) * 0.4) / 100) * 100
        snap_rows.append(
            dict(
                time=t, spot_price=close, open=op, high=hi, low=lo, close=close,
                volume=vol, oi=float(oi[i]), change_oi=chg_oi,
                pcr=float(pcr[i]), max_pain=float(mp),
            )
        )
        prev_close = close

    n_snap = upsert_snapshots(snap_rows)

    # Build a plausible option chain around the final price.
    final_price = snap_rows[-1]["close"]
    latest_time = snap_rows[-1]["time"]
    strikes = np.arange(settings.strike_lower, settings.strike_upper + 1, 100)
    chain_rows = []
    for k in strikes:
        dist = abs(k - final_price)
        # OI peaks away from spot (walls); calls heavier above, puts below.
        call_oi = max(0.0, rng.normal(2.5e6, 5e5) * np.exp(-((k - (final_price + 300)) ** 2) / (2 * 250 ** 2)))
        put_oi = max(0.0, rng.normal(2.5e6, 5e5) * np.exp(-((k - (final_price - 300)) ** 2) / (2 * 250 ** 2)))
        chain_rows.append(
            dict(
                time=latest_time, strike_price=float(k),
                call_oi=float(call_oi), put_oi=float(put_oi),
                call_chg_oi=float(rng.normal(0, 1.2e5)),
                put_chg_oi=float(rng.normal(0, 1.2e5)),
            )
        )
    n_chain = upsert_option_chain(chain_rows)
    return {"snapshots": n_snap, "option_chain": n_chain}


def ingest() -> dict[str, int]:
    """Entry point used by the scheduler: picks live or synthetic."""
    if settings.use_synthetic_data:
        return seed_synthetic()
    return ingest_live()
