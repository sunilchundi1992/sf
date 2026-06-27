"""Pull intraday Upstox data for MULTIPLE instruments into a local SQLite DB.

Three tables (all keyed per instrument so symbols never collide):
  - candles      : incremental intraday OHLCV+OI candle history (append-only)
  - option_chain : per-strike OI + change-in-OI snapshot (upsert)
  - maxpain_pcr  : per-time max-pain / pcr / spot (upsert)

Routing by instrument-key prefix:
  - NSE_FO|...                 -> candles
  - NSE_INDEX|... / NSE_EQ|... -> option_chain + maxpain_pcr (option underlyings)

Run on your PC:
    python store.py            # one pull across all symbols
    python store.py loop       # poll every INTERVAL_MINUTES until stopped
"""
from __future__ import annotations

import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# =====================================================
# CONFIG  (edit these for your machine / trading cycle)
# =====================================================
ACCESS_TOKEN_FILE = r"C:\Users\VEKSHA\Documents\Trading\Python codes\UPSTOX_FINAL\access_token.txt"

# instrument_key -> friendly symbol name. Add new rows here; the list can grow.
SYMBOLS = {
    "NSE_INDEX|Nifty Bank": "BANKNIFTY",
    "NSE_INDEX|Nifty 50": "NIFTY50",
    "NSE_EQ|INE040A01034": "HDFCBANK",
    "NSE_EQ|INE090A01021": "ICICIBANK",
    "NSE_EQ|INE062A01020": "SBIN",
    "NSE_EQ|INE237A01028": "KOTAKBANK",
    "NSE_EQ|INE238A01034": "AXISBANK",
    "NSE_FO|62329": "NIFTYFUT30JUN26",
    "NSE_FO|62326": "BANKNIFTYFUT30JUN26",
}

# Expiry used for option OI / max-pain / pcr. Override per instrument when they differ.
DEFAULT_EXPIRY = "2026-06-30"
EXPIRY_OVERRIDES: dict[str, str] = {
    # "NSE_EQ|INE040A01034": "2026-06-25",
}

INTERVAL_MINUTES = 3                   # candle bucket + poll interval
CHANGE_OI_INTERVAL = 1                 # interval param for change-oi endpoint

BASE = "https://api.upstox.com"
DB_PATH = Path(__file__).parent / "intraday_data.db"


# =====================================================
# HELPERS
# =====================================================
def _headers() -> dict:
    with open(ACCESS_TOKEN_FILE, "r") as fh:
        token = fh.read().strip()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _expiry(instrument_key: str) -> str:
    return EXPIRY_OVERRIDES.get(instrument_key, DEFAULT_EXPIRY)


def _is_candle_instrument(instrument_key: str) -> bool:
    """Tradable futures/options -> candles. Indices & equities -> option analytics."""
    return instrument_key.startswith("NSE_FO")


# =====================================================
# UPSTOX FETCHERS  (all parameterized by instrument_key)
# =====================================================
def fetch_candles(instrument_key: str) -> pd.DataFrame:
    """Intraday candles -> DataFrame[datetime, time, open, high, low, close, volume, oi]."""
    quote = requests.utils.quote(instrument_key)
    # intraday (today, up to now) -- used for live polling:
    url = f"{BASE}/v3/historical-candle/intraday/{quote}/minutes/{INTERVAL_MINUTES}"
    # historical (date range) alternative:
    # url = f"{BASE}/v3/historical-candle/{quote}/minutes/{INTERVAL_MINUTES}/<to_date>/<from_date>"
    res = requests.get(url, headers=_headers()).json()
    candles = (res.get("data") or {}).get("candles", [])
    cols = ["datetime", "time", "open", "high", "low", "close", "volume", "oi"]
    if not candles:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(
        candles,
        columns=["datetime", "open", "high", "low", "close", "volume", "oi"],
    )
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["time"] = df["datetime"].dt.strftime("%H:%M")
    df["datetime"] = df["datetime"].astype(str)
    return df[cols]


def fetch_option_chain(instrument_key: str) -> tuple[pd.DataFrame, float | None]:
    """Merge absolute OI + change-in-OI per strike (all strikes). Returns (df, spot)."""
    params = {
        "instrument_key": instrument_key,
        "expiry": _expiry(instrument_key),
        "date": _today(),
    }
    h = _headers()
    oi_res = requests.get(f"{BASE}/v2/market/oi", params=params, headers=h).json()
    chg_res = requests.get(
        f"{BASE}/v2/market/change-oi",
        params={**params, "interval": CHANGE_OI_INTERVAL},
        headers=h,
    ).json()

    cols = [
        "strike_price", "call_oi", "put_oi", "call_chg_oi", "put_chg_oi",
        "net_abs_oi", "net_chg_oi",
    ]
    if oi_res.get("status") != "success" or chg_res.get("status") != "success":
        return pd.DataFrame(columns=cols), None

    oi_data = oi_res.get("data") or {}
    chg_data = chg_res.get("data") or {}
    spot = oi_data.get("spot_closing_price")

    oi_list = oi_data.get("call_put_oi_data_list", [])
    chg_list = chg_data.get("call_put_oi_data_list", [])
    if not oi_list or not chg_list:
        return pd.DataFrame(columns=cols), spot

    df_oi = pd.DataFrame(oi_list)[["strike_price", "call_oi", "put_oi"]]
    df_chg = pd.DataFrame(chg_list).rename(
        columns={"call_change_oi": "call_chg_oi", "put_change_oi": "put_chg_oi"}
    )[["strike_price", "call_chg_oi", "put_chg_oi"]]

    df = pd.merge(df_oi, df_chg, on="strike_price", how="inner")
    df.sort_values("strike_price", inplace=True)
    df["net_abs_oi"] = df["put_oi"] - df["call_oi"]
    df["net_chg_oi"] = df["put_chg_oi"] - df["call_chg_oi"]
    return df.reset_index(drop=True), spot


def fetch_maxpain_pcr(instrument_key: str) -> tuple[float | None, float | None]:
    """Latest max-pain and pcr values (last available bucket of the day)."""
    params = {
        "instrument_key": instrument_key,
        "expiry": _expiry(instrument_key),
        "date": _today(),
        "bucket_interval": INTERVAL_MINUTES,
    }
    h = _headers()
    mp_res = requests.get(f"{BASE}/v2/market/max-pain", params=params, headers=h).json()
    pcr_res = requests.get(f"{BASE}/v2/market/pcr", params=params, headers=h).json()

    mp = (mp_res.get("data") or {}).get("insights", [])
    pcr = (pcr_res.get("data") or {}).get("insights", [])
    max_pain = mp[-1].get("max_pain") if mp else None
    pcr_val = pcr[-1].get("pcr") if pcr else None
    return max_pain, pcr_val


# =====================================================
# SQLITE STORE
# =====================================================
def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


# New-schema table definitions. init_db() creates these and migrates any
# old single-symbol tables that predate the instrument_key/symbol columns.
_TABLE_DDL = {
    "candles": """
        CREATE TABLE IF NOT EXISTS candles (
            instrument_key TEXT NOT NULL,
            symbol         TEXT,
            datetime       TEXT NOT NULL,
            time           TEXT,
            open           REAL,
            high           REAL,
            low            REAL,
            close          REAL,
            volume         REAL,
            oi             REAL,
            PRIMARY KEY (instrument_key, datetime)
        );
    """,
    "option_chain": """
        CREATE TABLE IF NOT EXISTS option_chain (
            instrument_key TEXT NOT NULL,
            symbol         TEXT,
            time           TEXT NOT NULL,
            captured_at    TEXT NOT NULL,
            strike_price   REAL NOT NULL,
            call_oi        REAL,
            put_oi         REAL,
            call_chg_oi    REAL,
            put_chg_oi     REAL,
            net_abs_oi     REAL,
            net_chg_oi     REAL,
            PRIMARY KEY (instrument_key, time, strike_price)
        );
    """,
    "maxpain_pcr": """
        CREATE TABLE IF NOT EXISTS maxpain_pcr (
            instrument_key TEXT NOT NULL,
            symbol         TEXT,
            time           TEXT NOT NULL,
            captured_at    TEXT NOT NULL,
            spot_price     REAL,
            max_pain       REAL,
            pcr            REAL,
            PRIMARY KEY (instrument_key, time)
        );
    """,
}


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]


def _migrate_table(conn: sqlite3.Connection, table: str, ddl: str) -> None:
    """Rebuild an old-schema table (missing instrument_key) into the new schema.

    Existing rows are preserved and tagged instrument_key='LEGACY', symbol='LEGACY'
    so no historical data is lost. No-op if the table is absent or already migrated.
    """
    existing = _table_columns(conn, table)
    if not existing or "instrument_key" in existing:
        return
    conn.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
    conn.execute(ddl)
    old_cols = _table_columns(conn, f"{table}_old")
    new_cols = _table_columns(conn, table)
    common = [c for c in old_cols if c in new_cols and c not in ("instrument_key", "symbol")]
    collist = ", ".join(common)
    conn.execute(
        f"INSERT INTO {table} (instrument_key, symbol, {collist}) "
        f"SELECT 'LEGACY', 'LEGACY', {collist} FROM {table}_old"
    )
    conn.execute(f"DROP TABLE {table}_old")
    print(f"[migrate] rebuilt '{table}' to multi-symbol schema (old rows tagged LEGACY)")


def init_db() -> None:
    with _connect() as conn:
        for table, ddl in _TABLE_DDL.items():
            _migrate_table(conn, table, ddl)
            conn.execute(ddl)


def append_candles(df: pd.DataFrame, instrument_key: str, symbol: str) -> int:
    """Incremental: only brand-new (instrument, datetime) rows inserted."""
    if df.empty:
        return 0
    payload = [
        (instrument_key, symbol, r.datetime, r.time, r.open, r.high,
         r.low, r.close, r.volume, r.oi)
        for r in df.itertuples(index=False)
    ]
    with _connect() as conn:
        cur = conn.executemany(
            """
            INSERT OR IGNORE INTO candles
                (instrument_key, symbol, datetime, time, open, high, low, close, volume, oi)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        return cur.rowcount


def upsert_option_chain(df: pd.DataFrame, instrument_key: str, symbol: str,
                        bucket_time: str) -> int:
    if df.empty:
        return 0
    captured = datetime.now().isoformat(timespec="seconds")
    payload = [
        (instrument_key, symbol, bucket_time, captured, r.strike_price,
         r.call_oi, r.put_oi, r.call_chg_oi, r.put_chg_oi, r.net_abs_oi, r.net_chg_oi)
        for r in df.itertuples(index=False)
    ]
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO option_chain
                (instrument_key, symbol, time, captured_at, strike_price,
                 call_oi, put_oi, call_chg_oi, put_chg_oi, net_abs_oi, net_chg_oi)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_key, time, strike_price) DO UPDATE SET
                captured_at = excluded.captured_at,
                call_oi     = excluded.call_oi,
                put_oi      = excluded.put_oi,
                call_chg_oi = excluded.call_chg_oi,
                put_chg_oi  = excluded.put_chg_oi,
                net_abs_oi  = excluded.net_abs_oi,
                net_chg_oi  = excluded.net_chg_oi
            """,
            payload,
        )
    return len(payload)


def upsert_maxpain_pcr(instrument_key: str, symbol: str, bucket_time: str,
                       spot, max_pain, pcr) -> int:
    captured = datetime.now().isoformat(timespec="seconds")
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO maxpain_pcr
                (instrument_key, symbol, time, captured_at, spot_price, max_pain, pcr)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_key, time) DO UPDATE SET
                captured_at = excluded.captured_at,
                spot_price  = excluded.spot_price,
                max_pain    = excluded.max_pain,
                pcr         = excluded.pcr
            """,
            (instrument_key, symbol, bucket_time, captured, spot, max_pain, pcr),
        )
    return 1


# =====================================================
# PULL ORCHESTRATION
# =====================================================
def pull_symbol(instrument_key: str, symbol: str) -> dict:
    """Pull whatever applies to this instrument and store it."""
    bucket_time = datetime.now().strftime("%H:%M")
    out = {"symbol": symbol, "type": "candle" if _is_candle_instrument(instrument_key) else "option"}

    if _is_candle_instrument(instrument_key):
        candles = fetch_candles(instrument_key)
        out["candles_added"] = append_candles(candles, instrument_key, symbol)
    else:
        chain, spot = fetch_option_chain(instrument_key)
        max_pain, pcr = fetch_maxpain_pcr(instrument_key)
        out["chain_strikes"] = upsert_option_chain(chain, instrument_key, symbol, bucket_time)
        upsert_maxpain_pcr(instrument_key, symbol, bucket_time, spot, max_pain, pcr)
        out.update({"spot": spot, "max_pain": max_pain, "pcr": pcr})
    return out


def pull_once() -> list[dict]:
    init_db()
    results = []
    for instrument_key, symbol in SYMBOLS.items():
        try:
            res = pull_symbol(instrument_key, symbol)
        except Exception as exc:  # one bad symbol must not stop the rest
            res = {"symbol": symbol, "error": str(exc)}
        results.append(res)
        print(f"[{datetime.now():%H:%M:%S}] {res}")
    return results


def loop() -> None:
    print(f"Polling {len(SYMBOLS)} symbols every {INTERVAL_MINUTES} min. Ctrl+C to stop.")
    try:
        while True:
            try:
                pull_once()
            except Exception as exc:  # keep polling through transient errors
                print(f"[error] {exc}")
            time.sleep(INTERVAL_MINUTES * 60)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "loop":
        loop()
    else:
        pull_once()
