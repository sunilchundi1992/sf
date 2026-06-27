"""
NIFTY Trading Data Collector — Upstox V3 (candles) + V2 (option chain)
- Pulls 1-min intraday candles for indices, stocks, futures   [V3]
- Pulls option chain → Max Pain, PCR, OI, Change in OI        [V2]
- Polls every 5 seconds during 09:15–15:30 IST, auto-exits after close
- Storage: SQLite3 (./db/trading_data.db)
"""

import os
import sys
import time
import sqlite3
import requests
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
load_dotenv()
TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
if not TOKEN:
    sys.exit("❌ UPSTOX_ACCESS_TOKEN missing in .env")

IST       = ZoneInfo("Asia/Kolkata")
MKT_OPEN  = dtime(9, 15)
MKT_CLOSE = dtime(15, 30)
POLL_SEC  = 5

BASE_V2 = "https://api.upstox.com/v2"
BASE_V3 = "https://api.upstox.com/v3"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

# Candle granularity (V3 uses unit + interval)
CANDLE_UNIT     = "minutes"
CANDLE_INTERVAL = "1"

DB_PATH = Path(__file__).parent / "db" / "trading_data.db"
DB_PATH.parent.mkdir(exist_ok=True)

SYMBOLS = {
    # Indices
    "NSE_INDEX|Nifty Bank":        "BANKNIFTY",
    "NSE_INDEX|Nifty 50":          "NIFTY50",
    "NSE_INDEX|India VIX":         "INDIAVIX",
    "NSE_INDEX|Nifty Fin Service": "FINNIFTY",
    # NIFTY heavyweight
    "NSE_EQ|INE002A01018":         "RELIANCE",
    # Bank stocks
    "NSE_EQ|INE040A01034":         "HDFCBANK",
    "NSE_EQ|INE090A01021":         "ICICIBANK",
    "NSE_EQ|INE062A01020":         "SBIN",
    "NSE_EQ|INE237A01036":         "KOTAKBANK",
    "NSE_EQ|INE238A01034":         "AXISBANK",
    # Futures
    "NSE_FO|62329":                "NIFTYFUT30JUN26",
    "NSE_FO|62326":                "BANKNIFTYFUT30JUN26",
}

# Option-chain instruments (spot indices)
OPTION_SYMBOLS = {
    "NSE_INDEX|Nifty 50":   "NIFTY50",
    "NSE_INDEX|Nifty Bank": "BANKNIFTY",
}

# In-memory baseline OI per (sym, expiry, strike, type) — first snapshot of the day
DAY_OI_BASELINE = {}

# ─────────────────────────────────────────────────────────────
# DB SETUP
# ─────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS intraday_candles (
        instrument_key TEXT NOT NULL,
        symbol         TEXT NOT NULL,
        datetime       TEXT NOT NULL,
        open  REAL, high REAL, low REAL, close REAL,
        volume INTEGER, oi INTEGER,
        PRIMARY KEY (instrument_key, datetime)
    );
    CREATE INDEX IF NOT EXISTS idx_candles_sym_dt
        ON intraday_candles(symbol, datetime);

    CREATE TABLE IF NOT EXISTS max_pain_pcr (
        snapshot_ts    TEXT NOT NULL,
        symbol         TEXT NOT NULL,
        expiry         TEXT NOT NULL,
        spot_price     REAL,
        max_pain       REAL,
        pcr_oi         REAL,
        pcr_volume     REAL,
        total_call_oi  INTEGER,
        total_put_oi   INTEGER,
        total_call_vol INTEGER,
        total_put_vol  INTEGER,
        PRIMARY KEY (snapshot_ts, symbol, expiry)
    );

    CREATE TABLE IF NOT EXISTS oi_coi_strikes (
        snapshot_ts  TEXT NOT NULL,
        symbol       TEXT NOT NULL,
        expiry       TEXT NOT NULL,
        strike       REAL NOT NULL,
        option_type  TEXT NOT NULL,
        ltp          REAL,
        oi           INTEGER,
        prev_oi      INTEGER,
        change_in_oi INTEGER,
        volume       INTEGER,
        iv           REAL,
        PRIMARY KEY (snapshot_ts, symbol, expiry, strike, option_type)
    );
    CREATE INDEX IF NOT EXISTS idx_oi_lookup
        ON oi_coi_strikes(symbol, expiry, strike, snapshot_ts);
    """)
    con.commit()
    return con

# ─────────────────────────────────────────────────────────────
# HTTP HELPER
# ─────────────────────────────────────────────────────────────
def get(url, params=None, timeout=10):
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
        if r.status_code != 200:
            print(f"  ⚠ HTTP {r.status_code}: {url}")
            return None
        return r.json()
    except Exception as e:
        print(f"  ⚠ Request error: {e}")
        return None

# ─────────────────────────────────────────────────────────────
# 1) INTRADAY CANDLES — V3
#    GET /v3/historical-candle/intraday/{instrument_key}/{unit}/{interval}
# ─────────────────────────────────────────────────────────────
def fetch_intraday_candles(con):
    inserted = 0
    cur = con.cursor()
    for ikey, sym in SYMBOLS.items():
        # instrument_key contains '|' which must NOT be pre-encoded; requests will encode it
        # url = f"{BASE_V3}/historical-candle/intraday/{ikey}/{CANDLE_UNIT}/{CANDLE_INTERVAL}"
        url = f"{BASE_V3}/historical-candle/{ikey}/{CANDLE_UNIT}/{CANDLE_INTERVAL}/2026-06-25/2026-06-24"
        data = get(url)
        if not data or data.get("status") != "success":
            continue
        candles = data.get("data", {}).get("candles", [])
        # V3 candle schema: [timestamp, open, high, low, close, volume, open_interest]
        rows = []
        for c in candles:
            ts = c[0]
            dt = datetime.fromisoformat(ts).astimezone(IST) \
                        .strftime("%Y-%m-%d %H:%M:%S")
            rows.append((
                ikey, sym, dt,
                c[1], c[2], c[3], c[4],
                int(c[5]) if c[5] is not None else 0,
                int(c[6]) if len(c) > 6 and c[6] is not None else 0,
            ))
        if rows:
            cur.executemany("""
                INSERT OR REPLACE INTO intraday_candles
                (instrument_key, symbol, datetime, open, high, low, close, volume, oi)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, rows)
            inserted += len(rows)
    con.commit()
    return inserted

# ─────────────────────────────────────────────────────────────
# 2) OPTION CHAIN — V2
#    GET /v2/option/contract?instrument_key=...
#    GET /v2/option/chain?instrument_key=...&expiry_date=YYYY-MM-DD
# ─────────────────────────────────────────────────────────────
def get_nearest_expiry(instrument_key):
    url = f"{BASE_V2}/option/contract"
    data = get(url, params={"instrument_key": instrument_key})
    if not data or data.get("status") != "success":
        return None
    expiries = sorted({c["expiry"][:10] for c in data.get("data", []) if c.get("expiry")})
    today = datetime.now(IST).strftime("%Y-%m-%d")
    future = [e for e in expiries if e >= today]
    return future[0] if future else None

def compute_max_pain(strikes_data, candidate_strikes):
    """
    Max Pain = strike K minimizing total writer payout at expiry.
      call_pain(K) = Σ max(K - Ki, 0) * CE_OI(Ki)
      put_pain(K)  = Σ max(Ki - K, 0) * PE_OI(Ki)
    """
    min_pain = float("inf")
    max_pain_strike = None
    for K in candidate_strikes:
        total = 0.0
        for s, leg in strikes_data.items():
            ce_oi = leg.get("CE", {}).get("oi", 0) or 0
            pe_oi = leg.get("PE", {}).get("oi", 0) or 0
            total += max(K - s, 0) * ce_oi
            total += max(s - K, 0) * pe_oi
        if total < min_pain:
            min_pain = total
            max_pain_strike = K
    return max_pain_strike

def fetch_option_chain(con, snapshot_ts):
    cur = con.cursor()
    inserted_summary = 0
    inserted_strikes = 0

    for ikey, sym in OPTION_SYMBOLS.items():
        expiry = get_nearest_expiry(ikey)
        if not expiry:
            continue

        url = f"{BASE_V2}/option/chain"
        data = get(url, params={"instrument_key": ikey, "expiry_date": expiry})
        if not data or data.get("status") != "success":
            continue

        chain = data.get("data", [])
        if not chain:
            continue

        spot = chain[0].get("underlying_spot_price")

        strikes_data = {}
        strike_rows  = []
        total_ce_oi = total_pe_oi = 0
        total_ce_vol = total_pe_vol = 0

        for row in chain:
            strike = row.get("strike_price")
            ce = row.get("call_options", {}) or {}
            pe = row.get("put_options", {}) or {}
            ce_md = ce.get("market_data", {}) or {}
            pe_md = pe.get("market_data", {}) or {}
            ce_og = ce.get("option_greeks", {}) or {}
            pe_og = pe.get("option_greeks", {}) or {}

            ce_oi  = ce_md.get("oi", 0) or 0
            pe_oi  = pe_md.get("oi", 0) or 0
            ce_vol = ce_md.get("volume", 0) or 0
            pe_vol = pe_md.get("volume", 0) or 0

            total_ce_oi  += ce_oi
            total_pe_oi  += pe_oi
            total_ce_vol += ce_vol
            total_pe_vol += pe_vol

            strikes_data[strike] = {"CE": {"oi": ce_oi}, "PE": {"oi": pe_oi}}

            for opt_type, md, og, oi_val, vol_val in [
                ("CE", ce_md, ce_og, ce_oi, ce_vol),
                ("PE", pe_md, pe_og, pe_oi, pe_vol),
            ]:
                key = (sym, expiry, strike, opt_type)
                baseline = DAY_OI_BASELINE.get(key)
                if baseline is None:
                    DAY_OI_BASELINE[key] = oi_val
                    baseline = oi_val
                strike_rows.append((
                    snapshot_ts, sym, expiry, strike, opt_type,
                    md.get("ltp"),
                    oi_val,
                    baseline,
                    oi_val - baseline,
                    vol_val,
                    og.get("iv"),
                ))

        max_pain = compute_max_pain(strikes_data, sorted(strikes_data.keys()))
        pcr_oi  = round(total_pe_oi  / total_ce_oi, 4)  if total_ce_oi  else None
        pcr_vol = round(total_pe_vol / total_ce_vol, 4) if total_ce_vol else None

        cur.execute("""
            INSERT OR REPLACE INTO max_pain_pcr
            (snapshot_ts, symbol, expiry, spot_price, max_pain,
             pcr_oi, pcr_volume, total_call_oi, total_put_oi,
             total_call_vol, total_put_vol)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (snapshot_ts, sym, expiry, spot, max_pain,
              pcr_oi, pcr_vol, total_ce_oi, total_pe_oi,
              total_ce_vol, total_pe_vol))
        inserted_summary += 1

        cur.executemany("""
            INSERT OR REPLACE INTO oi_coi_strikes
            (snapshot_ts, symbol, expiry, strike, option_type,
             ltp, oi, prev_oi, change_in_oi, volume, iv)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, strike_rows)
        inserted_strikes += len(strike_rows)

        print(f"  ✓ {sym} {expiry} | Spot={spot} | MaxPain={max_pain} "
              f"| PCR_OI={pcr_oi} | Strikes={len(chain)}")

    con.commit()
    return inserted_summary, inserted_strikes

# ─────────────────────────────────────────────────────────────
# MARKET WINDOW
# ─────────────────────────────────────────────────────────────
def in_market_hours(now):
    if now.weekday() >= 5:
        return False
    return MKT_OPEN <= now.time() < MKT_CLOSE

def seconds_until_open(now):
    open_dt = datetime.combine(now.date(), MKT_OPEN, tzinfo=IST)
    return max(0, (open_dt - now).total_seconds())

# ─────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────
def main():
    print("🚀 NIFTY Data Collector — Upstox V3 (candles) + V2 (option chain)")
    print(f"📂 DB: {DB_PATH}")
    print(f"⏱  Poll: {POLL_SEC}s | Window: 09:15–15:30 IST")
    print(f"🕯  Candles: {CANDLE_UNIT}/{CANDLE_INTERVAL}\n")

    con = init_db()
    now = datetime.now(IST)

    # if now.weekday() >= 5:
    #     sys.exit("📅 Weekend — exiting.")
    # if now.time() < MKT_OPEN:
    #     wait = seconds_until_open(now)
    #     print(f"⏳ Pre-market: sleeping {int(wait)}s until 09:15 IST...")
    #     time.sleep(wait)
    # elif now.time() >= MKT_CLOSE:
    #     sys.exit("🔔 Market already closed — exiting.")

    cycle = 0
    try:
        while True:
            now = datetime.now(IST)
            # if not in_market_hours(now):
            #     print("\n🔔 Market closed — exiting.")
            #     break

            cycle += 1
            snap_ts = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n──── Cycle #{cycle} @ {snap_ts} IST ────")

            c_rows = fetch_intraday_candles(con)
            print(f"  ✓ Candles upserted: {c_rows}")

            s_rows, k_rows = fetch_option_chain(con, snap_ts)
            print(f"  ✓ MaxPain/PCR rows: {s_rows} | Strike rows: {k_rows}")

            time.sleep(POLL_SEC)
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
    finally:
        con.close()
        print("✅ DB closed. Bye.")

if __name__ == "__main__":
    main()