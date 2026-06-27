"""
Backfill historical EOD option data (MaxPain, PCR, OI, COI) for past trading days.
Uses Upstox V2 Market Information APIs with date parameter.

Usage:
    py backfill_historical_options.py                       # all dates with intraday data
    py backfill_historical_options.py 2026-05-25 2026-06-25 # date range
    py backfill_historical_options.py 2026-06-25            # single date
    py backfill_historical_options.py --force               # overwrite existing
"""

import os
import sys
import time
import sqlite3
import requests
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
load_dotenv()
TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
if not TOKEN:
    sys.exit("❌ UPSTOX_ACCESS_TOKEN missing in .env")

IST     = ZoneInfo("Asia/Kolkata")
BASE_V2 = "https://api.upstox.com/v2"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}
HTTP_TIMEOUT = 8

BUCKET_INTERVAL   = 1
COI_INTERVAL_DAYS = 1

DB_PATH = Path(__file__).parent / "db" / "trading_data.db"

OPTION_SYMBOLS = {
    "NSE_INDEX|Nifty 50":   "NIFTY50",
    "NSE_INDEX|Nifty Bank": "BANKNIFTY",
}

FORCE = "--force" in sys.argv

# Cache: instrument_key -> sorted list of all expiries (fetched once)
EXPIRY_LIST_CACHE = {}

# ─────────────────────────────────────────────────────────────
# HTTP session
# ─────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
retry = Retry(total=2, backoff_factor=0.3,
              status_forcelist=(429, 500, 502, 503, 504),
              allowed_methods=frozenset(["GET"]))
SESSION.mount("https://", HTTPAdapter(pool_connections=8, pool_maxsize=8,
                                      max_retries=retry))

def get(url, params=None):
    try:
        r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 429:
            time.sleep(1.5)
            r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        return r.json(), None
    except Exception as e:
        return None, str(e)

def now_ist_str():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────
# Expiry resolution for a historical date
# ─────────────────────────────────────────────────────────────
def get_all_expiries(instrument_key):
    """Fetch & cache full expiry list (single call per symbol)."""
    if instrument_key in EXPIRY_LIST_CACHE:
        return EXPIRY_LIST_CACHE[instrument_key]
    data, err = get(f"{BASE_V2}/option/contract",
                    params={"instrument_key": instrument_key})
    if not data or data.get("status") != "success":
        return []
    expiries = sorted({c["expiry"][:10] for c in data.get("data", [])
                       if c.get("expiry")})
    EXPIRY_LIST_CACHE[instrument_key] = expiries
    return expiries

def get_nearest_expiry_for_date(instrument_key, target_date):
    """The expiry that was nearest as of target_date (i.e., the first expiry >= target_date)."""
    expiries = get_all_expiries(instrument_key)
    future = [e for e in expiries if e >= target_date]
    return future[0] if future else None

# ─────────────────────────────────────────────────────────────
# Backfill one date × one symbol
# ─────────────────────────────────────────────────────────────
def backfill_one(con, ikey, sym, target_date):
    cur = con.cursor()
    ingested_at = now_ist_str()
    result = {'date': target_date, 'symbol': sym, 'mp_pcr': 0, 'oi_coi': 0,
              'expiry': None, 'errors': []}

    expiry = get_nearest_expiry_for_date(ikey, target_date)
    if not expiry:
        result['errors'].append("no_expiry_found")
        return result
    result['expiry'] = expiry

    # ── 1) MP + PCR ──
    common = {"instrument_key": ikey, "expiry": expiry,
              "date": target_date, "bucket_interval": BUCKET_INTERVAL}
    mp, mp_err = get(f"{BASE_V2}/market/max-pain", params=common)
    pc, pc_err = get(f"{BASE_V2}/market/pcr",      params=common)

    mp_data = (mp or {}).get("data", {}) or {}
    pc_data = (pc or {}).get("data", {}) or {}
    mp_ins  = mp_data.get("insights", []) or []
    pc_ins  = pc_data.get("insights", []) or []
    last_mp = mp_ins[-1] if mp_ins else {}
    last_pc = pc_ins[-1] if pc_ins else {}

    eod_spot     = mp_data.get("spot_closing_price") or last_mp.get("spot_price")
    eod_max_pain = mp_data.get("max_pain")           or last_mp.get("max_pain")
    eod_pcr      = last_pc.get("pcr")
    last_bucket  = last_mp.get("time") or last_pc.get("time")

    if eod_max_pain is not None or eod_pcr is not None:
        cur.execute("""
            INSERT OR REPLACE INTO daily_max_pain_pcr
            (date, symbol, expiry, eod_spot, eod_max_pain, eod_pcr,
             last_bucket, snapshot_ts, ingested_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (target_date, sym, expiry, eod_spot, eod_max_pain, eod_pcr,
              last_bucket, ingested_at, ingested_at))
        result['mp_pcr'] = 1
    else:
        if mp_err: result['errors'].append(f"mp:{mp_err}")
        if pc_err: result['errors'].append(f"pc:{pc_err}")

    # ── 2) OI + COI ──
    oi, oi_err  = get(f"{BASE_V2}/market/oi", params={
        "instrument_key": ikey, "expiry": expiry, "date": target_date})
    coi, coi_err = get(f"{BASE_V2}/market/change-oi", params={
        "instrument_key": ikey, "expiry": expiry, "date": target_date,
        "interval": COI_INTERVAL_DAYS})

    oi_list  = ((oi  or {}).get("data", {}) or {}).get("call_put_oi_data_list",  []) or []
    coi_list = ((coi or {}).get("data", {}) or {}).get("call_put_oi_data_list", []) or []

    merged = {}
    for row in oi_list:
        k = row.get("strike_price")
        if k is None: continue
        merged.setdefault(k, {})
        merged[k]["call_oi"] = row.get("call_oi")
        merged[k]["put_oi"]  = row.get("put_oi")
    for row in coi_list:
        k = row.get("strike_price")
        if k is None: continue
        merged.setdefault(k, {})
        merged[k]["call_change_oi"] = row.get("call_change_oi")
        merged[k]["put_change_oi"]  = row.get("put_change_oi")

    rows = [(target_date, sym, expiry, k,
             v.get("call_oi"), v.get("put_oi"),
             v.get("call_change_oi"), v.get("put_change_oi"),
             COI_INTERVAL_DAYS, ingested_at, ingested_at)
            for k, v in merged.items()]
    if rows:
        cur.executemany("""
            INSERT OR REPLACE INTO daily_oi_coi
            (date, symbol, expiry, strike,
             eod_call_oi, eod_put_oi, eod_call_coi, eod_put_coi,
             interval_days, snapshot_ts, ingested_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        result['oi_coi'] = len(rows)
    else:
        if oi_err:  result['errors'].append(f"oi:{oi_err}")
        if coi_err: result['errors'].append(f"coi:{coi_err}")

    con.commit()
    return result

# ─────────────────────────────────────────────────────────────
# Pre-check: does data already exist?
# ─────────────────────────────────────────────────────────────
def already_have(con, date, sym):
    r1 = con.execute("SELECT COUNT(*) FROM daily_max_pain_pcr "
                     "WHERE date=? AND symbol=?", (date, sym)).fetchone()[0]
    r2 = con.execute("SELECT COUNT(*) FROM daily_oi_coi "
                     "WHERE date=? AND symbol=?", (date, sym)).fetchone()[0]
    return r1 > 0 and r2 > 0

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    if not DB_PATH.exists():
        sys.exit(f"❌ DB not found: {DB_PATH}")

    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    con = sqlite3.connect(DB_PATH)

    # Determine date list
    if len(args) == 0:
        dates = [r[0] for r in con.execute("""
            SELECT DISTINCT substr(datetime, 1, 10) AS d
            FROM intraday_candles
            WHERE symbol = 'NIFTY50'
            ORDER BY d
        """).fetchall()]
    elif len(args) == 1:
        dates = [args[0]]
    else:
        # Date range
        d0 = datetime.strptime(args[0], "%Y-%m-%d").date()
        d1 = datetime.strptime(args[1], "%Y-%m-%d").date()
        dates = []
        d = d0
        while d <= d1:
            if d.weekday() < 5:                # exclude weekends
                dates.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)

    if not dates:
        sys.exit("❌ No dates resolved.")

    print(f"\n{'═'*78}")
    print(f"  📥 HISTORICAL OPTION DATA BACKFILL")
    print(f"  Symbols : {list(OPTION_SYMBOLS.values())}")
    print(f"  Dates   : {dates[0]} → {dates[-1]} ({len(dates)} dates)")
    print(f"  Force   : {FORCE}")
    print(f"{'═'*78}")

    print(f"\n  {'Date':<12} {'Symbol':<10} {'Expiry':<12} "
          f"{'MP/PCR':>7} {'OI rows':>8}  Notes")
    print(f"  {'─'*12} {'─'*10} {'─'*12} {'─'*7} {'─'*8}  {'─'*30}")

    total_success = 0
    total_skip    = 0
    total_fail    = 0

    for d in dates:
        for ikey, sym in OPTION_SYMBOLS.items():
            if not FORCE and already_have(con, d, sym):
                total_skip += 1
                print(f"  {d:<12} {sym:<10} {'—':<12} "
                      f"{'—':>7} {'—':>8}  already exists (use --force)")
                continue
            r = backfill_one(con, ikey, sym, d)
            ok = r['mp_pcr'] > 0 or r['oi_coi'] > 0
            note = ', '.join(r['errors'][:2]) if r['errors'] else '✅'
            print(f"  {r['date']:<12} {r['symbol']:<10} "
                  f"{(r['expiry'] or '—'):<12} "
                  f"{r['mp_pcr']:>7} {r['oi_coi']:>8}  {note}")
            if ok:
                total_success += 1
            else:
                total_fail += 1
            time.sleep(0.15)        # polite pacing

    print(f"\n  {'═'*30} SUMMARY {'═'*40}")
    print(f"  Total attempts : {len(dates) * len(OPTION_SYMBOLS)}")
    print(f"  ✅ Backfilled   : {total_success}")
    print(f"  ⏭  Skipped      : {total_skip}")
    print(f"  ❌ Failed       : {total_fail}")

    # Coverage report
    print(f"\n  📊 Coverage now in DB:")
    for sym in OPTION_SYMBOLS.values():
        n_mp = con.execute("SELECT COUNT(*) FROM daily_max_pain_pcr "
                           "WHERE symbol=?", (sym,)).fetchone()[0]
        n_oi = con.execute("SELECT COUNT(DISTINCT date) FROM daily_oi_coi "
                           "WHERE symbol=?", (sym,)).fetchone()[0]
        d_min = con.execute("SELECT MIN(date) FROM daily_max_pain_pcr "
                            "WHERE symbol=?", (sym,)).fetchone()[0]
        d_max = con.execute("SELECT MAX(date) FROM daily_max_pain_pcr "
                            "WHERE symbol=?", (sym,)).fetchone()[0]
        print(f"    {sym:<10} MP_PCR rows: {n_mp} | OI dates: {n_oi} "
              f"| Range: {d_min} → {d_max}")

    print(f"\n  ▶ Next: py backtest_filtered.py NIFTY50")
    con.close()


if __name__ == "__main__":
    main()