"""
LIVE SETUP SCANNER — 3TR_SHORT_PRIME
Runs during market hours, scans the latest 3-min bars every 30 sec,
fires alerts when the validated setup forms, logs all features for ML.

Usage:
    py live_scanner.py NIFTY50
    py live_scanner.py NIFTY50 --debug
"""
import os
import sys
import time
import csv
import sqlite3
from pathlib import Path
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from setup_scanner_v3 import (
    resample, get_cpr, get_oi_walls,
    resistance_levels, support_levels,
    compute_structure_states,
    detect_3tr,
)

IST       = ZoneInfo("Asia/Kolkata")
DB_PATH   = Path(__file__).parent / "db" / "trading_data.db"
LOG_DIR   = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

SCAN_START   = dtime(10, 30)
SCAN_END     = dtime(12, 30)
HARD_EXIT    = dtime(14, 30)
POLL_SEC     = 30
DEBUG        = "--debug" in sys.argv

# Per-session memory
FIRED_SIGNALS = set()                    # de-dup by (date, ts, type)
TOUCHES       = {'res': {}, 'sup': {}}
LOSSES_TODAY  = 0                        # tracked via journal updates


# ─────────────────────────────────────────────────────────────
# Feature extraction for ML
# ─────────────────────────────────────────────────────────────
def extract_features(con, symbol, date, bars, idx, setup, cpr, ce_walls, pe_walls):
    """Capture 30+ features per signal for future ML training."""
    ts_iso = bars[idx][0]
    o, h, l, c, v = bars[idx][1:6]
    rng = max(h - l, 0.01)

    # Volume context
    avg_vol_10 = sum(b[5] for b in bars[max(0,idx-10):idx]) / max(min(idx,10),1)
    vol_ratio = v / avg_vol_10 if avg_vol_10 else 0

    # Recent momentum
    last_5_closes = [b[4] for b in bars[max(0,idx-5):idx+1]]
    momentum_5b = (last_5_closes[-1] - last_5_closes[0]) if len(last_5_closes) >= 2 else 0

    # VIX context (latest from intraday_candles)
    vix_row = con.execute("""
        SELECT close FROM intraday_candles
        WHERE symbol='INDIAVIX' AND datetime <= ?
        ORDER BY datetime DESC LIMIT 1
    """, (ts_iso,)).fetchone()
    vix_now = vix_row[0] if vix_row else None

    # Bank breadth proxy (% of bank stocks above their day's VWAP)
    banks = ('HDFCBANK','ICICIBANK','SBIN','KOTAKBANK','AXISBANK')
    above_count = 0; total = 0
    for bk in banks:
        r = con.execute("""
            SELECT close FROM intraday_candles
            WHERE symbol=? AND datetime <= ?
            ORDER BY datetime DESC LIMIT 1
        """, (bk, ts_iso)).fetchone()
        v_row = con.execute("""
            SELECT AVG(close) FROM intraday_candles
            WHERE symbol=? AND substr(datetime,1,10)=? AND datetime <= ?
        """, (bk, date, ts_iso)).fetchone()
        if r and v_row and v_row[0]:
            total += 1
            if r[0] > v_row[0]:
                above_count += 1
    bank_breadth_pct = (above_count * 100.0 / total) if total else None

    # PCR + MaxPain from real intraday data if available
    pcr_data = con.execute("""
        SELECT pcr, max_pain, spot_price FROM max_pain_pcr_intraday
        WHERE symbol=? AND date=? AND broker_time <= ?
        ORDER BY broker_time DESC LIMIT 1
    """, (symbol, date, ts_iso[11:16])).fetchone()
    pcr_now      = pcr_data[0] if pcr_data else None
    max_pain_now = pcr_data[1] if pcr_data else None

    return {
        'date': date,
        'ts': ts_iso,
        'symbol': symbol,
        'setup_type': setup['type'],
        'entry_estimate': c,
        'stop_estimate':  round(h * 1.0005, 2),
        'target_estimate': round(c - (h * 1.0005 - c) * 2, 2),
        'level_value': setup['level'][0],
        'level_source': setup['level'][1],
        'touch_count': setup['touch_count'],
        'structure_state': setup['state'],
        'bar_open': o, 'bar_high': h, 'bar_low': l, 'bar_close': c,
        'bar_range': round(rng, 2),
        'upper_wick_pct': round((h - max(o, c)) / rng * 100, 1),
        'body_pct':       round(abs(c - o) / rng * 100, 1),
        'bar_volume': v,
        'vol_ratio_10b': round(vol_ratio, 2),
        'momentum_5b': round(momentum_5b, 2),
        'vix_now': vix_now,
        'bank_breadth_pct': bank_breadth_pct,
        'pcr_now': pcr_now,
        'max_pain_now': max_pain_now,
        'spot_vs_maxpain_pct': round((c - max_pain_now) / max_pain_now * 100, 3) if max_pain_now else None,
        'pdh': cpr['pdh'], 'pdl': cpr['pdl'],
        'r1': round(cpr['r1'], 1), 'r2': round(cpr['r2'], 1),
        's1': round(cpr['s1'], 1), 's2': round(cpr['s2'], 1),
        'day_type': cpr['day_type'],
        'cpr_width_pct': cpr['cpr_width_pct'],
        'top_ce_oi_strike': ce_walls[0] if ce_walls else None,
        'top_pe_oi_strike': pe_walls[0] if pe_walls else None,
        'minutes_from_open': (datetime.fromisoformat(ts_iso).hour - 9) * 60
                              + datetime.fromisoformat(ts_iso).minute - 15,
        # Outcome fields — filled later by journal
        'outcome': None, 'exit_price': None, 'pnl': None, 'r_multiple': None,
        'bars_held': None, 'notes': '',
    }


def append_signal(features):
    """Append signal to today's CSV log."""
    today = datetime.now(IST).strftime("%Y-%m-%d")
    log_file = LOG_DIR / f"signals_{today}.csv"
    file_exists = log_file.exists()
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(features.keys()))
        if not file_exists:
            w.writeheader()
        w.writerow(features)


# ─────────────────────────────────────────────────────────────
# Loud alert printer
# ─────────────────────────────────────────────────────────────
def print_alert(features):
    print(f"\n{'='*72}")
    print(f"  🚨🚨🚨  SETUP FIRED: {features['setup_type']}  🚨🚨🚨")
    print(f"{'='*72}")
    print(f"  Time      : {features['ts']}")
    print(f"  Symbol    : {features['symbol']}")
    print(f"  Setup bar : O={features['bar_open']} H={features['bar_high']} "
          f"L={features['bar_low']} C={features['bar_close']}")
    print(f"  Wick%     : {features['upper_wick_pct']}%  (need ≥30%)")
    print(f"  Volume    : {features['vol_ratio_10b']}× of last 10 bars")
    print(f"  Level     : {features['level_source']} @ {features['level_value']}")
    print(f"  Touch #   : {features['touch_count']}")
    print(f"  Structure : {features['structure_state']}")
    print(f"\n  📋 PAPER TRADE ORDER:")
    print(f"     ENTRY  (next bar open): ~{features['entry_estimate']}")
    print(f"     STOP                  : {features['stop_estimate']}  "
          f"(risk {features['stop_estimate'] - features['entry_estimate']:.1f} pts)")
    print(f"     TARGET (1:2 R:R)      : {features['target_estimate']}  "
          f"(reward {features['entry_estimate'] - features['target_estimate']:.1f} pts)")
    print(f"     MAX HOLD              : 45 minutes (15 bars on 3-min)")
    print(f"     HARD EXIT             : 14:30 IST")
    print(f"\n  📊 Context:")
    print(f"     Day-type    : {features['day_type']}")
    print(f"     PCR now     : {features['pcr_now']}")
    print(f"     Max Pain    : {features['max_pain_now']}  "
          f"(spot {features['spot_vs_maxpain_pct']:+.2f}% from MP)" if features['max_pain_now'] else "")
    print(f"     India VIX   : {features['vix_now']}")
    print(f"     Bank breadth: {features['bank_breadth_pct']}% above VWAP")
    print(f"\n  💾 Logged to logs/signals_{features['date']}.csv")
    print(f"{'='*72}\n")


# ─────────────────────────────────────────────────────────────
# Main scan cycle
# ─────────────────────────────────────────────────────────────
def scan_once(con, symbol):
    global TOUCHES
    today = datetime.now(IST).strftime("%Y-%m-%d")

    bars = resample(con, symbol, today, 3)
    if len(bars) < 15:
        if DEBUG:
            print(f"  [debug] {datetime.now(IST):%H:%M:%S} — only {len(bars)} bars yet")
        return

    cpr = get_cpr(con, symbol, today)
    if not cpr:
        return
    if cpr['day_type'] not in ('NORMAL_DAY', 'TREND_DAY'):
        if DEBUG:
            print(f"  [debug] day_type={cpr['day_type']} — skip session")
        return

    ce_walls, pe_walls = get_oi_walls(con, symbol, today)
    res_lvls = resistance_levels(cpr, ce_walls)
    sup_lvls = support_levels(cpr, pe_walls)
    states, _, _ = compute_structure_states(bars)

    or_n = max(1, 30 // 3)

    # Rebuild touches from scratch each scan (idempotent)
    TOUCHES = {'res': {}, 'sup': {}}

    for i in range(len(bars)):
        if i < or_n:
            continue
        bar_time = bars[i][0][11:16]
        if not ('10:30' <= bar_time < '12:30'):
            # Still count touches so state stays accurate
            detect_3tr(bars, i, res_lvls, sup_lvls, TOUCHES, states[i])
            continue

        st = states[i]
        if st not in ('TOPPED', 'DOWNTREND'):
            continue

        setup = detect_3tr(bars, i, res_lvls, sup_lvls, TOUCHES, st)
        if not setup or setup['type'] != '3TR_SHORT':
            continue
        if not (3 <= setup['touch_count'] <= 6):
            continue

        # De-dup
        sig_key = (today, bars[i][0], setup['type'])
        if sig_key in FIRED_SIGNALS:
            continue
        FIRED_SIGNALS.add(sig_key)

        # Extract features + alert + log
        features = extract_features(con, symbol, today, bars, i, setup,
                                    cpr, ce_walls, pe_walls)
        print_alert(features)
        append_signal(features)


def in_scan_window(now):
    if now.weekday() >= 5: return False
    return SCAN_START <= now.time() < HARD_EXIT


def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 else "NIFTY50"
    if not DB_PATH.exists():
        sys.exit(f"❌ DB not found: {DB_PATH}")

    print(f"\n🎯 LIVE SETUP SCANNER — {symbol}")
    print(f"📂 DB     : {DB_PATH}")
    print(f"📝 Logs   : {LOG_DIR}")
    print(f"⏱  Scan   : every {POLL_SEC}s")
    print(f"🕐 Window : {SCAN_START.strftime('%H:%M')}–{SCAN_END.strftime('%H:%M')} "
          f"(monitor till {HARD_EXIT.strftime('%H:%M')})")
    print(f"🎯 Setup  : 3TR_SHORT_PRIME (touch 3-6, NORMAL/TREND day, "
          f"TOPPED/DOWNTREND state)")
    print(f"💡 Run `py main.py` in parallel to keep data fresh.\n")

    con = sqlite3.connect(DB_PATH)

    try:
        while True:
            now = datetime.now(IST)
            if not in_scan_window(now):
                if now.time() >= HARD_EXIT:
                    print(f"\n🛑 Past hard exit ({HARD_EXIT}) — closing scanner.")
                    break
                if now.weekday() >= 5:
                    sys.exit("📅 Weekend — exiting.")
                # Pre-window: sleep & re-check
                print(f"  ⏳ {now:%H:%M:%S} — waiting for scan window "
                      f"{SCAN_START.strftime('%H:%M')}", end='\r')
                time.sleep(30)
                continue

            scan_once(con, symbol)
            time.sleep(POLL_SEC)
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
    finally:
        con.close()
        n = len(FIRED_SIGNALS)
        print(f"\n✅ Session ended. {n} setup(s) fired today.")
        if n:
            print(f"   Review signals in: logs/signals_{datetime.now(IST):%Y-%m-%d}.csv")


if __name__ == "__main__":
    main()