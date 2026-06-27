"""
PRE-MARKET BRIEF — 8:45 AM IST daily routine
─────────────────────────────────────────────
Reads yesterday's daily candle, computes today's CPR/PDH/PDL/R1/R2/S1/S2,
checks VIX regime, identifies key OI walls, and outputs a TRADE/WAIT/SKIP
recommendation for 3TR_SHORT_PRIME on NIFTY50 + BANKNIFTY.

Usage:
    py premarket_brief.py                  # both NIFTY50 + BANKNIFTY
    py premarket_brief.py NIFTY50          # single symbol
    py premarket_brief.py --save           # also save to logs/brief_YYYY-MM-DD.txt
"""

import sys
import sqlite3
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

IST     = ZoneInfo("Asia/Kolkata")
DB_PATH = Path(__file__).parent / "db" / "trading_data.db"
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

SAVE_MODE = "--save" in sys.argv
SYMBOLS_TO_BRIEF = ['NIFTY50', 'BANKNIFTY']


# ─────────────────────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────────────────────
def get_last_two_daily_candles(con, symbol):
    """Returns yesterday + day-before-yesterday daily candles."""
    rows = con.execute("""
        SELECT date, open, high, low, close, volume
        FROM daily_candles
        WHERE symbol=?
        ORDER BY date DESC LIMIT 2
    """, (symbol,)).fetchall()
    if len(rows) < 2:
        return None, None
    return rows[0], rows[1]   # yesterday, day-before


def get_recent_vix(con):
    """Latest VIX close + 5-day avg."""
    rows = con.execute("""
        SELECT date, close FROM daily_candles
        WHERE symbol='INDIAVIX'
        ORDER BY date DESC LIMIT 6
    """).fetchall()
    if not rows:
        return None, None, None
    latest = rows[0]
    avg_5 = sum(r[1] for r in rows[1:6]) / max(len(rows[1:6]), 1)
    chg_pct = (latest[1] - avg_5) * 100.0 / avg_5 if avg_5 else 0
    return latest, avg_5, chg_pct


def get_oi_walls(con, symbol):
    """Top 3 CE + top 3 PE OI strikes from most recent daily_oi_coi date."""
    r = con.execute("""
        SELECT MAX(date) FROM daily_oi_coi WHERE symbol=?
    """, (symbol,)).fetchone()
    if not r or not r[0]:
        return None, [], [], None, None
    proxy_date = r[0]
    ce = con.execute("""
        SELECT strike, eod_call_oi FROM daily_oi_coi
        WHERE symbol=? AND date=?
        ORDER BY eod_call_oi DESC LIMIT 3
    """, (symbol, proxy_date)).fetchall()
    pe = con.execute("""
        SELECT strike, eod_put_oi FROM daily_oi_coi
        WHERE symbol=? AND date=?
        ORDER BY eod_put_oi DESC LIMIT 3
    """, (symbol, proxy_date)).fetchall()
    mp_pcr = con.execute("""
        SELECT eod_max_pain, eod_pcr FROM daily_max_pain_pcr
        WHERE symbol=? AND date=? LIMIT 1
    """, (symbol, proxy_date)).fetchone()
    max_pain = mp_pcr[0] if mp_pcr else None
    pcr      = mp_pcr[1] if mp_pcr else None
    return proxy_date, ce, pe, max_pain, pcr


# ─────────────────────────────────────────────────────────────
# Compute today's levels & day-type prediction
# ─────────────────────────────────────────────────────────────
def compute_levels(yesterday):
    _, _, y_h, y_l, y_c, _ = yesterday
    pivot = (y_h + y_l + y_c) / 3.0
    bc    = (y_h + y_l) / 2.0
    tc    = pivot * 2 - bc
    return {
        'pdh':   y_h, 'pdl': y_l, 'pdc': y_c,
        'pivot': pivot, 'bc': bc, 'tc': tc,
        'r1':    2 * pivot - y_l,
        's1':    2 * pivot - y_h,
        'r2':    pivot + (y_h - y_l),
        's2':    pivot - (y_h - y_l),
        'cpr_width_pct': abs(tc - bc) * 100.0 / y_c,
    }


def classify_day_type(cpr_width_pct):
    if cpr_width_pct < 0.15:
        return 'TREND_DAY', '✅ Likely to trend strongly — momentum favored'
    if cpr_width_pct < 0.40:
        return 'NORMAL_DAY', '✅ Balanced — reversal setups have good odds'
    if cpr_width_pct < 0.80:
        return 'RANGE_DAY', '⚠ Range-bound — skip directional setups'
    return 'SIDEWAYS_WIDE', '❌ Choppy — avoid all setups'


def classify_vix_regime(vix_close, vix_5d_avg, chg_pct):
    if vix_close is None:
        return 'UNKNOWN', '⚠ VIX data unavailable'
    if vix_close < 11:
        return 'COMPLACENT', '🟡 Very low vol — small moves expected'
    if vix_close < 14:
        return 'LOW_VOL', '✅ Healthy low vol — trend/reversal both work'
    if vix_close < 18:
        return 'NORMAL_VOL', '✅ Normal vol — all setups in play'
    if vix_close < 22:
        return 'ELEVATED_VOL', '⚠ High vol — wider stops needed'
    return 'CRISIS_VOL', '❌ Risk-off — paper trade only'


def gap_inference(yesterday_close, current_estimate=None):
    """We don't have pre-open price, so this is informational placeholder."""
    return None


# ─────────────────────────────────────────────────────────────
# Final TRADE / WAIT / SKIP recommendation
# ─────────────────────────────────────────────────────────────
def make_recommendation(day_type, vix_regime, oi_proxy_age_days):
    score = 0
    reasons = []

    # Day-type contribution
    if day_type == 'TREND_DAY':
        score += 3; reasons.append('✅ TREND_DAY (CPR narrow → momentum)')
    elif day_type == 'NORMAL_DAY':
        score += 3; reasons.append('✅ NORMAL_DAY (balanced)')
    elif day_type == 'RANGE_DAY':
        score -= 2; reasons.append('⚠ RANGE_DAY (whipsaw risk)')
    else:
        score -= 4; reasons.append('❌ SIDEWAYS_WIDE (avoid)')

    # VIX contribution
    if vix_regime in ('LOW_VOL', 'NORMAL_VOL'):
        score += 2; reasons.append(f'✅ {vix_regime} (clean execution)')
    elif vix_regime == 'COMPLACENT':
        score += 1; reasons.append('🟡 COMPLACENT VIX (small moves)')
    elif vix_regime == 'ELEVATED_VOL':
        score -= 1; reasons.append('⚠ Elevated VIX (wider stops)')
    elif vix_regime == 'CRISIS_VOL':
        score -= 3; reasons.append('❌ Crisis VIX (skip)')

    # OI freshness contribution
    if oi_proxy_age_days <= 1:
        score += 1; reasons.append('✅ OI data fresh')
    elif oi_proxy_age_days <= 3:
        reasons.append(f'🟡 OI data {oi_proxy_age_days}d old')
    else:
        score -= 1; reasons.append(f'⚠ OI data {oi_proxy_age_days}d old — verify')

    # Final verdict
    if score >= 5:
        verdict = '🟢 TRADE'
        action  = 'Run live_scanner.py at 10:25 AM. Take all signals.'
    elif score >= 3:
        verdict = '🟡 WAIT'
        action  = 'Run scanner, but only take A+ setups (touch 3-4, clean wick).'
    else:
        verdict = '🔴 SKIP'
        action  = 'Do not trade today. Observe only. Update journal.'

    return verdict, action, score, reasons


# ─────────────────────────────────────────────────────────────
# Pretty-printing
# ─────────────────────────────────────────────────────────────
def print_symbol_brief(con, symbol, output_lines):
    def L(s=""):
        output_lines.append(s)
        print(s)

    L(f"\n{'─' * 78}")
    L(f"  📊 {symbol}")
    L(f"{'─' * 78}")

    yesterday, day_before = get_last_two_daily_candles(con, symbol)
    if not yesterday:
        L(f"  ❌ No recent daily candle for {symbol} — run main.py once first.")
        return None

    y_date, y_o, y_h, y_l, y_c, y_v = yesterday

    # Levels
    levels = compute_levels(yesterday)
    day_type, day_note = classify_day_type(levels['cpr_width_pct'])

    # OI walls
    proxy_date, ce, pe, max_pain, pcr = get_oi_walls(con, symbol)
    if proxy_date:
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        oi_age = (datetime.strptime(today_str, "%Y-%m-%d")
                  - datetime.strptime(proxy_date, "%Y-%m-%d")).days
    else:
        oi_age = 999

    # Day's previous behavior
    L(f"\n  📅 Yesterday ({y_date}):")
    L(f"     Open: {y_o:>8.1f}  →  High: {y_h:>8.1f}  →  Low: {y_l:>8.1f}  "
      f"→  Close: {y_c:>8.1f}")
    L(f"     Range: {y_h - y_l:>5.1f} pts  |  Body: {abs(y_c - y_o):>5.1f} pts")

    if day_before:
        prev_c = day_before[4]
        gap_pts = y_o - prev_c
        gap_pct = gap_pts * 100 / prev_c
        L(f"     Prior close: {prev_c:.1f}  |  Yesterday's gap: {gap_pts:+.1f} ({gap_pct:+.2f}%)")

    # Levels
    L(f"\n  📐 TODAY'S KEY LEVELS:")
    L(f"     ┌─────────────────────────┐")
    L(f"     │ R2  : {levels['r2']:>10.1f}        │  ← Extreme resistance")
    L(f"     │ R1  : {levels['r1']:>10.1f}        │  ← First resistance")
    L(f"     │ TC  : {levels['tc']:>10.1f}        │  ┐")
    L(f"     │ Piv : {levels['pivot']:>10.1f}        │  ├── CPR (width {levels['cpr_width_pct']:.3f}%)")
    L(f"     │ BC  : {levels['bc']:>10.1f}        │  ┘")
    L(f"     │ S1  : {levels['s1']:>10.1f}        │  ← First support")
    L(f"     │ S2  : {levels['s2']:>10.1f}        │  ← Extreme support")
    L(f"     └─────────────────────────┘")
    L(f"     PDH: {levels['pdh']:.1f}  |  PDL: {levels['pdl']:.1f}  |  "
      f"PDC: {levels['pdc']:.1f}")

    # Day-type forecast
    L(f"\n  🎯 DAY-TYPE FORECAST: {day_type}")
    L(f"     {day_note}")

    # OI walls
    if proxy_date:
        L(f"\n  🏛️  OI WALLS (from {proxy_date}, age {oi_age}d):")
        if ce:
            L(f"     ↑ Top CE OI (resistance):")
            for strike, oi in ce:
                marker = '  ← KEY' if oi == max(c[1] for c in ce) else ''
                L(f"        {int(strike):>6}  ({oi/1e6:.1f}M){marker}")
        if pe:
            L(f"     ↓ Top PE OI (support):")
            for strike, oi in pe:
                marker = '  ← KEY' if oi == max(c[1] for c in pe) else ''
                L(f"        {int(strike):>6}  ({oi/1e6:.1f}M){marker}")
        if max_pain:
            L(f"     🎯 Max Pain : {max_pain:.0f}  |  EOD PCR: {pcr:.3f}")
            mp_vs_pdc = max_pain - y_c
            direction = 'UP (bullish lean)' if mp_vs_pdc > 0 else 'DOWN (bearish lean)' if mp_vs_pdc < 0 else 'FLAT'
            L(f"     Max Pain vs yesterday close: {mp_vs_pdc:+.0f} pts → {direction}")
    else:
        L(f"\n  ⚠ No OI data available for {symbol}")

    return {
        'symbol': symbol, 'day_type': day_type, 'levels': levels,
        'ce_walls': [s for s, _ in ce], 'pe_walls': [s for s, _ in pe],
        'max_pain': max_pain, 'pcr': pcr, 'oi_age': oi_age,
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    if not DB_PATH.exists():
        sys.exit(f"❌ DB not found: {DB_PATH}")

    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    symbols = args if args else SYMBOLS_TO_BRIEF

    con = sqlite3.connect(DB_PATH)
    output_lines = []
    def L(s=""):
        output_lines.append(s); print(s)

    today_iso = datetime.now(IST).strftime("%Y-%m-%d")
    now_str   = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    L(f"\n{'═' * 78}")
    L(f"  🌅 PRE-MARKET BRIEF  —  {now_str}")
    L(f"  Symbols: {', '.join(symbols)}")
    L(f"{'═' * 78}")

    # VIX context (shared across all symbols)
    vix_row, vix_avg, vix_chg = get_recent_vix(con)
    vix_regime = 'UNKNOWN'; vix_note = ''
    if vix_row:
        vix_regime, vix_note = classify_vix_regime(vix_row[1], vix_avg, vix_chg)
        L(f"\n  🌡️  INDIA VIX REGIME")
        L(f"     Yesterday close : {vix_row[1]:.2f}  (as of {vix_row[0]})")
        L(f"     5-day average   : {vix_avg:.2f}")
        L(f"     Change          : {vix_chg:+.2f}% vs 5d avg")
        L(f"     Regime          : {vix_regime} — {vix_note}")
    else:
        L(f"\n  ⚠ No VIX data — recommendation will be cautious")

    # Per-symbol brief
    symbol_summaries = []
    for sym in symbols:
        summary = print_symbol_brief(con, sym, output_lines)
        if summary:
            symbol_summaries.append(summary)

    # ── FINAL RECOMMENDATIONS ──
    L(f"\n{'═' * 78}")
    L(f"  🎯 FINAL RECOMMENDATIONS")
    L(f"{'═' * 78}")

    for s in symbol_summaries:
        verdict, action, score, reasons = make_recommendation(
            s['day_type'], vix_regime, s['oi_age'])
        L(f"\n  {s['symbol']}:  {verdict}  (score: {score:+d})")
        for r in reasons:
            L(f"     • {r}")
        L(f"     → {action}")

    # ── Pre-market checklist ──
    L(f"\n{'═' * 78}")
    L(f"  ✅ PRE-MARKET CHECKLIST")
    L(f"{'═' * 78}")
    L(f"     ☐ 08:50 — review this brief")
    L(f"     ☐ 09:00 — start main.py for live data collection")
    L(f"     ☐ 09:15 — market opens, watch first 30 min only")
    L(f"     ☐ 10:25 — start live_scanner.py (NIFTY50 + BANKNIFTY)")
    L(f"     ☐ 10:30 — scan window opens, take signals as they fire")
    L(f"     ☐ 12:30 — scan window closes (no NEW entries)")
    L(f"     ☐ 14:30 — hard exit all positions")
    L(f"     ☐ 15:30 — update journal, fill outcomes in CSV")

    L(f"\n{'═' * 78}")
    L(f"  💡 Reminders:")
    L(f"     • Max 2 losses → STOP for the day")
    L(f"     • Risk: 0.5% per trade, paper account only (until 4-week validation)")
    L(f"     • Trade ONLY 3TR_SHORT_PRIME (no exceptions, no improvisation)")
    L(f"     • Log every signal — taken or skipped — for ML data")
    L(f"{'═' * 78}\n")

    # Save brief if requested
    if SAVE_MODE:
        brief_file = LOG_DIR / f"brief_{today_iso}.txt"
        brief_file.write_text("\n".join(output_lines), encoding='utf-8')
        print(f"  💾 Saved to {brief_file}\n")

    con.close()


if __name__ == "__main__":
    main()