"""
NIFTY Daily Brief — runs all insights and prints a concise summary.
Usage:
    py analyze.py
"""

import sqlite3
import traceback
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

IST     = ZoneInfo("Asia/Kolkata")
DB_PATH = Path(__file__).parent / "db" / "trading_data.db"


def banner(t):
    print(f"\n{'═' * 64}\n  {t}\n{'═' * 64}")


def rows_to_table(cur, sql, params=()):
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    data = cur.fetchall()
    if not data:
        print("  (no data)")
        return
    widths = [max(len(c), *(len(str(r[i] if r[i] is not None else '-')) for r in data))
              for i, c in enumerate(cols)]
    print("  " + " │ ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    print("  " + "─┼─".join("─" * w for w in widths))
    for r in data:
        print("  " + " │ ".join(
            str(r[i] if r[i] is not None else '-').ljust(widths[i])
            for i in range(len(cols))
        ))


def safe_section(title, fn):
    """Run a section; if it fails, print error but continue."""
    banner(title)
    try:
        fn()
    except sqlite3.Error as e:
        print(f"  ⚠ SQL error: {e}")
    except Exception as e:
        print(f"  ⚠ Error: {e}")
        traceback.print_exc()


def daily_brief(con):
    cur = con.cursor()

    safe_section("🎯 MASTER DAILY BRIEF (NIFTY50 + BANKNIFTY)", lambda: rows_to_table(cur, """
        SELECT symbol, spot, max_pain, spot_vs_maxpain,
               pcr_current, pcr_regime, pcr_trend,
               maxpain_drift, day_type, gap_type
        FROM v_daily_brief
    """))

    safe_section("📐 CPR LEVELS (Today)", lambda: rows_to_table(cur, """
        SELECT c.symbol,
               ROUND(c.pivot,1) AS pivot,
               ROUND(c.bc,1)    AS bc,
               ROUND(c.tc,1)    AS tc,
               ROUND(c.pdh,1)   AS pdh,
               ROUND(c.pdl,1)   AS pdl,
               ROUND(c.r1,1)    AS r1,
               ROUND(c.s1,1)    AS s1,
               c.cpr_width_pct,
               dt.day_type
        FROM v_cpr_today c
        JOIN v_day_type dt USING (symbol)
        WHERE c.symbol IN ('NIFTY50','BANKNIFTY')
    """))

    safe_section("🌡️ INDIA VIX REGIME",
                 lambda: rows_to_table(cur, "SELECT * FROM v_vix_regime"))

    safe_section("📊 SECTOR RELATIVE STRENGTH (Today's % move)", lambda: rows_to_table(cur, """
        SELECT * FROM v_sector_rs ORDER BY day_change_pct DESC
    """))

    safe_section("🏦 BANK BREADTH (% of HDFC/ICICI/SBI/KOTAK/AXIS above VWAP)",
                 lambda: rows_to_table(cur, "SELECT * FROM v_bank_breadth"))

    safe_section("🎯 CONFLUENCE ZONES (≥2 sources agree → high-probability levels)",
                 lambda: rows_to_table(cur, """
        SELECT symbol, zone_price, confluence_count, sources
        FROM v_confluence_zones
        ORDER BY symbol, zone_price
    """))

    safe_section("🔝 TOP 5 CALL OI STRIKES (Resistance Map)", lambda: rows_to_table(cur, """
        SELECT symbol, ROUND(strike,0) AS strike, call_oi, ROUND(spot_price,1) AS spot
        FROM v_top_oi_strikes
        WHERE call_oi_rank <= 5
        ORDER BY symbol, call_oi DESC
    """))

    safe_section("🔝 TOP 5 PUT OI STRIKES (Support Map)", lambda: rows_to_table(cur, """
        SELECT symbol, ROUND(strike,0) AS strike, put_oi, ROUND(spot_price,1) AS spot
        FROM v_top_oi_strikes
        WHERE put_oi_rank <= 5
        ORDER BY symbol, put_oi DESC
    """))

    safe_section("⚡ OI BUILDUP — strikes with strongest signals (top 8 by |COI|)",
                 lambda: rows_to_table(cur, """
        SELECT symbol, ROUND(strike,0) AS strike,
               call_change_oi, call_oi_signal,
               put_change_oi,  put_oi_signal
        FROM v_oi_buildup
        ORDER BY ABS(COALESCE(call_change_oi,0)) + ABS(COALESCE(put_change_oi,0)) DESC
        LIMIT 8
    """))

    safe_section("🚀 COI VELOCITY — strikes with accelerating positioning (top 5)",
                 lambda: rows_to_table(cur, """
        SELECT symbol, ROUND(strike,0) AS strike,
               ROUND(call_coi_velocity,0) AS call_velocity,
               ROUND(put_coi_velocity, 0) AS put_velocity,
               latest_call_coi, latest_put_coi
        FROM v_coi_velocity
        WHERE call_coi_velocity IS NOT NULL
           OR put_coi_velocity  IS NOT NULL
        ORDER BY ABS(COALESCE(call_coi_velocity,0))
               + ABS(COALESCE(put_coi_velocity, 0)) DESC
        LIMIT 5
    """))

    # ── Trading bias verdict ──
    banner("🎬 TRADING BIAS SUMMARY")
    try:
        cur.execute("SELECT * FROM v_daily_brief")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        if not rows:
            print("  (no data)")
        for row in rows:
            d = dict(zip(cols, row))
            sym = d.get('symbol', '?')
            score = 0
            if d.get('pcr_regime') in ('BULLISH', 'STRONG_BULLISH'): score += 1
            if d.get('pcr_regime') == 'STRONG_BULLISH':              score += 1
            if d.get('pcr_regime') in ('BEARISH', 'STRONG_BEARISH'): score -= 1
            if d.get('pcr_regime') == 'STRONG_BEARISH':              score -= 1
            if d.get('pcr_trend')  == 'RISING':                      score += 1
            if d.get('pcr_trend')  == 'FALLING':                     score -= 1
            if d.get('maxpain_drift') == 'DRIFTING_UP':              score += 1
            if d.get('maxpain_drift') == 'DRIFTING_DOWN':            score -= 1
            verdict = ('STRONG_BULLISH' if score >=  3 else
                       'BULLISH'        if score >=  1 else
                       'STRONG_BEARISH' if score <= -3 else
                       'BEARISH'        if score <= -1 else 'NEUTRAL')
            print(f"  {sym:<10} → score={score:+d}  verdict={verdict}")
            print(f"    PCR={d.get('pcr_current')} "
                  f"({d.get('pcr_regime')}, {d.get('pcr_trend')})")
            spot       = d.get('spot') or 0
            max_pain   = d.get('max_pain') or 0
            gap        = d.get('spot_vs_maxpain') or 0
            gap_pct    = d.get('gap_pct') or 0
            print(f"    MaxPain={max_pain} | spot={spot} "
                  f"| gap={gap:+.0f} | drift={d.get('maxpain_drift')}")
            print(f"    Day-type: {d.get('day_type')} | Gap: {d.get('gap_type')} "
                  f"({gap_pct:+.2f}%)\n")
    except Exception as e:
        print(f"  ⚠ Error: {e}")


if __name__ == "__main__":
    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    print(f"🚀 NIFTY Daily Brief @ "
          f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")
    print(f"📂 DB: {DB_PATH}")
    daily_brief(con)
    con.close()
    print(f"\n✅ Done.\n")