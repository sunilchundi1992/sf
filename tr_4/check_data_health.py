"""Quick health check on intraday_candles + daily tables."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "db" / "trading_data.db"

def main():
    con = sqlite3.connect(DB_PATH)
    print(f"📂 DB: {DB_PATH}\n")

    # ─── 1. Intraday candles by date/symbol ───
    print("📊 INTRADAY CANDLES — by date & symbol")
    print(f"  {'Date':<12} {'Symbol':<14} {'Bars':>6}  {'First':<19} {'Last':<19}")
    print(f"  {'─'*12} {'─'*14} {'─'*6}  {'─'*19} {'─'*19}")
    for r in con.execute("""
        SELECT substr(datetime,1,10) AS d, symbol, COUNT(*) AS n,
               MIN(datetime), MAX(datetime)
        FROM intraday_candles
        GROUP BY d, symbol
        ORDER BY d DESC, symbol
        LIMIT 50
    """):
        d, sym, n, first, last = r
        print(f"  {d:<12} {sym:<14} {n:>6}  {first:<19} {last:<19}")

    # ─── 2. Daily candles ───
    print(f"\n📅 DAILY CANDLES — by symbol (last 30 days)")
    print(f"  {'Symbol':<14} {'Rows':>6} {'First':<12} {'Last':<12}")
    print(f"  {'─'*14} {'─'*6} {'─'*12} {'─'*12}")
    for r in con.execute("""
        SELECT symbol, COUNT(*) AS n, MIN(date), MAX(date)
        FROM daily_candles
        GROUP BY symbol
        ORDER BY symbol
    """):
        print(f"  {r[0]:<14} {r[1]:>6} {r[2]:<12} {r[3]:<12}")

    # ─── 3. Option data ───
    print(f"\n🎯 OPTION DATA — daily_max_pain_pcr")
    print(f"  {'Date':<12} {'Symbol':<14} {'Spot':>9} {'MaxPain':>9} {'PCR':>6}")
    print(f"  {'─'*12} {'─'*14} {'─'*9} {'─'*9} {'─'*6}")
    for r in con.execute("""
        SELECT date, symbol, eod_spot, eod_max_pain, ROUND(eod_pcr, 3)
        FROM daily_max_pain_pcr
        ORDER BY date DESC, symbol
        LIMIT 20
    """):
        d, s, spot, mp, pcr = r
        print(f"  {d:<12} {s:<14} {spot:>9.1f} {mp:>9.0f} {pcr:>6}")

    # ─── 4. Ingest log ───
    print(f"\n📋 DAILY INGEST LOG (last 10)")
    print(f"  {'Date':<12} {'Table':<24} {'Rows':>6} {'Status':<8} {'Source'}")
    print(f"  {'─'*12} {'─'*24} {'─'*6} {'─'*8} {'─'*25}")
    for r in con.execute("""
        SELECT date, table_name, rows_loaded, status, source
        FROM daily_ingest_log
        ORDER BY ingested_at DESC LIMIT 10
    """):
        d, t, n, s, src = r
        print(f"  {d:<12} {t:<24} {(n or 0):>6} {s:<8} {(src or ''):<25}")

    con.close()

if __name__ == "__main__":
    main()