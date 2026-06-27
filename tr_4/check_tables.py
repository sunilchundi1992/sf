"""Quick row-count check across old + new tables for a given date."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "db" / "trading_data.db"
TARGET_DATE = "2026-06-25"

# Each table → which column holds the timestamp/date
TABLES = {
    "intraday_candles":       "datetime",
    "max_pain_pcr":           "snapshot_ts",
    "oi_coi_strikes":         "snapshot_ts",
    "max_pain_pcr_intraday":  "date",
    "oi_coi_snapshots":       "snapshot_ts",
}

def table_exists(con, name):
    r = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return r is not None

def main():
    con = sqlite3.connect(DB_PATH)
    print(f"📂 DB: {DB_PATH}")
    print(f"📅 Checking date: {TARGET_DATE}\n")
    print(f"  {'Table':<26} {'Rows on date':>14}")
    print(f"  {'─'*26} {'─'*14}")
    for tbl, col in TABLES.items():
        if not table_exists(con, tbl):
            print(f"  {tbl:<26} {'(not found)':>14}")
            continue
        try:
            n = con.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE substr({col}, 1, 10) = ?",
                (TARGET_DATE,),
            ).fetchone()[0]
            print(f"  {tbl:<26} {n:>14}")
        except sqlite3.OperationalError as e:
            print(f"  {tbl:<26} {'ERR':>14}   {e}")
    con.close()

if __name__ == "__main__":
    main()