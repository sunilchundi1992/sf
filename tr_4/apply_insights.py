"""
Applies insights.sql to the SQLite DB — creates/refreshes all views.
Run anytime you change insights.sql.
"""
import sqlite3
from pathlib import Path

DB_PATH      = Path(__file__).parent / "db" / "trading_data.db"
INSIGHTS_SQL = Path(__file__).parent / "insights.sql"

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")
    if not INSIGHTS_SQL.exists():
        raise SystemExit(f"❌ insights.sql not found: {INSIGHTS_SQL}")

    sql = INSIGHTS_SQL.read_text(encoding="utf-8")
    con = sqlite3.connect(DB_PATH)
    try:
        con.executescript(sql)
        con.commit()
        # Show what got created
        cur = con.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
        views = [r[0] for r in cur.fetchall()]
        print(f"✅ Applied insights.sql to {DB_PATH}")
        print(f"📊 Views available ({len(views)}):")
        for v in views:
            print(f"   • {v}")
    finally:
        con.close()

if __name__ == "__main__":
    main()