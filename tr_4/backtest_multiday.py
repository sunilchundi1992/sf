"""
MULTI-DAY BACKTEST — runs setup_scanner_v3 across all available trading days,
aggregates statistics by setup, day-type, timeframe, time-of-day.

Usage:
    py backtest_multiday.py NIFTY50 5            # 5-min only
    py backtest_multiday.py NIFTY50 ALL          # 3, 5, 15-min
    py backtest_multiday.py NIFTY50 5 --csv      # also dump trades to CSV
"""

import sys
import sqlite3
import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Reuse v3 logic
from setup_scanner_v3 import (
    resample, get_cpr, get_oi_walls,
    resistance_levels, support_levels, compute_vwap_series,
    compute_structure_states,
    detect_frt, detect_fst, detect_3tr, detect_orbf,
    simulate_trade,
)

DB_PATH = Path(__file__).parent / "db" / "trading_data.db"
DUMP_CSV = "--csv" in sys.argv


# ─────────────────────────────────────────────────────────────
# Run setup detection on a single day
# ─────────────────────────────────────────────────────────────
def run_one_day(con, symbol, date, n_min):
    bars = resample(con, symbol, date, n_min)
    if len(bars) < 15:
        return [], None

    cpr = get_cpr(con, symbol, date)
    if not cpr:
        return [], None

    ce_walls, pe_walls = get_oi_walls(con, symbol, date)
    res_lvls = resistance_levels(cpr, ce_walls)
    sup_lvls = support_levels(cpr, pe_walls)

    states, _, _ = compute_structure_states(bars)
    or_n = max(1, 30 // n_min)
    or_high = max(b[2] for b in bars[:or_n])
    or_low  = min(b[3] for b in bars[:or_n])

    trades = []
    fired = set()
    touches = {'res': {}, 'sup': {}}

    for i in range(len(bars)):
        if i < or_n:
            continue

        # ── EOD cutoff: no new entries after 14:45 ──
        bar_time = bars[i][0][11:16]
        if bar_time >= '14:45':
            continue

        st = states[i]
        bar_setups = []  # collect all setups at this bar for dedup

        for det_fn in [
            lambda b, i: detect_frt(b, i, res_lvls, st),
            lambda b, i: detect_fst(b, i, sup_lvls, st),
            lambda b, i: detect_3tr(b, i, res_lvls, sup_lvls, touches, st),
            lambda b, i: detect_orbf(b, i, or_high, or_low, fired, cpr['day_type'], st),
        ]:
            setup = det_fn(bars, i)
            if setup:
                bar_setups.append(setup)

        # ── Dedup: if multiple setups at same bar, keep best-quality ──
        # Priority: 3TR > FRT > FST > ORBF (3TR has confluence built in)
        if bar_setups:
            priority = {'3TR': 4, 'FRT': 3, 'FST': 3, 'ORBF': 2}
            best = max(bar_setups,
                       key=lambda s: priority.get(s['type'].split('_')[0], 0))
            trade = simulate_trade(best, bars[i:])
            if trade:
                trades.append({
                    **best, **trade,
                    'timeframe_min': n_min,
                    'day_type': cpr['day_type'],
                    'date': date,
                    'symbol': symbol,
                })
                if 'ORBF' in best['type']:
                    fired.add(best['type'].lower())

    return trades, cpr


# ─────────────────────────────────────────────────────────────
# Aggregation & reporting
# ─────────────────────────────────────────────────────────────
def winrate(trades):
    if not trades:
        return 0, 0, 0
    wins = sum(1 for t in trades if t['outcome'] == 'WIN')
    wr = wins * 100.0 / len(trades)
    avg_pnl = sum(t['pnl'] for t in trades) / len(trades)
    return wr, avg_pnl, wins


def verdict(wr, n):
    if n < 10:        return '⚠ LOW_N'
    if wr >= 75:      return '🟢 STRONG_EDGE'
    if wr >= 65:      return '✅ TRADEABLE'
    if wr >= 55:      return '🟡 MARGINAL'
    return '❌ NO_EDGE'


def print_breakdown(trades, group_key, label, sort_by_n=False):
    print(f"\n  📊 By {label}:")
    print(f"  {label:<18} {'N':>4} {'Wins':>5} {'WR':>6} {'AvgPnL':>8} {'TotPnL':>8}  Verdict")
    print(f"  {'─'*18} {'─'*4} {'─'*5} {'─'*6} {'─'*8} {'─'*8}  {'─'*15}")
    groups = defaultdict(list)
    for t in trades:
        groups[group_key(t)].append(t)
    items = sorted(groups.items(),
                   key=lambda x: (-len(x[1]) if sort_by_n
                                  else str(x[0])))
    for k, ts in items:
        wr, avg, wins = winrate(ts)
        total = sum(t['pnl'] for t in ts)
        v = verdict(wr, len(ts))
        print(f"  {str(k):<18} {len(ts):>4} {wins:>5} {wr:>5.0f}% "
              f"{avg:>+8.1f} {total:>+8.0f}  {v}")


def time_bucket(t):
    hhmm = t['ts'][11:16]
    h = int(hhmm[:2])
    if h < 10:  return '09:30-10:00'
    if h < 11:  return '10:00-11:00'
    if h < 12:  return '11:00-12:00'
    if h < 13:  return '12:00-13:00'
    if h < 14:  return '13:00-14:00'
    return '14:00-14:45'


def equity_curve(trades):
    """Print compact equity curve (cumulative P&L by day)."""
    by_day = defaultdict(float)
    for t in trades:
        by_day[t['date']] += t['pnl']
    cumulative = 0
    print(f"\n  💰 Equity Curve (cumulative P&L by day):")
    print(f"  {'Date':<12} {'Day P&L':>9} {'Cumulative':>12}  {'Chart'}")
    print(f"  {'─'*12} {'─'*9} {'─'*12}  {'─'*30}")
    max_abs = max(abs(p) for p in by_day.values()) if by_day else 1
    for d in sorted(by_day):
        cumulative += by_day[d]
        chart_len = int(abs(by_day[d]) / max_abs * 20)
        chart = ('🟩' * chart_len) if by_day[d] >= 0 else ('🟥' * chart_len)
        print(f"  {d:<12} {by_day[d]:>+9.1f} {cumulative:>+12.1f}  {chart}")
    print(f"\n  Total P&L: {cumulative:+.1f} points across {len(by_day)} days")


def consecutive_stats(trades):
    """Find max consecutive wins/losses — risk-of-ruin indicator."""
    if not trades:
        return
    sorted_t = sorted(trades, key=lambda t: (t['date'], t['ts']))
    cur_w = cur_l = max_w = max_l = 0
    for t in sorted_t:
        if t['outcome'] == 'WIN':
            cur_w += 1
            cur_l = 0
        else:
            cur_l += 1
            cur_w = 0
        max_w = max(max_w, cur_w)
        max_l = max(max_l, cur_l)
    print(f"\n  🎲 Consecutive streaks: max wins = {max_w}, max losses = {max_l}")
    print(f"      → with 1% risk/trade, max drawdown ≈ {max_l}% from peak")


def dump_csv(trades, filename='multiday_trades.csv'):
    if not trades:
        return
    cols = ['date', 'ts', 'symbol', 'timeframe_min', 'day_type', 'state',
            'type', 'entry', 'exit', 'pnl', 'outcome', 'bars_held']
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        for t in trades:
            w.writerow(t)
    print(f"\n  💾 Dumped {len(trades)} trades to {filename}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    if not DB_PATH.exists():
        sys.exit(f"❌ DB not found: {DB_PATH}")

    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    symbol = args[0] if len(args) > 0 else 'NIFTY50'
    tf_arg = args[1] if len(args) > 1 else 'ALL'
    tfs = [3, 5, 15] if tf_arg.upper() == 'ALL' else [int(tf_arg)]

    con = sqlite3.connect(DB_PATH)
    # Get all distinct trading dates
    dates = [r[0] for r in con.execute("""
        SELECT DISTINCT substr(datetime, 1, 10) AS d
        FROM intraday_candles
        WHERE symbol = ?
        ORDER BY d
    """, (symbol,)).fetchall()]

    if not dates:
        sys.exit(f"❌ No data for {symbol}")

    print(f"\n{'═'*82}")
    print(f"  🎯 MULTI-DAY BACKTEST — {symbol}")
    print(f"  Timeframes: {tfs} min | Dates: {dates[0]} → {dates[-1]} "
          f"({len(dates)} days)")
    print(f"  Filters: EOD cutoff 14:45 | Same-bar dedup ON")
    print(f"{'═'*82}")

    all_trades = []
    day_types_seen = defaultdict(int)

    # ── Per-day execution ──
    for tf in tfs:
        print(f"\n  ━━━ Running {tf}-min across {len(dates)} days ━━━")
        tf_trades = []
        for d in dates:
            trades, cpr = run_one_day(con, symbol, d, tf)
            if cpr and tf == tfs[0]:  # count day_types once
                day_types_seen[cpr['day_type']] += 1
            tf_trades.extend(trades)
        wr, avg, wins = winrate(tf_trades)
        print(f"    {tf}-min: {len(tf_trades)} trades | "
              f"WR={wr:.0f}% | avg={avg:+.1f} | total={sum(t['pnl'] for t in tf_trades):+.0f}")
        all_trades.extend(tf_trades)

    if not all_trades:
        sys.exit("❌ No trades fired. Check data / detector thresholds.")

    # ── Day-type distribution ──
    print(f"\n  📅 Day-type distribution across {len(dates)} days:")
    for dt, n in sorted(day_types_seen.items(), key=lambda x: -x[1]):
        print(f"    {dt:<15} {n:>3} days ({n*100/len(dates):.0f}%)")

    # ── Aggregated reports ──
    print(f"\n  {'═'*30} STATISTICAL VALIDATION {'═'*28}")
    wr, avg, wins = winrate(all_trades)
    print(f"\n  📈 OVERALL: {len(all_trades)} trades | "
          f"WR={wr:.1f}% | Wins={wins} | "
          f"Avg P&L={avg:+.1f} | Total={sum(t['pnl'] for t in all_trades):+.0f}")

    print_breakdown(all_trades, lambda t: t['type'],            'Setup Type',     sort_by_n=True)
    print_breakdown(all_trades, lambda t: t['day_type'],        'Day-Type',       sort_by_n=True)
    print_breakdown(all_trades, lambda t: t['state'],           'Structure State',sort_by_n=True)
    print_breakdown(all_trades, lambda t: t['timeframe_min'],   'Timeframe',      sort_by_n=False)
    print_breakdown(all_trades, time_bucket,                    'Time-of-Day',    sort_by_n=False)

    # ── Best & worst setup combos ──
    print(f"\n  🏆 TOP 5 SETUP × DAY-TYPE combinations (min 5 trades):")
    combo = defaultdict(list)
    for t in all_trades:
        combo[(t['type'], t['day_type'])].append(t)
    rated = [(k, *winrate(v), len(v))
             for k, v in combo.items() if len(v) >= 5]
    rated.sort(key=lambda x: -x[1])
    for (st, dt), wr, avg, wins, n in rated[:5]:
        print(f"    {st:<14} × {dt:<14}  N={n:>3}  WR={wr:>5.0f}%  "
              f"avg={avg:>+6.1f}  {verdict(wr, n)}")

    # ── Equity curve & streaks ──
    equity_curve(all_trades)
    consecutive_stats(all_trades)

    # ── CSV dump ──
    if DUMP_CSV:
        dump_csv(all_trades)

    # ── Honest takeaways ──
    print(f"\n  {'═'*30} HONEST TAKEAWAYS {'═'*30}")
    print(f"  • Sample size: {len(all_trades)} trades over {len(dates)} days.")
    if len(all_trades) >= 30:
        print(f"    → Statistically meaningful for setup-level conclusions.")
    else:
        print(f"    → ⚠ Below 30 trades — directional indication only.")

    best_setup = max(
        ((st, *winrate([t for t in all_trades if t['type'] == st]),
          sum(1 for t in all_trades if t['type'] == st))
         for st in set(t['type'] for t in all_trades)),
        key=lambda x: x[1] if x[4] >= 10 else 0
    )
    if best_setup[4] >= 10:
        st, wr, avg, wins, n = best_setup
        print(f"  • Highest-conviction setup: {st} "
              f"({n} trades, {wr:.0f}% WR, +{avg:.1f} avg)")

    print(f"\n  📌 NEXT STEPS based on these results:")
    print(f"    1. Trade ONLY the top-2 setup × day-type combos in paper trading.")
    print(f"    2. Skip days where day-type prediction says SIDEWAYS_WIDE.")
    print(f"    3. Pass --csv to dump all trades for feature engineering.")
    con.close()


if __name__ == "__main__":
    main()