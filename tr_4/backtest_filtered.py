
"""
FILTERED BACKTEST — Only the 3TR_SHORT_PRIME setup.
Tests if the high-probability subset is genuinely tradeable across all 23 days.

Usage:
    py backtest_filtered.py NIFTY50
"""

import sys
import sqlite3
from pathlib import Path
from collections import defaultdict
from setup_scanner_v3 import (
    resample, get_cpr, get_oi_walls,
    resistance_levels, support_levels,
    compute_structure_states,
    detect_3tr,
    simulate_trade,
)

DB_PATH = Path(__file__).parent / "db" / "trading_data.db"


def run_filtered_day(con, symbol, date, n_min=3):
    """Run ONLY 3TR_SHORT_PRIME setup with strict filters."""
    bars = resample(con, symbol, date, n_min)
    if len(bars) < 15:
        return [], None

    cpr = get_cpr(con, symbol, date)
    if not cpr:
        return [], None

    # ── HARD FILTER 1: Day-type ──
    if cpr['day_type'] not in ('NORMAL_DAY', 'TREND_DAY'):
        return [], cpr  # SKIP this entire day

    ce_walls, pe_walls = get_oi_walls(con, symbol, date)
    res_lvls = resistance_levels(cpr, ce_walls)
    sup_lvls = support_levels(cpr, pe_walls)

    states, _, _ = compute_structure_states(bars)
    or_n = max(1, 30 // n_min)

    trades = []
    touches = {'res': {}, 'sup': {}}
    losses_today = 0

    for i in range(len(bars)):
        if i < or_n:
            continue

        bar_time = bars[i][0][11:16]

        # ── HARD FILTER 2: Time window 10:30 – 12:30 ──
        if not ('10:30' <= bar_time < '12:30'):
            # Still need to count touches even outside window for accurate state
            detect_3tr(bars, i, res_lvls, sup_lvls, touches, states[i])
            continue

        # ── HARD FILTER 3: Max 2 losses per day ──
        if losses_today >= 2:
            continue

        st = states[i]

        # ── HARD FILTER 4: Only SHORT side ──
        if st not in ('TOPPED', 'DOWNTREND'):
            continue

        setup = detect_3tr(bars, i, res_lvls, sup_lvls, touches, st)
        if not setup:
            continue

        # ── HARD FILTER 5: Only 3TR_SHORT, not 3TR_LONG ──
        if setup['type'] != '3TR_SHORT':
            continue

        # ── HARD FILTER 6: Touch count must be 3–6 ──
        if not (3 <= setup['touch_count'] <= 6):
            continue

        # Simulate
        trade = simulate_trade(setup, bars[i:], max_hold=15)
        if not trade:
            continue

        trades.append({
            **setup, **trade,
            'timeframe_min': n_min,
            'day_type': cpr['day_type'],
            'date': date,
            'symbol': symbol,
        })

        if trade['outcome'] == 'LOSS':
            losses_today += 1

    return trades, cpr


def winrate(trades):
    if not trades:
        return 0, 0, 0
    wins = sum(1 for t in trades if t['outcome'] == 'WIN')
    return wins * 100.0 / len(trades), sum(t['pnl'] for t in trades) / len(trades), wins


def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 else 'NIFTY50'
    con = sqlite3.connect(DB_PATH)
    dates = [r[0] for r in con.execute("""
        SELECT DISTINCT substr(datetime, 1, 10) AS d
        FROM intraday_candles
        WHERE symbol = ?
        ORDER BY d
    """, (symbol,)).fetchall()]

    print(f"\n{'═'*82}")
    print(f"  🎯 FILTERED BACKTEST — 3TR_SHORT_PRIME — {symbol}")
    print(f"  Strategy: 3TR_SHORT only | 10:30–12:30 | NORMAL/TREND days | touch 3–6")
    print(f"  Risk: max 2 losses/day | max hold 15 bars (45 min)")
    print(f"  Dates: {dates[0]} → {dates[-1]} ({len(dates)} days)")
    print(f"{'═'*82}")

    all_trades = []
    days_skipped = 0
    days_traded = 0
    days_no_signal = 0

    print(f"\n  {'Date':<12} {'Day-Type':<12} {'Trades':>7} {'Wins':>5} "
          f"{'Day P&L':>9}  Notes")
    print(f"  {'─'*12} {'─'*12} {'─'*7} {'─'*5} {'─'*9}  {'─'*30}")

    for d in dates:
        trades, cpr = run_filtered_day(con, symbol, d, n_min=3)
        if not cpr:
            continue
        if cpr['day_type'] not in ('NORMAL_DAY', 'TREND_DAY'):
            days_skipped += 1
            print(f"  {d:<12} {cpr['day_type']:<12} {'—':>7} {'—':>5} "
                  f"{'—':>9}  SKIPPED ({cpr['day_type']})")
            continue
        if not trades:
            days_no_signal += 1
            print(f"  {d:<12} {cpr['day_type']:<12} {0:>7} {0:>5} "
                  f"{0:>+9.1f}  no signal in window")
            continue
        days_traded += 1
        wins = sum(1 for t in trades if t['outcome'] == 'WIN')
        day_pnl = sum(t['pnl'] for t in trades)
        all_trades.extend(trades)
        print(f"  {d:<12} {cpr['day_type']:<12} {len(trades):>7} {wins:>5} "
              f"{day_pnl:>+9.1f}")

    # ── Summary ──
    print(f"\n  {'═'*30} SUMMARY {'═'*40}")
    print(f"  Days analyzed:    {len(dates)}")
    print(f"  Days SKIPPED:     {days_skipped} (wrong day-type)")
    print(f"  Days NO SIGNAL:   {days_no_signal} (filters too tight)")
    print(f"  Days TRADED:      {days_traded}")

    if not all_trades:
        print(f"\n  ❌ No trades fired with filters. Loosen criteria or accept zero.")
        return

    wr, avg, wins = winrate(all_trades)
    total = sum(t['pnl'] for t in all_trades)
    losses = len(all_trades) - wins
    avg_win = sum(t['pnl'] for t in all_trades if t['outcome'] == 'WIN') / max(wins, 1)
    avg_loss = sum(t['pnl'] for t in all_trades if t['outcome'] == 'LOSS') / max(losses, 1)

    print(f"\n  📈 OVERALL RESULTS")
    print(f"     Total trades:   {len(all_trades)}")
    print(f"     Wins / Losses:  {wins} / {losses}")
    print(f"     Win rate:       {wr:.1f}%")
    print(f"     Avg win:        {avg_win:+.1f} pts")
    print(f"     Avg loss:       {avg_loss:+.1f} pts")
    print(f"     Avg P&L/trade:  {avg:+.1f} pts")
    print(f"     Total P&L:      {total:+.0f} pts")
    print(f"     Trades/day:     {len(all_trades) / max(days_traded, 1):.1f}")

    # Expectancy & R:R
    if avg_loss < 0:
        expectancy = (wr / 100 * avg_win) + ((1 - wr / 100) * avg_loss)
        rr = abs(avg_win / avg_loss)
        print(f"     R:R achieved:   {rr:.2f}")
        print(f"     Expectancy:     {expectancy:+.2f} pts/trade")

    # Equity curve
    print(f"\n  💰 Equity Curve")
    by_day = defaultdict(float)
    for t in all_trades:
        by_day[t['date']] += t['pnl']
    cumulative = 0
    max_dd = 0
    peak = 0
    for d in sorted(by_day):
        cumulative += by_day[d]
        peak = max(peak, cumulative)
        dd = peak - cumulative
        max_dd = max(max_dd, dd)
    print(f"     Peak P&L:       {peak:+.0f} pts")
    print(f"     Max drawdown:   {max_dd:.0f} pts (from peak)")
    print(f"     Final P&L:      {cumulative:+.0f} pts")

    # Streaks
    sorted_t = sorted(all_trades, key=lambda t: (t['date'], t['ts']))
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
    print(f"\n  🎲 Max win streak:  {max_w}")
    print(f"     Max loss streak: {max_l}")
    print(f"     → with 0.5% risk/trade, worst drawdown ≈ {max_l * 0.5}% of capital")

    # Verdict
    print(f"\n  {'═'*30} HONEST VERDICT {'═'*40}")
    if wr >= 65 and len(all_trades) >= 30 and max_l <= 5:
        print(f"  ✅ TRADEABLE EDGE confirmed.")
        print(f"     • Win rate {wr:.0f}% beats threshold (65%)")
        print(f"     • Sample size {len(all_trades)} is statistically valid")
        print(f"     • Max loss streak {max_l} is manageable")
        print(f"     → Start paper trading Monday with 0.5% risk/trade.")
    elif wr >= 60 and len(all_trades) >= 20:
        print(f"  🟡 MARGINAL EDGE — needs more validation.")
        print(f"     • Win rate {wr:.0f}% is decent but not exceptional")
        print(f"     • Paper trade for 2 more weeks before live")
    else:
        print(f"  ❌ NO CLEAR EDGE in this filter set.")
        print(f"     • Either the OI proxy is misleading OR")
        print(f"     • The setup logic needs further refinement OR")
        print(f"     • Sample is still too small")
        print(f"     → Wait for fresh daily OI data (Mon-Fri) before next iteration.")

    con.close()


if __name__ == "__main__":
    if not DB_PATH.exists():
        sys.exit(f"❌ DB not found: {DB_PATH}")
    main()
