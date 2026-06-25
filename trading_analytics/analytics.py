"""Quant analytics engine.

Every function is pure: it takes the master DataFrame (and/or the option-chain
DataFrame) and returns plain dicts/values. No I/O, no globals. This keeps each
metric independently testable and makes adding a new indicator a one-function
change.

Master columns expected:
  time, spot_price, open, high, low, close, volume, oi, change_oi, pcr, max_pain
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from config import settings

EPS = 1e-9


# --------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------
def _last(df: pd.DataFrame, col: str, default=np.nan):
    if df.empty or col not in df:
        return default
    s = df[col].dropna()
    return s.iloc[-1] if not s.empty else default


def _prev(df: pd.DataFrame, col: str, default=np.nan):
    if df.empty or col not in df:
        return default
    s = df[col].dropna()
    return s.iloc[-2] if len(s) >= 2 else default


def _pct(a, b) -> float:
    if b is None or b == 0 or np.isnan(b):
        return 0.0
    return float((a - b) / b * 100.0)


def _slope(series: pd.Series, window: int = 5) -> float:
    s = series.dropna().tail(window)
    if len(s) < 2:
        return 0.0
    x = np.arange(len(s))
    return float(np.polyfit(x, s.values, 1)[0])


def _bars_for_minutes(minutes: int) -> int:
    step = max(1, settings.interval_minutes)
    return max(1, minutes // step)


def _clip100(x: float) -> float:
    return float(max(0.0, min(100.0, x)))


# ====================================================================
# 1. PRICE vs OI
# ====================================================================
def price_oi(df: pd.DataFrame) -> dict:
    if len(df) < 2:
        return {"signal": "Insufficient data", "strength": 0, "duration_bars": 0}
    dp = _last(df, "close") - _prev(df, "close")
    doi = _last(df, "oi") - _prev(df, "oi")

    if dp > 0 and doi > 0:
        sig = "Long Buildup"
    elif dp < 0 and doi > 0:
        sig = "Short Buildup"
    elif dp > 0 and doi < 0:
        sig = "Short Covering"
    elif dp < 0 and doi < 0:
        sig = "Long Unwinding"
    else:
        sig = "Neutral"

    # strength = normalized magnitude of the price*oi move
    price_chg = abs(_pct(_last(df, "close"), _prev(df, "close")))
    oi_chg = abs(_pct(_last(df, "oi"), _prev(df, "oi")))
    strength = _clip100((price_chg * 8) + (oi_chg * 4))

    # duration: how many consecutive recent bars share the same signal
    duration = _signal_duration(df)
    return {
        "signal": sig,
        "strength": round(strength, 1),
        "duration_bars": duration,
        "price_change": round(float(dp), 2),
        "oi_change": round(float(doi), 0),
    }


def _signal_duration(df: pd.DataFrame) -> int:
    closes = df["close"].diff()
    ois = df["oi"].diff()
    if closes.empty:
        return 0
    last_sign = (np.sign(closes.iloc[-1]), np.sign(ois.iloc[-1]))
    count = 0
    for i in range(len(df) - 1, 0, -1):
        cur = (np.sign(closes.iloc[i]), np.sign(ois.iloc[i]))
        if cur == last_sign and not any(np.isnan(v) for v in cur):
            count += 1
        else:
            break
    return count


# ====================================================================
# 2. PRICE vs VOLUME
# ====================================================================
def price_volume(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    vol = df["volume"].dropna()
    cur_vol = _last(df, "volume", 0.0)
    avg_vol = float(vol.tail(20).mean()) if len(vol) else 0.0
    rvol = float(cur_vol / (avg_vol + EPS)) if avg_vol else 0.0
    spike_pct = _pct(cur_vol, avg_vol)

    if rvol >= settings.rvol_spike:
        state = "Volume Expansion"
    elif rvol <= 0.6:
        state = "Volume Contraction"
    else:
        state = "Normal"

    # breakout = volume spike + range expansion on the latest bar
    rng = _last(df, "high", 0) - _last(df, "low", 0)
    avg_rng = float((df["high"] - df["low"]).tail(20).mean()) if len(df) else 0.0
    breakout = bool(rvol >= settings.rvol_spike and rng > 1.3 * (avg_rng + EPS))

    return {
        "rvol": round(rvol, 2),
        "relative_volume": round(rvol, 2),
        "volume_spike_pct": round(spike_pct, 1),
        "state": state,
        "volume_breakout": breakout,
        "current_volume": round(float(cur_vol), 0),
        "avg_volume": round(avg_vol, 0),
    }


# ====================================================================
# 3. OI MOMENTUM (+ velocity / acceleration)
# ====================================================================
def oi_momentum(df: pd.DataFrame) -> dict:
    if len(df) < 2:
        return {}
    oi = df["oi"].astype(float)

    def mom(minutes: int) -> float:
        b = _bars_for_minutes(minutes)
        if len(oi) <= b:
            return 0.0
        return _pct(oi.iloc[-1], oi.iloc[-1 - b])

    growth_rate = _pct(oi.iloc[-1], oi.iloc[-2])
    velocity = float(oi.diff().iloc[-1]) / max(1, settings.interval_minutes)  # ΔOI/min
    accel = float(oi.diff().diff().iloc[-1]) if len(oi) >= 3 else 0.0

    if growth_rate > 0.5 and accel > 0:
        state = "Aggressive Position Building"
    elif growth_rate < -0.5:
        state = "Position Unwinding"
    else:
        state = "Stable"

    return {
        "oi_growth_rate_pct": round(growth_rate, 3),
        "momentum_5m": round(mom(5), 3),
        "momentum_15m": round(mom(15), 3),
        "momentum_30m": round(mom(30), 3),
        "oi_velocity_per_min": round(velocity, 1),
        "oi_acceleration": round(accel, 1),
        "state": state,
    }


# ====================================================================
# 4. PCR ANALYSIS
# ====================================================================
def pcr_analysis(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    pcr = df["pcr"].astype(float)
    cur = _last(df, "pcr")
    slope = _slope(pcr, _bars_for_minutes(15))
    momentum = float(pcr.iloc[-1] - pcr.iloc[-2]) if len(pcr) >= 2 else 0.0

    trend = "Rising" if slope > 0.002 else "Falling" if slope < -0.002 else "Flat"

    dp = _last(df, "close") - _prev(df, "close")
    if momentum > 0 and dp > 0:
        combo = "PCR Rising + Price Rising (put writers confident, bullish)"
    elif momentum > 0 and dp < 0:
        combo = "PCR Rising + Price Falling (put writers trapped, watch reversal)"
    elif momentum < 0 and dp > 0:
        combo = "PCR Falling + Price Rising (call writers covering, bullish)"
    else:
        combo = "PCR Falling + Price Falling (call writers confident, bearish)"

    if cur >= settings.pcr_bull_extreme:
        extreme = "Overbought / put-heavy (PCR high)"
    elif cur <= settings.pcr_bear_extreme:
        extreme = "Oversold / call-heavy (PCR low)"
    else:
        extreme = "Neutral zone"

    return {
        "current_pcr": round(float(cur), 3),
        "trend": trend,
        "slope": round(slope, 4),
        "momentum": round(momentum, 4),
        "price_combo": combo,
        "extreme": extreme,
    }


# ====================================================================
# 5. MAX PAIN
# ====================================================================
def max_pain(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    spot = _last(df, "close")
    mp = _last(df, "max_pain")
    prev_mp = _prev(df, "max_pain")
    dist = float(spot - mp) if not np.isnan(mp) else 0.0
    dist_pct = _pct(spot, mp)
    drift = float(mp - prev_mp) if not np.isnan(prev_mp) else 0.0

    pinning = bool(abs(dist_pct) <= settings.pinning_distance_pct)
    if pinning:
        behavior = "Pinning Effect (price magnetized to max pain)"
    elif abs(dist_pct) < 0.6:
        behavior = "Gravitation toward max pain"
    else:
        behavior = "Divergence from max pain"

    return {
        "current_max_pain": round(float(mp), 0),
        "spot": round(float(spot), 2),
        "distance": round(dist, 2),
        "distance_pct": round(dist_pct, 3),
        "max_pain_drift": round(drift, 1),
        "pinning": pinning,
        "behavior": behavior,
    }


# ====================================================================
# 6. TREND (EMA / VWAP)
# ====================================================================
def trend(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    close = df["close"].astype(float)
    ema9 = close.ewm(span=9, adjust=False).mean().iloc[-1]
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    ema50 = close.ewm(span=50, adjust=False).mean().iloc[-1]

    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].fillna(0)
    vwap = float((tp * vol).cumsum().iloc[-1] / (vol.cumsum().iloc[-1] + EPS))

    price = float(close.iloc[-1])
    score = 0
    score += 1 if price > ema9 else -1
    score += 1 if ema9 > ema20 else -1
    score += 1 if ema20 > ema50 else -1
    score += 1 if price > vwap else -1  # range -4..+4

    if score >= 3:
        direction = "Strong Bullish"
    elif score >= 1:
        direction = "Bullish"
    elif score <= -3:
        direction = "Strong Bearish"
    elif score <= -1:
        direction = "Bearish"
    else:
        direction = "Neutral"

    strength = _clip100(abs(score) / 4 * 100)
    return {
        "ema9": round(float(ema9), 2),
        "ema20": round(float(ema20), 2),
        "ema50": round(float(ema50), 2),
        "vwap": round(vwap, 2),
        "trend_score": score,
        "trend_strength": round(strength, 1),
        "direction": direction,
    }


# ====================================================================
# 7. INSTITUTIONAL ACTIVITY
# ====================================================================
def institutional(df: pd.DataFrame) -> dict:
    if len(df) < 2:
        return {}
    po = price_oi(df)
    pv = price_volume(df)
    sig = po.get("signal", "Neutral")

    label_map = {
        "Long Buildup": "Fresh Longs",
        "Short Buildup": "Fresh Shorts",
        "Short Covering": "Short Covering",
        "Long Unwinding": "Long Unwinding",
    }
    activity = label_map.get(sig, "No clear footprint")

    rvol = pv.get("rvol", 0.0)
    # institutional footprint = strong directional OI move on high volume
    conf = _clip100(po.get("strength", 0) * 0.6 + min(rvol, 3) / 3 * 40)
    if conf >= 60 and rvol >= settings.rvol_spike:
        if sig in ("Long Buildup", "Short Covering"):
            footprint = "Possible Institutional Entry (long side)"
        elif sig in ("Short Buildup",):
            footprint = "Possible Institutional Entry (short side)"
        else:
            footprint = "Possible Institutional Exit"
    else:
        footprint = "Retail / mixed flow"

    return {
        "activity": activity,
        "footprint": footprint,
        "confidence": round(conf, 1),
        "rvol": rvol,
    }


# ====================================================================
# 8. MARKET REGIME
# ====================================================================
def regime(df: pd.DataFrame) -> dict:
    if len(df) < 5:
        return {"regime": "Insufficient data"}
    close = df["close"].astype(float)
    ret = close.pct_change().dropna()
    vol = float(ret.tail(20).std() * 100)  # % volatility of returns
    slope = _slope(close, _bars_for_minutes(30))
    rng = float(close.tail(20).max() - close.tail(20).min())
    atr_like = float((df["high"] - df["low"]).tail(20).mean())
    directionality = abs(slope) / (atr_like + EPS)

    if directionality > 0.5 and slope > 0:
        reg = "Trending Up"
    elif directionality > 0.5 and slope < 0:
        reg = "Trending Down"
    elif vol > 0.25 and directionality <= 0.5:
        reg = "Volatile"
    elif directionality <= 0.2:
        reg = "Range Bound"
    else:
        reg = "Mean Reversion Mode"

    # breakout overrides if latest bar escapes the recent range on volume
    pv = price_volume(df)
    if pv.get("volume_breakout"):
        reg = "Breakout Mode"

    return {
        "regime": reg,
        "volatility_pct": round(vol, 3),
        "directionality": round(directionality, 3),
        "range_points": round(rng, 1),
    }


# ====================================================================
# 9. SMART MONEY
# ====================================================================
def smart_money(df: pd.DataFrame, chain: pd.DataFrame | None = None) -> dict:
    if len(df) < 2:
        return {}
    # net OI pressure: rising OI with rising price = bullish, etc.
    dp = _last(df, "close") - _prev(df, "close")
    doi = _last(df, "oi") - _prev(df, "oi")
    dvol = _last(df, "volume", 0)

    net_oi_pressure = float(np.sign(dp) * doi)
    net_vol_pressure = float(np.sign(dp) * dvol)

    pcr = _last(df, "pcr", 1.0)
    # bull/bear split blends PCR, trend score and OI pressure into 0..100
    tr = trend(df)
    base = 50 + tr.get("trend_score", 0) * 8
    base += 6 if net_oi_pressure > 0 else -6
    base += (pcr - 1.0) * 15  # high PCR (put writing) leans bullish
    bull_pct = _clip100(base)
    bear_pct = round(100 - bull_pct, 1)

    direction = "Bullish" if bull_pct > 58 else "Bearish" if bull_pct < 42 else "Neutral"

    pv = price_volume(df)
    participation = _clip100(min(pv.get("rvol", 0), 3) / 3 * 100)

    # Smart Money Index: directional conviction weighted by participation
    smi = round((bull_pct - 50) * 2 * (participation / 100), 1)

    return {
        "direction": direction,
        "bullish_pct": round(bull_pct, 1),
        "bearish_pct": bear_pct,
        "net_oi_pressure": round(net_oi_pressure, 0),
        "net_volume_pressure": round(net_vol_pressure, 0),
        "participation_score": round(participation, 1),
        "smart_money_index": smi,
    }


# ====================================================================
# ADVANCED METRICS (+ option-chain derived)
# ====================================================================
def advanced_metrics(df: pd.DataFrame, chain: pd.DataFrame | None = None) -> dict:
    out: dict = {}
    if len(df) >= 2:
        oi = df["oi"].astype(float)
        vol = df["volume"].astype(float)
        close = df["close"].astype(float)

        out["oi_velocity_per_min"] = round(
            float(oi.diff().iloc[-1]) / max(1, settings.interval_minutes), 1
        )
        out["oi_acceleration"] = round(
            float(oi.diff().diff().iloc[-1]) if len(oi) >= 3 else 0.0, 1
        )
        out["volume_to_oi_ratio"] = round(
            float(vol.iloc[-1] / (oi.iloc[-1] + EPS)), 5
        )
        price_chg = float(close.iloc[-1] - close.iloc[-2])
        out["price_efficiency"] = round(
            price_chg / (vol.iloc[-1] + EPS) * 1e5, 4
        )  # points moved per 100k volume
        out["pcr_momentum"] = round(
            float(df["pcr"].iloc[-1] - df["pcr"].iloc[-2]), 4
        )
        out["max_pain_drift"] = round(
            float(df["max_pain"].iloc[-1] - df["max_pain"].iloc[-2]), 1
        )

    # ---- option-chain derived (OI walls / writing pressure / pinning) ----
    if chain is not None and not chain.empty:
        out.update(_chain_metrics(df, chain))
    return out


def _chain_metrics(df: pd.DataFrame, chain: pd.DataFrame) -> dict:
    call_oi = chain["call_oi"].fillna(0)
    put_oi = chain["put_oi"].fillna(0)
    call_chg = chain["call_chg_oi"].fillna(0)
    put_chg = chain["put_chg_oi"].fillna(0)
    strikes = chain["strike_price"]

    total_call = float(call_oi.sum())
    total_put = float(put_oi.sum())

    resistance = float(strikes.iloc[int(call_oi.values.argmax())])
    support = float(strikes.iloc[int(put_oi.values.argmax())])

    # writing pressure = fresh OI added on each side
    call_writing = float(call_chg[call_chg > 0].sum())
    put_writing = float(put_chg[put_chg > 0].sum())

    # Net Option Pressure Index: -1 (call writers/bearish) .. +1 (put writers/bullish)
    denom = put_writing + call_writing + EPS
    nopi = float((put_writing - call_writing) / denom)

    # OI concentration: Herfindahl on total OI across strikes (0..1, higher=clustered)
    total_oi = call_oi + put_oi
    shares = (total_oi / (total_oi.sum() + EPS)) ** 2
    concentration = float(shares.sum())

    # Expiry pinning probability: closeness of spot to max-OI strike + concentration
    spot = _last(df, "close")
    max_oi_strike = float(strikes.iloc[int(total_oi.values.argmax())])
    nearness = 1.0 - min(1.0, abs(spot - max_oi_strike) / (max_oi_strike * 0.01 + EPS))
    pinning_prob = _clip100((0.6 * max(0, nearness) + 0.4 * concentration) * 100)

    return {
        "support_oi_wall": support,
        "resistance_oi_wall": resistance,
        "total_call_oi": round(total_call, 0),
        "total_put_oi": round(total_put, 0),
        "call_writing_pressure": round(call_writing, 0),
        "put_writing_pressure": round(put_writing, 0),
        "net_option_pressure_index": round(nopi, 3),
        "oi_concentration_score": round(concentration, 4),
        "max_oi_strike": max_oi_strike,
        "expiry_pinning_probability": round(pinning_prob, 1),
    }


# ====================================================================
# Aggregate everything for the API
# ====================================================================
def compute_all(df: pd.DataFrame, chain: pd.DataFrame | None = None) -> dict:
    return {
        "price_oi": price_oi(df),
        "price_volume": price_volume(df),
        "oi_momentum": oi_momentum(df),
        "pcr": pcr_analysis(df),
        "max_pain": max_pain(df),
        "trend": trend(df),
        "institutional": institutional(df),
        "regime": regime(df),
        "smart_money": smart_money(df, chain),
        "advanced": advanced_metrics(df, chain),
    }
