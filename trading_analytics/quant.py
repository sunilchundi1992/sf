"""Quant decision layer.

Turns the raw analytics into:
  - a single market bias (Bullish/Bearish/Neutral) + confidence 0-100
  - the summary cards payload
  - ranked "market-understanding points": each is an evidence-backed,
    plain-language conclusion of the form
        metric -> current reading -> implication -> conviction(0-100)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import analytics
from config import settings


# --------------------------------------------------------------------
# Bias + confidence
# --------------------------------------------------------------------
def market_bias(a: dict) -> dict:
    """Weighted vote across signals -> bias + confidence (0-100)."""
    score = 0.0   # signed, roughly -100..+100
    weight = 0.0

    def vote(value: float, w: float):
        nonlocal score, weight
        score += value * w
        weight += w

    tr = a.get("trend", {})
    vote(tr.get("trend_score", 0) / 4 * 100, 0.30)

    sm = a.get("smart_money", {})
    vote((sm.get("bullish_pct", 50) - 50) * 2, 0.25)

    po = a.get("price_oi", {})
    po_map = {
        "Long Buildup": 100, "Short Covering": 60,
        "Short Buildup": -100, "Long Unwinding": -60, "Neutral": 0,
    }
    vote(po_map.get(po.get("signal", "Neutral"), 0), 0.20)

    pcr = a.get("pcr", {})
    vote(np.sign(pcr.get("momentum", 0)) * 50, 0.10)

    adv = a.get("advanced", {})
    vote(adv.get("net_option_pressure_index", 0) * 100, 0.15)

    final = score / (weight + 1e-9)
    if final > 15:
        bias = "Bullish"
    elif final < -15:
        bias = "Bearish"
    else:
        bias = "Neutral"

    # confidence = magnitude of conviction, lifted by agreement & participation
    participation = sm.get("participation_score", 0)
    confidence = min(100.0, abs(final) * 0.8 + participation * 0.2)
    return {
        "bias": bias,
        "confidence": round(confidence, 1),
        "raw_score": round(final, 1),
    }


# --------------------------------------------------------------------
# Summary cards
# --------------------------------------------------------------------
def summary_cards(df: pd.DataFrame, a: dict) -> dict:
    if df.empty:
        return {}
    mp = a.get("max_pain", {})
    bias = market_bias(a)
    last = df.iloc[-1]
    return {
        "time": last["time"],
        "spot_price": _r(last.get("spot_price"), 2),
        "pcr": _r(last.get("pcr"), 3),
        "max_pain": _r(last.get("max_pain"), 0),
        "distance_from_max_pain": mp.get("distance"),
        "distance_pct": mp.get("distance_pct"),
        "oi": _r(last.get("oi"), 0),
        "change_oi": _r(last.get("change_oi"), 0),
        "volume": _r(last.get("volume"), 0),
        "market_bias": bias["bias"],
        "confidence": bias["confidence"],
    }


def _r(v, n):
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------
# Market-understanding points (the core deliverable)
# --------------------------------------------------------------------
def understanding_points(df: pd.DataFrame, a: dict) -> list[dict]:
    """Ranked list of evidence-backed conclusions."""
    pts: list[dict] = []

    def add(category, headline, evidence, implication, conviction, tone):
        pts.append({
            "category": category,
            "headline": headline,
            "evidence": evidence,
            "implication": implication,
            "conviction": round(_clip100(conviction), 1),
            "tone": tone,  # bullish / bearish / neutral
        })

    po = a.get("price_oi", {})
    pv = a.get("price_volume", {})
    oim = a.get("oi_momentum", {})
    pcr = a.get("pcr", {})
    mp = a.get("max_pain", {})
    tr = a.get("trend", {})
    inst = a.get("institutional", {})
    reg = a.get("regime", {})
    sm = a.get("smart_money", {})
    adv = a.get("advanced", {})

    # --- Price vs OI ---
    if po:
        tone_map = {
            "Long Buildup": "bullish", "Short Covering": "bullish",
            "Short Buildup": "bearish", "Long Unwinding": "bearish",
            "Neutral": "neutral",
        }
        tone = tone_map.get(po.get("signal"), "neutral")
        add(
            "Price/OI", po.get("signal"),
            f"ΔPrice {po.get('price_change')} pts with ΔOI {po.get('oi_change'):,.0f} "
            f"sustained {po.get('duration_bars')} bars",
            _po_implication(po.get("signal")),
            po.get("strength", 0), tone,
        )

    # --- Volume ---
    if pv:
        if pv.get("volume_breakout"):
            add("Volume", "Volume breakout",
                f"RVOL {pv.get('rvol')}x with range expansion",
                "Breakout has participation behind it; move likely to follow through.",
                min(100, pv.get("rvol", 0) * 30), "neutral")
        elif pv.get("state") == "Volume Expansion":
            add("Volume", "Volume expansion",
                f"RVOL {pv.get('rvol')}x ({pv.get('volume_spike_pct')}% vs avg)",
                "Rising participation confirms the prevailing move.",
                min(100, pv.get("rvol", 0) * 25), "neutral")
        elif pv.get("state") == "Volume Contraction":
            add("Volume", "Volume contraction",
                f"RVOL {pv.get('rvol')}x",
                "Thin participation; current move lacks conviction, fade risk.",
                40, "neutral")

    # --- OI momentum ---
    if oim:
        if oim.get("state") == "Aggressive Position Building":
            add("OI Momentum", "Aggressive position building",
                f"OI vel {oim.get('oi_velocity_per_min'):,.0f}/min, "
                f"accel {oim.get('oi_acceleration'):,.0f}, 15m mom {oim.get('momentum_15m')}%",
                "Positions being added fast — conviction behind the current direction.",
                min(100, abs(oim.get("momentum_15m", 0)) * 20 + 40),
                "neutral")
        elif oim.get("state") == "Position Unwinding":
            add("OI Momentum", "Position unwinding",
                f"OI growth {oim.get('oi_growth_rate_pct')}%, 15m mom {oim.get('momentum_15m')}%",
                "Open positions being closed — trend may be losing fuel.",
                min(100, abs(oim.get("momentum_15m", 0)) * 20 + 30),
                "neutral")

    # --- PCR ---
    if pcr:
        tone = "bullish" if pcr.get("momentum", 0) > 0 else "bearish"
        add("PCR", f"PCR {pcr.get('current_pcr')} {pcr.get('trend').lower()}",
            f"slope {pcr.get('slope')}, momentum {pcr.get('momentum')}; {pcr.get('extreme')}",
            pcr.get("price_combo"),
            min(100, abs(pcr.get("slope", 0)) * 8000 + 35), tone)

    # --- Max pain ---
    if mp:
        if mp.get("pinning"):
            add("Max Pain", "Expiry pinning pressure",
                f"Spot {mp.get('spot')} vs Max Pain {mp.get('current_max_pain'):,.0f} "
                f"({mp.get('distance_pct')}%), drift {mp.get('max_pain_drift')}",
                "Price magnetized toward max pain; expect range-bound pinning into expiry.",
                adv.get("expiry_pinning_probability", 60), "neutral")
        else:
            tone = "bullish" if mp.get("distance", 0) > 0 else "bearish"
            add("Max Pain", mp.get("behavior"),
                f"Distance {mp.get('distance')} pts ({mp.get('distance_pct')}%) "
                f"from max pain {mp.get('current_max_pain'):,.0f}",
                "Spot trading away from max pain — directional, not pinned.",
                min(100, abs(mp.get("distance_pct", 0)) * 60 + 30), tone)

    # --- Trend ---
    if tr:
        tone = "bullish" if tr.get("trend_score", 0) > 0 else "bearish" if tr.get("trend_score", 0) < 0 else "neutral"
        add("Trend", tr.get("direction"),
            f"EMA9 {tr.get('ema9')} / EMA20 {tr.get('ema20')} / EMA50 {tr.get('ema50')}, "
            f"VWAP {tr.get('vwap')}, score {tr.get('trend_score')}/4",
            "EMA stack and VWAP alignment define the intraday trend backbone.",
            tr.get("trend_strength", 0), tone)

    # --- Institutional ---
    if inst and inst.get("footprint", "").startswith("Possible Institutional"):
        tone = "bullish" if "long" in inst.get("footprint", "").lower() else "bearish"
        add("Institutional", inst.get("footprint"),
            f"{inst.get('activity')} on RVOL {inst.get('rvol')}x",
            "Large directional flow detected — institutional footprint likely.",
            inst.get("confidence", 0), tone)

    # --- Smart money / option pressure ---
    if sm:
        tone = "bullish" if sm.get("direction") == "Bullish" else "bearish" if sm.get("direction") == "Bearish" else "neutral"
        add("Smart Money", f"Smart money {sm.get('direction').lower()}",
            f"Bull {sm.get('bullish_pct')}% / Bear {sm.get('bearish_pct')}%, "
            f"SMI {sm.get('smart_money_index')}, participation {sm.get('participation_score')}",
            "Aggregated OI/volume/PCR pressure points this direction.",
            abs(sm.get("smart_money_index", 0)) + 40, tone)

    if adv.get("net_option_pressure_index") is not None:
        nopi = adv["net_option_pressure_index"]
        tone = "bullish" if nopi > 0 else "bearish"
        add("Option Pressure", "Net option writing pressure",
            f"NOPI {nopi} (put writing {adv.get('put_writing_pressure', 0):,.0f} vs "
            f"call writing {adv.get('call_writing_pressure', 0):,.0f})",
            "Put writing dominance is bullish support; call writing dominance is bearish.",
            min(100, abs(nopi) * 100 + 30), tone)

    if adv.get("support_oi_wall") is not None:
        add("OI Walls", "Key OI walls",
            f"Support {adv.get('support_oi_wall'):,.0f} (max put OI), "
            f"Resistance {adv.get('resistance_oi_wall'):,.0f} (max call OI), "
            f"concentration {adv.get('oi_concentration_score')}",
            "These strikes act as intraday magnets / barriers for price.",
            min(100, adv.get("oi_concentration_score", 0) * 200 + 40), "neutral")

    # --- Regime headline (always last, frames everything) ---
    if reg:
        add("Regime", f"Market regime: {reg.get('regime')}",
            f"volatility {reg.get('volatility_pct')}%, directionality {reg.get('directionality')}, "
            f"range {reg.get('range_points')} pts",
            _regime_implication(reg.get("regime")),
            60, "neutral")

    pts.sort(key=lambda p: p["conviction"], reverse=True)
    return pts


def _po_implication(sig: str) -> str:
    return {
        "Long Buildup": "Fresh longs entering — bullish continuation favored.",
        "Short Buildup": "Fresh shorts entering — bearish pressure building.",
        "Short Covering": "Shorts exiting — short-term bullish pop, may not sustain.",
        "Long Unwinding": "Longs exiting — weakness, trend losing support.",
        "Neutral": "No clear positioning bias.",
    }.get(sig, "")


def _regime_implication(reg: str) -> str:
    return {
        "Trending Up": "Favor longs on dips; momentum strategies work.",
        "Trending Down": "Favor shorts on rallies; momentum strategies work.",
        "Range Bound": "Fade extremes toward the mean; avoid breakout bets.",
        "Volatile": "Wider stops, reduced size; whipsaw risk high.",
        "Breakout Mode": "Trade in breakout direction with confirmation.",
        "Mean Reversion Mode": "Expect pullbacks toward VWAP / max pain.",
    }.get(reg, "")


def _clip100(x: float) -> float:
    return float(max(0.0, min(100.0, x)))


# --------------------------------------------------------------------
# One call -> full payload for the dashboard
# --------------------------------------------------------------------
def full_payload(df: pd.DataFrame, chain: pd.DataFrame | None = None) -> dict:
    a = analytics.compute_all(df, chain)
    return {
        "summary": summary_cards(df, a),
        "analytics": a,
        "understanding": understanding_points(df, a),
    }
