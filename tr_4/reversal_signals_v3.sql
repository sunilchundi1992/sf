-- ============================================================
--  REVERSAL DETECTION ENGINE
--  Identifies where price is likely to reverse, not continue.
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- STRUCTURAL LEVELS (where reversals tend to happen)
-- ─────────────────────────────────────────────────────────────

-- All resistance levels stacked (ranked by strength)
DROP VIEW IF EXISTS v_resistance_levels;
CREATE VIEW v_resistance_levels AS
SELECT symbol, strike AS level, 'MAX_CALL_OI' AS source,
       call_oi AS strength, 10 AS weight
  FROM v_top_oi_strikes WHERE call_oi_rank = 1
UNION ALL
SELECT symbol, strike, 'TOP3_CALL_OI', call_oi, 7
  FROM v_top_oi_strikes WHERE call_oi_rank BETWEEN 2 AND 3
UNION ALL
SELECT symbol, pdh, 'PDH', NULL, 8 FROM v_cpr_today
UNION ALL
SELECT symbol, r1, 'CPR_R1', NULL, 6 FROM v_cpr_today
UNION ALL
SELECT symbol, r2, 'CPR_R2', NULL, 9 FROM v_cpr_today
UNION ALL
SELECT symbol, tc, 'CPR_TC', NULL, 4 FROM v_cpr_today;

-- All support levels stacked (ranked by strength)
DROP VIEW IF EXISTS v_support_levels;
CREATE VIEW v_support_levels AS
SELECT symbol, strike AS level, 'MAX_PUT_OI' AS source,
       put_oi AS strength, 10 AS weight
  FROM v_top_oi_strikes WHERE put_oi_rank = 1
UNION ALL
SELECT symbol, strike, 'TOP3_PUT_OI', put_oi, 7
  FROM v_top_oi_strikes WHERE put_oi_rank BETWEEN 2 AND 3
UNION ALL
SELECT symbol, pdl, 'PDL', NULL, 8 FROM v_cpr_today
UNION ALL
SELECT symbol, s1, 'CPR_S1', NULL, 6 FROM v_cpr_today
UNION ALL
SELECT symbol, s2, 'CPR_S2', NULL, 9 FROM v_cpr_today
UNION ALL
SELECT symbol, bc, 'CPR_BC', NULL, 4 FROM v_cpr_today;

-- ─────────────────────────────────────────────────────────────
-- CURRENT SPOT (single source of truth)
-- ─────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_current_spot;
CREATE VIEW v_current_spot AS
WITH latest AS (
    SELECT symbol, MAX(datetime) AS mts
    FROM intraday_candles
    WHERE substr(datetime,1,10) =
          (SELECT MAX(substr(datetime,1,10)) FROM intraday_candles)
    GROUP BY symbol
)
SELECT i.symbol, i.datetime, i.close AS spot
FROM intraday_candles i
JOIN latest l ON i.symbol=l.symbol AND i.datetime=l.mts;

-- ─────────────────────────────────────────────────────────────
-- PROXIMITY — how close is spot to nearest S/R (in %)?
-- ─────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_reversal_proximity;
CREATE VIEW v_reversal_proximity AS
WITH s AS (SELECT symbol, spot FROM v_current_spot),
nearest_res AS (
    SELECT r.symbol,
           MIN(r.level) AS res_level,
           (SELECT source FROM v_resistance_levels r2
              WHERE r2.symbol=r.symbol AND r2.level >= s.spot
              ORDER BY r2.level ASC LIMIT 1) AS res_source,
           (SELECT weight FROM v_resistance_levels r2
              WHERE r2.symbol=r.symbol AND r2.level >= s.spot
              ORDER BY r2.level ASC LIMIT 1) AS res_weight
    FROM v_resistance_levels r
    JOIN s USING (symbol)
    WHERE r.level >= s.spot
    GROUP BY r.symbol
),
nearest_sup AS (
    SELECT r.symbol,
           MAX(r.level) AS sup_level,
           (SELECT source FROM v_support_levels r2
              WHERE r2.symbol=r.symbol AND r2.level <= s.spot
              ORDER BY r2.level DESC LIMIT 1) AS sup_source,
           (SELECT weight FROM v_support_levels r2
              WHERE r2.symbol=r.symbol AND r2.level <= s.spot
              ORDER BY r2.level DESC LIMIT 1) AS sup_weight
    FROM v_support_levels r
    JOIN s USING (symbol)
    WHERE r.level <= s.spot
    GROUP BY r.symbol
)
SELECT
    s.symbol, s.spot,
    nr.res_level, nr.res_source, nr.res_weight,
    ROUND((nr.res_level - s.spot) * 100.0 / s.spot, 3) AS dist_to_res_pct,
    ns.sup_level, ns.sup_source, ns.sup_weight,
    ROUND((s.spot - ns.sup_level) * 100.0 / s.spot, 3) AS dist_to_sup_pct
FROM s
LEFT JOIN nearest_res nr ON s.symbol=nr.symbol
LEFT JOIN nearest_sup ns ON s.symbol=ns.symbol;

-- ─────────────────────────────────────────────────────────────
-- EXHAUSTION SIGNALS — is momentum dying?
-- ─────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_exhaustion;
CREATE VIEW v_exhaustion AS
WITH last10 AS (
    SELECT
        symbol, datetime, open, high, low, close, volume,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY datetime DESC) AS rn
    FROM intraday_candles
    WHERE substr(datetime,1,10) =
          (SELECT MAX(substr(datetime,1,10)) FROM intraday_candles)
),
metrics AS (
    SELECT
        symbol,
        -- Volume trend: avg of last 3 vs prior 5
        AVG(CASE WHEN rn <= 3 THEN volume END)              AS vol_last3,
        AVG(CASE WHEN rn BETWEEN 4 AND 8 THEN volume END)   AS vol_prev5,
        -- Range trend: avg of last 3 vs prior 5
        AVG(CASE WHEN rn <= 3 THEN (high - low) END)        AS range_last3,
        AVG(CASE WHEN rn BETWEEN 4 AND 8 THEN (high - low) END) AS range_prev5,
        -- Body trend: how decisive are recent candles?
        AVG(CASE WHEN rn <= 3 THEN ABS(close - open) END)   AS body_last3,
        AVG(CASE WHEN rn BETWEEN 4 AND 8 THEN ABS(close - open) END) AS body_prev5,
        -- Direction of last 5 candles
        SUM(CASE WHEN rn <= 5 AND close > open THEN 1
                 WHEN rn <= 5 AND close < open THEN -1
                 ELSE 0 END) AS dir_last5
    FROM last10
    GROUP BY symbol
)
SELECT
    symbol,
    ROUND((vol_last3 - vol_prev5) * 100.0 / NULLIF(vol_prev5,0), 1)
        AS volume_change_pct,
    ROUND((range_last3 - range_prev5) * 100.0 / NULLIF(range_prev5,0), 1)
        AS range_change_pct,
    ROUND((body_last3 - body_prev5) * 100.0 / NULLIF(body_prev5,0), 1)
        AS body_change_pct,
    dir_last5,
    -- Boolean flags
    CASE WHEN vol_last3   < vol_prev5   * 0.8 THEN 1 ELSE 0 END AS vol_drying,
    CASE WHEN range_last3 < range_prev5 * 0.8 THEN 1 ELSE 0 END AS range_contracting,
    CASE WHEN body_last3  < body_prev5  * 0.7 THEN 1 ELSE 0 END AS bodies_shrinking
FROM metrics;

-- ─────────────────────────────────────────────────────────────
-- DIVERGENCES — when other markets disagree with current move
-- ─────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_divergences;
CREATE VIEW v_divergences AS
WITH today_moves AS (
    SELECT symbol, day_change_pct FROM v_sector_rs
)
SELECT
    'BANK_DIVERGE_NIFTY' AS divergence_type,
    CASE
      WHEN (SELECT day_change_pct FROM today_moves WHERE symbol='NIFTY50')
         - (SELECT day_change_pct FROM today_moves WHERE symbol='BANKNIFTY') > 0.3 THEN 1
      ELSE 0
    END AS is_diverging,
    'NIFTY rallying without bank support (weak rally)' AS interpretation
UNION ALL
SELECT
    'VIX_RISING_WITH_NIFTY',
    CASE
      WHEN (SELECT day_change_pct FROM today_moves WHERE symbol='NIFTY50') > 0
       AND (SELECT day_change_pct FROM today_moves WHERE symbol='INDIAVIX') > 0.5 THEN 1
      ELSE 0
    END,
    'VIX rising during rally (smart money buying protection — reversal warning)'
UNION ALL
SELECT
    'RELIANCE_DIVERGE_NIFTY',
    CASE
      WHEN ABS((SELECT day_change_pct FROM today_moves WHERE symbol='NIFTY50')
             - (SELECT day_change_pct FROM today_moves WHERE symbol='RELIANCE')) > 0.5 THEN 1
      ELSE 0
    END,
    'RELIANCE not confirming NIFTY direction (~9% weight divergence)';

-- ─────────────────────────────────────────────────────────────
-- ⭐ MASTER REVERSAL RADAR — combines everything into one score
-- ─────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_reversal_radar;
CREATE VIEW v_reversal_radar AS
WITH base AS (
    SELECT
        p.symbol, p.spot,
        p.res_level, p.res_source, p.dist_to_res_pct,
        p.sup_level, p.sup_source, p.dist_to_sup_pct,
        ex.vol_drying, ex.range_contracting, ex.bodies_shrinking,
        ex.volume_change_pct, ex.range_change_pct,
        pcr.pcr_current, pcr.pcr_regime, pcr.pcr_trend,
        mp.drift_direction, mp.max_pain_now,
        dt.day_type,
        (SELECT is_diverging FROM v_divergences WHERE divergence_type='BANK_DIVERGE_NIFTY')      AS bank_diverge,
        (SELECT is_diverging FROM v_divergences WHERE divergence_type='VIX_RISING_WITH_NIFTY')   AS vix_warn,
        (SELECT is_diverging FROM v_divergences WHERE divergence_type='RELIANCE_DIVERGE_NIFTY')  AS rel_diverge
    FROM v_reversal_proximity p
    LEFT JOIN v_exhaustion     ex  ON p.symbol = ex.symbol
    LEFT JOIN v_pcr_slope      pcr ON p.symbol = pcr.symbol
    LEFT JOIN v_maxpain_drift  mp  ON p.symbol = mp.symbol
    LEFT JOIN v_day_type       dt  ON p.symbol = dt.symbol
),
scored AS (
    SELECT
        b.*,
        -- ─── BEARISH REVERSAL SCORE (at resistance, expecting move DOWN) ───
        (CASE WHEN dist_to_res_pct < 0.05 THEN 3
              WHEN dist_to_res_pct < 0.15 THEN 2
              WHEN dist_to_res_pct < 0.30 THEN 1 ELSE 0 END) +    -- proximity
        (CASE WHEN res_source = 'MAX_CALL_OI' THEN 2 ELSE 0 END) + -- hard wall
        (CASE WHEN pcr_current >= 1.30 THEN 2
              WHEN pcr_current >= 1.15 THEN 1 ELSE 0 END) +        -- PCR exhaustion
        (CASE WHEN vol_drying = 1 THEN 1 ELSE 0 END) +
        (CASE WHEN range_contracting = 1 THEN 1 ELSE 0 END) +
        (CASE WHEN bodies_shrinking = 1 THEN 1 ELSE 0 END) +
        (CASE WHEN bank_diverge = 1 THEN 1 ELSE 0 END) +
        (CASE WHEN vix_warn = 1 THEN 1 ELSE 0 END) +
        (CASE WHEN day_type = 'RANGE_DAY' THEN 1 ELSE 0 END) +
        (CASE WHEN spot > max_pain_now * 1.005 THEN 1 ELSE 0 END)  -- pull toward MP
        AS bearish_reversal_score,

        -- ─── BULLISH REVERSAL SCORE (at support, expecting move UP) ───
        (CASE WHEN dist_to_sup_pct < 0.05 THEN 3
              WHEN dist_to_sup_pct < 0.15 THEN 2
              WHEN dist_to_sup_pct < 0.30 THEN 1 ELSE 0 END) +
        (CASE WHEN sup_source = 'MAX_PUT_OI' THEN 2 ELSE 0 END) +
        (CASE WHEN pcr_current <= 0.70 THEN 2
              WHEN pcr_current <= 0.85 THEN 1 ELSE 0 END) +
        (CASE WHEN vol_drying = 1 THEN 1 ELSE 0 END) +
        (CASE WHEN range_contracting = 1 THEN 1 ELSE 0 END) +
        (CASE WHEN bodies_shrinking = 1 THEN 1 ELSE 0 END) +
        (CASE WHEN day_type = 'RANGE_DAY' THEN 1 ELSE 0 END) +
        (CASE WHEN spot < max_pain_now * 0.995 THEN 1 ELSE 0 END)
        AS bullish_reversal_score
    FROM base b
)
SELECT
    symbol,
    spot,
    res_level, res_source, dist_to_res_pct,
    sup_level, sup_source, dist_to_sup_pct,
    pcr_current, day_type,
    bearish_reversal_score,
    bullish_reversal_score,
    CASE
        WHEN bearish_reversal_score >= 8 THEN '🔴 STRONG_REVERSAL_DOWN (high conviction)'
        WHEN bearish_reversal_score >= 6 THEN '🟠 LIKELY_REVERSAL_DOWN'
        WHEN bullish_reversal_score >= 8 THEN '🟢 STRONG_REVERSAL_UP (high conviction)'
        WHEN bullish_reversal_score >= 6 THEN '🟡 LIKELY_REVERSAL_UP'
        WHEN bearish_reversal_score >= 4 THEN '⚠ WATCH (bearish setup forming)'
        WHEN bullish_reversal_score >= 4 THEN '⚠ WATCH (bullish setup forming)'
        ELSE                                  '— TREND CONTINUATION (no reversal signal)'
    END AS verdict,
    vol_drying, range_contracting, bodies_shrinking,
    bank_diverge, vix_warn, rel_diverge
FROM scored;