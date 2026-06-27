-- ============================================================
--  NIFTY ANALYTICS — Reusable Views & Signals
--  Run once to create views: sqlite3 db/trading_data.db < insights.sql
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- LAYER 1 — FOUNDATION VIEWS
-- ─────────────────────────────────────────────────────────────

-- 1.1) Trading session context (yesterday / today)
DROP VIEW IF EXISTS v_session_context;
CREATE VIEW v_session_context AS
WITH ranked AS (
    SELECT symbol, date, open, high, low, close, volume,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
    FROM daily_candles
)
SELECT
    t.symbol,
    y.date  AS y_date,
    y.open  AS y_open,  y.high AS y_high,
    y.low   AS y_low,   y.close AS y_close,
    t.date  AS t_date,
    t.open  AS t_open,  t.high AS t_high,
    t.low   AS t_low,   t.close AS t_close
FROM ranked t
JOIN ranked y
  ON t.symbol = y.symbol AND y.rn = t.rn + 1
WHERE t.rn = 1;

-- 1.2) CPR (Central Pivot Range) + PDH / PDL for TODAY
--      Based on yesterday's H/L/C
DROP VIEW IF EXISTS v_cpr_today;
CREATE VIEW v_cpr_today AS
SELECT
    symbol, y_date, t_date,
    y_high                              AS pdh,
    y_low                               AS pdl,
    y_close                             AS pdc,
    (y_high + y_low + y_close) / 3.0    AS pivot,
    (y_high + y_low) / 2.0              AS bc,
    ((y_high + y_low + y_close) / 3.0) * 2 - ((y_high + y_low) / 2.0)  AS tc,
    -- CPR width as % of price → classifies day-type
    ROUND(ABS(((y_high + y_low + y_close) / 3.0) * 2
              - ((y_high + y_low) / 2.0)
              - ((y_high + y_low) / 2.0)) * 100.0 / y_close, 3) AS cpr_width_pct,
    -- Standard pivot levels
    2 * ((y_high + y_low + y_close) / 3.0) - y_low   AS r1,
    2 * ((y_high + y_low + y_close) / 3.0) - y_high  AS s1,
    ((y_high + y_low + y_close) / 3.0)
        + (y_high - y_low)                            AS r2,
    ((y_high + y_low + y_close) / 3.0)
        - (y_high - y_low)                            AS s2
FROM v_session_context;

-- 1.3) Day-type classifier (CPR width based)
DROP VIEW IF EXISTS v_day_type;
CREATE VIEW v_day_type AS
SELECT
    symbol, t_date, cpr_width_pct,
    CASE
        WHEN cpr_width_pct < 0.15 THEN 'TREND_DAY'
        WHEN cpr_width_pct < 0.40 THEN 'NORMAL_DAY'
        WHEN cpr_width_pct < 0.80 THEN 'RANGE_DAY'
        ELSE                            'SIDEWAYS_WIDE'
    END AS day_type
FROM v_cpr_today;

-- 1.4) Gap classification (today's open vs yesterday's close)
DROP VIEW IF EXISTS v_gap_today;
CREATE VIEW v_gap_today AS
SELECT
    symbol, t_date,
    y_close,
    t_open,
    ROUND((t_open - y_close), 2)                       AS gap_pts,
    ROUND((t_open - y_close) * 100.0 / y_close, 3)     AS gap_pct,
    CASE
        WHEN ABS((t_open - y_close) * 100.0 / y_close) < 0.10 THEN 'FLAT'
        WHEN (t_open - y_close) * 100.0 / y_close >=  0.75    THEN 'STRONG_GAP_UP'
        WHEN (t_open - y_close) * 100.0 / y_close >=  0.25    THEN 'GAP_UP'
        WHEN (t_open - y_close) * 100.0 / y_close <= -0.75    THEN 'STRONG_GAP_DOWN'
        WHEN (t_open - y_close) * 100.0 / y_close <= -0.25    THEN 'GAP_DOWN'
        ELSE 'FLAT'
    END AS gap_type
FROM v_session_context;

-- 1.5) VWAP — intraday cumulative for today
DROP VIEW IF EXISTS v_vwap_today;
CREATE VIEW v_vwap_today AS
WITH today_data AS (
    SELECT
        symbol, datetime, close, volume,
        substr(datetime, 1, 10) AS d
    FROM intraday_candles
    WHERE substr(datetime, 1, 10) =
          (SELECT MAX(substr(datetime, 1, 10)) FROM intraday_candles)
)
SELECT
    symbol,
    datetime,
    close,
    volume,
    SUM(close * volume) OVER (PARTITION BY symbol ORDER BY datetime
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    * 1.0 /
    NULLIF(SUM(volume) OVER (PARTITION BY symbol ORDER BY datetime
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)
    AS vwap
FROM today_data;

-- 1.6) Opening Range (first 15 min) — High / Low / Mid
DROP VIEW IF EXISTS v_orb_today;
CREATE VIEW v_orb_today AS
WITH today_first15 AS (
    SELECT symbol, high, low
    FROM intraday_candles
    WHERE substr(datetime, 1, 10) =
          (SELECT MAX(substr(datetime, 1, 10)) FROM intraday_candles)
      AND time(datetime) BETWEEN '09:15:00' AND '09:29:59'
)
SELECT
    symbol,
    MAX(high) AS or_high,
    MIN(low)  AS or_low,
    (MAX(high) + MIN(low)) / 2.0 AS or_mid,
    MAX(high) - MIN(low)         AS or_range
FROM today_first15
GROUP BY symbol;

-- 1.7) 3-min resampled candles (for cleaner intraday analysis)
DROP VIEW IF EXISTS v_candles_3m;
CREATE VIEW v_candles_3m AS
WITH bucketed AS (
    SELECT
        symbol,
        substr(datetime, 1, 14) ||
            printf('%02d:00', (CAST(substr(datetime, 15, 2) AS INT) / 3) * 3)
            AS bucket,
        datetime, open, high, low, close, volume, oi
    FROM intraday_candles
)
SELECT
    symbol,
    bucket AS datetime,
    (SELECT b2.open  FROM bucketed b2
      WHERE b2.symbol=b1.symbol AND b2.bucket=b1.bucket
      ORDER BY b2.datetime ASC  LIMIT 1) AS open,
    MAX(high) AS high,
    MIN(low)  AS low,
    (SELECT b2.close FROM bucketed b2
      WHERE b2.symbol=b1.symbol AND b2.bucket=b1.bucket
      ORDER BY b2.datetime DESC LIMIT 1) AS close,
    SUM(volume) AS volume,
    MAX(oi)     AS oi
FROM bucketed b1
GROUP BY symbol, bucket;

-- ─────────────────────────────────────────────────────────────
-- LAYER 2 — SIGNAL QUERIES (PCR, OI, COI, Max Pain)
-- ─────────────────────────────────────────────────────────────

-- 2.1) PCR slope & regime — last 5 buckets vs prior 5 buckets
DROP VIEW IF EXISTS v_pcr_slope;
CREATE VIEW v_pcr_slope AS
WITH ranked AS (
    SELECT
        symbol, expiry, date, broker_time, pcr, spot_price, max_pain,
        ROW_NUMBER() OVER (PARTITION BY symbol, expiry, date
                           ORDER BY broker_time DESC) AS rn
    FROM max_pain_pcr_intraday
    WHERE date = (SELECT MAX(date) FROM max_pain_pcr_intraday)
),
agg AS (
    SELECT
        symbol, expiry, date,
        AVG(CASE WHEN rn <=  5 THEN pcr END) AS pcr_last5,
        AVG(CASE WHEN rn BETWEEN 6 AND 10 THEN pcr END) AS pcr_prev5,
        MAX(CASE WHEN rn = 1 THEN pcr END)        AS pcr_current,
        MAX(CASE WHEN rn = 1 THEN spot_price END) AS spot_now,
        MAX(CASE WHEN rn = 1 THEN max_pain END)   AS max_pain_now,
        MAX(CASE WHEN rn = 1 THEN broker_time END) AS last_bucket
    FROM ranked
    GROUP BY symbol, expiry, date
)
SELECT
    symbol, expiry, date, last_bucket,
    ROUND(pcr_current, 3)   AS pcr_current,
    ROUND(pcr_last5,  3)    AS pcr_avg_last5,
    ROUND(pcr_prev5,  3)    AS pcr_avg_prev5,
    ROUND(pcr_last5 - pcr_prev5, 3) AS pcr_slope_5b,
    CASE
        WHEN pcr_current >= 1.30                              THEN 'STRONG_BULLISH'
        WHEN pcr_current >= 1.05                              THEN 'BULLISH'
        WHEN pcr_current <= 0.70                              THEN 'STRONG_BEARISH'
        WHEN pcr_current <= 0.90                              THEN 'BEARISH'
        ELSE                                                       'NEUTRAL'
    END AS pcr_regime,
    CASE
        WHEN (pcr_last5 - pcr_prev5) >  0.05 THEN 'RISING'
        WHEN (pcr_last5 - pcr_prev5) < -0.05 THEN 'FALLING'
        ELSE                                       'FLAT'
    END AS pcr_trend,
    spot_now, max_pain_now,
    ROUND(spot_now - max_pain_now, 2) AS spot_vs_maxpain
FROM agg;

-- 2.2) Max Pain drift — intraday movement
DROP VIEW IF EXISTS v_maxpain_drift;
CREATE VIEW v_maxpain_drift AS
WITH today AS (
    SELECT
        symbol, expiry, date, broker_time, max_pain, spot_price,
        ROW_NUMBER() OVER (PARTITION BY symbol, expiry, date
                           ORDER BY broker_time) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY symbol, expiry, date
                           ORDER BY broker_time DESC) AS rn_desc
    FROM max_pain_pcr_intraday
    WHERE date = (SELECT MAX(date) FROM max_pain_pcr_intraday)
)
SELECT
    symbol, expiry, date,
    MAX(CASE WHEN rn_asc  = 1 THEN broker_time END)  AS first_bucket,
    MAX(CASE WHEN rn_desc = 1 THEN broker_time END)  AS last_bucket,
    MAX(CASE WHEN rn_asc  = 1 THEN max_pain END)     AS max_pain_open,
    MAX(CASE WHEN rn_desc = 1 THEN max_pain END)     AS max_pain_now,
    MAX(CASE WHEN rn_desc = 1 THEN spot_price END)   AS spot_now,
    MAX(CASE WHEN rn_desc = 1 THEN max_pain END)
        - MAX(CASE WHEN rn_asc = 1 THEN max_pain END) AS drift_pts,
    CASE
        WHEN MAX(CASE WHEN rn_desc = 1 THEN max_pain END)
             > MAX(CASE WHEN rn_asc = 1 THEN max_pain END) THEN 'DRIFTING_UP'
        WHEN MAX(CASE WHEN rn_desc = 1 THEN max_pain END)
             < MAX(CASE WHEN rn_asc = 1 THEN max_pain END) THEN 'DRIFTING_DOWN'
        ELSE                                                    'STABLE'
    END AS drift_direction
FROM today
GROUP BY symbol, expiry, date;

-- 2.3) Top OI strikes (current snapshot) — resistance / support map
DROP VIEW IF EXISTS v_top_oi_strikes;
CREATE VIEW v_top_oi_strikes AS
WITH latest AS (
    SELECT symbol, expiry, MAX(snapshot_ts) AS mts
    FROM oi_coi_snapshots
    GROUP BY symbol, expiry
)
SELECT
    o.symbol, o.expiry, o.snapshot_ts,
    o.strike, o.call_oi, o.put_oi, o.spot_price,
    RANK() OVER (PARTITION BY o.symbol, o.expiry ORDER BY o.call_oi DESC) AS call_oi_rank,
    RANK() OVER (PARTITION BY o.symbol, o.expiry ORDER BY o.put_oi  DESC) AS put_oi_rank,
    CASE
        WHEN o.strike > o.spot_price THEN 'OTM_CALL / ITM_PUT'
        WHEN o.strike < o.spot_price THEN 'ITM_CALL / OTM_PUT'
        ELSE                              'ATM'
    END AS moneyness
FROM oi_coi_snapshots o
JOIN latest l
  ON o.symbol=l.symbol AND o.expiry=l.expiry AND o.snapshot_ts=l.mts;

-- 2.4) OI buildup classification — joins OI with intraday price action
DROP VIEW IF EXISTS v_oi_buildup;
CREATE VIEW v_oi_buildup AS
WITH latest_oi AS (
    SELECT symbol, expiry, MAX(snapshot_ts) AS mts
    FROM oi_coi_snapshots
    GROUP BY symbol, expiry
),
underlying_move AS (
    SELECT
        symbol,
        (SELECT close FROM intraday_candles i2
           WHERE i2.symbol=i1.symbol AND substr(i2.datetime,1,10) = substr(i1.datetime,1,10)
           ORDER BY datetime DESC LIMIT 1) AS last_close,
        (SELECT open  FROM intraday_candles i2
           WHERE i2.symbol=i1.symbol AND substr(i2.datetime,1,10) = substr(i1.datetime,1,10)
           ORDER BY datetime ASC  LIMIT 1) AS first_open
    FROM intraday_candles i1
    WHERE substr(datetime,1,10) =
          (SELECT MAX(substr(datetime,1,10)) FROM intraday_candles)
    GROUP BY symbol
)
SELECT
    o.symbol, o.expiry, o.strike, o.snapshot_ts,
    o.call_oi, o.put_oi,
    o.call_change_oi, o.put_change_oi,
    u.first_open, u.last_close,
    ROUND(u.last_close - u.first_open, 2) AS price_move,
    -- Classic OI Buildup Matrix
    CASE
        WHEN u.last_close > u.first_open AND o.call_change_oi > 0 THEN 'CALL_WRITING (Resistance↑)'
        WHEN u.last_close < u.first_open AND o.call_change_oi > 0 THEN 'CALL_BUILDUP (Bearish)'
        WHEN u.last_close > u.first_open AND o.call_change_oi < 0 THEN 'CALL_UNWINDING (Bullish)'
        WHEN u.last_close < u.first_open AND o.call_change_oi < 0 THEN 'CALL_SHORT_COVER (Bullish)'
        ELSE 'NEUTRAL'
    END AS call_oi_signal,
    CASE
        WHEN u.last_close > u.first_open AND o.put_change_oi > 0 THEN 'PUT_WRITING (Support↑, Bullish)'
        WHEN u.last_close < u.first_open AND o.put_change_oi > 0 THEN 'PUT_BUILDUP (Bearish)'
        WHEN u.last_close > u.first_open AND o.put_change_oi < 0 THEN 'PUT_UNWINDING (Bearish)'
        WHEN u.last_close < u.first_open AND o.put_change_oi < 0 THEN 'PUT_SHORT_COVER (Bearish)'
        ELSE 'NEUTRAL'
    END AS put_oi_signal
FROM oi_coi_snapshots o
JOIN latest_oi l ON o.symbol=l.symbol AND o.expiry=l.expiry AND o.snapshot_ts=l.mts
LEFT JOIN underlying_move u ON o.symbol=u.symbol;

-- 2.5) COI velocity — change-in-OI per minute (last 5 snapshots vs prior 5)
DROP VIEW IF EXISTS v_coi_velocity;
CREATE VIEW v_coi_velocity AS
WITH ranked AS (
    SELECT
        symbol, expiry, strike, snapshot_ts,
        call_change_oi, put_change_oi,
        ROW_NUMBER() OVER (PARTITION BY symbol, expiry, strike
                           ORDER BY snapshot_ts DESC) AS rn
    FROM oi_coi_snapshots
    WHERE substr(snapshot_ts,1,10) =
          (SELECT MAX(substr(snapshot_ts,1,10)) FROM oi_coi_snapshots)
)
SELECT
    symbol, expiry, strike,
    AVG(CASE WHEN rn <= 5 THEN call_change_oi END) -
    AVG(CASE WHEN rn BETWEEN 6 AND 10 THEN call_change_oi END) AS call_coi_velocity,
    AVG(CASE WHEN rn <= 5 THEN put_change_oi END) -
    AVG(CASE WHEN rn BETWEEN 6 AND 10 THEN put_change_oi END) AS put_coi_velocity,
    MAX(CASE WHEN rn = 1 THEN call_change_oi END) AS latest_call_coi,
    MAX(CASE WHEN rn = 1 THEN put_change_oi END)  AS latest_put_coi
FROM ranked
GROUP BY symbol, expiry, strike;

-- 2.6) Bank breadth — % of bank stocks above today's VWAP
DROP VIEW IF EXISTS v_bank_breadth;
CREATE VIEW v_bank_breadth AS
WITH today_vwap AS (
    SELECT symbol,
           SUM(close * volume) * 1.0 / NULLIF(SUM(volume), 0) AS vwap_so_far,
           MAX(close) AS last_close
    FROM intraday_candles
    WHERE substr(datetime,1,10) =
          (SELECT MAX(substr(datetime,1,10)) FROM intraday_candles)
      AND symbol IN ('HDFCBANK','ICICIBANK','SBIN','KOTAKBANK','AXISBANK')
    GROUP BY symbol
)
SELECT
    COUNT(*)                                                  AS bank_count,
    SUM(CASE WHEN last_close > vwap_so_far THEN 1 ELSE 0 END) AS above_vwap_count,
    ROUND(
        SUM(CASE WHEN last_close > vwap_so_far THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 1)                                        AS pct_above_vwap,
    CASE
        WHEN SUM(CASE WHEN last_close > vwap_so_far THEN 1 ELSE 0 END) >= 4 THEN 'STRONG_BULLISH'
        WHEN SUM(CASE WHEN last_close > vwap_so_far THEN 1 ELSE 0 END) >= 3 THEN 'BULLISH'
        WHEN SUM(CASE WHEN last_close > vwap_so_far THEN 1 ELSE 0 END) >= 2 THEN 'NEUTRAL'
        ELSE                                                                      'BEARISH'
    END AS breadth_signal
FROM today_vwap;

-- 2.7) Sectoral relative strength — NIFTY vs BANKNIFTY vs FINNIFTY vs RELIANCE
DROP VIEW IF EXISTS v_sector_rs;
CREATE VIEW v_sector_rs AS
WITH today AS (
    SELECT symbol,
           (SELECT open  FROM intraday_candles i2
              WHERE i2.symbol=i1.symbol AND substr(i2.datetime,1,10)=substr(i1.datetime,1,10)
              ORDER BY datetime ASC  LIMIT 1) AS d_open,
           (SELECT close FROM intraday_candles i2
              WHERE i2.symbol=i1.symbol AND substr(i2.datetime,1,10)=substr(i1.datetime,1,10)
              ORDER BY datetime DESC LIMIT 1) AS d_close
    FROM intraday_candles i1
    WHERE substr(datetime,1,10) =
          (SELECT MAX(substr(datetime,1,10)) FROM intraday_candles)
      AND symbol IN ('NIFTY50','BANKNIFTY','FINNIFTY','RELIANCE','INDIAVIX')
    GROUP BY symbol
)
SELECT
    symbol,
    d_open, d_close,
    ROUND((d_close - d_open) * 100.0 / d_open, 3) AS day_change_pct
FROM today;

-- 2.8) India VIX regime (volatility filter)
DROP VIEW IF EXISTS v_vix_regime;
CREATE VIEW v_vix_regime AS
WITH today AS (
    SELECT (SELECT close FROM intraday_candles
              WHERE symbol='INDIAVIX'
                AND substr(datetime,1,10) =
                    (SELECT MAX(substr(datetime,1,10)) FROM intraday_candles)
              ORDER BY datetime DESC LIMIT 1) AS vix_now,
           (SELECT AVG(close) FROM daily_candles
              WHERE symbol='INDIAVIX'
                AND date >= date('now','-30 days','localtime')) AS vix_30d_avg
)
SELECT
    vix_now, vix_30d_avg,
    ROUND((vix_now - vix_30d_avg) * 100.0 / vix_30d_avg, 2) AS vix_premium_pct,
    CASE
        WHEN vix_now < 11 THEN 'COMPLACENT     (favor range strategies)'
        WHEN vix_now < 14 THEN 'LOW_VOL        (favor trend continuation)'
        WHEN vix_now < 18 THEN 'NORMAL_VOL     (balanced)'
        WHEN vix_now < 22 THEN 'ELEVATED_VOL   (favor breakouts)'
        ELSE                  'HIGH_VOL        (caution, wide stops)'
    END AS vix_regime
FROM today;

-- ─────────────────────────────────────────────────────────────
-- LAYER 3 — DECISION SYNTHESIS
-- ─────────────────────────────────────────────────────────────

-- 3.1) Confluence Zones — high-probability S/R levels (where Max OI strikes + CPR + PDH/PDL stack)
DROP VIEW IF EXISTS v_confluence_zones;
CREATE VIEW v_confluence_zones AS
WITH oi_levels AS (
    SELECT symbol, strike AS level, 'MAX_CALL_OI' AS source
    FROM v_top_oi_strikes WHERE call_oi_rank = 1
    UNION ALL
    SELECT symbol, strike, 'MAX_PUT_OI'
    FROM v_top_oi_strikes WHERE put_oi_rank = 1
),
cpr_levels AS (
    SELECT symbol, pivot AS level, 'PIVOT'  AS source FROM v_cpr_today UNION ALL
    SELECT symbol, bc,             'CPR_BC'           FROM v_cpr_today UNION ALL
    SELECT symbol, tc,             'CPR_TC'           FROM v_cpr_today UNION ALL
    SELECT symbol, pdh,            'PDH'              FROM v_cpr_today UNION ALL
    SELECT symbol, pdl,            'PDL'              FROM v_cpr_today UNION ALL
    SELECT symbol, r1,             'R1'               FROM v_cpr_today UNION ALL
    SELECT symbol, s1,             'S1'               FROM v_cpr_today
),
all_levels AS (
    SELECT * FROM oi_levels UNION ALL SELECT * FROM cpr_levels
)
SELECT
    symbol,
    ROUND(level, 0) AS zone_price,
    COUNT(*)        AS confluence_count,
    GROUP_CONCAT(source, ' + ') AS sources
FROM all_levels
WHERE level IS NOT NULL
GROUP BY symbol, ROUND(level, 0)
HAVING confluence_count >= 2
ORDER BY symbol, zone_price;

-- 3.2) Master Daily Brief — one row per symbol with everything
DROP VIEW IF EXISTS v_daily_brief;
CREATE VIEW v_daily_brief AS
SELECT
    p.symbol,
    p.date                                  AS trading_date,
    p.last_bucket,
    p.spot_now                              AS spot,
    p.max_pain_now                          AS max_pain,
    p.spot_vs_maxpain,
    p.pcr_current,
    p.pcr_slope_5b,
    p.pcr_regime,
    p.pcr_trend,
    d.drift_direction                       AS maxpain_drift,
    d.drift_pts                             AS maxpain_drift_pts,
    c.pivot, c.bc, c.tc, c.pdh, c.pdl, c.r1, c.s1,
    c.cpr_width_pct,
    dt.day_type,
    g.gap_type, g.gap_pct
FROM v_pcr_slope p
LEFT JOIN v_maxpain_drift d ON p.symbol=d.symbol AND p.expiry=d.expiry AND p.date=d.date
LEFT JOIN v_cpr_today     c ON p.symbol=c.symbol
LEFT JOIN v_day_type      dt ON p.symbol=dt.symbol
LEFT JOIN v_gap_today     g ON p.symbol=g.symbol;