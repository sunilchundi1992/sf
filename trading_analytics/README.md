# Trading Analytics — Pure Quant (FastAPI)

Institutional-style intraday options analytics for NIFTY / BANKNIFTY built on your
Upstox data. Ingests **Spot, OI, Change-OI, Volume, PCR, Max Pain** on a timer,
stores a master time-series in SQLite, and serves a **single FastAPI app** that
computes a full quant stack and renders a live, auto-refreshing dashboard.

No alerts, no backtesting — the focus is **market-understanding points**: ranked,
evidence-backed conclusions of the form *metric → reading → implication → conviction*.

---

## Architecture (10 files, flat)

```
trading_analytics/
├── config.py        # .env-driven settings
├── storage.py       # SQLAlchemy models (snapshots, option_chain) + upsert/read
├── ingestion.py     # Upstox client + 3 fetchers + master builder + synthetic seed
├── analytics.py     # all quant metric functions (sections 1–9 + advanced)
├── quant.py         # bias + confidence + ranked understanding points
├── app.py           # FastAPI: routes, SSE /stream, APScheduler, serves UI
├── web/
│   ├── index.html   # dashboard (Tailwind + Plotly.js + AG Grid via CDN)
│   └── static/app.js
├── requirements.txt
└── .env.example
```

Data flow: `APScheduler → ingestion.ingest() → upsert SQLite → SSE push → browser`.
FastAPI **only reads** from SQLite; the scheduler is the only writer.

---

## Quick start

```bash
cd trading_analytics
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                   # Windows: copy .env.example .env
uvicorn app:app --reload
```

Open **http://localhost:8000**.

By default `USE_SYNTHETIC_DATA=true`, so the app seeds a realistic intraday session
and the dashboard works immediately — **before** you wire your live Upstox token.

---

## Going live (Upstox)

Edit `.env`:

```ini
USE_SYNTHETIC_DATA=false
UPSTOX_TOKEN_FILE=C:\path\to\access_token.txt
INSTRUMENT_KEY=NSE_INDEX|Nifty 50
CANDLE_INSTRUMENT_KEY=NSE_FO|62329     # the FO series used for intraday candles
EXPIRY_DATE=2026-06-30
INTERVAL_MINUTES=3
POLL_INTERVAL_SECONDS=180
```

The three original scripts map directly:

| Original script                  | Module function                          |
|----------------------------------|------------------------------------------|
| `get_intraday_maxpain_pcr.py`    | `ingestion.fetch_maxpain_pcr_candles()`  |
| `get_OI_data.py`                 | `ingestion._fetch_chain_raw()` (OI side) |
| `get_change_OI_data.py`          | `ingestion._fetch_chain_raw()` (ΔOI side)|

`spot_price` is taken from the candle `close`. `change_oi` in the master series is
the chain-wide net change for the latest bucket.

### Postgres instead of SQLite
```ini
DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/trading
```
(`pip install psycopg2-binary` as well.)

---

## API

| Method | Path                    | Description                                   |
|--------|-------------------------|-----------------------------------------------|
| GET    | `/`                     | Dashboard HTML                                |
| GET    | `/health`               | Status + whether synthetic mode is on         |
| GET    | `/summary`              | 11 summary cards (incl. bias + confidence)    |
| GET    | `/master`               | Full master time-series                       |
| GET    | `/analytics`            | All metric blocks                             |
| GET    | `/analytics/{module}`   | One block (`price_oi`, `pcr`, `trend`, …)     |
| GET    | `/understanding`        | Ranked market-understanding points            |
| GET    | `/chain`                | Latest strike-level option chain              |
| GET    | `/all`                  | Everything in one payload                     |
| POST   | `/refresh`              | Force an immediate ingest                     |
| GET    | `/stream`               | **SSE** live push on every scheduler tick     |

---

## Quant coverage

**Core (sections 1–9):** Price↔OI (long/short buildup, covering, unwinding +
strength/duration) · Price↔Volume (RVOL, expansion/contraction, breakout, spike%) ·
OI momentum (5/15/30-min, velocity, acceleration) · PCR (trend, slope, momentum,
extremes, 4 price/PCR regimes) · Max Pain (distance, drift, pinning, gravitation/
divergence) · Trend (EMA 9/20/50, VWAP, score/strength/direction) · Institutional
activity (fresh longs/shorts, covering, entry/exit + confidence) · Market regime
(trending/range/volatile/breakout/mean-reversion) · Smart money (direction, bull/
bear %, net OI & volume pressure, SMI, participation).

**Advanced metrics:** OI velocity & acceleration · volume-to-OI ratio · price
efficiency · max-pain drift · PCR momentum · OI walls (support/resistance) · OI
concentration score · call/put writing pressure · Net Option Pressure Index ·
expiry pinning probability · market participation index.

**Decision layer (`quant.py`):** weighted vote across all signals → market bias +
confidence (0–100); ranked understanding points sorted by conviction.

---

## Adding a new indicator

1. Add a pure function to `analytics.py` taking `df` (and optionally `chain`).
2. Register it in `analytics.compute_all(...)`.
3. (Optional) emit an understanding point in `quant.understanding_points(...)`.

No other file changes needed — the API and UI pick it up automatically.

---

## Notes

- Thresholds (PCR extremes, RVOL spike, pinning distance) are in `.env` — no code edits.
- Re-polling the same `time` bucket **updates** the row (upsert), never duplicates.
- The scheduler keeps running through transient Upstox errors; last-good data stays served.
