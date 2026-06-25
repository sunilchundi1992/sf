"""FastAPI application: API + SSE live stream + scheduler + UI serving.

Run:  uvicorn app:app --reload
Open: http://localhost:8000
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import numpy as np
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import ingestion
import quant
import storage
from config import settings

WEB_DIR = Path(__file__).parent / "web"

# event used to notify SSE clients that fresh data has landed
_data_event = asyncio.Event()
_scheduler: BackgroundScheduler | None = None
_main_loop: asyncio.AbstractEventLoop | None = None


def _poll_job():
    """Runs in scheduler thread: ingest, then signal the async loop."""
    try:
        result = ingestion.ingest()
        print(f"[scheduler] ingested {result}")
    except Exception as exc:  # keep the scheduler alive on transient errors
        print(f"[scheduler] ingest error: {exc}")
        return
    if _main_loop is not None:
        _main_loop.call_soon_threadsafe(_data_event.set)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler, _main_loop
    _main_loop = asyncio.get_running_loop()
    storage.init_db()

    # Ensure there is something to show immediately.
    if not storage.has_data():
        try:
            ingestion.ingest()
        except Exception as exc:
            print(f"[startup] initial ingest failed ({exc}); seeding synthetic")
            ingestion.seed_synthetic()

    if settings.enable_scheduler:
        _scheduler = BackgroundScheduler(daemon=True)
        _scheduler.add_job(
            _poll_job,
            "interval",
            seconds=settings.poll_interval_seconds,
            id="poll",
            max_instances=1,
            coalesce=True,
        )
        _scheduler.start()
        print(f"[startup] scheduler every {settings.poll_interval_seconds}s")

    yield

    if _scheduler:
        _scheduler.shutdown(wait=False)


app = FastAPI(title="Trading Analytics — Pure Quant", lifespan=lifespan)


# --------------------------------------------------------------------
# JSON sanitizing (NaN/inf -> None)
# --------------------------------------------------------------------
def _clean(obj):
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, (np.floating, float)):
        f = float(obj)
        return None if (np.isnan(f) or np.isinf(f)) else f
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    return obj


def _load():
    df = storage.load_master()
    chain = storage.load_latest_chain()
    return df, chain


def _payload() -> dict:
    df, chain = _load()
    if df.empty:
        return {"summary": {}, "analytics": {}, "understanding": [],
                "master": [], "chain": []}
    payload = quant.full_payload(df, chain)
    payload["master"] = df[
        ["time", "spot_price", "open", "high", "low", "close",
         "volume", "oi", "change_oi", "pcr", "max_pain"]
    ].to_dict(orient="records")
    payload["chain"] = chain.to_dict(orient="records") if not chain.empty else []
    return _clean(payload)


# --------------------------------------------------------------------
# API routes
# --------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "has_data": storage.has_data(),
            "synthetic": settings.use_synthetic_data}


@app.get("/summary")
def summary():
    return _payload()["summary"]


@app.get("/master")
def master():
    df, _ = _load()
    return _clean(df.to_dict(orient="records")) if not df.empty else []


@app.get("/analytics")
def analytics_all():
    return _payload()["analytics"]


@app.get("/analytics/{module}")
def analytics_module(module: str):
    a = _payload()["analytics"]
    return a.get(module, {"error": f"unknown module '{module}'",
                          "available": list(a.keys())})


@app.get("/understanding")
def understanding():
    return _payload()["understanding"]


@app.get("/chain")
def chain():
    return _payload()["chain"]


@app.get("/all")
def all_data():
    return _payload()


@app.post("/refresh")
def refresh():
    """Force an immediate ingest (manual poll)."""
    result = ingestion.ingest()
    if _main_loop is not None:
        _main_loop.call_soon_threadsafe(_data_event.set)
    return {"ingested": result}


# --------------------------------------------------------------------
# SSE live stream
# --------------------------------------------------------------------
@app.get("/stream")
async def stream():
    async def event_gen():
        # send an initial snapshot right away
        yield f"data: {json.dumps(_payload())}\n\n"
        while True:
            try:
                await asyncio.wait_for(_data_event.wait(), timeout=30)
                _data_event.clear()
                yield f"data: {json.dumps(_payload())}\n\n"
            except asyncio.TimeoutError:
                # heartbeat keeps the connection alive through proxies
                yield ": keep-alive\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream")


# --------------------------------------------------------------------
# UI
# --------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def index():
    return (WEB_DIR / "index.html").read_text(encoding="utf-8")


app.mount("/static", StaticFiles(directory=WEB_DIR / "static"), name="static")
