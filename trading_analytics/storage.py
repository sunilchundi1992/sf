"""Persistence layer: SQLAlchemy models + read/write helpers.

Two tables:
  - snapshots:    master intraday time-series (one row per `time` bucket)
  - option_chain: strike-level OI snapshot (for OI walls / heatmap)

FastAPI only ever READS from here; the scheduler WRITES via upsert.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    select,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings

Base = declarative_base()
engine = create_engine(settings.database_url, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, future=True)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Snapshot(Base):
    __tablename__ = "snapshots"
    __table_args__ = (UniqueConstraint("time", name="uq_snapshot_time"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(String, nullable=False, index=True)  # HH:MM bucket
    captured_at = Column(DateTime, default=_utcnow)
    spot_price = Column(Float)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    oi = Column(Float)
    change_oi = Column(Float)
    pcr = Column(Float)
    max_pain = Column(Float)


class OptionChain(Base):
    __tablename__ = "option_chain"
    __table_args__ = (
        UniqueConstraint("time", "strike_price", name="uq_chain_time_strike"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(String, nullable=False, index=True)
    captured_at = Column(DateTime, default=_utcnow)
    strike_price = Column(Float, nullable=False)
    call_oi = Column(Float)
    put_oi = Column(Float)
    call_chg_oi = Column(Float)
    put_chg_oi = Column(Float)


def init_db() -> None:
    Base.metadata.create_all(engine)


# --------------------------------------------------------------------
# Write helpers (upsert so re-polling the same bucket updates, not dup)
# --------------------------------------------------------------------
def upsert_snapshots(rows: Iterable[dict]) -> int:
    rows = [r for r in rows if r.get("time")]
    if not rows:
        return 0
    with engine.begin() as conn:
        for r in rows:
            r = {**r, "captured_at": _utcnow()}
            stmt = sqlite_insert(Snapshot).values(**r)
            update_cols = {
                k: stmt.excluded[k]
                for k in r
                if k not in ("id", "time")
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=["time"], set_=update_cols
            )
            conn.execute(stmt)
    return len(rows)


def upsert_option_chain(rows: Iterable[dict]) -> int:
    rows = [r for r in rows if r.get("time") and r.get("strike_price") is not None]
    if not rows:
        return 0
    with engine.begin() as conn:
        for r in rows:
            r = {**r, "captured_at": _utcnow()}
            stmt = sqlite_insert(OptionChain).values(**r)
            update_cols = {
                k: stmt.excluded[k]
                for k in r
                if k not in ("id", "time", "strike_price")
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=["time", "strike_price"], set_=update_cols
            )
            conn.execute(stmt)
    return len(rows)


# --------------------------------------------------------------------
# Read helpers -> pandas
# --------------------------------------------------------------------
def load_master() -> pd.DataFrame:
    """Full master time-series, sorted by `time`."""
    with engine.connect() as conn:
        df = pd.read_sql(select(Snapshot), conn)
    if df.empty:
        return df
    df = df.sort_values("time").reset_index(drop=True)
    return df


def load_latest_chain() -> pd.DataFrame:
    """Strike-level option chain for the most recent `time` bucket."""
    with engine.connect() as conn:
        df = pd.read_sql(select(OptionChain), conn)
    if df.empty:
        return df
    latest_time = df["time"].max()
    df = df[df["time"] == latest_time].sort_values("strike_price").reset_index(drop=True)
    return df


def has_data() -> bool:
    with engine.connect() as conn:
        row = conn.execute(select(Snapshot.id).limit(1)).first()
    return row is not None
