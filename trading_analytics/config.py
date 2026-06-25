"""Application configuration loaded from environment / .env file."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Upstox ---
    upstox_token_file: str = "access_token.txt"
    instrument_key: str = "NSE_INDEX|Nifty 50"
    candle_instrument_key: str = "NSE_FO|62329"
    expiry_date: str = "2026-06-30"
    interval_minutes: int = 3
    strike_lower: int = 21000
    strike_upper: int = 27000

    # --- Scheduler ---
    poll_interval_seconds: int = 180
    enable_scheduler: bool = True

    # --- Database ---
    database_url: str = "sqlite:///./trading_analytics.db"

    # --- Analytics thresholds ---
    pcr_bull_extreme: float = 1.30
    pcr_bear_extreme: float = 0.70
    rvol_spike: float = 1.50
    pinning_distance_pct: float = 0.25

    # --- Data source ---
    use_synthetic_data: bool = True

    @property
    def upstox_base(self) -> str:
        return "https://api.upstox.com"


settings = Settings()
