from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from os import environ


def _bool(value: str | bool | None, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _float(env: Mapping[str, str], key: str, default: float) -> float:
    raw = env.get(key)
    return default if raw is None or raw == "" else float(raw)


@dataclass(frozen=True)
class AppConfig:
    environment: str = "local"
    database_url: str = "sqlite:///data/local/catalyst_radar.db"
    log_level: str = "INFO"
    price_min: float = 5
    market_cap_min: float = 300_000_000
    avg_dollar_volume_min: float = 10_000_000
    risk_per_trade_pct: float = 0.005
    max_single_name_pct: float = 0.08
    max_sector_pct: float = 0.30
    max_theme_pct: float = 0.35
    enable_premium_llm: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AppConfig:
        source = environ if env is None else env
        return cls(
            environment=source.get("CATALYST_ENV", "local"),
            database_url=source.get(
                "CATALYST_DATABASE_URL", "sqlite:///data/local/catalyst_radar.db"
            ),
            log_level=source.get("CATALYST_LOG_LEVEL", "INFO"),
            price_min=_float(source, "CATALYST_PRICE_MIN", 5),
            market_cap_min=_float(source, "CATALYST_MARKET_CAP_MIN", 300_000_000),
            avg_dollar_volume_min=_float(
                source, "CATALYST_AVG_DOLLAR_VOLUME_MIN", 10_000_000
            ),
            risk_per_trade_pct=_float(source, "CATALYST_RISK_PER_TRADE_PCT", 0.005),
            max_single_name_pct=_float(source, "CATALYST_MAX_SINGLE_NAME_PCT", 0.08),
            max_sector_pct=_float(source, "CATALYST_MAX_SECTOR_PCT", 0.30),
            max_theme_pct=_float(source, "CATALYST_MAX_THEME_PCT", 0.35),
            enable_premium_llm=_bool(source.get("CATALYST_ENABLE_PREMIUM_LLM"), False),
        )
