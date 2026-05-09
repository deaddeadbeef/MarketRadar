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


def _positive_float(env: Mapping[str, str], key: str, default: float) -> float:
    value = _float(env, key, default)
    if value <= 0:
        raise ValueError(f"{key} must be greater than zero")
    return value


def _int(env: Mapping[str, str], key: str, default: int) -> int:
    raw = env.get(key)
    return default if raw is None or raw == "" else int(raw)


def _positive_int(env: Mapping[str, str], key: str, default: int) -> int:
    value = _int(env, key, default)
    if value <= 0:
        raise ValueError(f"{key} must be greater than zero")
    return value


def _optional_str(env: Mapping[str, str], key: str) -> str | None:
    raw = env.get(key)
    if raw is None or raw == "":
        return None
    return raw


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
    portfolio_value: float = 0.0
    portfolio_cash: float = 0.0
    enable_premium_llm: bool = False
    market_provider: str = "csv"
    polygon_api_key: str | None = None
    polygon_base_url: str = "https://api.polygon.io"
    sec_enable_live: bool = False
    sec_user_agent: str | None = None
    sec_base_url: str = "https://data.sec.gov"
    http_timeout_seconds: float = 10.0
    provider_availability_policy: str = "live_fetch"
    universe_name: str = "liquid-us"
    universe_min_price: float = 5.0
    universe_min_avg_dollar_volume: float = 10_000_000.0
    universe_require_sector: bool = False
    universe_include_etfs: bool = False
    universe_include_adrs: bool = True
    scan_batch_size: int = 500

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
            portfolio_value=_float(source, "CATALYST_PORTFOLIO_VALUE", 0.0),
            portfolio_cash=_float(source, "CATALYST_PORTFOLIO_CASH", 0.0),
            enable_premium_llm=_bool(source.get("CATALYST_ENABLE_PREMIUM_LLM"), False),
            market_provider=source.get("CATALYST_MARKET_PROVIDER", "csv"),
            polygon_api_key=_optional_str(source, "CATALYST_POLYGON_API_KEY"),
            polygon_base_url=source.get(
                "CATALYST_POLYGON_BASE_URL", "https://api.polygon.io"
            ),
            sec_enable_live=_bool(source.get("CATALYST_SEC_ENABLE_LIVE"), False),
            sec_user_agent=_optional_str(source, "CATALYST_SEC_USER_AGENT"),
            sec_base_url=source.get("CATALYST_SEC_BASE_URL", "https://data.sec.gov"),
            http_timeout_seconds=_positive_float(
                source, "CATALYST_HTTP_TIMEOUT_SECONDS", 10.0
            ),
            provider_availability_policy=source.get(
                "CATALYST_PROVIDER_AVAILABILITY_POLICY", "live_fetch"
            ),
            universe_name=source.get("CATALYST_UNIVERSE_NAME", "liquid-us"),
            universe_min_price=_positive_float(
                source, "CATALYST_UNIVERSE_MIN_PRICE", 5.0
            ),
            universe_min_avg_dollar_volume=_positive_float(
                source, "CATALYST_UNIVERSE_MIN_AVG_DOLLAR_VOLUME", 10_000_000.0
            ),
            universe_require_sector=_bool(
                source.get("CATALYST_UNIVERSE_REQUIRE_SECTOR"), False
            ),
            universe_include_etfs=_bool(
                source.get("CATALYST_UNIVERSE_INCLUDE_ETFS"), False
            ),
            universe_include_adrs=_bool(
                source.get("CATALYST_UNIVERSE_INCLUDE_ADRS"), True
            ),
            scan_batch_size=_positive_int(source, "CATALYST_SCAN_BATCH_SIZE", 500),
        )
