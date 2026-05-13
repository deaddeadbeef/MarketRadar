from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from os import environ

from catalyst_radar.security.redaction import redact_value


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


def _optional_float(env: Mapping[str, str], key: str) -> float | None:
    raw = env.get(key)
    return None if raw is None or raw == "" else float(raw)


def _optional_nonnegative_float(env: Mapping[str, str], key: str) -> float | None:
    value = _optional_float(env, key)
    if value is not None and value < 0:
        raise ValueError(f"{key} must be greater than or equal to zero")
    return value


def _nonnegative_float(env: Mapping[str, str], key: str, default: float) -> float:
    value = _float(env, key, default)
    if value < 0:
        raise ValueError(f"{key} must be greater than or equal to zero")
    return value


def _ratio_float(env: Mapping[str, str], key: str, default: float) -> float:
    value = _nonnegative_float(env, key, default)
    if value > 1:
        raise ValueError(f"{key} must be between 0.0 and 1.0")
    return value


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
    if raw is None:
        return None
    value = raw.strip()
    if value == "":
        return None
    return value


def _task_caps(env: Mapping[str, str], key: str) -> Mapping[str, int]:
    raw = env.get(key)
    if raw is None or raw.strip() == "":
        return {}
    caps: dict[str, int] = {}
    for item in raw.split(","):
        if "=" not in item:
            raise ValueError(f"{key} entries must use name=value")
        name, value = item.split("=", maxsplit=1)
        name = name.strip()
        if not name:
            raise ValueError(f"{key} task name must not be blank")
        try:
            cap = int(value.strip())
        except ValueError as exc:
            raise ValueError(f"{key} cap must be an integer") from exc
        if cap < 0:
            raise ValueError(f"{key} cap must be greater than or equal to zero")
        caps[name] = cap
    return caps


@dataclass(frozen=True)
class AppConfig:
    environment: str = "local"
    database_url: str = "sqlite:///data/local/catalyst_radar.db"
    log_level: str = "INFO"
    api_auth_mode: str = "disabled"
    dashboard_auth_mode: str = "disabled"
    dashboard_role: str = "admin"
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
    llm_provider: str = "none"
    llm_evidence_model: str | None = None
    llm_skeptic_model: str | None = None
    llm_decision_card_model: str | None = None
    llm_input_cost_per_1m: float | None = None
    llm_cached_input_cost_per_1m: float | None = None
    llm_output_cost_per_1m: float | None = None
    llm_pricing_updated_at: str | None = None
    llm_pricing_stale_after_days: int = 30
    llm_daily_budget_usd: float = 0.0
    llm_monthly_budget_usd: float = 0.0
    llm_monthly_soft_cap_pct: float = 0.80
    llm_task_daily_caps: Mapping[str, int] = field(default_factory=dict)
    market_provider: str = "csv"
    polygon_api_key: str | None = None
    polygon_base_url: str = "https://api.polygon.io"
    sec_enable_live: bool = False
    sec_user_agent: str | None = None
    sec_base_url: str = "https://data.sec.gov"
    sec_daily_max_tickers: int = 5
    schwab_client_id: str | None = None
    schwab_client_secret: str | None = None
    schwab_redirect_uri: str | None = None
    schwab_env: str = "production"
    schwab_base_url: str = "https://api.schwabapi.com"
    schwab_auth_base_url: str = "https://api.schwabapi.com/v1/oauth"
    schwab_order_submission_enabled: bool = False
    schwab_sync_min_interval_seconds: int = 900
    schwab_market_sync_min_interval_seconds: int = 300
    schwab_market_sync_max_tickers: int = 5
    broker_token_encryption_key: str | None = None
    http_timeout_seconds: float = 10.0
    provider_availability_policy: str = "live_fetch"
    daily_market_provider: str = "csv"
    csv_securities_path: str = "data/sample/securities.csv"
    csv_daily_bars_path: str = "data/sample/daily_bars.csv"
    csv_holdings_path: str | None = "data/sample/holdings.csv"
    daily_event_provider: str = "news_fixture"
    news_fixture_path: str = "data/sample/news_events_aaa.json"
    universe_name: str = "liquid-us"
    universe_min_price: float = 5.0
    universe_min_avg_dollar_volume: float = 10_000_000.0
    universe_require_sector: bool = False
    universe_include_etfs: bool = False
    universe_include_adrs: bool = True
    scan_batch_size: int = 500

    def sanitized(self) -> dict[str, object]:
        return redact_value(asdict(self))

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AppConfig:
        source = environ if env is None else env
        return cls(
            environment=source.get("CATALYST_ENV", "local"),
            database_url=source.get(
                "CATALYST_DATABASE_URL", "sqlite:///data/local/catalyst_radar.db"
            ),
            log_level=source.get("CATALYST_LOG_LEVEL", "INFO"),
            api_auth_mode=source.get("CATALYST_API_AUTH_MODE", "disabled")
            .strip()
            .lower(),
            dashboard_auth_mode=source.get("CATALYST_DASHBOARD_AUTH_MODE", "disabled")
            .strip()
            .lower(),
            dashboard_role=source.get("CATALYST_DASHBOARD_ROLE", "admin").strip().lower(),
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
            llm_provider=source.get("CATALYST_LLM_PROVIDER", "none"),
            llm_evidence_model=_optional_str(source, "CATALYST_LLM_EVIDENCE_MODEL"),
            llm_skeptic_model=_optional_str(source, "CATALYST_LLM_SKEPTIC_MODEL"),
            llm_decision_card_model=_optional_str(
                source, "CATALYST_LLM_DECISION_CARD_MODEL"
            ),
            llm_input_cost_per_1m=_optional_nonnegative_float(
                source, "CATALYST_LLM_INPUT_COST_PER_1M"
            ),
            llm_cached_input_cost_per_1m=_optional_nonnegative_float(
                source, "CATALYST_LLM_CACHED_INPUT_COST_PER_1M"
            ),
            llm_output_cost_per_1m=_optional_nonnegative_float(
                source, "CATALYST_LLM_OUTPUT_COST_PER_1M"
            ),
            llm_pricing_updated_at=_optional_str(
                source, "CATALYST_LLM_PRICING_UPDATED_AT"
            ),
            llm_pricing_stale_after_days=_positive_int(
                source, "CATALYST_LLM_PRICING_STALE_AFTER_DAYS", 30
            ),
            llm_daily_budget_usd=_nonnegative_float(
                source, "CATALYST_LLM_DAILY_BUDGET_USD", 0.0
            ),
            llm_monthly_budget_usd=_nonnegative_float(
                source, "CATALYST_LLM_MONTHLY_BUDGET_USD", 0.0
            ),
            llm_monthly_soft_cap_pct=_ratio_float(
                source, "CATALYST_LLM_MONTHLY_SOFT_CAP_PCT", 0.80
            ),
            llm_task_daily_caps=_task_caps(source, "CATALYST_LLM_TASK_DAILY_CAPS"),
            market_provider=source.get("CATALYST_MARKET_PROVIDER", "csv"),
            polygon_api_key=_optional_str(source, "CATALYST_POLYGON_API_KEY"),
            polygon_base_url=source.get(
                "CATALYST_POLYGON_BASE_URL", "https://api.polygon.io"
            ),
            sec_enable_live=_bool(source.get("CATALYST_SEC_ENABLE_LIVE"), False),
            sec_user_agent=_optional_str(source, "CATALYST_SEC_USER_AGENT"),
            sec_base_url=source.get("CATALYST_SEC_BASE_URL", "https://data.sec.gov"),
            sec_daily_max_tickers=_positive_int(
                source, "CATALYST_SEC_DAILY_MAX_TICKERS", 5
            ),
            schwab_client_id=_optional_str(source, "SCHWAB_CLIENT_ID"),
            schwab_client_secret=_optional_str(source, "SCHWAB_CLIENT_SECRET"),
            schwab_redirect_uri=_optional_str(source, "SCHWAB_REDIRECT_URI"),
            schwab_env=source.get("SCHWAB_ENV", "production").strip().lower(),
            schwab_base_url=source.get(
                "SCHWAB_BASE_URL", "https://api.schwabapi.com"
            ).rstrip("/"),
            schwab_auth_base_url=source.get(
                "SCHWAB_AUTH_BASE_URL", "https://api.schwabapi.com/v1/oauth"
            ).rstrip("/"),
            schwab_order_submission_enabled=_bool(
                source.get("SCHWAB_ORDER_SUBMISSION_ENABLED"), False
            ),
            schwab_sync_min_interval_seconds=_positive_int(
                source,
                "SCHWAB_SYNC_MIN_INTERVAL_SECONDS",
                900,
            ),
            schwab_market_sync_min_interval_seconds=_positive_int(
                source,
                "SCHWAB_MARKET_SYNC_MIN_INTERVAL_SECONDS",
                300,
            ),
            schwab_market_sync_max_tickers=_positive_int(
                source,
                "SCHWAB_MARKET_SYNC_MAX_TICKERS",
                5,
            ),
            broker_token_encryption_key=_optional_str(
                source, "BROKER_TOKEN_ENCRYPTION_KEY"
            ),
            http_timeout_seconds=_positive_float(
                source, "CATALYST_HTTP_TIMEOUT_SECONDS", 10.0
            ),
            provider_availability_policy=source.get(
                "CATALYST_PROVIDER_AVAILABILITY_POLICY", "live_fetch"
            ),
            daily_market_provider=source.get(
                "CATALYST_DAILY_MARKET_PROVIDER", "csv"
            ).strip(),
            csv_securities_path=source.get(
                "CATALYST_CSV_SECURITIES_PATH", "data/sample/securities.csv"
            ),
            csv_daily_bars_path=source.get(
                "CATALYST_CSV_DAILY_BARS_PATH", "data/sample/daily_bars.csv"
            ),
            csv_holdings_path=_optional_str(source, "CATALYST_CSV_HOLDINGS_PATH")
            if "CATALYST_CSV_HOLDINGS_PATH" in source
            else "data/sample/holdings.csv",
            daily_event_provider=source.get(
                "CATALYST_DAILY_EVENT_PROVIDER", "news_fixture"
            ).strip(),
            news_fixture_path=source.get(
                "CATALYST_NEWS_FIXTURE_PATH", "data/sample/news_events_aaa.json"
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
