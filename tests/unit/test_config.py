import pytest

from catalyst_radar.core.config import AppConfig


def test_config_defaults_are_deterministic_only() -> None:
    config = AppConfig.from_env({})

    assert config.environment == "local"
    assert config.enable_premium_llm is False
    assert config.price_min == 5
    assert config.avg_dollar_volume_min == 10_000_000


def test_config_reads_risk_settings_from_env() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_RISK_PER_TRADE_PCT": "0.01",
            "CATALYST_MAX_SINGLE_NAME_PCT": "0.05",
            "CATALYST_MAX_SECTOR_PCT": "0.25",
        }
    )

    assert config.risk_per_trade_pct == 0.01
    assert config.max_single_name_pct == 0.05
    assert config.max_sector_pct == 0.25


def test_config_reads_auth_modes_from_env() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_API_AUTH_MODE": "HEADER",
            "CATALYST_DASHBOARD_AUTH_MODE": "HEADER",
            "CATALYST_DASHBOARD_ROLE": "Analyst",
        }
    )

    assert config.api_auth_mode == "header"
    assert config.dashboard_auth_mode == "header"
    assert config.dashboard_role == "analyst"


def test_config_reads_portfolio_value_and_cash_from_env() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_PORTFOLIO_VALUE": "100000",
            "CATALYST_PORTFOLIO_CASH": "25000",
        }
    )

    assert config.portfolio_value == 100000
    assert config.portfolio_cash == 25000


def test_config_rejects_invalid_boolean_env_value() -> None:
    with pytest.raises(ValueError, match="Invalid boolean value"):
        AppConfig.from_env({"CATALYST_ENABLE_PREMIUM_LLM": "treu"})


def test_config_reads_explicit_boolean_env_values() -> None:
    false_config = AppConfig.from_env({"CATALYST_ENABLE_PREMIUM_LLM": "false"})
    true_config = AppConfig.from_env({"CATALYST_ENABLE_PREMIUM_LLM": "true"})

    assert false_config.enable_premium_llm is False
    assert true_config.enable_premium_llm is True


def test_llm_config_defaults_fail_closed() -> None:
    config = AppConfig.from_env({})

    assert config.enable_premium_llm is False
    assert config.llm_provider == "none"
    assert config.llm_evidence_model is None
    assert config.llm_skeptic_model is None
    assert config.llm_decision_card_model is None
    assert config.llm_input_cost_per_1m is None
    assert config.llm_cached_input_cost_per_1m is None
    assert config.llm_output_cost_per_1m is None
    assert config.llm_daily_budget_usd == 0.0
    assert config.llm_monthly_budget_usd == 0.0
    assert config.llm_task_daily_caps == {}


def test_llm_config_reads_pricing_and_caps() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_ENABLE_PREMIUM_LLM": "true",
            "CATALYST_LLM_PROVIDER": "openai",
            "CATALYST_LLM_EVIDENCE_MODEL": "model-review",
            "CATALYST_LLM_SKEPTIC_MODEL": "model-skeptic",
            "CATALYST_LLM_DECISION_CARD_MODEL": "model-decision",
            "CATALYST_LLM_INPUT_COST_PER_1M": "5.00",
            "CATALYST_LLM_CACHED_INPUT_COST_PER_1M": "0.50",
            "CATALYST_LLM_OUTPUT_COST_PER_1M": "30.00",
            "CATALYST_LLM_PRICING_UPDATED_AT": "2026-05-10",
            "CATALYST_LLM_PRICING_STALE_AFTER_DAYS": "14",
            "CATALYST_LLM_DAILY_BUDGET_USD": "2.50",
            "CATALYST_LLM_MONTHLY_BUDGET_USD": "50.00",
            "CATALYST_LLM_MONTHLY_SOFT_CAP_PCT": "0.75",
            "CATALYST_LLM_TASK_DAILY_CAPS": "mid_review=3,gpt55_decision_card=1",
        }
    )

    assert config.enable_premium_llm is True
    assert config.llm_provider == "openai"
    assert config.llm_evidence_model == "model-review"
    assert config.llm_skeptic_model == "model-skeptic"
    assert config.llm_decision_card_model == "model-decision"
    assert config.llm_input_cost_per_1m == 5.0
    assert config.llm_cached_input_cost_per_1m == 0.5
    assert config.llm_output_cost_per_1m == 30.0
    assert config.llm_pricing_updated_at == "2026-05-10"
    assert config.llm_pricing_stale_after_days == 14
    assert config.llm_daily_budget_usd == 2.5
    assert config.llm_monthly_budget_usd == 50.0
    assert config.llm_monthly_soft_cap_pct == 0.75
    assert config.llm_task_daily_caps["mid_review"] == 3
    assert config.llm_task_daily_caps["gpt55_decision_card"] == 1


def test_llm_config_reads_and_redacts_openai_api_key() -> None:
    config = AppConfig.from_env({"OPENAI_API_KEY": "sk-test-secret"})

    assert config.openai_api_key == "sk-test-secret"
    assert config.sanitized()["openai_api_key"] == "<redacted>"


def test_llm_config_trims_optional_model_values() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_LLM_EVIDENCE_MODEL": "   ",
            "CATALYST_LLM_SKEPTIC_MODEL": "  model-skeptic  ",
        }
    )

    assert config.llm_evidence_model is None
    assert config.llm_skeptic_model == "model-skeptic"


@pytest.mark.parametrize("raw", ["-0.01", "1.01"])
def test_llm_config_rejects_monthly_soft_cap_pct_outside_ratio(raw: str) -> None:
    with pytest.raises(ValueError):
        AppConfig.from_env({"CATALYST_LLM_MONTHLY_SOFT_CAP_PCT": raw})


@pytest.mark.parametrize(
    "raw",
    ["mid_review", "=3", "mid_review=-1", "mid_review=two"],
)
def test_llm_config_rejects_malformed_task_daily_caps(raw: str) -> None:
    with pytest.raises(ValueError):
        AppConfig.from_env({"CATALYST_LLM_TASK_DAILY_CAPS": raw})


def test_config_reads_sec_live_settings_from_env() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_SEC_ENABLE_LIVE": "true",
            "CATALYST_SEC_USER_AGENT": "CatalystRadar/0.1 contact@example.com",
            "CATALYST_SEC_BASE_URL": "https://sec.example.com",
            "CATALYST_SEC_DAILY_MAX_TICKERS": "7",
        }
    )

    assert config.sec_enable_live is True
    assert config.sec_user_agent == "CatalystRadar/0.1 contact@example.com"
    assert config.sec_base_url == "https://sec.example.com"
    assert config.sec_daily_max_tickers == 7


def test_config_reads_polygon_ticker_page_cap_from_env() -> None:
    config = AppConfig.from_env({"CATALYST_POLYGON_TICKERS_MAX_PAGES": "3"})

    assert config.polygon_tickers_max_pages == 3


@pytest.mark.parametrize("value", ["0", "-1"])
def test_config_rejects_invalid_polygon_ticker_page_cap(value: str) -> None:
    with pytest.raises(ValueError):
        AppConfig.from_env({"CATALYST_POLYGON_TICKERS_MAX_PAGES": value})


def test_config_reads_schwab_settings_from_env() -> None:
    config = AppConfig.from_env(
        {
            "SCHWAB_CLIENT_ID": "client-id",
            "SCHWAB_CLIENT_SECRET": "client-secret",
            "SCHWAB_REDIRECT_URI": "https://127.0.0.1/callback",
            "SCHWAB_ENV": "PRODUCTION",
            "SCHWAB_BASE_URL": "https://schwab.example.com/",
            "SCHWAB_AUTH_BASE_URL": "https://auth.schwab.example.com/",
            "SCHWAB_ORDER_SUBMISSION_ENABLED": "true",
            "SCHWAB_SYNC_MIN_INTERVAL_SECONDS": "120",
            "SCHWAB_MARKET_SYNC_MIN_INTERVAL_SECONDS": "45",
            "SCHWAB_MARKET_SYNC_MAX_TICKERS": "3",
            "BROKER_TOKEN_ENCRYPTION_KEY": "local-dev-key",
        }
    )

    assert config.schwab_client_id == "client-id"
    assert config.schwab_client_secret == "client-secret"
    assert config.schwab_redirect_uri == "https://127.0.0.1/callback"
    assert config.schwab_env == "production"
    assert config.schwab_base_url == "https://schwab.example.com"
    assert config.schwab_auth_base_url == "https://auth.schwab.example.com"
    assert config.schwab_order_submission_enabled is True
    assert config.schwab_sync_min_interval_seconds == 120
    assert config.schwab_market_sync_min_interval_seconds == 45
    assert config.schwab_market_sync_max_tickers == 3
    assert config.broker_token_encryption_key == "local-dev-key"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("SCHWAB_SYNC_MIN_INTERVAL_SECONDS", "0"),
        ("SCHWAB_MARKET_SYNC_MIN_INTERVAL_SECONDS", "-1"),
        ("SCHWAB_MARKET_SYNC_MAX_TICKERS", "0"),
    ],
)
def test_config_rejects_invalid_schwab_rate_limit_settings(
    key: str,
    value: str,
) -> None:
    with pytest.raises(ValueError):
        AppConfig.from_env({key: value})
