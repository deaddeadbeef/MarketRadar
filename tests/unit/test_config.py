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
