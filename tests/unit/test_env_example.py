from __future__ import annotations

from pathlib import Path


def test_env_example_covers_live_activation_keys() -> None:
    values = _env_example_values()

    required = {
        "CATALYST_DAILY_MARKET_PROVIDER",
        "CATALYST_DAILY_PROVIDER",
        "CATALYST_POLYGON_API_KEY",
        "CATALYST_POLYGON_TICKERS_MAX_PAGES",
        "CATALYST_DAILY_EVENT_PROVIDER",
        "CATALYST_SEC_ENABLE_LIVE",
        "CATALYST_SEC_USER_AGENT",
        "CATALYST_SEC_DAILY_MAX_TICKERS",
        "CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS",
        "CATALYST_WORKER_INTERVAL_SECONDS",
        "CATALYST_WORKER_LOCK_TTL_SECONDS",
        "CATALYST_WORKER_OWNER",
        "CATALYST_WORKER_LOCK_NAME",
        "CATALYST_ENABLE_PREMIUM_LLM",
        "CATALYST_LLM_PROVIDER",
        "CATALYST_LLM_SKEPTIC_MODEL",
        "CATALYST_LLM_INPUT_COST_PER_1M",
        "CATALYST_LLM_CACHED_INPUT_COST_PER_1M",
        "CATALYST_LLM_OUTPUT_COST_PER_1M",
        "CATALYST_LLM_PRICING_UPDATED_AT",
        "CATALYST_LLM_DAILY_BUDGET_USD",
        "CATALYST_LLM_MONTHLY_BUDGET_USD",
        "CATALYST_LLM_TASK_DAILY_CAPS",
        "OPENAI_API_KEY",
    }

    assert required <= set(values)


def test_env_example_defaults_are_safe_fixture_and_dry_run() -> None:
    values = _env_example_values()

    assert values["CATALYST_MARKET_PROVIDER"] == "csv"
    assert values["CATALYST_DAILY_MARKET_PROVIDER"] == "csv"
    assert values["CATALYST_DAILY_EVENT_PROVIDER"] == "news_fixture"
    assert values["CATALYST_SEC_ENABLE_LIVE"] == "false"
    assert values["CATALYST_ENABLE_PREMIUM_LLM"] == "false"
    assert values["CATALYST_LLM_PROVIDER"] == "none"
    assert values["CATALYST_RUN_LLM"] == "false"
    assert values["CATALYST_LLM_DRY_RUN"] == "true"
    assert values["CATALYST_DRY_RUN_ALERTS"] == "true"
    assert values["CATALYST_WORKER_INTERVAL_SECONDS"] == "86400"
    assert values["CATALYST_WORKER_LOCK_TTL_SECONDS"] == "2700"
    assert values["CATALYST_WORKER_LOCK_NAME"] == "daily-run"
    assert values["SCHWAB_ORDER_SUBMISSION_ENABLED"] == "false"


def _env_example_values() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in Path(".env.example").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        assert separator == "=", f"invalid env line: {raw_line!r}"
        assert key not in values, f"duplicate env key: {key}"
        values[key] = value
    return values
