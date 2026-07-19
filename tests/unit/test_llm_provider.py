from __future__ import annotations

from catalyst_radar.agents.llm_provider import (
    DEFAULT_GROK_PRIMARY_MODEL,
    DEFAULT_XAI_BASE_URL,
    agent_sdk_provider_ready,
    is_grok_provider,
    is_premium_llm_provider,
    llm_api_key,
    llm_base_url,
    llm_primary_model,
    normalize_llm_provider,
)
from catalyst_radar.core.config import AppConfig


def test_normalize_llm_provider_aliases() -> None:
    assert normalize_llm_provider("grok") == "grok"
    assert normalize_llm_provider("xai") == "grok"
    assert normalize_llm_provider("X-AI") == "grok"
    assert normalize_llm_provider("openai") == "openai"
    assert normalize_llm_provider("none") == "none"


def test_grok_config_resolves_xai_key_and_base_url() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_LLM_PROVIDER": "grok",
            "XAI_API_KEY": "xai-secret",
            "CATALYST_AGENT_SDK_MODEL": "grok-4.5",
        }
    )
    assert is_premium_llm_provider(config.llm_provider)
    assert is_grok_provider(config.llm_provider)
    assert llm_api_key(config) == "xai-secret"
    assert llm_base_url(config) == DEFAULT_XAI_BASE_URL
    assert llm_primary_model(config) == "grok-4.5"
    assert config.sanitized()["xai_api_key"] == "<redacted>"


def test_agent_sdk_ready_for_grok() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_ENABLE_PREMIUM_LLM": "true",
            "CATALYST_ENABLE_AGENT_SDK": "true",
            "CATALYST_LLM_PROVIDER": "grok",
            "XAI_API_KEY": "xai-secret",
            "CATALYST_AGENT_SDK_MODEL": DEFAULT_GROK_PRIMARY_MODEL,
        }
    )
    ready, missing = agent_sdk_provider_ready(config)
    assert ready is True
    assert missing == []


def test_agent_sdk_blocked_without_xai_key() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_ENABLE_PREMIUM_LLM": "true",
            "CATALYST_ENABLE_AGENT_SDK": "true",
            "CATALYST_LLM_PROVIDER": "grok",
            "CATALYST_AGENT_SDK_MODEL": "grok-4.5",
        }
    )
    ready, missing = agent_sdk_provider_ready(config)
    assert ready is False
    assert "XAI_API_KEY" in missing
