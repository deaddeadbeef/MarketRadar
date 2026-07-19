"""Premium LLM provider helpers — Grok (xAI) is the primary AI backend."""

from __future__ import annotations

from collections.abc import Mapping

from catalyst_radar.core.config import AppConfig

# xAI Grok defaults (OpenAI-compatible Responses API).
DEFAULT_XAI_BASE_URL = "https://api.x.ai/v1"
DEFAULT_GROK_PRIMARY_MODEL = "grok-4.5"
DEFAULT_GROK_FAST_MODEL = "grok-4-1-fast-non-reasoning"
DEFAULT_GROK_INPUT_COST_PER_1M = 3.0
DEFAULT_GROK_CACHED_INPUT_COST_PER_1M = 0.75
DEFAULT_GROK_OUTPUT_COST_PER_1M = 15.0

# Historical OpenAI-only code paths still check provider names.
GROK_PROVIDER_ALIASES = frozenset({"grok", "xai", "x-ai"})
OPENAI_PROVIDER_ALIASES = frozenset({"openai"})
PREMIUM_LLM_PROVIDERS = GROK_PROVIDER_ALIASES | OPENAI_PROVIDER_ALIASES


def normalize_llm_provider(value: object) -> str:
    text = str(value or "none").strip().lower()
    if text in GROK_PROVIDER_ALIASES:
        return "grok"
    if text in OPENAI_PROVIDER_ALIASES:
        return "openai"
    if text in {"", "none", "off", "disabled"}:
        return "none"
    if text == "fake":
        return "fake"
    return text


def is_premium_llm_provider(value: object) -> bool:
    return normalize_llm_provider(value) in {"grok", "openai"}


def is_grok_provider(value: object) -> bool:
    return normalize_llm_provider(value) == "grok"


def llm_api_key(config: AppConfig) -> str | None:
    provider = normalize_llm_provider(config.llm_provider)
    if provider == "grok":
        return config.xai_api_key or config.openai_api_key
    if provider == "openai":
        return config.openai_api_key
    return config.xai_api_key or config.openai_api_key


def llm_base_url(config: AppConfig) -> str | None:
    provider = normalize_llm_provider(config.llm_provider)
    if provider == "grok":
        return (config.xai_base_url or DEFAULT_XAI_BASE_URL).rstrip("/")
    if provider == "openai":
        return None  # OpenAI SDK default
    if config.xai_base_url:
        return config.xai_base_url.rstrip("/")
    return None


def llm_primary_model(config: AppConfig) -> str | None:
    return (
        config.agent_sdk_model
        or config.llm_decision_card_model
        or config.llm_evidence_model
        or (DEFAULT_GROK_PRIMARY_MODEL if is_grok_provider(config.llm_provider) else None)
    )


def llm_fast_model(config: AppConfig) -> str | None:
    return (
        config.agent_sdk_fast_model
        or config.llm_skeptic_model
        or (
            DEFAULT_GROK_FAST_MODEL
            if is_grok_provider(config.llm_provider)
            else llm_primary_model(config)
        )
    )


def premium_llm_gate_missing(config: AppConfig) -> list[str]:
    """Return missing env/config items for real premium LLM / agent mode."""
    missing: list[str] = []
    if not config.enable_premium_llm:
        missing.append("CATALYST_ENABLE_PREMIUM_LLM=true")
    provider = normalize_llm_provider(config.llm_provider)
    if provider == "none":
        missing.append("CATALYST_LLM_PROVIDER=grok")
    elif provider not in {"grok", "openai", "fake"}:
        missing.append("CATALYST_LLM_PROVIDER=grok")
    if provider == "grok":
        if not (config.xai_api_key or config.openai_api_key):
            missing.append("XAI_API_KEY")
        if not (
            config.agent_sdk_model
            or config.llm_decision_card_model
            or config.llm_evidence_model
            or config.llm_skeptic_model
        ):
            missing.append(f"CATALYST_AGENT_SDK_MODEL={DEFAULT_GROK_PRIMARY_MODEL}")
    elif provider == "openai":
        if not config.openai_api_key:
            missing.append("OPENAI_API_KEY")
        if not (
            config.agent_sdk_model
            or config.llm_decision_card_model
            or config.llm_evidence_model
            or config.llm_skeptic_model
        ):
            missing.append("CATALYST_AGENT_SDK_MODEL")
    return list(dict.fromkeys(missing))


def agent_sdk_provider_ready(config: AppConfig) -> tuple[bool, list[str]]:
    missing: list[str] = []
    if not config.enable_agent_sdk:
        missing.append("CATALYST_ENABLE_AGENT_SDK=true")
    missing.extend(premium_llm_gate_missing(config))
    if not config.agent_sdk_model and is_premium_llm_provider(config.llm_provider):
        # Allow default Grok model resolution at runtime, but prefer explicit.
        if not is_grok_provider(config.llm_provider):
            missing.append("CATALYST_AGENT_SDK_MODEL")
    return (not missing, list(dict.fromkeys(missing)))


def configure_agents_sdk_for_config(config: AppConfig) -> None:
    """Point the OpenAI Agents SDK at Grok (xAI) or OpenAI before Runner calls."""
    provider = normalize_llm_provider(config.llm_provider)
    api_key = llm_api_key(config)
    if not api_key:
        msg = "llm_api_key_missing"
        raise RuntimeError(msg)
    base_url = llm_base_url(config)

    try:
        from openai import AsyncOpenAI
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("openai package is not installed") from exc

    client_kwargs: dict[str, object] = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url
    client = AsyncOpenAI(**client_kwargs)

    try:
        from agents import set_default_openai_client
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("openai-agents package is not installed") from exc

    set_default_openai_client(client)

    # Some agents SDK builds also honor OPENAI_BASE_URL / OPENAI_API_KEY in-process.
    import os

    os.environ["OPENAI_API_KEY"] = api_key
    if base_url:
        os.environ["OPENAI_BASE_URL"] = base_url
    elif "OPENAI_BASE_URL" in os.environ and provider == "openai":
        # Leave explicit user base URL alone for openai.
        pass


def provider_display_name(config: AppConfig) -> str:
    provider = normalize_llm_provider(config.llm_provider)
    if provider == "grok":
        return "grok"
    if provider == "openai":
        return "openai"
    return provider


def external_llm_call_bucket(config: AppConfig) -> str:
    """JSON counter key for LLM external calls (legacy key remains openai)."""
    # Keep schema stability for dashboards that read external_calls_made.openai.
    return "openai"


def runtime_models_payload(config: AppConfig) -> Mapping[str, object]:
    primary = llm_primary_model(config)
    fast = llm_fast_model(config)
    return {
        "primary": primary,
        "fast": fast,
        "fast_fallback_to_primary": bool(fast and primary and fast == primary),
        "provider": provider_display_name(config),
        "base_url": llm_base_url(config),
    }
