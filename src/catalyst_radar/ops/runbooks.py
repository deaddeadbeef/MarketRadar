from __future__ import annotations

provider_failure = "docs/runbooks/provider-failure.md"
llm_failure = "docs/runbooks/llm-failure.md"
score_drift = "docs/runbooks/score-drift.md"


def all_runbooks() -> dict[str, str]:
    return {
        "provider_failure": provider_failure,
        "llm_failure": llm_failure,
        "score_drift": score_drift,
    }


def provider_runbook(provider: str | None) -> str:
    normalized = str(provider or "").strip().lower()
    if normalized in {"llm", "openai", "anthropic", "gpt"}:
        return llm_failure
    return provider_failure


__all__ = ["all_runbooks", "llm_failure", "provider_failure", "provider_runbook", "score_drift"]
