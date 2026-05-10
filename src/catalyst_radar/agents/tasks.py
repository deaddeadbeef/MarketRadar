from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from catalyst_radar.agents.models import LLMTaskName
from catalyst_radar.core.models import ActionState


@dataclass(frozen=True)
class LLMTask:
    name: LLMTaskName
    eligible_states: tuple[ActionState, ...]
    default_daily_cap: int
    max_input_tokens: int
    max_output_tokens: int
    prompt_version: str
    schema_version: str
    model_config_key: str
    manual_only: bool = False


DEFAULT_TASKS: Mapping[str, LLMTask] = {
    "mini_extraction": LLMTask(
        name=LLMTaskName.MINI_EXTRACTION,
        eligible_states=(ActionState.RESEARCH_ONLY, ActionState.ADD_TO_WATCHLIST),
        default_daily_cap=200,
        max_input_tokens=4000,
        max_output_tokens=700,
        prompt_version="mini_extraction_v1",
        schema_version="evidence-review-v1",
        model_config_key="llm_evidence_model",
    ),
    "mid_review": LLMTask(
        name=LLMTaskName.MID_REVIEW,
        eligible_states=(
            ActionState.WARNING,
            ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        ),
        default_daily_cap=50,
        max_input_tokens=8000,
        max_output_tokens=1200,
        prompt_version="evidence_review_v1",
        schema_version="evidence-review-v1",
        model_config_key="llm_evidence_model",
    ),
    "skeptic_review": LLMTask(
        name=LLMTaskName.SKEPTIC_REVIEW,
        eligible_states=(
            ActionState.WARNING,
            ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        ),
        default_daily_cap=20,
        max_input_tokens=9000,
        max_output_tokens=1400,
        prompt_version="skeptic_review_v1",
        schema_version="evidence-review-v1",
        model_config_key="llm_skeptic_model",
    ),
    "gpt55_decision_card": LLMTask(
        name=LLMTaskName.GPT55_DECISION_CARD,
        eligible_states=(ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,),
        default_daily_cap=8,
        max_input_tokens=12000,
        max_output_tokens=2200,
        prompt_version="decision_card_v1",
        schema_version="decision-card-v1",
        model_config_key="llm_decision_card_model",
    ),
    "full_transcript_deep_dive": LLMTask(
        name=LLMTaskName.FULL_TRANSCRIPT_DEEP_DIVE,
        eligible_states=(
            ActionState.WARNING,
            ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        ),
        default_daily_cap=0,
        max_input_tokens=40000,
        max_output_tokens=4000,
        prompt_version="full_transcript_deep_dive_v1",
        schema_version="evidence-review-v1",
        model_config_key="llm_skeptic_model",
        manual_only=True,
    ),
}


__all__ = ["DEFAULT_TASKS", "LLMTask"]
