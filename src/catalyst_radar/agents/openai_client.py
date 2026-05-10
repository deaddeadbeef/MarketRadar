from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any

from catalyst_radar.agents.models import TokenUsage
from catalyst_radar.agents.router import LLMClientRequest, LLMClientResult
from catalyst_radar.agents.tasks import LLMTask


class OpenAIResponsesClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        sdk_client: Any | None = None,
    ) -> None:
        self._api_key = api_key
        self._sdk_client = sdk_client

    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        response = self._client().responses.create(
            model=request.model,
            instructions=_prompt_text(request),
            input=_request_input_json(request),
            max_output_tokens=request.max_output_tokens,
            store=False,
            text={
                "format": {
                    "type": "json_schema",
                    "name": request.schema_version.replace("-", "_"),
                    "schema": schema_for_task(request.task),
                    "strict": True,
                }
            },
        )
        payload = json.loads(_response_output_text(response))
        if not isinstance(payload, Mapping):
            msg = "openai_response_payload_not_object"
            raise RuntimeError(msg)
        return LLMClientResult(
            payload=payload,
            token_usage=_token_usage_from_response(response),
            model=str(_field(response, "model", request.model)),
            provider="openai",
        )

    def _client(self) -> Any:
        if self._sdk_client is not None:
            return self._sdk_client

        api_key = (self._api_key or os.environ.get("OPENAI_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("openai_api_key_missing")

        from openai import OpenAI

        self._sdk_client = OpenAI(api_key=api_key)
        return self._sdk_client


def schema_for_task(task: LLMTask) -> Mapping[str, Any]:
    if task.schema_version == "skeptic-review-v1":
        return _skeptic_review_schema()
    if task.schema_version == "decision-card-v1":
        return _decision_card_schema()
    return _evidence_review_schema()


def _request_input_json(request: LLMClientRequest) -> str:
    return json.dumps(
        {
            "task": request.task.name.value,
            "prompt_version": request.prompt_version,
            "schema_version": request.schema_version,
            "candidate_packet": json.loads(request.candidate_json),
            "agent_evidence_packet": request.evidence_packet,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _prompt_text(request: LLMClientRequest) -> str:
    prompts = {
        "mini_extraction_v1": _EVIDENCE_REVIEW_PROMPT,
        "evidence_review_v1": _EVIDENCE_REVIEW_PROMPT,
        "skeptic_review_v1": _SKEPTIC_REVIEW_PROMPT,
        "decision_card_v1": _DECISION_CARD_PROMPT,
    }
    return prompts.get(request.prompt_version, _EVIDENCE_REVIEW_PROMPT)


def _response_output_text(response: Any) -> str:
    output_text = _field(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text
    if isinstance(response, Mapping):
        output_text = response.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text
    msg = "openai_response_missing_output_text"
    raise RuntimeError(msg)


def _token_usage_from_response(response: Any) -> TokenUsage:
    usage = _field(response, "usage", {})
    input_tokens = _int_field(usage, "input_tokens", "prompt_tokens")
    output_tokens = _int_field(usage, "output_tokens", "completion_tokens")
    details = _field(usage, "input_tokens_details", None)
    if details is None:
        details = _field(usage, "prompt_tokens_details", {})
    cached_input_tokens = _int_field(
        details,
        "cached_tokens",
        "cached_input_tokens",
        "cached_prompt_tokens",
    )
    return TokenUsage(
        input_tokens=input_tokens,
        cached_input_tokens=cached_input_tokens,
        output_tokens=output_tokens,
    )


def _field(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _int_field(source: Any, *names: str) -> int:
    for name in names:
        value = _field(source, name, None)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return 0


def _base_schema(properties: Mapping[str, Any], required: list[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": dict(properties),
        "required": required,
        "additionalProperties": False,
    }


def _evidence_review_schema() -> Mapping[str, Any]:
    claim = _base_schema(
        {
            "claim": {"type": "string"},
            "source_id": _nullable_string_schema(),
            "computed_feature_id": _nullable_string_schema(),
            "source_quality": {"type": "number", "minimum": 0, "maximum": 1},
            "evidence_type": {"type": "string"},
            "sentiment": {"type": "number", "minimum": -1, "maximum": 1},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "uncertainty_notes": {"type": "string"},
        },
        [
            "claim",
            "source_id",
            "computed_feature_id",
            "source_quality",
            "evidence_type",
            "sentiment",
            "confidence",
            "uncertainty_notes",
        ],
    )
    return _base_schema(
        {
            "ticker": {"type": "string"},
            "as_of": {"type": "string"},
            "claims": {"type": "array", "items": claim},
            "bear_case": {"type": "array", "items": {"type": "string"}},
            "unresolved_conflicts": {"type": "array"},
            "recommended_policy_downgrade": {"type": "boolean"},
        },
        [
            "ticker",
            "as_of",
            "claims",
            "bear_case",
            "unresolved_conflicts",
            "recommended_policy_downgrade",
        ],
    )


def _skeptic_review_schema() -> Mapping[str, Any]:
    bear_case_item = _base_schema(
        {
            "claim": {"type": "string"},
            "source_id": _nullable_string_schema(),
            "computed_feature_id": _nullable_string_schema(),
            "severity": {"type": "string", "enum": ["low", "medium", "high"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "why_it_matters": {"type": "string"},
        },
        [
            "claim",
            "source_id",
            "computed_feature_id",
            "severity",
            "confidence",
            "why_it_matters",
        ],
    )
    return _base_schema(
        {
            "ticker": {"type": "string"},
            "as_of": {"type": "string"},
            "schema_version": {"type": "string", "const": "skeptic-review-v1"},
            "bear_case": {"type": "array", "items": bear_case_item},
            "missing_evidence": {"type": "array", "items": {"type": "string"}},
            "contradictions": {"type": "array", "items": {"type": "string"}},
            "recommended_policy_downgrade": {"type": "boolean"},
            "manual_review_notes": {"type": "string"},
        },
        [
            "ticker",
            "as_of",
            "schema_version",
            "bear_case",
            "missing_evidence",
            "contradictions",
            "recommended_policy_downgrade",
            "manual_review_notes",
        ],
    )


def _decision_card_schema() -> Mapping[str, Any]:
    point = _base_schema(
        {
            "text": {"type": "string"},
            "source_id": _nullable_string_schema(),
            "computed_feature_id": _nullable_string_schema(),
            "confidence": _nullable_number_schema(minimum=0, maximum=1),
        },
        ["text", "source_id", "computed_feature_id", "confidence"],
    )
    return _base_schema(
        {
            "ticker": {"type": "string"},
            "as_of": {"type": "string"},
            "schema_version": {"type": "string", "const": "decision-card-v1"},
            "summary": {"type": "string"},
            "supporting_points": {"type": "array", "items": point},
            "risks": {"type": "array", "items": point},
            "questions_for_human": {"type": "array", "items": {"type": "string"}},
            "manual_review_only": {"type": "boolean", "const": True},
        },
        [
            "ticker",
            "as_of",
            "schema_version",
            "summary",
            "supporting_points",
            "risks",
            "questions_for_human",
            "manual_review_only",
        ],
    )


def _nullable_string_schema() -> dict[str, Any]:
    return {"type": ["string", "null"]}


def _nullable_number_schema(*, minimum: float, maximum: float) -> dict[str, Any]:
    return {"type": ["number", "null"], "minimum": minimum, "maximum": maximum}


_EVIDENCE_REVIEW_PROMPT = """You are reviewing a source-linked equity candidate packet
for investment decision support.

Rules:
- Do not compute scores, risk limits, sizing, or portfolio exposure.
- Do not recommend an autonomous buy or sell action.
- Use only the provided candidate packet, computed features, and evidence snippets.
- Every factual claim must include source_id or computed_feature_id.
- Return only JSON matching schema evidence-review-v1."""

_SKEPTIC_REVIEW_PROMPT = """You are producing a bear-case review for a human investment reviewer.

Rules:
- Use only the supplied agent evidence packet.
- Every factual claim must include source_id or computed_feature_id.
- Do not compute scores, risk limits, sizing, portfolio exposure, or price targets.
- Do not recommend autonomous buying, selling, or order placement.
- Return only JSON matching schema skeptic-review-v1."""

_DECISION_CARD_PROMPT = """You are drafting narrative sections for a human-review-only
Decision Card.

Rules:
- Use only the supplied agent evidence packet and deterministic Decision Card payload.
- Do not alter action state, scores, trade plan, position sizing, portfolio impact,
  hard blocks, or next review time.
- Every factual point must include source_id or computed_feature_id.
- Do not say the system will buy, sell, execute, or place orders.
- Return only JSON matching schema decision-card-v1."""


__all__ = ["OpenAIResponsesClient", "schema_for_task"]
