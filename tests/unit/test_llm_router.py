from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import create_engine

from catalyst_radar.agents.budget import BudgetController
from catalyst_radar.agents.evidence import build_agent_evidence_packet
from catalyst_radar.agents.models import LLMCallStatus, LLMSkipReason, TokenUsage
from catalyst_radar.agents.openai_client import OpenAIResponsesClient
from catalyst_radar.agents.router import (
    FakeLLMClient,
    LLMClientRequest,
    LLMClientResult,
    LLMRouter,
)
from catalyst_radar.agents.tasks import DEFAULT_TASKS
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState
from catalyst_radar.pipeline.candidate_packet import (
    CandidatePacket,
    EvidenceItem,
    canonical_packet_json,
)
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.db import create_schema

NOW = datetime(2026, 5, 10, 14, tzinfo=UTC)
AS_OF = datetime(2026, 5, 8, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 8, 20, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 8, 21, 5, tzinfo=UTC)


def test_router_returns_skip_when_budget_blocks() -> None:
    repo = _repo()
    router = _router(repo=repo, config=AppConfig.from_env({}))

    result = router.review_candidate(
        task=DEFAULT_TASKS["mid_review"],
        candidate=_candidate(),
        available_at=NOW,
    )

    entries = repo.list_entries(available_at=NOW)
    assert result.decision.skip is True
    assert result.decision.reason == LLMSkipReason.PREMIUM_LLM_DISABLED
    assert result.status == LLMCallStatus.SKIPPED
    assert len(entries) == 1
    assert entries[0].status == LLMCallStatus.SKIPPED
    assert entries[0].skip_reason == LLMSkipReason.PREMIUM_LLM_DISABLED


def test_router_dry_run_logs_estimate_without_client_call() -> None:
    repo = _repo()
    client = CountingClient()
    router = _router(repo=repo, client=client)

    result = router.review_candidate(
        task=DEFAULT_TASKS["mid_review"],
        candidate=_candidate(),
        available_at=NOW,
        dry_run=True,
    )

    entries = repo.list_entries(available_at=NOW)
    assert client.calls == 0
    assert result.status == LLMCallStatus.DRY_RUN
    assert entries[0].status == LLMCallStatus.DRY_RUN
    assert entries[0].estimated_cost > 0
    assert entries[0].actual_cost == 0.0


def test_router_fake_client_logs_completed_entry() -> None:
    repo = _repo()
    router = _router(repo=repo, client=FakeLLMClient())

    result = router.review_candidate(
        task=DEFAULT_TASKS["mid_review"],
        candidate=_candidate(),
        available_at=NOW,
    )

    entries = repo.list_entries(available_at=NOW)
    assert result.status == LLMCallStatus.COMPLETED
    assert result.payload is not None
    assert result.payload["claims"][0]["source_id"] == "event-msft"
    assert entries[0].status == LLMCallStatus.COMPLETED
    assert entries[0].model == "model-review"
    assert entries[0].prompt_version == "evidence_review_v1"
    assert entries[0].schema_version == "evidence-review-v1"
    assert entries[0].actual_cost > 0
    assert entries[0].ticker == "MSFT"
    assert entries[0].candidate_packet_id == "packet-msft"
    assert entries[0].candidate_state_id == "state-msft"
    assert entries[0].candidate_state == ActionState.WARNING.value
    assert entries[0].outcome_label == "evidence_review"


def test_router_fake_client_logs_skeptic_review_entry() -> None:
    repo = _repo()
    router = _router(repo=repo, client=FakeLLMClient())

    result = router.review_candidate(
        task=DEFAULT_TASKS["skeptic_review"],
        candidate=_candidate(),
        available_at=NOW,
    )

    entries = repo.list_entries(available_at=NOW)
    assert result.status == LLMCallStatus.COMPLETED
    assert result.payload is not None
    assert result.payload["schema_version"] == "skeptic-review-v1"
    assert result.payload["bear_case"][0]["computed_feature_id"] == "feature-valuation"
    assert entries[0].status == LLMCallStatus.COMPLETED
    assert entries[0].model == "model-skeptic"
    assert entries[0].prompt_version == "skeptic_review_v1"
    assert entries[0].schema_version == "skeptic-review-v1"
    assert entries[0].outcome_label == "skeptic_review"
    assert entries[0].payload["bear_case"][0]["computed_feature_id"] == (
        "feature-valuation"
    )


def test_router_fake_client_logs_decision_card_draft_entry() -> None:
    repo = _repo()
    router = _router(repo=repo, client=FakeLLMClient())

    result = router.review_candidate(
        task=DEFAULT_TASKS["gpt55_decision_card"],
        candidate=_candidate(
            state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
            final_score=95.0,
        ),
        available_at=NOW,
    )

    entries = repo.list_entries(available_at=NOW)
    assert result.status == LLMCallStatus.COMPLETED
    assert result.payload is not None
    assert result.payload["schema_version"] == "decision-card-v1"
    assert result.payload["supporting_points"][0]["source_id"] == "event-msft"
    assert result.payload["risks"][0]["computed_feature_id"] == "feature-valuation"
    assert result.payload["manual_review_only"] is True
    assert entries[0].status == LLMCallStatus.COMPLETED
    assert entries[0].model == "model-decision"
    assert entries[0].prompt_version == "decision_card_v1"
    assert entries[0].schema_version == "decision-card-v1"
    assert entries[0].outcome_label == "decision_card_draft"


def test_router_rejects_schema_failure_and_logs_schema_rejected() -> None:
    repo = _repo()
    router = _router(repo=repo, client=InvalidSchemaClient())

    result = router.review_candidate(
        task=DEFAULT_TASKS["mid_review"],
        candidate=_candidate(),
        available_at=NOW,
    )

    entries = repo.list_entries(available_at=NOW)
    assert result.status == LLMCallStatus.SCHEMA_REJECTED
    assert entries[0].status == LLMCallStatus.SCHEMA_REJECTED
    assert entries[0].skip_reason == LLMSkipReason.SCHEMA_VALIDATION_FAILED


def test_router_rejects_skeptic_review_with_unknown_source() -> None:
    repo = _repo()
    router = _router(repo=repo, client=UnknownSourceSkepticClient())

    result = router.review_candidate(
        task=DEFAULT_TASKS["skeptic_review"],
        candidate=_candidate(),
        available_at=NOW,
    )

    entries = repo.list_entries(available_at=NOW)
    assert result.status == LLMCallStatus.SCHEMA_REJECTED
    assert result.error is not None
    assert "allowed_reference_ids" in result.error
    assert len(entries) == 1
    assert entries[0].status == LLMCallStatus.SCHEMA_REJECTED
    assert entries[0].skip_reason == LLMSkipReason.SCHEMA_VALIDATION_FAILED
    assert entries[0].task.value == "skeptic_review"
    assert entries[0].schema_version == "skeptic-review-v1"
    assert entries[0].actual_cost > 0
    assert "allowed_reference_ids" in entries[0].payload["error"]


def test_router_logs_failed_entry_when_client_raises() -> None:
    repo = _repo()
    router = _router(repo=repo, client=FailingClient())

    result = router.review_candidate(
        task=DEFAULT_TASKS["mid_review"],
        candidate=_candidate(),
        available_at=NOW,
    )

    entries = repo.list_entries(available_at=NOW)
    assert result.status == LLMCallStatus.FAILED
    assert len(entries) == 1
    assert entries[0].status == LLMCallStatus.FAILED
    assert entries[0].skip_reason == LLMSkipReason.CLIENT_ERROR


def test_router_does_not_mutate_candidate_packet_payload() -> None:
    repo = _repo()
    candidate = _candidate()
    before = canonical_packet_json(candidate)
    router = _router(repo=repo, client=FakeLLMClient())

    router.review_candidate(
        task=DEFAULT_TASKS["mid_review"],
        candidate=candidate,
        available_at=NOW,
    )

    assert canonical_packet_json(candidate) == before


def test_router_does_not_mutate_packet_or_decision_card_payloads() -> None:
    repo = _repo()
    decision_card_payload = {
        "identity": {
            "ticker": "MSFT",
            "action_state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
        },
        "scores": {"final_score": 95.0},
        "trade_plan": {"entry_zone": [300.0, 305.0]},
        "position_sizing": {"max_notional": 1000.0},
        "portfolio_impact": {"single_name_after_pct": 4.0},
        "controls": {"manual_review_only": True},
    }
    candidate = _candidate(
        state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        final_score=95.0,
        payload={"decision_card": copy.deepcopy(decision_card_payload)},
    )
    candidate_before = canonical_packet_json(candidate)
    decision_card_before = json.dumps(
        decision_card_payload,
        sort_keys=True,
        separators=(",", ":"),
    )
    router = _router(repo=repo, client=FakeLLMClient())

    result = router.review_candidate(
        task=DEFAULT_TASKS["gpt55_decision_card"],
        candidate=candidate,
        available_at=NOW,
    )

    assert result.status == LLMCallStatus.COMPLETED
    assert canonical_packet_json(candidate) == candidate_before
    assert json.dumps(decision_card_payload, sort_keys=True, separators=(",", ":")) == (
        decision_card_before
    )
    assert candidate.payload["decision_card"]["scores"]["final_score"] == 95.0


def test_openai_client_builds_responses_request_with_strict_json_schema() -> None:
    task = DEFAULT_TASKS["skeptic_review"]
    request = _openai_request(task=task)
    sdk_client = FakeOpenAISdk(
        payload={
            "ticker": "MSFT",
            "as_of": AS_OF.isoformat(),
            "schema_version": "skeptic-review-v1",
            "bear_case": [],
            "missing_evidence": [],
            "contradictions": [],
            "recommended_policy_downgrade": False,
            "manual_review_notes": "Review the linked evidence.",
        },
    )

    result = OpenAIResponsesClient(sdk_client=sdk_client).complete(request)

    assert result.provider == "openai"
    assert result.payload["ticker"] == "MSFT"
    call = sdk_client.responses.calls[0]
    assert call["model"] == "model-skeptic"
    assert call["store"] is False
    assert call["max_output_tokens"] == task.max_output_tokens
    assert "schema skeptic-review-v1" in call["instructions"]
    request_payload = json.loads(call["input"])
    assert request_payload["task"] == "skeptic_review"
    assert request_payload["candidate_packet"] == json.loads(request.candidate_json)
    assert request_payload["agent_evidence_packet"] == request.evidence_packet
    assert call["text"]["format"]["type"] == "json_schema"
    assert call["text"]["format"]["name"] == "skeptic_review_v1"
    assert call["text"]["format"]["schema"]["required"] == [
        "ticker",
        "as_of",
        "schema_version",
        "bear_case",
        "missing_evidence",
        "contradictions",
        "recommended_policy_downgrade",
        "manual_review_notes",
    ]
    assert call["text"]["format"]["strict"] is True


def test_openai_client_converts_usage_to_token_usage() -> None:
    sdk_client = FakeOpenAISdk(
        payload={
            "ticker": "MSFT",
            "as_of": AS_OF.isoformat(),
            "claims": [],
            "bear_case": [],
            "unresolved_conflicts": [],
            "recommended_policy_downgrade": False,
        },
        usage={
            "input_tokens": 321,
            "output_tokens": 45,
            "input_tokens_details": {"cached_tokens": 67},
        },
    )

    result = OpenAIResponsesClient(sdk_client=sdk_client).complete(_openai_request())

    assert result.token_usage == TokenUsage(
        input_tokens=321,
        cached_input_tokens=67,
        output_tokens=45,
    )


def test_openai_client_requires_api_key_for_real_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="^openai_api_key_missing$"):
        OpenAIResponsesClient().complete(_openai_request())


class CountingClient:
    calls = 0

    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        self.calls += 1
        return FakeLLMClient().complete(request)


class InvalidSchemaClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        return LLMClientResult(
            payload={
                "ticker": request.candidate.ticker,
                "as_of": request.candidate.as_of.isoformat(),
            },
            token_usage=request.estimated_usage,
            model=request.model,
            provider="fake",
        )


class UnknownSourceSkepticClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        return LLMClientResult(
            payload={
                "ticker": request.candidate.ticker,
                "as_of": request.candidate.as_of.isoformat(),
                "schema_version": request.schema_version,
                "bear_case": [
                    {
                        "claim": "A hallucinated source weakens the setup.",
                        "source_id": "event-unknown",
                        "severity": "medium",
                        "confidence": 0.5,
                        "why_it_matters": "Unknown sources cannot support review.",
                    }
                ],
                "missing_evidence": [],
                "contradictions": [],
                "recommended_policy_downgrade": False,
                "manual_review_notes": "Human reviewer should inspect source quality.",
            },
            token_usage=request.estimated_usage,
            model=request.model,
            provider="fake",
        )


class FailingClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        raise RuntimeError("provider unavailable")


class FakeOpenAISdk:
    def __init__(
        self,
        *,
        payload: Mapping[str, Any],
        usage: Mapping[str, Any] | None = None,
    ) -> None:
        self.responses = FakeResponsesResource(payload=payload, usage=usage or {})


class FakeResponsesResource:
    def __init__(self, *, payload: Mapping[str, Any], usage: Mapping[str, Any]) -> None:
        self.payload = payload
        self.usage = usage
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        return FakeOpenAIResponse(
            output_text=json.dumps(self.payload),
            usage=self.usage,
            model=kwargs["model"],
        )


class FakeOpenAIResponse:
    def __init__(self, *, output_text: str, usage: Mapping[str, Any], model: str) -> None:
        self.output_text = output_text
        self.usage = usage
        self.model = model


def _router(
    *,
    repo: BudgetLedgerRepository,
    client: Any = None,
    config: AppConfig | None = None,
) -> LLMRouter:
    budget = BudgetController(
        config=config or _config(),
        ledger_repo=repo,
        now=lambda: NOW,
    )
    return LLMRouter(budget=budget, client=client or FakeLLMClient(), now=lambda: NOW)


def _openai_request(
    *,
    task: Any = DEFAULT_TASKS["mid_review"],
    candidate: CandidatePacket | None = None,
) -> LLMClientRequest:
    candidate = candidate or _candidate()
    return LLMClientRequest(
        task=task,
        candidate=candidate,
        candidate_json=canonical_packet_json(candidate),
        evidence_packet=build_agent_evidence_packet(candidate),
        prompt_version=task.prompt_version,
        schema_version=task.schema_version,
        model={
            "mid_review": "model-review",
            "skeptic_review": "model-skeptic",
            "gpt55_decision_card": "model-decision",
        }.get(task.name.value, "model-review"),
        max_output_tokens=task.max_output_tokens,
        estimated_usage=TokenUsage(input_tokens=100, output_tokens=task.max_output_tokens),
    )


def _config() -> AppConfig:
    return AppConfig.from_env(
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
            "CATALYST_LLM_DAILY_BUDGET_USD": "2.00",
            "CATALYST_LLM_MONTHLY_BUDGET_USD": "20.00",
        }
    )


def _repo() -> BudgetLedgerRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    return BudgetLedgerRepository(engine)


def _candidate(
    *,
    state: ActionState = ActionState.WARNING,
    final_score: float = 82.5,
    payload: dict[str, object] | None = None,
) -> CandidatePacket:
    return CandidatePacket(
        id="packet-msft",
        ticker="MSFT",
        as_of=AS_OF,
        candidate_state_id="state-msft",
        state=state,
        final_score=final_score,
        supporting_evidence=(
            EvidenceItem(
                kind="event",
                title="Cloud revenue guidance raised",
                summary="Company raised cloud revenue guidance.",
                polarity="supporting",
                strength=0.9,
                source_id="event-msft",
                source_quality=0.95,
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
            ),
        ),
        disconfirming_evidence=(
            EvidenceItem(
                kind="valuation",
                title="Valuation is extended",
                summary="Multiple remains above peer median.",
                polarity="disconfirming",
                strength=0.4,
                computed_feature_id="feature-valuation",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
            ),
        ),
        conflicts=({"kind": "event_conflict", "source_id": "conflict-msft"},),
        hard_blocks=(),
        payload=payload
        or {
            "ticker": "MSFT",
            "as_of": AS_OF.isoformat(),
            "supporting_evidence": [
                {
                    "source_id": "event-msft",
                    "summary": "Company raised cloud revenue guidance.",
                }
            ],
        },
        source_ts=SOURCE_TS,
        available_at=AVAILABLE_AT,
    )
