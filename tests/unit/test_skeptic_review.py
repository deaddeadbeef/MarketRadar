from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import create_engine

from catalyst_radar.agents.budget import BudgetController
from catalyst_radar.agents.models import LLMCallStatus, LLMSkipReason
from catalyst_radar.agents.router import (
    FakeLLMClient,
    LLMClientRequest,
    LLMClientResult,
    LLMRouter,
)
from catalyst_radar.agents.skeptic import run_skeptic_review
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


@pytest.mark.parametrize(
    ("state", "expected_status", "expected_reason"),
    [
        (
            ActionState.RESEARCH_ONLY,
            LLMCallStatus.SKIPPED,
            LLMSkipReason.CANDIDATE_STATE_NOT_ELIGIBLE,
        ),
        (
            ActionState.ADD_TO_WATCHLIST,
            LLMCallStatus.SKIPPED,
            LLMSkipReason.CANDIDATE_STATE_NOT_ELIGIBLE,
        ),
        (ActionState.WARNING, LLMCallStatus.COMPLETED, None),
        (ActionState.THESIS_WEAKENING, LLMCallStatus.COMPLETED, None),
        (ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW, LLMCallStatus.COMPLETED, None),
        (ActionState.BLOCKED, LLMCallStatus.SKIPPED, LLMSkipReason.CANDIDATE_STATE_NOT_ELIGIBLE),
    ],
)
def test_skeptic_review_runs_only_for_warning_or_higher_candidates(
    state: ActionState,
    expected_status: LLMCallStatus,
    expected_reason: LLMSkipReason | None,
) -> None:
    repo = _repo()
    router = _router(repo=repo)

    result = run_skeptic_review(
        router=router,
        candidate=_candidate(state=state),
        available_at=NOW,
    )

    assert result.status == expected_status
    assert result.decision.reason == expected_reason
    assert repo.list_entries(available_at=NOW)[0].status == expected_status


def test_skeptic_review_returns_schema_rejected_for_unfaithful_output() -> None:
    repo = _repo()
    router = _router(repo=repo, client=UnknownSourceSkepticClient())

    result = run_skeptic_review(
        router=router,
        candidate=_candidate(),
        available_at=NOW,
    )

    entries = repo.list_entries(available_at=NOW)
    assert result.status == LLMCallStatus.SCHEMA_REJECTED
    assert result.error is not None
    assert "allowed_reference_ids" in result.error
    assert entries[0].status == LLMCallStatus.SCHEMA_REJECTED
    assert entries[0].skip_reason == LLMSkipReason.SCHEMA_VALIDATION_FAILED


def test_skeptic_review_result_never_changes_candidate_state() -> None:
    repo = _repo()
    candidate = _candidate(state=ActionState.WARNING)
    before_json = canonical_packet_json(candidate)
    before_state = candidate.state
    router = _router(repo=repo)

    result = run_skeptic_review(
        router=router,
        candidate=candidate,
        available_at=NOW,
    )

    assert result.status == LLMCallStatus.COMPLETED
    assert candidate.state == before_state
    assert canonical_packet_json(candidate) == before_json


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


def _router(
    *,
    repo: BudgetLedgerRepository,
    client: Any = None,
) -> LLMRouter:
    budget = BudgetController(
        config=_config(),
        ledger_repo=repo,
        now=lambda: NOW,
    )
    return LLMRouter(budget=budget, client=client or FakeLLMClient(), now=lambda: NOW)


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
        conflicts=(),
        hard_blocks=(),
        payload={
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
