from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import create_engine

from catalyst_radar.agents.budget import BudgetController
from catalyst_radar.agents.models import LLMCallStatus, LLMSkipReason
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


def test_router_rejects_unsupported_decision_card_schema_without_completion() -> None:
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
    assert result.status == LLMCallStatus.SCHEMA_REJECTED
    assert result.error == "unsupported schema version: decision-card-v1"
    assert len(entries) == 1
    assert entries[0].status == LLMCallStatus.SCHEMA_REJECTED
    assert entries[0].skip_reason == LLMSkipReason.SCHEMA_VALIDATION_FAILED
    assert entries[0].task.value == "gpt55_decision_card"
    assert entries[0].schema_version == "decision-card-v1"
    assert entries[0].actual_cost > 0
    assert entries[0].payload == {"error": "unsupported schema version: decision-card-v1"}


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


class FailingClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        raise RuntimeError("provider unavailable")


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
        conflicts=({"kind": "event_conflict", "source_id": "conflict-msft"},),
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
