from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine

from catalyst_radar.agents.budget import BudgetController
from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMSkipReason,
    LLMTaskName,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.agents.tasks import DEFAULT_TASKS
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.db import create_schema

NOW = datetime(2026, 5, 10, 14, tzinfo=UTC)


def test_estimates_cost_with_cached_tokens() -> None:
    controller = _controller()

    cost = controller.estimate_cost(
        TokenUsage(input_tokens=1_000, cached_input_tokens=400, output_tokens=100)
    )

    assert cost == 0.0062


def test_blocks_when_premium_llm_disabled() -> None:
    controller = _controller(config=AppConfig.from_env({}))

    decision = _allow_mid_review(controller)

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.PREMIUM_LLM_DISABLED


def test_blocks_ineligible_candidate_state() -> None:
    controller = _controller()

    decision = _allow_mid_review(controller, candidate_state=ActionState.NO_ACTION)

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.CANDIDATE_STATE_NOT_ELIGIBLE


def test_blocks_missing_model_or_pricing() -> None:
    missing_model = _controller(
        config=_config({"CATALYST_LLM_EVIDENCE_MODEL": ""}),
    )
    assert _allow_mid_review(missing_model).reason == LLMSkipReason.MODEL_NOT_CONFIGURED

    missing_pricing = _controller(
        config=_config({"CATALYST_LLM_OUTPUT_COST_PER_1M": ""}),
    )
    assert _allow_mid_review(missing_pricing).reason == LLMSkipReason.PRICING_MISSING


def test_blocks_blank_whitespace_model() -> None:
    controller = _controller(
        config=_config({"CATALYST_LLM_EVIDENCE_MODEL": "   "}),
    )

    decision = _allow_mid_review(controller)

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.MODEL_NOT_CONFIGURED


def test_blocks_stale_pricing() -> None:
    controller = _controller(
        config=_config(
            {
                "CATALYST_LLM_PRICING_UPDATED_AT": "2026-04-01",
                "CATALYST_LLM_PRICING_STALE_AFTER_DAYS": "30",
            }
        ),
    )

    decision = _allow_mid_review(controller)

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.PRICING_STALE
    assert decision.estimated_cost > 0


def test_blocks_future_pricing_date() -> None:
    controller = _controller(
        config=_config({"CATALYST_LLM_PRICING_UPDATED_AT": "2026-05-11"}),
    )

    decision = _allow_mid_review(controller)

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.PRICING_STALE


def test_blocks_per_task_daily_cap() -> None:
    repo = _repo()
    _insert_entry(repo, task=LLMTaskName.MID_REVIEW, actual_cost=0.10)
    controller = _controller(
        config=_config({"CATALYST_LLM_TASK_DAILY_CAPS": "mid_review=1"}),
        repo=repo,
    )

    decision = _allow_mid_review(controller)

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.TASK_DAILY_CAP_EXCEEDED
    assert decision.task_daily_count == 1


def test_blocks_per_task_daily_cap_after_schema_rejected_attempt() -> None:
    repo = _repo()
    _insert_entry(
        repo,
        task=LLMTaskName.MID_REVIEW,
        status=LLMCallStatus.SCHEMA_REJECTED,
        actual_cost=0.10,
    )
    controller = _controller(
        config=_config({"CATALYST_LLM_TASK_DAILY_CAPS": "mid_review=1"}),
        repo=repo,
    )

    decision = _allow_mid_review(controller)

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.TASK_DAILY_CAP_EXCEEDED
    assert decision.task_daily_count == 1


def test_blocks_daily_and_monthly_budget_caps() -> None:
    repo = _repo()
    _insert_entry(repo, task=LLMTaskName.MID_REVIEW, actual_cost=0.10)
    daily_controller = _controller(
        config=_config(
            {
                "CATALYST_LLM_DAILY_BUDGET_USD": "0.10",
                "CATALYST_LLM_MONTHLY_BUDGET_USD": "5.00",
            }
        ),
        repo=repo,
    )

    daily_decision = _allow_mid_review(daily_controller)

    assert daily_decision.allowed is False
    assert daily_decision.reason == LLMSkipReason.DAILY_BUDGET_EXCEEDED
    assert daily_decision.daily_spend == 0.10

    monthly_controller = _controller(
        config=_config(
            {
                "CATALYST_LLM_DAILY_BUDGET_USD": "5.00",
                "CATALYST_LLM_MONTHLY_BUDGET_USD": "0.10",
            }
        ),
        repo=repo,
    )

    monthly_decision = _allow_mid_review(monthly_controller)

    assert monthly_decision.allowed is False
    assert monthly_decision.reason == LLMSkipReason.MONTHLY_BUDGET_EXCEEDED
    assert monthly_decision.monthly_spend == 0.10


def test_blocks_daily_budget_after_failed_and_rejected_paid_attempts() -> None:
    repo = _repo()
    _insert_entry(
        repo,
        task=LLMTaskName.MID_REVIEW,
        status=LLMCallStatus.SCHEMA_REJECTED,
        actual_cost=0.06,
    )
    _insert_entry(
        repo,
        task=LLMTaskName.MID_REVIEW,
        status=LLMCallStatus.FAILED,
        actual_cost=0.05,
    )
    controller = _controller(
        config=_config(
            {
                "CATALYST_LLM_DAILY_BUDGET_USD": "0.10",
                "CATALYST_LLM_MONTHLY_BUDGET_USD": "5.00",
            }
        ),
        repo=repo,
    )

    decision = _allow_mid_review(controller)

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.DAILY_BUDGET_EXCEEDED
    assert decision.daily_spend == 0.11


def test_blocks_gpt55_below_score_after_soft_monthly_cap() -> None:
    repo = _repo()
    _insert_entry(repo, task=LLMTaskName.MID_REVIEW, actual_cost=0.81)
    controller = _controller(
        config=_config(
            {
                "CATALYST_LLM_MONTHLY_BUDGET_USD": "1.00",
                "CATALYST_LLM_MONTHLY_SOFT_CAP_PCT": "0.80",
                "CATALYST_LLM_TASK_DAILY_CAPS": "gpt55_decision_card=8",
            }
        ),
        repo=repo,
    )

    decision = controller.allow_llm_call(
        task=DEFAULT_TASKS["gpt55_decision_card"],
        ticker="MSFT",
        candidate_state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        final_score=89.9,
        estimated_usage=_usage(),
        available_at=NOW,
    )

    assert decision.allowed is False
    assert decision.reason == LLMSkipReason.MONTHLY_SOFT_CAP_REQUIRES_HIGH_SCORE


def test_allows_when_all_gates_pass() -> None:
    repo = _repo()
    _insert_entry(repo, task=LLMTaskName.MID_REVIEW, actual_cost=0.10)
    controller = _controller(repo=repo)

    decision = _allow_mid_review(controller)

    assert decision.allowed is True
    assert decision.reason is None
    assert decision.estimated_cost == 0.00875
    assert decision.daily_spend == 0.10
    assert decision.monthly_spend == 0.10
    assert decision.task_daily_count == 1


def _controller(
    *,
    config: AppConfig | None = None,
    repo: BudgetLedgerRepository | None = None,
) -> BudgetController:
    return BudgetController(
        config=config or _config(),
        ledger_repo=repo or _repo(),
        now=lambda: NOW,
    )


def _config(overrides: dict[str, str] | None = None) -> AppConfig:
    env = {
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
    env.update(overrides or {})
    return AppConfig.from_env(env)


def _repo() -> BudgetLedgerRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    return BudgetLedgerRepository(engine)


def _allow_mid_review(
    controller: BudgetController,
    *,
    candidate_state: ActionState = ActionState.WARNING,
) -> object:
    return controller.allow_llm_call(
        task=DEFAULT_TASKS["mid_review"],
        ticker="MSFT",
        candidate_state=candidate_state,
        final_score=85.0,
        estimated_usage=_usage(),
        available_at=NOW,
    )


def _usage() -> TokenUsage:
    return TokenUsage(input_tokens=1_000, cached_input_tokens=500, output_tokens=200)


def _insert_entry(
    repo: BudgetLedgerRepository,
    *,
    task: LLMTaskName,
    actual_cost: float,
    status: LLMCallStatus = LLMCallStatus.COMPLETED,
) -> None:
    repo.upsert_entry(
        BudgetLedgerEntry(
            id=budget_ledger_id(
                task=task.value,
                ticker="MSFT",
                candidate_packet_id=f"packet-{task.value}-{status.value}",
                status=status.value,
                available_at=NOW,
                prompt_version="test_v1",
            ),
            ts=NOW - timedelta(minutes=5),
            available_at=NOW,
            task=task,
            status=status,
            estimated_cost=actual_cost,
            actual_cost=actual_cost,
            ticker="MSFT",
            candidate_state_id="state-MSFT",
            candidate_packet_id=f"packet-{task.value}-{status.value}",
            decision_card_id="card-MSFT",
            model="model-review",
            provider="openai",
            token_usage=_usage(),
            candidate_state=ActionState.WARNING.value,
            prompt_version="test_v1",
            schema_version="test-v1",
            outcome_label="reviewed",
        )
    )
