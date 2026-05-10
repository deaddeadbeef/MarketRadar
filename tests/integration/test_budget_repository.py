from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine

from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMSkipReason,
    LLMTaskName,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.db import create_schema

VISIBLE_AT = datetime(2026, 5, 10, 14, tzinfo=UTC)
FUTURE_AT = VISIBLE_AT + timedelta(hours=2)


def test_budget_repository_upserts_and_lists_visible_entries(seeded_repo) -> None:
    repo, visible_entry, _ = seeded_repo

    assert repo.list_entries(available_at=VISIBLE_AT) == [visible_entry]
    assert repo.list_entries(available_at=VISIBLE_AT, ticker="msft") == [visible_entry]
    assert repo.list_entries(available_at=VISIBLE_AT, ticker="aapl") == []
    assert repo.list_entries(available_at=VISIBLE_AT, task="mid_review") == [
        visible_entry
    ]
    assert repo.list_entries(available_at=VISIBLE_AT, status="completed") == [
        visible_entry
    ]

    replacement = BudgetLedgerEntry(
        **{
            **_entry_kwargs(
                ticker="msft",
                task=LLMTaskName.MID_REVIEW,
                status=LLMCallStatus.COMPLETED,
                available_at=VISIBLE_AT,
                estimated_cost=0.13,
                actual_cost=0.11,
                model="model-review",
            ),
            "payload": {"phase": "replacement"},
        }
    )
    repo.upsert_entry(replacement)

    assert repo.list_entries(available_at=VISIBLE_AT) == [replacement]


def test_budget_repository_filters_future_entries(seeded_repo) -> None:
    repo, visible_entry, future_entry = seeded_repo

    assert repo.list_entries(available_at=VISIBLE_AT) == [visible_entry]
    assert repo.list_entries(available_at=FUTURE_AT, ticker="aapl") == [future_entry]
    assert repo.list_entries(available_at=VISIBLE_AT - timedelta(minutes=1)) == []


def test_budget_repository_default_list_hides_future_entries(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'budget-default-list.db'}", future=True)
    create_schema(engine)
    repo = BudgetLedgerRepository(engine)
    visible_entry = BudgetLedgerEntry(
        **_entry_kwargs(
            ticker="msft",
            task=LLMTaskName.MID_REVIEW,
            status=LLMCallStatus.COMPLETED,
            available_at=datetime.now(UTC) - timedelta(hours=1),
            estimated_cost=0.22,
            actual_cost=0.19,
            model="model-review",
        )
    )
    future_entry = BudgetLedgerEntry(
        **_entry_kwargs(
            ticker="aapl",
            task=LLMTaskName.MID_REVIEW,
            status=LLMCallStatus.SKIPPED,
            available_at=datetime.now(UTC) + timedelta(days=1),
            estimated_cost=0.0,
            actual_cost=0.0,
            model="model-review",
            skip_reason=LLMSkipReason.PREMIUM_LLM_DISABLED,
        )
    )
    repo.upsert_entry(visible_entry)
    repo.upsert_entry(future_entry)

    assert repo.list_entries() == [visible_entry]


def test_budget_repository_summarizes_by_task_model_and_status(seeded_repo) -> None:
    repo, _, _ = seeded_repo

    summary = repo.summary(available_at=FUTURE_AT)

    assert summary["currency"] == "USD"
    assert summary["total_actual_cost_usd"] == 0.19
    assert summary["total_estimated_cost_usd"] == 0.22
    assert summary["attempt_count"] == 2
    assert summary["status_counts"] == {"completed": 1, "skipped": 1}
    assert summary["by_task"][0]["task"] == "mid_review"
    assert summary["by_task"][0]["actual_cost_usd"] == 0.19
    assert summary["by_model"] == [
        {
            "model": "model-review",
            "estimated_cost_usd": 0.22,
            "actual_cost_usd": 0.19,
            "attempt_count": 2,
        }
    ]
    assert [row["id"] for row in summary["rows"]] == [
        repo.list_entries(available_at=FUTURE_AT)[0].id,
        repo.list_entries(available_at=FUTURE_AT)[1].id,
    ]

    assert repo.spend_between(start=VISIBLE_AT - timedelta(days=1), end=FUTURE_AT) == 0.19
    assert (
        repo.task_count_between(
            task="mid_review",
            start=VISIBLE_AT - timedelta(days=1),
            end=FUTURE_AT,
        )
        == 1
    )


def test_budget_repository_summary_aggregates_all_visible_rows_with_limited_display(
    tmp_path,
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'budget-summary-limit.db'}", future=True)
    create_schema(engine)
    repo = BudgetLedgerRepository(engine)
    for index in range(205):
        available_at = VISIBLE_AT + timedelta(seconds=index)
        repo.upsert_entry(
            BudgetLedgerEntry(
                **_entry_kwargs(
                    ticker=f"t{index:03d}",
                    task=LLMTaskName.MID_REVIEW,
                    status=LLMCallStatus.COMPLETED,
                    available_at=available_at,
                    estimated_cost=0.02,
                    actual_cost=0.01,
                    model="model-review",
                )
            )
        )

    summary = repo.summary(available_at=VISIBLE_AT + timedelta(minutes=10))

    assert summary["attempt_count"] == 205
    assert summary["total_actual_cost_usd"] == 2.05
    assert summary["total_estimated_cost_usd"] == 4.1
    assert summary["status_counts"] == {"completed": 205}
    assert summary["by_task"][0]["attempt_count"] == 205
    assert summary["by_model"][0]["attempt_count"] == 205
    assert len(summary["rows"]) == 200


def test_budget_repository_default_summary_hides_future_entries(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'budget-default-summary.db'}", future=True)
    create_schema(engine)
    repo = BudgetLedgerRepository(engine)
    visible_entry = BudgetLedgerEntry(
        **_entry_kwargs(
            ticker="msft",
            task=LLMTaskName.MID_REVIEW,
            status=LLMCallStatus.COMPLETED,
            available_at=datetime.now(UTC) - timedelta(hours=1),
            estimated_cost=0.22,
            actual_cost=0.19,
            model="model-review",
        )
    )
    future_entry = BudgetLedgerEntry(
        **_entry_kwargs(
            ticker="aapl",
            task=LLMTaskName.MID_REVIEW,
            status=LLMCallStatus.SKIPPED,
            available_at=datetime.now(UTC) + timedelta(days=1),
            estimated_cost=0.0,
            actual_cost=0.0,
            model="model-review",
            skip_reason=LLMSkipReason.PREMIUM_LLM_DISABLED,
        )
    )
    repo.upsert_entry(visible_entry)
    repo.upsert_entry(future_entry)

    summary = repo.summary()

    assert summary["attempt_count"] == 1
    assert summary["total_actual_cost_usd"] == 0.19
    assert [row["id"] for row in summary["rows"]] == [visible_entry.id]


@pytest.fixture
def seeded_repo(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'budget.db'}", future=True)
    create_schema(engine)
    repo = BudgetLedgerRepository(engine)
    visible_entry = BudgetLedgerEntry(
        **_entry_kwargs(
            ticker="msft",
            task=LLMTaskName.MID_REVIEW,
            status=LLMCallStatus.COMPLETED,
            available_at=VISIBLE_AT,
            estimated_cost=0.22,
            actual_cost=0.19,
            model="model-review",
        )
    )
    future_entry = BudgetLedgerEntry(
        **_entry_kwargs(
            ticker="aapl",
            task=LLMTaskName.MID_REVIEW,
            status=LLMCallStatus.SKIPPED,
            available_at=FUTURE_AT,
            estimated_cost=0.0,
            actual_cost=0.0,
            model="model-review",
            skip_reason=LLMSkipReason.PREMIUM_LLM_DISABLED,
        )
    )
    repo.upsert_entry(visible_entry)
    repo.upsert_entry(future_entry)
    return repo, visible_entry, future_entry


def _entry_kwargs(
    *,
    ticker: str,
    task: LLMTaskName,
    status: LLMCallStatus,
    available_at: datetime,
    estimated_cost: float,
    actual_cost: float,
    model: str,
    skip_reason: LLMSkipReason | None = None,
) -> dict[str, object]:
    return {
        "id": budget_ledger_id(
            task=task.value,
            ticker=ticker,
            candidate_packet_id=f"candidate-packet-{ticker.upper()}",
            status=status.value,
            available_at=available_at,
            prompt_version="evidence_review_v1",
        ),
        "ts": available_at - timedelta(minutes=5),
        "available_at": available_at,
        "ticker": ticker,
        "candidate_state_id": f"candidate-state-{ticker.upper()}",
        "candidate_packet_id": f"candidate-packet-{ticker.upper()}",
        "decision_card_id": f"decision-card-{ticker.upper()}",
        "task": task,
        "model": model,
        "provider": "openai",
        "status": status,
        "skip_reason": skip_reason,
        "token_usage": TokenUsage(
            input_tokens=1_000,
            cached_input_tokens=100,
            output_tokens=250,
        ),
        "tool_calls": [{"name": "evidence_review", "arguments": {"ticker": ticker}}],
        "estimated_cost": estimated_cost,
        "actual_cost": actual_cost,
        "currency": "USD",
        "candidate_state": "Warning",
        "prompt_version": "evidence_review_v1",
        "schema_version": "evidence-review-v1",
        "outcome_label": "reviewed",
        "payload": {"ticker": ticker, "status": status.value},
        "created_at": available_at,
    }
