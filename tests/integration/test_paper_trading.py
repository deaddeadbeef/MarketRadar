from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine

from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import (
    PaperDecision,
    PaperTrade,
    PaperTradeState,
    UsefulAlertLabel,
    ValidationResult,
    ValidationRun,
    ValidationRunStatus,
)
from catalyst_radar.validation.paper import create_paper_trade_from_card, update_trade_outcome

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)
OUTCOME_AT = datetime(2026, 7, 15, 21, 5, tzinfo=UTC)


def test_paper_trade_repository_records_manual_workflow_without_execution(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'paper.db').as_posix()}", future=True)
    create_schema(engine)
    repo = ValidationRepository(engine)

    trade = create_paper_trade_from_card(
        _decision_card(),
        PaperDecision.APPROVED,
        available_at=AVAILABLE_AT,
        entry_price=100.0,
        entry_at=AVAILABLE_AT,
    )
    repo.upsert_paper_trade(trade)

    stored = repo.latest_paper_trade_for_card("card-MSFT", OUTCOME_AT)
    assert stored is not None
    assert stored.id.endswith(":approved")
    assert stored.state == PaperTradeState.OPEN
    assert stored.payload["manual_review_only"] is True
    assert stored.payload["no_execution"] is True
    assert stored.payload["next_review_at"] == "2026-05-12T21:00:00+00:00"
    assert stored.shares == 20.0
    assert stored.notional == 2080.0
    assert stored.max_loss == 200.0

    updated = update_trade_outcome(
        stored,
        {"target_20d_25": True, "invalidated": False},
        OUTCOME_AT,
    )
    repo.upsert_paper_trade(updated)

    stored_at_decision = repo.latest_paper_trade_for_card("card-MSFT", AVAILABLE_AT)
    assert stored_at_decision is not None
    assert stored_at_decision.id == trade.id
    assert stored_at_decision.state == PaperTradeState.OPEN

    stored = repo.latest_paper_trade_for_card("card-MSFT", OUTCOME_AT)
    assert stored is not None
    assert stored.id == updated.id
    assert stored.state == PaperTradeState.CLOSED
    assert stored.outcome_labels["target_20d_25"] is True

    future_trade = PaperTrade(
        id=f"{trade.id}:future",
        decision_card_id=trade.decision_card_id,
        ticker=trade.ticker,
        as_of=trade.as_of,
        decision=trade.decision,
        state=PaperTradeState.INVALIDATED,
        entry_price=trade.entry_price,
        entry_at=trade.entry_at,
        invalidation_price=trade.invalidation_price,
        shares=trade.shares,
        notional=trade.notional,
        max_loss=trade.max_loss,
        outcome_labels={"invalidated": True},
        source_ts=trade.source_ts,
        available_at=OUTCOME_AT + (OUTCOME_AT - AVAILABLE_AT),
        payload=trade.payload,
    )
    repo.upsert_paper_trade(future_trade)

    stored = repo.latest_paper_trade_for_card("card-MSFT", OUTCOME_AT)
    assert stored is not None
    assert stored.id == updated.id


def test_validation_repository_round_trips_runs_results_and_useful_labels(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'validation.db').as_posix()}", future=True)
    create_schema(engine)
    repo = ValidationRepository(engine)

    run = ValidationRun(
        id="validation-run-1",
        run_type="point_in_time_replay",
        as_of_start=AS_OF,
        as_of_end=AS_OF,
        decision_available_at=AVAILABLE_AT,
        status=ValidationRunStatus.RUNNING,
        config={"states": [ActionState.WARNING.value]},
    )
    repo.upsert_validation_run(run)
    repo.finish_validation_run(
        run.id,
        ValidationRunStatus.SUCCESS,
        {"candidate_count": 1},
        finished_at=AVAILABLE_AT,
    )

    stored_run = repo.latest_validation_run(run.id)
    assert stored_run is not None
    assert stored_run.status == ValidationRunStatus.SUCCESS
    assert stored_run.metrics["candidate_count"] == 1

    result = ValidationResult(
        id="validation-result-1",
        run_id=run.id,
        ticker="msft",
        as_of=AS_OF,
        available_at=AVAILABLE_AT,
        state=ActionState.WARNING,
        final_score=78.0,
        candidate_state_id="state-msft",
        decision_card_id="card-MSFT",
        labels={"target_20d_25": True},
        leakage_flags=(),
        payload={"audit": {"external_calls": False}},
    )
    assert repo.upsert_validation_results([result]) == 1

    stored_results = repo.list_validation_results(run.id)
    assert len(stored_results) == 1
    assert stored_results[0].ticker == "MSFT"
    assert stored_results[0].labels["target_20d_25"] is True

    future_result = ValidationResult(
        id="validation-result-future",
        run_id=run.id,
        ticker="MSFT",
        as_of=AS_OF,
        available_at=OUTCOME_AT,
        state=ActionState.WARNING,
        final_score=88.0,
        labels={"target_20d_25": False},
        leakage_flags=("future",),
        payload={},
    )
    assert repo.upsert_validation_results([future_result]) == 1
    point_in_time_results = repo.list_validation_results(
        run.id,
        available_at=AVAILABLE_AT,
    )
    assert [row.id for row in point_in_time_results] == ["validation-result-1"]

    label = UsefulAlertLabel(
        id="label-1",
        artifact_type="decision_card",
        artifact_id="card-MSFT",
        ticker="msft",
        label="useful",
        created_at=AVAILABLE_AT,
    )
    repo.insert_useful_alert_label(label)
    repo.insert_useful_alert_label(
        UsefulAlertLabel(
            id="label-2",
            artifact_type="decision_card",
            artifact_id="card-MSFT",
            ticker="msft",
            label="ignored",
            created_at=OUTCOME_AT,
        )
    )

    stored_label = repo.latest_useful_alert_label(
        artifact_type="decision_card",
        artifact_id="card-MSFT",
    )
    assert stored_label is not None
    assert stored_label.ticker == "MSFT"
    assert stored_label.label == "ignored"
    assert [item.label for item in repo.list_useful_alert_labels(available_at=AVAILABLE_AT)] == [
        "useful"
    ]
    assert [item.label for item in repo.list_useful_alert_labels(available_at=OUTCOME_AT)] == [
        "ignored"
    ]


def _decision_card() -> dict[str, object]:
    return {
        "id": "card-MSFT",
        "ticker": "MSFT",
        "as_of": AS_OF,
        "source_ts": SOURCE_TS,
        "available_at": AVAILABLE_AT,
        "next_review_at": datetime(2026, 5, 12, 21, tzinfo=UTC),
        "payload": {
            "trade_plan": {
                "invalidation_price": 94.0,
                "max_loss_if_wrong": 200.0,
            },
            "position_sizing": {
                "shares": 20.0,
                "notional": 2080.0,
            },
            "portfolio_impact": {
                "max_loss": 210.0,
            },
        },
    }
