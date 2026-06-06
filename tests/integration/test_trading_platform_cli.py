from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, insert, select

from catalyst_radar.cli import main
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.schema import broker_order_tickets, decision_cards, paper_trades

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT_TEXT = "2026-05-10T21:05:00+00:00"
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)
NEXT_REVIEW_AT = datetime(2026, 5, 12, 21, tzinfo=UTC)


def test_trading_platform_plan_cli_is_zero_call_zero_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'trading-platform.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_PORTFOLIO_VALUE", "25000")
    assert main(["init-db"]) == 0
    capsys.readouterr()
    _insert_manual_review_decision_card(database_url)

    assert (
        main(
            [
                "trading-platform-plan",
                "--decision-card-id",
                "card-MSFT",
                "--available-at",
                AVAILABLE_AT_TEXT,
                "--entry-price",
                "100",
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "agentic-trading-platform-plan-v1"
    assert payload["status"] == "ready_for_paper_trade"
    assert payload["risk_approval"]["approved_for_paper_trade"] is True
    assert payload["risk_approval"]["approved_for_live_submission"] is False
    assert payload["execution_controls"]["external_calls_made"] == 0
    assert payload["execution_controls"]["db_writes_made"] == 0
    assert payload["execution_controls"]["broker_order_submitted"] is False
    assert payload["execution_controls"]["order_submission_allowed"] is False
    assert payload["execution_controls"]["no_execution"] is True
    assert payload["order_intent"]["route"] == "paper_trade_only"
    assert payload["order_intent"]["submission_allowed"] is False
    assert "broker_submission_disabled" in payload["risk_approval"]["live_submission_blocks"]
    assert "--execute" in payload["supervision"]["paper_decision_execute_command"]

    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        assert list(conn.execute(select(paper_trades))) == []
        assert list(conn.execute(select(broker_order_tickets))) == []


def test_trading_platform_plan_cli_reports_missing_card(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'trading-platform-missing.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()

    assert (
        main(
            [
                "trading-platform-plan",
                "--decision-card-id",
                "missing-card",
                "--available-at",
                AVAILABLE_AT_TEXT,
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert "decision card not found: missing-card" in captured.err


def _insert_manual_review_decision_card(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    with engine.begin() as conn:
        conn.execute(
            insert(decision_cards).values(
                id="card-MSFT",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_packet_id="packet-MSFT",
                action_state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                setup_type="breakout_continuation",
                final_score=84.0,
                schema_version="decision-card-v1",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                next_review_at=NEXT_REVIEW_AT,
                user_decision=None,
                payload={
                    "identity": {
                        "action_state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                        "setup_type": "breakout_continuation",
                    },
                    "scores": {
                        "final_score": 84.0,
                        "reward_risk": 2.4,
                    },
                    "trade_plan": {
                        "entry_zone": [99.0, 102.0],
                        "invalidation_price": 94.0,
                        "reward_risk": 2.4,
                        "max_loss_if_wrong": 200.0,
                        "target_price": 116.0,
                        "missing_fields": [],
                    },
                    "position_sizing": {
                        "shares": 20.0,
                        "notional": 2000.0,
                        "risk_per_trade_pct": 0.005,
                    },
                    "portfolio_impact": {
                        "max_loss": 200.0,
                        "hard_blocks": [],
                    },
                    "evidence": [
                        {
                            "title": "Cloud guidance raised",
                            "summary": "Company raised cloud revenue guidance.",
                        },
                    ],
                    "disconfirming_evidence": [],
                    "controls": {
                        "hard_blocks": [],
                        "next_review_at": NEXT_REVIEW_AT.isoformat(),
                    },
                    "audit": {
                        "source": "test",
                        "candidate_packet_id": "packet-MSFT",
                    },
                },
                created_at=AVAILABLE_AT,
            )
        )
