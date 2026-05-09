from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from catalyst_radar.core.models import ActionState
from catalyst_radar.validation.replay import (
    ReplayRow,
    build_replay_results,
    build_replay_row,
    canonical_replay_json,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)


def test_build_replay_row_uses_visible_candidate_packet_and_decision_card() -> None:
    row = build_replay_row(
        _candidate_input(ticker="msft"),
        candidate_packet=_packet(),
        decision_card=_decision_card(),
        decision_available_at=AVAILABLE_AT,
    )

    assert isinstance(row, ReplayRow)
    assert row.ticker == "MSFT"
    assert row.state == ActionState.WARNING
    assert row.final_score == 78.0
    assert row.candidate_state_id == "state-MSFT"
    assert row.candidate_packet_id == "packet-MSFT"
    assert row.decision_card_id == "card-MSFT"
    assert row.hard_blocks == ("trade_plan_missing",)
    assert row.transition_reasons == ("score_requires_manual_review",)
    assert row.score_delta_5d == 4.0
    assert row.leakage_flags == ()
    assert row.payload["audit"]["score_recomputed"] is False
    assert row.payload["audit"]["external_calls"] is False


def test_build_replay_row_rejects_future_candidate_state() -> None:
    with pytest.raises(ValueError, match="candidate_state.created_at"):
        build_replay_row(
            _candidate_input(created_at=AVAILABLE_AT + timedelta(seconds=1)),
            decision_available_at=AVAILABLE_AT,
        )


def test_build_replay_row_excludes_future_packet_and_card_with_flags() -> None:
    row = build_replay_row(
        _candidate_input(),
        candidate_packet=_packet(available_at=AVAILABLE_AT + timedelta(minutes=1)),
        decision_card=_decision_card(available_at=AVAILABLE_AT + timedelta(minutes=1)),
        decision_available_at=AVAILABLE_AT,
    )

    assert row.candidate_packet_id is None
    assert row.decision_card_id is None
    assert row.leakage_flags == (
        "candidate_packet_future_available_at",
        "decision_card_future_available_at",
    )
    assert row.payload["packet"] is None
    assert row.payload["decision_card"] is None


def test_build_replay_row_flags_missing_packet_and_card_availability() -> None:
    packet = _packet()
    card = _decision_card()
    packet.pop("available_at")
    card.pop("available_at")

    row = build_replay_row(
        _candidate_input(),
        candidate_packet=packet,
        decision_card=card,
        decision_available_at=AVAILABLE_AT,
    )

    assert row.candidate_packet_id is None
    assert row.decision_card_id is None
    assert row.leakage_flags == (
        "candidate_packet_missing_available_at",
        "decision_card_missing_available_at",
    )


def test_build_replay_results_uses_repository_lookups_and_returns_stable_dicts() -> None:
    repo = _ReplayRepo()

    first = build_replay_results(
        repo,
        as_of_start=AS_OF,
        as_of_end=AS_OF,
        decision_available_at=AVAILABLE_AT,
        states=[ActionState.WARNING],
        tickers=["msft"],
        run_id="validation-replay-test",
    )
    second = build_replay_results(
        repo,
        as_of_start=AS_OF,
        as_of_end=AS_OF,
        decision_available_at=AVAILABLE_AT,
        states=[ActionState.WARNING],
        tickers=["msft"],
        run_id="validation-replay-test",
    )

    assert repo.list_calls
    assert len(first) == 1
    assert _get(first[0], "run_id") == "validation-replay-test"
    assert _get(first[0], "ticker") == "MSFT"
    assert _get(first[0], "state") == ActionState.WARNING
    assert _get(first[0], "candidate_packet_id") == "packet-MSFT"
    assert _get(first[0], "decision_card_id") == "card-MSFT"
    assert dict(_get(first[0], "labels")) == {}
    assert _get(first[0], "leakage_flags") == ()
    assert canonical_replay_json(first) == canonical_replay_json(second)


@dataclass(frozen=True)
class _ObjectPacket:
    id: str
    ticker: str
    as_of: datetime
    state: ActionState
    final_score: float
    available_at: datetime
    payload: dict[str, Any]


class _ReplayRepo:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, Any]] = []

    def list_candidate_inputs(
        self,
        *,
        as_of: datetime,
        available_at: datetime,
        states: tuple[ActionState, ...] | None = None,
        tickers: tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        self.list_calls.append(
            {
                "as_of": as_of,
                "available_at": available_at,
                "states": states,
                "tickers": tickers,
            }
        )
        return [_candidate_input()]

    def latest_candidate_packet(
        self,
        ticker: str,
        as_of: datetime,
        available_at: datetime,
    ) -> _ObjectPacket:
        assert (ticker, as_of, available_at) == ("MSFT", AS_OF, AVAILABLE_AT)
        return _ObjectPacket(
            id="packet-MSFT",
            ticker=ticker,
            as_of=as_of,
            state=ActionState.WARNING,
            final_score=78.0,
            available_at=available_at,
            payload={"audit": {"available_at": available_at.isoformat()}},
        )

    def latest_decision_card(
        self,
        ticker: str,
        as_of: datetime,
        available_at: datetime,
    ) -> dict[str, Any]:
        assert (ticker, as_of, available_at) == ("MSFT", AS_OF, AVAILABLE_AT)
        return _decision_card()


def _candidate_input(
    *,
    ticker: str = "MSFT",
    created_at: datetime = AVAILABLE_AT,
) -> dict[str, Any]:
    ticker = ticker.upper()
    return {
        "candidate_state": {
            "id": f"state-{ticker}",
            "ticker": ticker,
            "as_of": AS_OF,
            "state": ActionState.WARNING.value,
            "previous_state": ActionState.RESEARCH_ONLY.value,
            "final_score": 78.0,
            "score_delta_5d": 4.0,
            "hard_blocks": ["trade_plan_missing"],
            "transition_reasons": ["score_requires_manual_review"],
            "feature_version": "score-v4-options-theme",
            "policy_version": "policy-v2-events",
            "created_at": created_at,
        },
        "signal_payload": {
            "candidate": {
                "ticker": ticker,
                "as_of": AS_OF.isoformat(),
                "final_score": 78.0,
                "metadata": {
                    "source_ts": SOURCE_TS.isoformat(),
                    "available_at": AVAILABLE_AT.isoformat(),
                },
            },
            "policy": {
                "state": ActionState.WARNING.value,
                "hard_blocks": ["trade_plan_missing"],
                "reasons": ["score_requires_manual_review"],
            },
        },
    }


def _packet(
    *,
    available_at: datetime = AVAILABLE_AT,
) -> dict[str, Any]:
    return {
        "id": "packet-MSFT",
        "ticker": "MSFT",
        "as_of": AS_OF,
        "state": ActionState.WARNING.value,
        "final_score": 78.0,
        "source_ts": SOURCE_TS,
        "available_at": available_at,
        "payload": {
            "identity": {"ticker": "MSFT", "as_of": AS_OF.isoformat()},
            "audit": {"available_at": available_at.isoformat()},
        },
    }


def _decision_card(
    *,
    available_at: datetime = AVAILABLE_AT,
) -> dict[str, Any]:
    return {
        "id": "card-MSFT",
        "ticker": "MSFT",
        "as_of": AS_OF,
        "candidate_packet_id": "packet-MSFT",
        "action_state": ActionState.WARNING.value,
        "final_score": 78.0,
        "source_ts": SOURCE_TS,
        "available_at": available_at,
        "payload": {
            "identity": {"ticker": "MSFT", "as_of": AS_OF.isoformat()},
            "audit": {"available_at": available_at.isoformat()},
        },
    }


def _get(item: Any, key: str) -> Any:
    if isinstance(item, Mapping):
        return item[key]
    return getattr(item, key)
