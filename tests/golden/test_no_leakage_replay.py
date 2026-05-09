from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from catalyst_radar.core.models import ActionState
from catalyst_radar.validation.replay import build_replay_results, canonical_replay_json

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)
FUTURE_AVAILABLE_AT = AVAILABLE_AT + timedelta(minutes=1)


def test_no_leakage_replay_is_deterministic_and_matches_golden_digest() -> None:
    first = build_replay_results(
        _LeakyArtifactRepo(),
        as_of_start=AS_OF,
        as_of_end=AS_OF,
        decision_available_at=AVAILABLE_AT,
        states=[ActionState.WARNING],
        run_id="golden-no-leakage-replay",
    )
    second = build_replay_results(
        _LeakyArtifactRepo(),
        as_of_start=AS_OF,
        as_of_end=AS_OF,
        decision_available_at=AVAILABLE_AT,
        states=[ActionState.WARNING],
        run_id="golden-no-leakage-replay",
    )

    canonical = canonical_replay_json(first)

    assert canonical == canonical_replay_json(second)
    assert _get(first[0], "candidate_packet_id") is None
    assert _get(first[0], "decision_card_id") is None
    assert _get(first[0], "payload")["payload"]["audit"] == {
        "candidate_state_created_at": AVAILABLE_AT.isoformat(),
        "decision_available_at": AVAILABLE_AT.isoformat(),
        "decision_card_available_at": None,
        "external_calls": False,
        "packet_available_at": None,
        "score_recomputed": False,
    }
    assert _get(first[0], "leakage_flags") == (
        "candidate_packet_future_available_at",
        "decision_card_future_available_at",
    )
    assert hashlib.sha256(canonical.encode()).hexdigest() == (
        "dd2b83df52c579cefee538031a4f237df949ba9e68fb310a637c47abe2ba0fbe"
    )


class _LeakyArtifactRepo:
    def list_candidate_inputs(
        self,
        *,
        as_of: datetime,
        available_at: datetime,
        states: tuple[ActionState, ...] | None = None,
    ) -> list[dict[str, Any]]:
        assert as_of == AS_OF
        assert available_at == AVAILABLE_AT
        assert states == (ActionState.WARNING,)
        return [
            {
                "candidate_state": {
                    "id": "state-MSFT",
                    "ticker": "MSFT",
                    "as_of": AS_OF,
                    "state": ActionState.WARNING.value,
                    "previous_state": ActionState.RESEARCH_ONLY.value,
                    "final_score": 78.0,
                    "score_delta_5d": 4.0,
                    "hard_blocks": [],
                    "transition_reasons": ["score_requires_manual_review"],
                    "feature_version": "score-v4-options-theme",
                    "policy_version": "policy-v2-events",
                    "created_at": AVAILABLE_AT,
                },
                "signal_payload": {
                    "candidate": {
                        "ticker": "MSFT",
                        "as_of": AS_OF.isoformat(),
                        "final_score": 78.0,
                        "metadata": {
                            "source_ts": SOURCE_TS.isoformat(),
                            "available_at": AVAILABLE_AT.isoformat(),
                        },
                    },
                    "policy": {
                        "state": ActionState.WARNING.value,
                        "hard_blocks": [],
                        "reasons": ["score_requires_manual_review"],
                    },
                },
            }
        ]

    def latest_candidate_packet(
        self,
        ticker: str,
        as_of: datetime,
        available_at: datetime,
    ) -> dict[str, Any]:
        assert (ticker, as_of, available_at) == ("MSFT", AS_OF, AVAILABLE_AT)
        return {
            "id": "packet-MSFT-future",
            "ticker": ticker,
            "as_of": as_of,
            "state": ActionState.WARNING.value,
            "final_score": 90.0,
            "source_ts": SOURCE_TS,
            "available_at": FUTURE_AVAILABLE_AT,
            "payload": {
                "identity": {"ticker": ticker, "as_of": as_of.isoformat()},
                "audit": {"available_at": FUTURE_AVAILABLE_AT.isoformat()},
                "future_only_score": 90.0,
            },
        }

    def latest_decision_card(
        self,
        ticker: str,
        as_of: datetime,
        available_at: datetime,
    ) -> dict[str, Any]:
        assert (ticker, as_of, available_at) == ("MSFT", AS_OF, AVAILABLE_AT)
        return {
            "id": "card-MSFT-future",
            "ticker": ticker,
            "as_of": as_of,
            "candidate_packet_id": "packet-MSFT-future",
            "action_state": ActionState.WARNING.value,
            "final_score": 90.0,
            "source_ts": SOURCE_TS,
            "available_at": FUTURE_AVAILABLE_AT,
            "payload": {
                "identity": {"ticker": ticker, "as_of": as_of.isoformat()},
                "audit": {"available_at": FUTURE_AVAILABLE_AT.isoformat()},
                "future_only_card": True,
            },
        }


def _get(item: Any, key: str) -> Any:
    if isinstance(item, Mapping):
        return item[key]
    return getattr(item, key)
