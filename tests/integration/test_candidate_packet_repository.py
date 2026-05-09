from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine, func, insert, select

from catalyst_radar.core.models import ActionState
from catalyst_radar.decision_cards.builder import build_decision_card
from catalyst_radar.pipeline.candidate_packet import (
    CandidatePacket,
    EvidenceItem,
    build_candidate_packet,
)
from catalyst_radar.storage.candidate_packet_repositories import (
    CandidatePacketRepository,
)
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import (
    candidate_packets,
    candidate_states,
    decision_cards,
    signal_features,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)


def test_upsert_candidate_packet_replaces_by_deterministic_id() -> None:
    repo = _repo()
    packet = _packet(final_score=78.0)
    replacement = _packet(final_score=82.0)

    repo.upsert_candidate_packet(packet)
    repo.upsert_candidate_packet(replacement)

    with repo.engine.connect() as conn:
        count = conn.execute(select(func.count()).select_from(candidate_packets)).scalar_one()
        row = conn.execute(select(candidate_packets)).one()

    assert count == 1
    assert row.final_score == 82.0
    assert row.payload["scores"]["final"] == 82.0


def test_latest_candidate_packet_respects_as_of_and_available_at() -> None:
    repo = _repo()
    visible = _packet()
    future = _packet(
        as_of=AS_OF + timedelta(days=1),
        source_ts=SOURCE_TS + timedelta(days=1),
        available_at=AVAILABLE_AT + timedelta(days=1),
    )
    repo.upsert_candidate_packet(visible)
    repo.upsert_candidate_packet(future)

    packet = repo.latest_candidate_packet("msft", AS_OF, AVAILABLE_AT)

    assert packet is not None
    assert packet.id == visible.id
    assert packet.supporting_evidence[0].computed_feature_id
    assert packet.disconfirming_evidence[0].computed_feature_id


def test_later_rebuild_does_not_overwrite_earlier_point_in_time_packet() -> None:
    repo = _repo()
    visible = _packet()
    later_available_at = AVAILABLE_AT + timedelta(hours=2)
    later = _packet(
        final_score=82.0,
        source_ts=SOURCE_TS + timedelta(hours=1),
        available_at=later_available_at,
    )

    repo.upsert_candidate_packet(visible)
    repo.upsert_candidate_packet(later)

    with repo.engine.connect() as conn:
        count = conn.execute(select(func.count()).select_from(candidate_packets)).scalar_one()
    early = repo.latest_candidate_packet("MSFT", AS_OF, AVAILABLE_AT)
    late = repo.latest_candidate_packet("MSFT", AS_OF, later_available_at)

    assert count == 2
    assert early is not None
    assert early.id == visible.id
    assert late is not None
    assert late.id == later.id


def test_future_available_packet_is_excluded() -> None:
    repo = _repo()
    repo.upsert_candidate_packet(
        _packet(
            source_ts=SOURCE_TS + timedelta(hours=1),
            available_at=AVAILABLE_AT + timedelta(days=1),
        )
    )

    packet = repo.latest_candidate_packet("MSFT", AS_OF, AVAILABLE_AT)

    assert packet is None


def test_decision_card_round_trips_and_latest_lookup_returns_full_payload() -> None:
    repo = _repo()
    packet = _packet(state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW, final_score=88.0)
    card = build_decision_card(packet)
    repo.upsert_candidate_packet(packet)
    repo.upsert_decision_card(card)

    latest = repo.latest_decision_card("MSFT", AS_OF, AVAILABLE_AT)
    cards = repo.list_latest_cards(AS_OF, AVAILABLE_AT)

    assert latest is not None
    assert latest.id == card.id
    assert latest.payload["identity"]["ticker"] == "MSFT"
    assert latest.payload["evidence"]
    assert cards[0].id == card.id
    with repo.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(decision_cards)).scalar_one() == 1


def test_later_decision_card_rebuild_does_not_overwrite_earlier_card() -> None:
    repo = _repo()
    visible_packet = _packet(state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW)
    later_available_at = AVAILABLE_AT + timedelta(hours=2)
    later_packet = _packet(
        state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        final_score=90.0,
        source_ts=SOURCE_TS + timedelta(hours=1),
        available_at=later_available_at,
    )
    visible_card = build_decision_card(visible_packet, available_at=AVAILABLE_AT)
    later_card = build_decision_card(later_packet, available_at=later_available_at)

    repo.upsert_decision_card(visible_card)
    repo.upsert_decision_card(later_card)

    early = repo.latest_decision_card("MSFT", AS_OF, AVAILABLE_AT)
    late = repo.latest_decision_card("MSFT", AS_OF, later_available_at)
    latest_cards = repo.list_latest_cards(AS_OF, later_available_at)

    assert early is not None
    assert early.id == visible_card.id
    assert late is not None
    assert late.id == later_card.id
    assert [card.id for card in latest_cards] == [later_card.id]


def test_list_candidate_inputs_returns_persisted_state_and_signal_payload() -> None:
    repo = _repo()
    with repo.engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                id="state-msft",
                ticker="MSFT",
                as_of=AS_OF,
                state=ActionState.WARNING.value,
                previous_state=None,
                final_score=78.0,
                score_delta_5d=4.0,
                hard_blocks=[],
                transition_reasons=["score_requires_manual_review"],
                feature_version="score-v4-options-theme",
                policy_version="policy-v2-events",
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(signal_features).values(
                ticker="MSFT",
                as_of=AS_OF,
                feature_version="score-v4-options-theme",
                price_strength=80.0,
                volume_score=70.0,
                liquidity_score=90.0,
                risk_penalty=4.0,
                portfolio_penalty=1.0,
                final_score=78.0,
                payload={
                    "candidate": {
                        "ticker": "MSFT",
                        "as_of": AS_OF.isoformat(),
                        "final_score": 78.0,
                        "features": {"feature_version": "score-v4-options-theme"},
                        "metadata": {
                            "source_ts": SOURCE_TS.isoformat(),
                            "available_at": AVAILABLE_AT.isoformat(),
                        },
                    },
                    "policy": {"state": "Warning"},
                },
            )
        )

    inputs = repo.list_candidate_inputs(
        as_of=AS_OF,
        available_at=AVAILABLE_AT,
        states=[ActionState.WARNING],
    )

    assert len(inputs) == 1
    assert inputs[0]["candidate_state"]["id"] == "state-msft"
    assert inputs[0]["signal_payload"]["candidate"]["ticker"] == "MSFT"


def test_list_candidate_inputs_excludes_missing_or_future_signal_payloads() -> None:
    repo = _repo()
    _insert_state(
        repo,
        ticker="MSFT",
        created_at=AVAILABLE_AT,
        signal_available_at=AVAILABLE_AT + timedelta(days=1),
    )
    _insert_state(
        repo,
        ticker="AAPL",
        created_at=AVAILABLE_AT + timedelta(days=1),
        signal_available_at=AVAILABLE_AT,
    )

    inputs = repo.list_candidate_inputs(
        as_of=AS_OF,
        available_at=AVAILABLE_AT,
        states=[ActionState.WARNING],
    )

    assert inputs == []


def test_invalid_persisted_packet_state_raises_instead_of_coercing_to_no_action() -> None:
    repo = _repo()
    packet = _packet()
    repo.upsert_candidate_packet(packet)
    with repo.engine.begin() as conn:
        conn.execute(
            candidate_packets.update()
            .where(candidate_packets.c.id == packet.id)
            .values(state="InvalidState")
        )

    try:
        repo.latest_candidate_packet("MSFT", AS_OF, AVAILABLE_AT)
    except ValueError as exc:
        assert "InvalidState" in str(exc)
    else:
        raise AssertionError("invalid state should raise")


def _repo() -> CandidatePacketRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    return CandidatePacketRepository(engine)


def _insert_state(
    repo: CandidatePacketRepository,
    *,
    ticker: str,
    created_at: datetime,
    signal_available_at: datetime,
) -> None:
    with repo.engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                id=f"state-{ticker}",
                ticker=ticker,
                as_of=AS_OF,
                state=ActionState.WARNING.value,
                previous_state=None,
                final_score=78.0,
                score_delta_5d=4.0,
                hard_blocks=[],
                transition_reasons=["score_requires_manual_review"],
                feature_version="score-v4-options-theme",
                policy_version="policy-v2-events",
                created_at=created_at,
            )
        )
        conn.execute(
            insert(signal_features).values(
                ticker=ticker,
                as_of=AS_OF,
                feature_version="score-v4-options-theme",
                price_strength=80.0,
                volume_score=70.0,
                liquidity_score=90.0,
                risk_penalty=4.0,
                portfolio_penalty=1.0,
                final_score=78.0,
                payload={
                    "candidate": {
                        "ticker": ticker,
                        "as_of": AS_OF.isoformat(),
                        "final_score": 78.0,
                        "features": {"feature_version": "score-v4-options-theme"},
                        "metadata": {
                            "source_ts": SOURCE_TS.isoformat(),
                            "available_at": signal_available_at.isoformat(),
                        },
                    },
                    "policy": {"state": "Warning"},
                },
            )
        )


def _packet(
    *,
    state: ActionState = ActionState.WARNING,
    final_score: float = 78.0,
    as_of: datetime = AS_OF,
    source_ts: datetime = SOURCE_TS,
    available_at: datetime = AVAILABLE_AT,
) -> CandidatePacket:
    return build_candidate_packet(
        candidate_state={
            "id": "state-msft",
            "ticker": "MSFT",
            "as_of": as_of,
            "state": state.value,
            "final_score": final_score,
            "score_delta_5d": 4.0,
            "hard_blocks": [],
            "transition_reasons": ["score_requires_manual_review"],
            "feature_version": "score-v4-options-theme",
            "policy_version": "policy-v2-events",
            "created_at": available_at,
        },
        signal_features_payload={
            "candidate": {
                "ticker": "MSFT",
                "as_of": as_of.isoformat(),
                "features": {
                    "ticker": "MSFT",
                    "as_of": as_of.isoformat(),
                    "feature_version": "score-v4-options-theme",
                },
                "final_score": final_score,
                "risk_penalty": 4.0,
                "portfolio_penalty": 1.0,
                "entry_zone": [100.0, 104.0],
                "invalidation_price": 94.0,
                "reward_risk": 2.7,
                "metadata": {
                    "source_ts": source_ts.isoformat(),
                    "available_at": available_at.isoformat(),
                    "setup_type": "breakout",
                    "target_price": 125.0,
                    "pillar_scores": {
                        "price_strength": 86.0,
                        "relative_strength": 81.0,
                        "volume_liquidity": 72.0,
                    },
                    "position_size": {
                        "risk_per_trade_pct": 0.5,
                        "shares": 20.0,
                        "notional": 2080.0,
                        "cash_check": "pass",
                    },
                    "portfolio_impact": {
                        "single_name_after_pct": 4.0,
                        "sector_after_pct": 14.0,
                        "theme_after_pct": 6.0,
                        "correlated_after_pct": 8.0,
                        "proposed_notional": 2080.0,
                        "max_loss": 200.0,
                        "portfolio_penalty": 1.0,
                        "hard_blocks": [],
                    },
                },
            },
            "policy": {
                "state": state.value,
                "hard_blocks": [],
                "reasons": ["score_requires_manual_review"],
                "missing_trade_plan": [],
                "policy_version": "policy-v2-events",
            },
        },
        requested_available_at=available_at,
    )


def _manual_packet(
    *,
    as_of: datetime = AS_OF,
    source_ts: datetime = SOURCE_TS,
    available_at: datetime = AVAILABLE_AT,
) -> CandidatePacket:
    support = EvidenceItem(
        kind="computed_feature",
        title="Support",
        summary="Support.",
        polarity="supporting",
        strength=0.5,
        computed_feature_id="signal_features:MSFT:x",
        source_ts=source_ts,
        available_at=available_at,
    )
    disconfirming = EvidenceItem(
        kind="computed_feature",
        title="Risk",
        summary="Risk.",
        polarity="disconfirming",
        strength=0.4,
        computed_feature_id="signal_features:MSFT:risk",
        source_ts=source_ts,
        available_at=available_at,
    )
    payload = {
        "identity": {"ticker": "MSFT", "as_of": as_of.isoformat(), "state": "Warning"},
        "scores": {"final": 78.0},
        "trade_plan": {},
        "portfolio_impact": {},
        "supporting_evidence": [],
        "disconfirming_evidence": [],
        "conflicts": [],
        "hard_blocks": [],
        "audit": {
            "source_ts": source_ts.isoformat(),
            "available_at": available_at.isoformat(),
        },
    }
    return CandidatePacket(
        id="manual",
        ticker="MSFT",
        as_of=as_of,
        candidate_state_id="state-msft",
        state=ActionState.WARNING,
        final_score=78.0,
        supporting_evidence=(support,),
        disconfirming_evidence=(disconfirming,),
        conflicts=(),
        hard_blocks=(),
        payload=payload,
        source_ts=source_ts,
        available_at=available_at,
    )
