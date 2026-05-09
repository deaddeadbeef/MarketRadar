from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, and_, delete, insert, select

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import ActionState
from catalyst_radar.decision_cards.models import DecisionCard
from catalyst_radar.pipeline.candidate_packet import CandidatePacket, EvidenceItem
from catalyst_radar.storage.schema import (
    candidate_packets,
    candidate_states,
    decision_cards,
    signal_features,
)


class CandidatePacketRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_candidate_packet(self, packet: CandidatePacket) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                delete(candidate_packets).where(candidate_packets.c.id == packet.id)
            )
            conn.execute(insert(candidate_packets).values(**_candidate_packet_row(packet)))

    def upsert_decision_card(self, card: DecisionCard) -> None:
        with self.engine.begin() as conn:
            conn.execute(delete(decision_cards).where(decision_cards.c.id == card.id))
            conn.execute(insert(decision_cards).values(**_decision_card_row(card)))

    def latest_candidate_packet(
        self,
        ticker: str,
        as_of: datetime,
        available_at: datetime,
    ) -> CandidatePacket | None:
        stmt = (
            select(candidate_packets)
            .where(
                candidate_packets.c.ticker == ticker.upper(),
                candidate_packets.c.as_of <= _to_utc_datetime(as_of, "as_of"),
                candidate_packets.c.available_at
                <= _to_utc_datetime(available_at, "available_at"),
            )
            .order_by(
                candidate_packets.c.as_of.desc(),
                candidate_packets.c.available_at.desc(),
                candidate_packets.c.created_at.desc(),
                candidate_packets.c.id.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _candidate_packet_from_row(row._mapping) if row is not None else None

    def latest_decision_card(
        self,
        ticker: str,
        as_of: datetime,
        available_at: datetime,
    ) -> DecisionCard | None:
        stmt = (
            select(decision_cards)
            .where(
                decision_cards.c.ticker == ticker.upper(),
                decision_cards.c.as_of <= _to_utc_datetime(as_of, "as_of"),
                decision_cards.c.available_at
                <= _to_utc_datetime(available_at, "available_at"),
            )
            .order_by(
                decision_cards.c.as_of.desc(),
                decision_cards.c.available_at.desc(),
                decision_cards.c.created_at.desc(),
                decision_cards.c.id.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _decision_card_from_row(row._mapping) if row is not None else None

    def list_latest_cards(
        self,
        as_of: datetime,
        available_at: datetime,
        *,
        limit: int = 200,
    ) -> list[DecisionCard]:
        stmt = (
            select(decision_cards)
            .where(
                decision_cards.c.as_of <= _to_utc_datetime(as_of, "as_of"),
                decision_cards.c.available_at
                <= _to_utc_datetime(available_at, "available_at"),
            )
            .order_by(
                decision_cards.c.ticker,
                decision_cards.c.as_of.desc(),
                decision_cards.c.action_state,
                decision_cards.c.available_at.desc(),
                decision_cards.c.created_at.desc(),
                decision_cards.c.id.desc(),
            )
        )
        latest: dict[tuple[str, datetime, str], DecisionCard] = {}
        with self.engine.connect() as conn:
            for row in conn.execute(stmt):
                card = _decision_card_from_row(row._mapping)
                key = (card.ticker, card.as_of, card.action_state.value)
                latest.setdefault(key, card)
        return sorted(
            latest.values(),
            key=lambda card: (-card.final_score, card.as_of, card.ticker),
        )[:limit]

    def list_candidate_inputs(
        self,
        *,
        as_of: datetime,
        available_at: datetime,
        tickers: Iterable[str] | None = None,
        states: Iterable[ActionState | str] | None = None,
    ) -> list[dict[str, Any]]:
        as_of_utc = _to_utc_datetime(as_of, "as_of")
        available_at_utc = _to_utc_datetime(available_at, "available_at")
        filters = [
            candidate_states.c.as_of == as_of_utc,
            candidate_states.c.created_at <= available_at_utc,
        ]
        normalized_tickers = (
            sorted({ticker.upper() for ticker in tickers if ticker.strip()})
            if tickers is not None
            else []
        )
        if normalized_tickers:
            filters.append(candidate_states.c.ticker.in_(normalized_tickers))
        normalized_states = [_state_value(state) for state in states or ()]
        if normalized_states:
            filters.append(candidate_states.c.state.in_(normalized_states))

        stmt = (
            select(
                candidate_states,
                signal_features.c.payload.label("signal_payload"),
            )
            .join(
                signal_features,
                and_(
                    signal_features.c.ticker == candidate_states.c.ticker,
                    signal_features.c.as_of == candidate_states.c.as_of,
                    signal_features.c.feature_version == candidate_states.c.feature_version,
                ),
            )
            .where(*filters)
            .order_by(
                candidate_states.c.as_of.desc(),
                candidate_states.c.final_score.desc(),
                candidate_states.c.ticker,
            )
        )
        with self.engine.connect() as conn:
            inputs = []
            for row in conn.execute(stmt):
                item = _candidate_input_from_row(
                    row._mapping,
                    available_at=available_at_utc,
                )
                if item is not None:
                    inputs.append(item)
            return inputs


def _candidate_packet_row(packet: CandidatePacket) -> dict[str, Any]:
    return {
        "id": packet.id,
        "ticker": packet.ticker,
        "as_of": packet.as_of,
        "candidate_state_id": packet.candidate_state_id,
        "state": _state_value(packet.state),
        "final_score": packet.final_score,
        "schema_version": packet.schema_version,
        "source_ts": packet.source_ts,
        "available_at": packet.available_at,
        "payload": thaw_json_value(packet.payload),
        "created_at": datetime.now(UTC),
    }


def _decision_card_row(card: DecisionCard) -> dict[str, Any]:
    return {
        "id": card.id,
        "ticker": card.ticker,
        "as_of": card.as_of,
        "candidate_packet_id": card.candidate_packet_id,
        "action_state": _state_value(card.action_state),
        "setup_type": card.setup_type,
        "final_score": card.final_score,
        "schema_version": card.schema_version,
        "source_ts": card.source_ts,
        "available_at": card.available_at,
        "next_review_at": card.next_review_at,
        "user_decision": card.user_decision,
        "payload": thaw_json_value(card.payload),
        "created_at": datetime.now(UTC),
    }


def _candidate_packet_from_row(row: Any) -> CandidatePacket:
    payload = row["payload"]
    return CandidatePacket(
        id=row["id"],
        ticker=row["ticker"],
        as_of=_as_datetime(row["as_of"]),
        candidate_state_id=row["candidate_state_id"],
        state=_action_state(row["state"]),
        final_score=row["final_score"],
        supporting_evidence=tuple(
            EvidenceItem.from_mapping(item)
            for item in payload.get("supporting_evidence", ())
        ),
        disconfirming_evidence=tuple(
            EvidenceItem.from_mapping(item)
            for item in payload.get("disconfirming_evidence", ())
        ),
        conflicts=tuple(payload.get("conflicts", ())),
        hard_blocks=tuple(str(block) for block in payload.get("hard_blocks", ())),
        payload=payload,
        schema_version=row["schema_version"],
        source_ts=_as_datetime(row["source_ts"]),
        available_at=_as_datetime(row["available_at"]),
    )


def _decision_card_from_row(row: Any) -> DecisionCard:
    return DecisionCard(
        id=row["id"],
        ticker=row["ticker"],
        as_of=_as_datetime(row["as_of"]),
        candidate_packet_id=row["candidate_packet_id"],
        action_state=_action_state(row["action_state"]),
        setup_type=row["setup_type"],
        final_score=row["final_score"],
        next_review_at=_as_datetime(row["next_review_at"]),
        payload=row["payload"],
        schema_version=row["schema_version"],
        source_ts=_as_datetime(row["source_ts"]),
        available_at=_as_datetime(row["available_at"]),
        user_decision=row["user_decision"],
    )


def _candidate_input_from_row(row: Any, *, available_at: datetime) -> dict[str, Any] | None:
    values = dict(row)
    signal_payload = values.pop("signal_payload", None)
    if not isinstance(signal_payload, dict) or not signal_payload:
        return None
    signal_available_at = _signal_payload_available_at(signal_payload)
    if signal_available_at is None or signal_available_at > available_at:
        return None
    return {
        "candidate_state": {
            "id": values["id"],
            "ticker": values["ticker"],
            "as_of": _as_datetime(values["as_of"]),
            "state": values["state"],
            "previous_state": values["previous_state"],
            "final_score": values["final_score"],
            "score_delta_5d": values["score_delta_5d"],
            "hard_blocks": values["hard_blocks"],
            "transition_reasons": values["transition_reasons"],
            "feature_version": values["feature_version"],
            "policy_version": values["policy_version"],
            "created_at": _as_datetime(values["created_at"]),
        },
        "signal_payload": signal_payload,
        "available_at": _to_utc_datetime(available_at, "available_at"),
    }


def _state_value(value: ActionState | str) -> str:
    return value.value if isinstance(value, ActionState) else str(value)


def _action_state(value: str) -> ActionState:
    return ActionState(value)


def _signal_payload_available_at(payload: dict[str, Any]) -> datetime | None:
    candidate = payload.get("candidate", {})
    if not isinstance(candidate, dict):
        return None
    metadata = candidate.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
    value = (
        metadata.get("available_at")
        or candidate.get("available_at")
        or payload.get("available_at")
    )
    if value is None:
        return None
    try:
        return _as_datetime(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except (AttributeError, TypeError, ValueError):
        if isinstance(value, datetime):
            return _as_datetime(value)
        return None


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _as_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["CandidatePacketRepository"]
