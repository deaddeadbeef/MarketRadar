from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from catalyst_radar.alerts.dedupe import alert_dedupe_key, decide_dedupe
from catalyst_radar.alerts.models import (
    Alert,
    AlertRoute,
    AlertStatus,
    AlertSuppression,
    alert_id,
    alert_suppression_id,
)
from catalyst_radar.alerts.routing import (
    AlertCandidate,
    AlertRouteDecision,
    alert_summary,
    alert_title,
    route_alert,
)
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.schema import candidate_packets, candidate_states, decision_cards


@dataclass(frozen=True)
class AlertPlanResult:
    alerts: tuple[Alert, ...]
    suppressions: tuple[AlertSuppression, ...]


def plan_alerts(
    alert_repo: AlertRepository,
    *,
    as_of: datetime,
    available_at: datetime,
    ticker: str | None = None,
    limit: int = 200,
) -> AlertPlanResult:
    as_of_utc = _aware_utc(as_of, "as_of")
    available_at_utc = _aware_utc(available_at, "available_at")
    candidates = _latest_candidate_states(
        alert_repo,
        as_of=as_of_utc,
        available_at=available_at_utc,
        ticker=ticker,
        limit=limit,
    )

    planned: list[Alert] = []
    suppressions: list[AlertSuppression] = []
    for state_row in candidates:
        packet_row = _latest_packet_for_state(
            alert_repo,
            candidate_state_id=state_row["id"],
            available_at=available_at_utc,
        )
        card_row = (
            _latest_decision_card_for_packet(
                alert_repo,
                packet_id=packet_row["id"],
                available_at=available_at_utc,
            )
            if packet_row is not None
            else None
        )
        candidate = _alert_candidate(
            state_row,
            packet_row=packet_row,
            card_row=card_row,
            available_at=available_at_utc,
        )
        decision = route_alert(candidate)
        dedupe_key = alert_dedupe_key(candidate, decision)
        if not decision.should_alert:
            suppression = _suppression(
                candidate,
                decision,
                dedupe_key=dedupe_key,
                reason=decision.reason,
                available_at=available_at_utc,
            )
            alert_repo.insert_suppression(suppression)
            suppressions.append(suppression)
            continue

        existing = alert_repo.latest_alert_by_dedupe_key(dedupe_key, available_at_utc)
        dedupe = decide_dedupe(existing, dedupe_key)
        if not dedupe.emit:
            suppression = _suppression(
                candidate,
                decision,
                dedupe_key=dedupe_key,
                reason=dedupe.reason or "duplicate_trigger",
                available_at=available_at_utc,
            )
            alert_repo.insert_suppression(suppression)
            suppressions.append(suppression)
            continue

        alert = _alert(candidate, decision, dedupe_key=dedupe_key)
        alert_repo.upsert_alert(alert)
        planned.append(alert)

    return AlertPlanResult(alerts=tuple(planned), suppressions=tuple(suppressions))


def _latest_candidate_states(
    alert_repo: AlertRepository,
    *,
    as_of: datetime,
    available_at: datetime,
    ticker: str | None,
    limit: int,
) -> list[Mapping[str, Any]]:
    filters = [
        candidate_states.c.as_of <= as_of,
        candidate_states.c.created_at <= available_at,
    ]
    if ticker is not None and ticker.strip():
        filters.append(candidate_states.c.ticker == ticker.upper())
    stmt = (
        select(candidate_states)
        .where(*filters)
        .order_by(
            candidate_states.c.ticker,
            candidate_states.c.as_of.desc(),
            candidate_states.c.created_at.desc(),
            candidate_states.c.id.desc(),
        )
    )
    latest: dict[str, Mapping[str, Any]] = {}
    with alert_repo.engine.connect() as conn:
        for row in conn.execute(stmt):
            values = _row_mapping(row._mapping)
            latest.setdefault(str(values["ticker"]).upper(), values)
    return sorted(latest.values(), key=_candidate_sort_key)[: _positive_limit(limit)]


def _latest_packet_for_state(
    alert_repo: AlertRepository,
    *,
    candidate_state_id: str,
    available_at: datetime,
) -> Mapping[str, Any] | None:
    stmt = (
        select(candidate_packets)
        .where(
            candidate_packets.c.candidate_state_id == candidate_state_id,
            candidate_packets.c.available_at <= available_at,
            candidate_packets.c.created_at <= available_at,
        )
        .order_by(
            candidate_packets.c.available_at.desc(),
            candidate_packets.c.created_at.desc(),
            candidate_packets.c.id.desc(),
        )
        .limit(1)
    )
    with alert_repo.engine.connect() as conn:
        row = conn.execute(stmt).first()
    return _row_mapping(row._mapping) if row is not None else None


def _latest_decision_card_for_packet(
    alert_repo: AlertRepository,
    *,
    packet_id: str,
    available_at: datetime,
) -> Mapping[str, Any] | None:
    stmt = (
        select(decision_cards)
        .where(
            decision_cards.c.candidate_packet_id == packet_id,
            decision_cards.c.available_at <= available_at,
            decision_cards.c.created_at <= available_at,
        )
        .order_by(
            decision_cards.c.available_at.desc(),
            decision_cards.c.created_at.desc(),
            decision_cards.c.id.desc(),
        )
        .limit(1)
    )
    with alert_repo.engine.connect() as conn:
        row = conn.execute(stmt).first()
    return _row_mapping(row._mapping) if row is not None else None


def _alert_candidate(
    state_row: Mapping[str, Any],
    *,
    packet_row: Mapping[str, Any] | None,
    card_row: Mapping[str, Any] | None,
    available_at: datetime,
) -> AlertCandidate:
    state_payload = _state_payload(state_row)
    packet_payload = _mapping_payload(packet_row)
    card_payload = _mapping_payload(card_row)
    return AlertCandidate(
        ticker=str(state_row["ticker"]),
        as_of=_as_datetime(state_row["as_of"]),
        source_ts=_source_ts(state_row, packet_row, card_row, state_payload),
        available_at=available_at,
        candidate_state_id=str(state_row["id"]),
        action_state=ActionState(str(state_row["state"])),
        previous_state=_optional_action_state(state_row.get("previous_state")),
        final_score=float(state_row["final_score"]),
        score_delta_5d=float(state_row["score_delta_5d"] or 0.0),
        hard_blocks=tuple(str(block) for block in state_row.get("hard_blocks") or ()),
        candidate_packet_id=str(packet_row["id"]) if packet_row is not None else None,
        decision_card_id=str(card_row["id"]) if card_row is not None else None,
        top_supporting_evidence=_top_supporting_evidence(packet_payload, card_payload),
        entry_zone=_entry_zone(state_payload, packet_payload, card_payload),
        invalidation_price=_invalidation_price(state_payload, packet_payload, card_payload),
        payload={
            "candidate_state": _state_audit_payload(state_row),
            "candidate_packet": _source_payload(packet_row),
            "decision_card": _source_payload(card_row),
            "audit": {
                "planner": "alert-planner-v1",
                "available_at": available_at.isoformat(),
                "point_in_time": True,
            },
        },
    )


def _alert(
    candidate: AlertCandidate,
    decision: AlertRouteDecision,
    *,
    dedupe_key: str,
) -> Alert:
    if decision.route is None or decision.channel is None or decision.priority is None:
        msg = "alert decision is missing route, channel, or priority"
        raise ValueError(msg)
    planned_alert_id = alert_id(
        ticker=candidate.ticker,
        route=decision.route.value,
        dedupe_key=dedupe_key,
        available_at=candidate.available_at,
    )
    return Alert(
        id=planned_alert_id,
        ticker=candidate.ticker,
        as_of=candidate.as_of,
        source_ts=candidate.source_ts,
        available_at=candidate.available_at,
        candidate_state_id=candidate.candidate_state_id,
        candidate_packet_id=candidate.candidate_packet_id,
        decision_card_id=candidate.decision_card_id,
        action_state=candidate.action_state.value,
        route=decision.route,
        channel=decision.channel,
        priority=decision.priority,
        status=AlertStatus.PLANNED,
        dedupe_key=dedupe_key,
        trigger_kind=decision.trigger_kind,
        trigger_fingerprint=decision.trigger_fingerprint,
        title=alert_title(candidate, decision),
        summary=alert_summary(candidate, decision),
        feedback_url=f"/api/alerts/{planned_alert_id}/feedback",
        payload={
            **dict(candidate.payload),
            "route_reason": decision.reason,
            "dedupe_key": dedupe_key,
            "trigger_kind": decision.trigger_kind,
            "trigger_fingerprint": decision.trigger_fingerprint,
        },
        created_at=candidate.available_at,
    )


def _suppression(
    candidate: AlertCandidate,
    decision: AlertRouteDecision,
    *,
    dedupe_key: str,
    reason: str,
    available_at: datetime,
) -> AlertSuppression:
    route = decision.route or _fallback_route(candidate)
    return AlertSuppression(
        id=alert_suppression_id(
            dedupe_key=dedupe_key,
            reason=reason,
            available_at=available_at,
        ),
        ticker=candidate.ticker,
        as_of=candidate.as_of,
        available_at=available_at,
        candidate_state_id=candidate.candidate_state_id,
        decision_card_id=candidate.decision_card_id,
        route=route,
        dedupe_key=dedupe_key,
        trigger_kind=decision.trigger_kind,
        trigger_fingerprint=decision.trigger_fingerprint,
        reason=reason,
        payload={
            **dict(candidate.payload),
            "route_reason": decision.reason,
            "dedupe_key": dedupe_key,
            "suppression_reason": reason,
        },
        created_at=available_at,
    )


def _fallback_route(candidate: AlertCandidate) -> AlertRoute:
    if candidate.action_state == ActionState.WARNING:
        return AlertRoute.WARNING_DIGEST
    if candidate.action_state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW:
        return AlertRoute.IMMEDIATE_MANUAL_REVIEW
    if candidate.action_state in {ActionState.THESIS_WEAKENING, ActionState.EXIT_INVALIDATE_REVIEW}:
        return AlertRoute.POSITION_WATCH
    return AlertRoute.DAILY_DIGEST


def _candidate_sort_key(row: Mapping[str, Any]) -> tuple[int, float, str, str, str]:
    state = str(row.get("state") or "")
    rank = {
        ActionState.EXIT_INVALIDATE_REVIEW.value: 0,
        ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value: 1,
        ActionState.THESIS_WEAKENING.value: 2,
        ActionState.WARNING.value: 3,
        ActionState.ADD_TO_WATCHLIST.value: 4,
        ActionState.RESEARCH_ONLY.value: 5,
        ActionState.BLOCKED.value: 6,
        ActionState.NO_ACTION.value: 7,
    }.get(state, 99)
    return (
        rank,
        -float(row.get("final_score") or 0.0),
        _as_datetime(row["as_of"]).isoformat(),
        _as_datetime(row["created_at"]).isoformat(),
        str(row.get("id") or ""),
    )


def _state_payload(state_row: Mapping[str, Any]) -> Mapping[str, Any]:
    transition_reasons = state_row.get("transition_reasons")
    if isinstance(transition_reasons, Mapping):
        candidate = transition_reasons.get("candidate")
        if isinstance(candidate, Mapping):
            return candidate
    return {}


def _mapping_payload(row: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if row is None:
        return {}
    payload = row.get("payload")
    return payload if isinstance(payload, Mapping) else {}


def _top_supporting_evidence(
    packet_payload: Mapping[str, Any],
    card_payload: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    for payload, key in (
        (card_payload, "evidence"),
        (packet_payload, "supporting_evidence"),
    ):
        evidence = payload.get(key)
        if isinstance(evidence, list) and evidence:
            first = evidence[0]
            if isinstance(first, Mapping):
                return first
    return None


def _entry_zone(*payloads: Mapping[str, Any]) -> tuple[float, ...] | None:
    value = _nested_first(
        payloads,
        ("trade_plan", "entry_zone"),
        ("candidate", "entry_zone"),
        ("entry_zone",),
    )
    if not isinstance(value, list | tuple) or not value:
        return None
    return tuple(float(item) for item in value)


def _invalidation_price(*payloads: Mapping[str, Any]) -> float | None:
    value = _nested_first(
        payloads,
        ("trade_plan", "invalidation_price"),
        ("candidate", "invalidation_price"),
        ("invalidation_price",),
    )
    return float(value) if value is not None else None


def _nested_first(
    payloads: tuple[Mapping[str, Any], ...],
    *paths: tuple[str, ...],
) -> Any:
    for payload in payloads:
        for path in paths:
            value: Any = payload
            for part in path:
                if not isinstance(value, Mapping) or part not in value:
                    value = None
                    break
                value = value[part]
            if value is not None:
                return value
    return None


def _source_ts(
    state_row: Mapping[str, Any],
    packet_row: Mapping[str, Any] | None,
    card_row: Mapping[str, Any] | None,
    state_payload: Mapping[str, Any],
) -> datetime:
    for row in (card_row, packet_row):
        if row is not None and row.get("source_ts") is not None:
            return _as_datetime(row["source_ts"])
    metadata = state_payload.get("metadata") if isinstance(state_payload, Mapping) else None
    if isinstance(metadata, Mapping) and metadata.get("source_ts") is not None:
        return _as_datetime(metadata["source_ts"])
    return _as_datetime(state_row["as_of"])


def _state_audit_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "ticker": str(row["ticker"]),
        "as_of": _as_datetime(row["as_of"]).isoformat(),
        "state": str(row["state"]),
        "previous_state": row.get("previous_state"),
        "final_score": float(row["final_score"]),
        "score_delta_5d": float(row["score_delta_5d"] or 0.0),
        "created_at": _as_datetime(row["created_at"]).isoformat(),
    }


def _source_payload(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = {
        "id": str(row["id"]),
        "available_at": _as_datetime(row["available_at"]).isoformat(),
        "source_ts": _as_datetime(row["source_ts"]).isoformat(),
    }
    if "candidate_state_id" in row:
        payload["candidate_state_id"] = row["candidate_state_id"]
    if "candidate_packet_id" in row:
        payload["candidate_packet_id"] = row["candidate_packet_id"]
    return payload


def _row_mapping(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return {
        key: _as_datetime(value) if isinstance(value, datetime) else value
        for key, value in row.items()
    }


def _optional_action_state(value: Any) -> ActionState | None:
    if value is None or str(value).strip() == "":
        return None
    return ActionState(str(value))


def _positive_limit(value: int) -> int:
    return max(1, int(value))


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _as_datetime(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["AlertPlanResult", "plan_alerts"]
