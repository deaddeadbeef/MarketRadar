from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from catalyst_radar.alerts.models import AlertChannel, AlertPriority, AlertRoute
from catalyst_radar.core.immutability import freeze_mapping
from catalyst_radar.core.models import ActionState

FORBIDDEN_ALERT_COPY = (
    "buy now",
    "sell now",
    "execute",
    "place order",
    "automatic trade",
)


@dataclass(frozen=True)
class AlertCandidate:
    ticker: str
    as_of: datetime
    source_ts: datetime
    available_at: datetime
    candidate_state_id: str
    action_state: ActionState
    previous_state: ActionState | None
    final_score: float
    score_delta_5d: float
    hard_blocks: Sequence[str] = ()
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    top_supporting_evidence: Mapping[str, Any] | None = None
    entry_zone: Sequence[float] | None = None
    invalidation_price: float | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _aware_utc(self.as_of, "as_of"))
        object.__setattr__(self, "source_ts", _aware_utc(self.source_ts, "source_ts"))
        object.__setattr__(
            self,
            "available_at",
            _aware_utc(self.available_at, "available_at"),
        )
        object.__setattr__(
            self,
            "candidate_state_id",
            _required_text(self.candidate_state_id, "candidate_state_id"),
        )
        object.__setattr__(self, "action_state", _action_state(self.action_state))
        if self.previous_state is not None:
            object.__setattr__(self, "previous_state", _action_state(self.previous_state))
        object.__setattr__(self, "final_score", _finite_float(self.final_score, "final_score"))
        object.__setattr__(
            self,
            "score_delta_5d",
            _finite_float(self.score_delta_5d, "score_delta_5d"),
        )
        object.__setattr__(
            self,
            "hard_blocks",
            tuple(_required_text(block, "hard_block") for block in self.hard_blocks),
        )
        if self.candidate_packet_id is not None:
            object.__setattr__(
                self,
                "candidate_packet_id",
                _optional_text(self.candidate_packet_id),
            )
        if self.decision_card_id is not None:
            object.__setattr__(
                self,
                "decision_card_id",
                _optional_text(self.decision_card_id),
            )
        if self.top_supporting_evidence is not None:
            object.__setattr__(
                self,
                "top_supporting_evidence",
                freeze_mapping(self.top_supporting_evidence, "top_supporting_evidence"),
            )
        if self.entry_zone is not None:
            object.__setattr__(
                self,
                "entry_zone",
                tuple(_finite_float(value, "entry_zone") for value in self.entry_zone),
            )
        if self.invalidation_price is not None:
            object.__setattr__(
                self,
                "invalidation_price",
                _finite_float(self.invalidation_price, "invalidation_price"),
            )
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


@dataclass(frozen=True)
class AlertRouteDecision:
    should_alert: bool
    route: AlertRoute | None
    channel: AlertChannel | None
    priority: AlertPriority | None
    trigger_kind: str
    trigger_fingerprint: str
    reason: str


def route_alert(
    candidate: AlertCandidate,
    *,
    warning_delta_threshold: float = 10.0,
) -> AlertRouteDecision:
    threshold = _finite_float(warning_delta_threshold, "warning_delta_threshold")
    state = candidate.action_state

    if state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW:
        if not candidate.decision_card_id:
            return _suppressed_decision(
                candidate,
                trigger_kind="state_transition",
                reason="manual_review_missing_decision_card",
            )
        return _alert_decision(
            candidate,
            route=AlertRoute.IMMEDIATE_MANUAL_REVIEW,
            channel=AlertChannel.DASHBOARD,
            priority=AlertPriority.HIGH,
            trigger_kind="state_transition",
            reason="manual_review_candidate",
        )

    if state == ActionState.WARNING:
        if candidate.score_delta_5d < threshold:
            return _suppressed_decision(
                candidate,
                trigger_kind="score_delta",
                reason="warning_delta_below_threshold",
            )
        return _alert_decision(
            candidate,
            route=AlertRoute.WARNING_DIGEST,
            channel=AlertChannel.DIGEST,
            priority=AlertPriority.HIGH,
            trigger_kind="score_delta",
            reason="warning_score_delta_threshold",
        )

    if state in {ActionState.RESEARCH_ONLY, ActionState.ADD_TO_WATCHLIST}:
        return _alert_decision(
            candidate,
            route=AlertRoute.DAILY_DIGEST,
            channel=AlertChannel.DIGEST,
            priority=AlertPriority.NORMAL,
            trigger_kind=_daily_digest_trigger_kind(candidate),
            reason="daily_candidate_review",
        )

    if state == ActionState.THESIS_WEAKENING:
        return _alert_decision(
            candidate,
            route=AlertRoute.POSITION_WATCH,
            channel=AlertChannel.DASHBOARD,
            priority=AlertPriority.HIGH,
            trigger_kind="invalidation",
            reason="thesis_weakening_review",
        )

    if state == ActionState.EXIT_INVALIDATE_REVIEW:
        return _alert_decision(
            candidate,
            route=AlertRoute.POSITION_WATCH,
            channel=AlertChannel.DASHBOARD,
            priority=AlertPriority.CRITICAL,
            trigger_kind="invalidation",
            reason="exit_invalidation_review",
        )

    return _suppressed_decision(
        candidate,
        trigger_kind="state_transition",
        reason="state_not_alertable",
    )


def alert_title(candidate: AlertCandidate, decision: AlertRouteDecision) -> str:
    if not decision.should_alert:
        return _checked_copy(f"{candidate.ticker} alert suppressed", "title")
    if decision.route == AlertRoute.IMMEDIATE_MANUAL_REVIEW:
        return _checked_copy(f"{candidate.ticker} manual review alert", "title")
    if decision.route == AlertRoute.WARNING_DIGEST:
        return _checked_copy(f"{candidate.ticker} warning evidence digest", "title")
    if decision.route == AlertRoute.DAILY_DIGEST:
        return _checked_copy(f"{candidate.ticker} candidate evidence digest", "title")
    if decision.route == AlertRoute.POSITION_WATCH:
        return _checked_copy(f"{candidate.ticker} position watch review", "title")
    return _checked_copy(f"{candidate.ticker} candidate alert", "title")


def alert_summary(candidate: AlertCandidate, decision: AlertRouteDecision) -> str:
    route = _enum_value(decision.route) if decision.route is not None else "no_alert"
    summary = (
        f"{candidate.ticker} candidate review route={route} reason={decision.reason}; "
        f"state={candidate.action_state.value} score={candidate.final_score:.1f} "
        f"delta_5d={candidate.score_delta_5d:.1f} "
        f"trigger={decision.trigger_kind}:{decision.trigger_fingerprint}."
    )
    return _checked_copy(summary, "summary")


def candidate_trigger_fingerprint(candidate: AlertCandidate, trigger_kind: str) -> str:
    if trigger_kind == "score_delta":
        return f"score_delta:{_score_delta_bucket(candidate.score_delta_5d)}"
    if trigger_kind == "event":
        return f"event:{_evidence_fingerprint(candidate.top_supporting_evidence)}"
    if trigger_kind == "invalidation":
        invalidation = (
            _format_number(candidate.invalidation_price)
            if candidate.invalidation_price is not None
            else candidate.action_state.value
        )
        return f"invalidation:{candidate.action_state.value}:{invalidation}"
    return (
        f"state_transition:{_state_value(candidate.previous_state)}"
        f"->{candidate.action_state.value}"
    )


def _alert_decision(
    candidate: AlertCandidate,
    *,
    route: AlertRoute,
    channel: AlertChannel,
    priority: AlertPriority,
    trigger_kind: str,
    reason: str,
) -> AlertRouteDecision:
    return AlertRouteDecision(
        should_alert=True,
        route=route,
        channel=channel,
        priority=priority,
        trigger_kind=trigger_kind,
        trigger_fingerprint=candidate_trigger_fingerprint(candidate, trigger_kind),
        reason=reason,
    )


def _suppressed_decision(
    candidate: AlertCandidate,
    *,
    trigger_kind: str,
    reason: str,
) -> AlertRouteDecision:
    return AlertRouteDecision(
        should_alert=False,
        route=None,
        channel=None,
        priority=None,
        trigger_kind=trigger_kind,
        trigger_fingerprint=candidate_trigger_fingerprint(candidate, trigger_kind),
        reason=reason,
    )


def _daily_digest_trigger_kind(candidate: AlertCandidate) -> str:
    return "event" if candidate.top_supporting_evidence else "state_transition"


def _score_delta_bucket(score_delta_5d: float) -> str:
    bucket = math.floor(score_delta_5d / 5.0) * 5
    return _format_number(float(bucket))


def _evidence_fingerprint(evidence: Mapping[str, Any] | None) -> str:
    if evidence is None:
        return "missing"
    source_id = _optional_text(evidence.get("source_id"))
    if source_id is not None:
        return f"source_id_hash:{_short_hash(source_id)}"
    source_url = _optional_text(evidence.get("source_url"))
    if source_url is not None:
        return f"source_url_hash:{_short_hash(_canonical_url(source_url))}"
    title_hash = _optional_text(evidence.get("title_hash"))
    if title_hash is not None:
        return f"title_hash:{_short_hash(title_hash)}"
    title = _optional_text(evidence.get("title"))
    if title is None:
        return "missing"
    normalized = " ".join(title.lower().split())
    return f"title_hash:{_short_hash(normalized)}"


def _canonical_url(value: str) -> str:
    parsed = urlsplit(value)
    filtered_query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
    ]
    query = urlencode(sorted(filtered_query), doseq=True)
    return urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            query,
            "",
        )
    )


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _state_value(value: ActionState | None) -> str:
    return value.value if value is not None else "None"


def _enum_value(value: Any) -> str:
    enum_value = getattr(value, "value", None)
    return str(enum_value if enum_value is not None else value)


def _format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.12g}"


def _required_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        msg = f"{field_name} must be a non-empty string"
        raise ValueError(msg)
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must include timezone information"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _action_state(value: ActionState | str) -> ActionState:
    if isinstance(value, ActionState):
        return value
    return ActionState(str(value))


def _finite_float(value: Any, field_name: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        msg = f"{field_name} must be finite"
        raise ValueError(msg)
    return number


def _checked_copy(value: str, field_name: str) -> str:
    lowered = value.lower()
    for phrase in FORBIDDEN_ALERT_COPY:
        if phrase in lowered:
            msg = f"{field_name} contains prohibited alert wording: {phrase!r}"
            raise ValueError(msg)
    return value
