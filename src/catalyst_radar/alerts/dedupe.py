from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from catalyst_radar.alerts.models import Alert
from catalyst_radar.alerts.routing import (
    AlertCandidate,
    AlertRouteDecision,
    candidate_trigger_fingerprint,
)


@dataclass(frozen=True)
class DedupeDecision:
    emit: bool
    dedupe_key: str
    reason: str | None = None


def trigger_fingerprint(candidate: AlertCandidate, decision: AlertRouteDecision) -> str:
    return candidate_trigger_fingerprint(candidate, decision.trigger_kind)


def alert_dedupe_key(candidate: AlertCandidate, decision: AlertRouteDecision) -> str:
    return ":".join(
        (
            "alert-dedupe-v1",
            candidate.ticker,
            _value_or_none(decision.route),
            _value_or_none(candidate.action_state),
            decision.trigger_kind,
            trigger_fingerprint(candidate, decision),
        )
    )


def decide_dedupe(existing_alert: Alert | None, dedupe_key: str) -> DedupeDecision:
    if existing_alert is not None:
        return DedupeDecision(
            emit=False,
            dedupe_key=dedupe_key,
            reason="duplicate_trigger",
        )
    return DedupeDecision(emit=True, dedupe_key=dedupe_key)


def _value_or_none(value: Any) -> str:
    if value is None:
        return "none"
    enum_value = getattr(value, "value", None)
    return str(enum_value if enum_value is not None else value)
