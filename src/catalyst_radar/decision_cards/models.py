from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping
from catalyst_radar.core.models import ActionState

DECISION_CARD_SCHEMA_VERSION = "decision-card-v1"
MANUAL_REVIEW_DISCLAIMER = (
    "Manual review only. This card is deterministic decision support for a human reviewer "
    "and does not make decisions, send instructions, or route orders."
)
FORBIDDEN_EXECUTION_PHRASES = (
    "buy now",
    "sell now",
    "execute",
    "place order",
    "automatic trade",
)
REQUIRED_PAYLOAD_FIELDS = (
    "identity",
    "scores",
    "trade_plan",
    "position_sizing",
    "portfolio_impact",
    "evidence",
    "disconfirming_evidence",
    "controls",
    "disclaimer",
    "audit",
)


@dataclass(frozen=True)
class DecisionCard:
    id: str
    ticker: str
    as_of: datetime
    candidate_packet_id: str
    action_state: ActionState
    setup_type: str | None
    final_score: float
    next_review_at: datetime
    payload: Mapping[str, Any]
    schema_version: str
    source_ts: datetime
    available_at: datetime
    user_decision: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string(self.id, "id")
        _require_non_empty_string(self.candidate_packet_id, "candidate_packet_id")
        _require_aware_datetime(self.as_of, "as_of")
        _require_aware_datetime(self.next_review_at, "next_review_at")
        _require_aware_datetime(self.source_ts, "source_ts")
        _require_aware_datetime(self.available_at, "available_at")

        ticker = str(self.ticker).upper()
        _require_non_empty_string(ticker, "ticker")
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "action_state", ActionState(self.action_state))
        object.__setattr__(self, "final_score", float(self.final_score))

        if self.schema_version != DECISION_CARD_SCHEMA_VERSION:
            msg = f"schema_version must be {DECISION_CARD_SCHEMA_VERSION!r}"
            raise ValueError(msg)
        validate_decision_card_payload(self.payload)
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


def validate_decision_card_payload(payload: Mapping[str, Any]) -> None:
    if not isinstance(payload, Mapping):
        msg = "payload must be a mapping"
        raise TypeError(msg)

    missing_fields = [field for field in REQUIRED_PAYLOAD_FIELDS if field not in payload]
    if missing_fields:
        msg = f"payload missing required fields: {', '.join(missing_fields)}"
        raise ValueError(msg)

    disclaimer = payload.get("disclaimer")
    if not isinstance(disclaimer, str) or "manual review only" not in disclaimer.lower():
        msg = "payload disclaimer must contain manual-review-only wording"
        raise ValueError(msg)

    forbidden = _first_forbidden_phrase(_walk_strings(payload))
    if forbidden is not None:
        msg = f"decision card payload contains forbidden execution wording: {forbidden!r}"
        raise ValueError(msg)


def _require_non_empty_string(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        msg = f"{field_name} must be a non-empty string"
        raise ValueError(msg)


def _require_aware_datetime(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must include timezone information"
        raise ValueError(msg)


def _walk_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _walk_strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk_strings(item)


def _first_forbidden_phrase(values: Iterable[str]) -> str | None:
    for value in values:
        lowered = value.lower()
        for phrase in FORBIDDEN_EXECUTION_PHRASES:
            if phrase in lowered:
                return phrase
    return None
