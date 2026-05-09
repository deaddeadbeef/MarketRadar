from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime, time, timedelta
from enum import Enum
from typing import Any

from catalyst_radar.core.models import ActionState
from catalyst_radar.decision_cards.models import (
    DECISION_CARD_SCHEMA_VERSION,
    MANUAL_REVIEW_DISCLAIMER,
    DecisionCard,
)

_MISSING = object()
_MAX_EVIDENCE_ITEMS = 5


def build_decision_card(
    packet: Any,
    *,
    available_at: datetime | str | None = None,
    user_decision: str | None = None,
    max_evidence_items: int = _MAX_EVIDENCE_ITEMS,
) -> DecisionCard:
    packet_payload = _as_mapping(_read(packet, "payload", {}))
    candidate_packet_id_value = _first_existing(
        _read(packet, "id", _MISSING),
        _nested(packet_payload, "audit", "packet_id", default=_MISSING),
        _nested(packet_payload, "audit", "candidate_packet_id", default=_MISSING),
    )
    if candidate_packet_id_value is _MISSING:
        msg = "candidate packet id is required"
        raise ValueError(msg)
    candidate_packet_id = str(candidate_packet_id_value)
    if not candidate_packet_id:
        msg = "candidate packet id is required"
        raise ValueError(msg)

    ticker_value = _first_existing(
        _read(packet, "ticker", _MISSING),
        _nested(packet_payload, "identity", "ticker", default=_MISSING),
    )
    if ticker_value is _MISSING:
        msg = "candidate packet ticker is required"
        raise ValueError(msg)
    ticker = str(ticker_value).upper()
    if not ticker:
        msg = "candidate packet ticker is required"
        raise ValueError(msg)

    as_of = _aware_datetime(
        _first_existing(
            _read(packet, "as_of", _MISSING),
            _nested(packet_payload, "identity", "as_of", default=_MISSING),
        ),
        "as_of",
    )
    source_ts = _aware_datetime(
        _first_existing(
            _read(packet, "source_ts", _MISSING),
            _nested(packet_payload, "audit", "source_ts", default=_MISSING),
        ),
        "source_ts",
    )
    card_available_at = _aware_datetime(
        available_at
        if available_at is not None
        else _first_existing(
            _read(packet, "available_at", _MISSING),
            _nested(packet_payload, "audit", "available_at", default=_MISSING),
        ),
        "available_at",
    )

    source_action_state = _action_state(
        _first_existing(
            _read(packet, "state", _MISSING),
            _read(packet, "action_state", _MISSING),
            _nested(packet_payload, "identity", "action_state", default=_MISSING),
            _nested(packet_payload, "identity", "state", default=_MISSING),
        )
    )
    hard_blocks = _hard_blocks(packet, packet_payload)
    action_state = _card_action_state(source_action_state, hard_blocks)
    next_review_at = _next_review_at(as_of, action_state)
    setup_type = _optional_string(
        _first_existing(
            _nested(packet_payload, "identity", "setup_type", default=_MISSING),
            _nested(packet_payload, "trade_plan", "setup_type", default=_MISSING),
            _nested(packet_payload, "metadata", "setup_type", default=_MISSING),
            _nested(_metadata(packet), "setup_type", default=_MISSING),
        )
    )
    final_score = _float_value(
        _first_existing(
            _read(packet, "final_score", _MISSING),
            _nested(packet_payload, "scores", "final_score", default=_MISSING),
            _nested(packet_payload, "scores", "final", default=_MISSING),
        ),
        "final_score",
    )

    portfolio_impact_source = _portfolio_impact_source(packet, packet_payload)
    trade_plan, missing_trade_plan = _trade_plan(packet, packet_payload, portfolio_impact_source)
    position_sizing = _position_sizing(packet, packet_payload)
    portfolio_impact = _portfolio_impact(portfolio_impact_source, hard_blocks)
    evidence = _top_evidence(
        _evidence_items(packet, packet_payload, supporting=True),
        max_evidence_items=max_evidence_items,
    )
    disconfirming_evidence = _top_evidence(
        _evidence_items(packet, packet_payload, supporting=False),
        max_evidence_items=max_evidence_items,
    )
    if not disconfirming_evidence:
        disconfirming_evidence = [_disconfirming_gap(candidate_packet_id)]

    controls = {
        "hard_blocks": hard_blocks,
        "conflicts": _as_json_list(
            _first_existing(
                _read(packet, "conflicts", _MISSING),
                _read(packet, "event_conflicts", _MISSING),
                _nested(packet_payload, "conflicts", default=_MISSING),
                _nested(packet_payload, "controls", "conflicts", default=_MISSING),
            )
        ),
        "missing_trade_plan": missing_trade_plan,
        "upcoming_events": _as_json_list(
            _first_existing(
                _nested(packet_payload, "controls", "upcoming_events", default=_MISSING),
                _nested(packet_payload, "metadata", "upcoming_events", default=_MISSING),
                _nested(_metadata(packet), "upcoming_events", default=_MISSING),
            )
        ),
        "next_review_at": next_review_at.isoformat(),
        "user_decision": user_decision,
        "manual_review_only": True,
    }
    payload = {
        "identity": {
            "ticker": ticker,
            "company": _company_name(packet, packet_payload),
            "version": DECISION_CARD_SCHEMA_VERSION,
            "as_of": as_of.isoformat(),
            "action_state": action_state.value,
            "source_action_state": source_action_state.value,
            "setup_type": setup_type,
            "card_type": _card_type(action_state),
        },
        "scores": _scores(packet, packet_payload, final_score),
        "trade_plan": trade_plan,
        "position_sizing": position_sizing,
        "portfolio_impact": portfolio_impact,
        "evidence": evidence,
        "disconfirming_evidence": disconfirming_evidence,
        "controls": controls,
        "disclaimer": MANUAL_REVIEW_DISCLAIMER,
        "audit": {
            "candidate_packet_id": candidate_packet_id,
            "schema_version": DECISION_CARD_SCHEMA_VERSION,
            "source_ts": source_ts.isoformat(),
            "available_at": card_available_at.isoformat(),
        },
    }

    if action_state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW:
        _validate_eligible_payload(payload)

    return DecisionCard(
        id=deterministic_decision_card_id(
            ticker=ticker,
            as_of=as_of,
            action_state=action_state,
            available_at=card_available_at,
        ),
        ticker=ticker,
        as_of=as_of,
        candidate_packet_id=candidate_packet_id,
        action_state=action_state,
        setup_type=setup_type,
        final_score=final_score,
        next_review_at=next_review_at,
        payload=payload,
        schema_version=DECISION_CARD_SCHEMA_VERSION,
        source_ts=source_ts,
        available_at=card_available_at,
        user_decision=user_decision,
    )


def deterministic_decision_card_id(
    *,
    ticker: str,
    as_of: datetime,
    action_state: ActionState | str,
    available_at: datetime,
    schema_version: str = DECISION_CARD_SCHEMA_VERSION,
) -> str:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        msg = "as_of must include timezone information"
        raise ValueError(msg)
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        msg = "available_at must include timezone information"
        raise ValueError(msg)
    normalized_ticker = str(ticker).upper()
    normalized_state = ActionState(action_state)
    key = {
        "action_state": normalized_state.value,
        "as_of": as_of.isoformat(),
        "available_at": available_at.isoformat(),
        "schema_version": schema_version,
        "ticker": normalized_ticker,
    }
    digest = hashlib.sha256(serialize_decision_card_payload(key).encode("utf-8")).hexdigest()[:16]
    return (
        f"{schema_version}:{normalized_ticker}:{as_of.isoformat()}:"
        f"{normalized_state.value}:{available_at.isoformat()}:{digest}"
    )


def serialize_decision_card_payload(payload: Mapping[str, Any]) -> str:
    return json.dumps(
        _json_ready(payload),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _validate_eligible_payload(payload: Mapping[str, Any]) -> None:
    missing = []
    trade_plan = _as_mapping(payload.get("trade_plan"))
    if not trade_plan.get("entry_zone"):
        missing.append("entry_zone")
    if trade_plan.get("invalidation_price") is None:
        missing.append("invalidation_price")
    if trade_plan.get("reward_risk") is None:
        missing.append("reward_risk")

    position_sizing = _as_mapping(payload.get("position_sizing"))
    for field_name in ("risk_per_trade_pct", "shares", "notional", "cash_check"):
        if position_sizing.get(field_name) is None:
            missing.append(f"position_sizing.{field_name}")

    portfolio_impact = _as_mapping(payload.get("portfolio_impact"))
    if not portfolio_impact:
        missing.append("portfolio_impact")
    if not payload.get("evidence"):
        missing.append("evidence")
    if not payload.get("disconfirming_evidence"):
        missing.append("disconfirming_evidence")
    controls = _as_mapping(payload.get("controls"))
    if "hard_blocks" not in controls:
        missing.append("controls.hard_blocks")
    if "next_review_at" not in controls:
        missing.append("controls.next_review_at")

    if missing:
        msg = "EligibleForManualBuyReview decision card missing required fields: "
        raise ValueError(msg + ", ".join(missing))


def _card_action_state(source_action_state: ActionState, hard_blocks: Sequence[str]) -> ActionState:
    if source_action_state == ActionState.BLOCKED or hard_blocks:
        return ActionState.BLOCKED
    return source_action_state


def _next_review_at(as_of: datetime, action_state: ActionState) -> datetime:
    if action_state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW:
        review_date = as_of.astimezone(UTC).date() + timedelta(days=1)
        return datetime.combine(review_date, time(13, 30), tzinfo=UTC)
    if action_state == ActionState.WARNING:
        return as_of + timedelta(days=2)
    return as_of + timedelta(days=7)


def _card_type(action_state: ActionState) -> str:
    if action_state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW:
        return "eligible_manual_review"
    if action_state == ActionState.BLOCKED:
        return "blocked_research"
    return "research"


def _scores(
    packet: Any,
    packet_payload: Mapping[str, Any],
    final_score: float,
) -> Mapping[str, Any]:
    metadata = _metadata(packet)
    score_payload = _as_mapping(packet_payload.get("scores"))
    return {
        "final_score": final_score,
        "pillar_scores": _json_ready(
            _first_existing(
                _nested(score_payload, "pillar_scores", default=_MISSING),
                _nested(score_payload, "pillars", default=_MISSING),
                _nested(metadata, "pillar_scores", default=_MISSING),
                {},
            )
        ),
        "risk_penalty": _first_existing(
            _read(packet, "risk_penalty", _MISSING),
            _nested(score_payload, "risk_penalty", default=_MISSING),
            _nested(metadata, "risk_penalty", default=None),
        ),
        "portfolio_penalty": _first_existing(
            _read(packet, "portfolio_penalty", _MISSING),
            _nested(score_payload, "portfolio_penalty", default=_MISSING),
            _nested(metadata, "portfolio_penalty", default=None),
        ),
        "score_delta": _first_existing(
            _nested(score_payload, "score_delta", default=_MISSING),
            _nested(score_payload, "score_delta_5d", default=_MISSING),
            _nested(score_payload, "score_deltas", default=_MISSING),
            _nested(metadata, "score_delta", default=None),
        ),
    }


def _trade_plan(
    packet: Any,
    packet_payload: Mapping[str, Any],
    portfolio_impact_source: Mapping[str, Any],
) -> tuple[Mapping[str, Any], list[str]]:
    plan = _as_mapping(packet_payload.get("trade_plan"))
    entry_zone = _first_existing(
        _nested(plan, "entry_zone", default=_MISSING),
        _read(packet, "entry_zone", _MISSING),
        None,
    )
    invalidation_price = _first_existing(
        _nested(plan, "invalidation_price", default=_MISSING),
        _nested(plan, "invalidation", default=_MISSING),
        _read(packet, "invalidation_price", _MISSING),
        None,
    )
    reward_risk = _first_existing(
        _nested(plan, "reward_risk", default=_MISSING),
        _nested(plan, "reward_to_risk", default=_MISSING),
        _read(packet, "reward_risk", _MISSING),
        None,
    )
    max_loss = _first_existing(
        _nested(plan, "max_loss_if_wrong", default=_MISSING),
        _nested(plan, "max_loss", default=_MISSING),
        _nested(portfolio_impact_source, "max_loss", default=None),
    )
    missing_trade_plan = _missing_trade_plan(packet, packet_payload, plan)
    return (
        {
            "setup_type": _first_existing(
                _nested(plan, "setup_type", default=_MISSING),
                _nested(packet_payload, "identity", "setup_type", default=None),
            ),
            "entry_zone": _json_ready(entry_zone),
            "invalidation_price": invalidation_price,
            "max_loss_if_wrong": max_loss,
            "reward_risk": reward_risk,
            "missing_fields": missing_trade_plan,
        },
        missing_trade_plan,
    )


def _missing_trade_plan(
    packet: Any,
    packet_payload: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> list[str]:
    explicit = _as_string_list(
        _first_existing(
            _read(packet, "missing_trade_plan", _MISSING),
            _nested(packet_payload, "missing_trade_plan", default=_MISSING),
            _nested(plan, "missing_fields", default=_MISSING),
            _nested(packet_payload, "controls", "missing_trade_plan", default=_MISSING),
            (),
        )
    )
    inferred = []
    if not _first_existing(
        _nested(plan, "entry_zone", default=_MISSING),
        _read(packet, "entry_zone", _MISSING),
        None,
    ):
        inferred.append("entry_zone")
    if (
        _first_existing(
            _nested(plan, "invalidation_price", default=_MISSING),
            _nested(plan, "invalidation", default=_MISSING),
            _read(packet, "invalidation_price", _MISSING),
            None,
        )
        is None
    ):
        inferred.append("invalidation_price")
    reward_risk = _first_existing(
        _nested(plan, "reward_risk", default=_MISSING),
        _nested(plan, "reward_to_risk", default=_MISSING),
        _read(packet, "reward_risk", _MISSING),
        None,
    )
    if reward_risk is None:
        inferred.append("reward_risk")
    return _unique_strings([*explicit, *inferred])


def _position_sizing(packet: Any, packet_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    source = _first_mapping(
        _read(packet, "position_sizing", _MISSING),
        _nested(packet_payload, "position_sizing", default=_MISSING),
        _nested(packet_payload, "trade_plan", "position_size", default=_MISSING),
        _nested(packet_payload, "metadata", "position_sizing", default=_MISSING),
        _nested(_metadata(packet), "position_sizing", default=_MISSING),
    )
    hard_blocks = _hard_blocks(packet, packet_payload)
    notional = _first_existing(
        _nested(source, "notional", default=_MISSING),
        _nested(source, "proposed_notional", default=None),
    )
    return {
        "risk_per_trade_pct": _first_existing(
            _nested(source, "risk_per_trade_pct", default=_MISSING),
            _nested(source, "risk_pct", default=None),
        ),
        "shares": _nested(source, "shares", default=None),
        "notional": notional,
        "cash_check": _first_existing(
            _nested(source, "cash_check", default=_MISSING),
            "insufficient"
            if "insufficient_cash_hard_block" in hard_blocks
            else ("pass" if _safe_float(notional) > 0 else None),
        ),
        "sizing_notes": _as_string_list(_nested(source, "sizing_notes", default=())),
    }


def _portfolio_impact_source(packet: Any, packet_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _first_mapping(
        _read(packet, "portfolio_impact", _MISSING),
        _nested(packet_payload, "portfolio_impact", default=_MISSING),
        _nested(packet_payload, "metadata", "portfolio_impact", default=_MISSING),
        _nested(_metadata(packet), "portfolio_impact", default=_MISSING),
    )


def _portfolio_impact(source: Mapping[str, Any], hard_blocks: Sequence[str]) -> Mapping[str, Any]:
    if not source:
        return {}
    return {
        "single_name": _exposure(source, "single_name", "single_name"),
        "sector": _exposure(source, "sector", "sector"),
        "theme": _exposure(source, "theme", "theme"),
        "correlated_basket": _exposure(source, "correlated", "correlated_basket"),
        "proposed_notional": _nested(source, "proposed_notional", default=None),
        "max_loss": _nested(source, "max_loss", default=None),
        "portfolio_penalty": _nested(source, "portfolio_penalty", default=None),
        "hard_blocks": list(hard_blocks),
    }


def _exposure(source: Mapping[str, Any], flat_prefix: str, nested_key: str) -> Mapping[str, Any]:
    nested = _as_mapping(source.get(nested_key))
    if nested:
        return {
            "before_pct": _first_existing(
                _nested(nested, "before_pct", default=_MISSING),
                _nested(nested, "before", default=None),
            ),
            "after_pct": _first_existing(
                _nested(nested, "after_pct", default=_MISSING),
                _nested(nested, "after", default=None),
            ),
        }
    return {
        "before_pct": _first_existing(
            _nested(source, f"{flat_prefix}_before_pct", default=_MISSING),
            _nested(source, f"{nested_key}_before_pct", default=None),
        ),
        "after_pct": _first_existing(
            _nested(source, f"{flat_prefix}_after_pct", default=_MISSING),
            _nested(source, f"{nested_key}_after_pct", default=None),
        ),
    }


def _evidence_items(
    packet: Any,
    packet_payload: Mapping[str, Any],
    *,
    supporting: bool,
) -> list[Mapping[str, Any]]:
    polarity = "supporting" if supporting else "disconfirming"
    attr_name = "supporting_evidence" if supporting else "disconfirming_evidence"
    items = [
        *_as_sequence(_read(packet, attr_name, _MISSING)),
        *_as_sequence(_nested(packet_payload, attr_name, default=_MISSING)),
    ]
    for item in _as_sequence(_nested(packet_payload, "evidence", default=_MISSING)):
        if str(_read(item, "polarity", "")).lower() == polarity:
            items.append(item)
    return [_normalise_evidence_item(item, polarity) for item in items]


def _normalise_evidence_item(item: Any, polarity: str) -> Mapping[str, Any]:
    payload = _as_mapping(_read(item, "payload", {}))
    return {
        "kind": str(_first_existing(_read(item, "kind", _MISSING), "evidence")),
        "title": str(
            _first_existing(_read(item, "title", _MISSING), _read(item, "kind", "Evidence"))
        ),
        "summary": str(_first_existing(_read(item, "summary", _MISSING), "")),
        "polarity": str(_first_existing(_read(item, "polarity", _MISSING), polarity)),
        "strength": _safe_float(_first_existing(_read(item, "strength", _MISSING), 0.0)),
        "source_id": _optional_string(_read(item, "source_id", None)),
        "source_url": _optional_string(_read(item, "source_url", None)),
        "computed_feature_id": _optional_string(_read(item, "computed_feature_id", None)),
        "source_quality": _optional_float(_read(item, "source_quality", None)),
        "source_ts": _json_ready(_read(item, "source_ts", None)),
        "available_at": _json_ready(_read(item, "available_at", None)),
        "payload": _json_ready(payload),
    }


def _top_evidence(
    items: Sequence[Mapping[str, Any]],
    *,
    max_evidence_items: int,
) -> list[Mapping[str, Any]]:
    deduped: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
    for item in items:
        key = (
            str(item.get("kind") or ""),
            str(item.get("source_id") or ""),
            str(item.get("source_url") or ""),
            str(item.get("computed_feature_id") or ""),
        )
        existing = deduped.get(key)
        if existing is None or (
            _safe_float(item.get("strength")),
            _safe_float(item.get("source_quality")),
        ) > (
            _safe_float(existing.get("strength")),
            _safe_float(existing.get("source_quality")),
        ):
            deduped[key] = item
    return sorted(
        deduped.values(),
        key=lambda item: (
            -_safe_float(item.get("strength")),
            -_safe_float(item.get("source_quality")),
            str(item.get("title") or ""),
            str(item.get("summary") or ""),
        ),
    )[:max_evidence_items]


def _disconfirming_gap(candidate_packet_id: str) -> Mapping[str, Any]:
    return {
        "kind": "evidence_gap",
        "title": "Disconfirming evidence gap",
        "summary": "No stronger disconfirming evidence was present in deterministic packet inputs.",
        "polarity": "disconfirming",
        "strength": 0.0,
        "source_id": None,
        "source_url": None,
        "computed_feature_id": (
            f"candidate_packets:{candidate_packet_id}:disconfirming_evidence_gap"
        ),
        "source_quality": None,
        "source_ts": None,
        "available_at": None,
        "payload": {},
    }


def _hard_blocks(packet: Any, packet_payload: Mapping[str, Any]) -> list[str]:
    portfolio_impact = _portfolio_impact_source(packet, packet_payload)
    return _unique_strings(
        [
            *_as_string_list(_read(packet, "hard_blocks", _MISSING)),
            *_as_string_list(_nested(packet_payload, "hard_blocks", default=_MISSING)),
            *_as_string_list(_nested(packet_payload, "controls", "hard_blocks", default=_MISSING)),
            *_as_string_list(_nested(portfolio_impact, "hard_blocks", default=_MISSING)),
        ]
    )


def _company_name(packet: Any, packet_payload: Mapping[str, Any]) -> str | None:
    return _optional_string(
        _first_existing(
            _read(packet, "company", _MISSING),
            _read(packet, "name", _MISSING),
            _nested(packet_payload, "identity", "company", default=_MISSING),
            _nested(packet_payload, "identity", "name", default=_MISSING),
            _nested(packet_payload, "metadata", "company", default=_MISSING),
            _nested(packet_payload, "metadata", "name", default=_MISSING),
            _nested(_metadata(packet), "company", default=_MISSING),
            _nested(_metadata(packet), "name", default=None),
        )
    )


def _metadata(packet: Any) -> Mapping[str, Any]:
    return _as_mapping(_read(packet, "metadata", {}))


def _action_state(value: Any) -> ActionState:
    if value is _MISSING:
        msg = "candidate packet action state is required"
        raise ValueError(msg)
    return ActionState(value)


def _aware_datetime(value: Any, field_name: str) -> datetime:
    if value is _MISSING:
        msg = f"{field_name} is required"
        raise ValueError(msg)
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must include timezone information"
        raise ValueError(msg)
    return value


def _float_value(value: Any, field_name: str) -> float:
    if value is _MISSING:
        msg = f"{field_name} is required"
        raise ValueError(msg)
    return float(value)


def _optional_float(value: Any) -> float | None:
    if value is None or value is _MISSING:
        return None
    return float(value)


def _safe_float(value: Any) -> float:
    if value is None or value is _MISSING:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _optional_string(value: Any) -> str | None:
    if value is None or value is _MISSING:
        return None
    text = str(value)
    return text if text else None


def _read(source: Any, key: str, default: Any = None) -> Any:
    if source is _MISSING or source is None:
        return default
    if isinstance(source, Mapping):
        return source.get(key, default)
    if is_dataclass(source) and not isinstance(source, type):
        return getattr(source, key, default)
    return getattr(source, key, default)


def _nested(source: Any, *keys: str, default: Any = None) -> Any:
    value = source
    for key in keys:
        value = _read(value, key, _MISSING)
        if value is _MISSING:
            return default
    return value


def _first_existing(*values: Any) -> Any:
    for value in values:
        if value is not _MISSING:
            return value
    return _MISSING


def _first_mapping(*values: Any) -> Mapping[str, Any]:
    for value in values:
        mapping = _as_mapping(value)
        if mapping:
            return mapping
    return {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if value is None or value is _MISSING:
        return {}
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    return {}


def _as_sequence(value: Any) -> list[Any]:
    if value is None or value is _MISSING:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return list(value)
    return [value]


def _as_json_list(value: Any) -> list[Any]:
    return [_json_ready(item) for item in _as_sequence(value)]


def _as_string_list(value: Any) -> list[str]:
    return [str(item) for item in _as_sequence(value) if str(item)]


def _unique_strings(values: Sequence[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        text = str(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _json_ready(value: Any) -> Any:
    if value is _MISSING:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {
            str(key): _json_ready(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _json_ready(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value
