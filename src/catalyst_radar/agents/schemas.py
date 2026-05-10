from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from catalyst_radar.agents.evidence import source_faithfulness_violations
from catalyst_radar.decision_cards.models import (
    DECISION_CARD_SCHEMA_VERSION,
    FORBIDDEN_EXECUTION_PHRASES,
)

SKEPTIC_REVIEW_SCHEMA_VERSION = "skeptic-review-v1"
DECISION_CARD_DRAFT_SCHEMA_VERSION = DECISION_CARD_SCHEMA_VERSION
_SKEPTIC_SEVERITIES = frozenset({"low", "medium", "high"})
_PROTECTED_DECISION_CARD_FIELDS = frozenset(
    {
        "identity",
        "scores",
        "trade_plan",
        "position_sizing",
        "portfolio_impact",
        "controls",
    }
)


class AgentSchemaError(ValueError):
    pass


def validate_evidence_review_output(
    payload: Mapping[str, Any],
    *,
    ticker: str,
    as_of: datetime,
    evidence_packet: Mapping[str, Any],
) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        msg = "evidence review payload must be a mapping"
        raise AgentSchemaError(msg)

    normalized = _json_safe_mapping(payload, "payload")
    expected_ticker = _required_text(ticker, "ticker").upper()
    actual_ticker = _required_text(normalized.get("ticker"), "ticker").upper()
    if actual_ticker != expected_ticker:
        msg = f"ticker must match {expected_ticker}"
        raise AgentSchemaError(msg)
    normalized["ticker"] = actual_ticker

    expected_as_of = _require_aware_utc(as_of, "as_of")
    actual_as_of = _require_aware_utc(normalized.get("as_of"), "as_of")
    if actual_as_of != expected_as_of:
        msg = f"as_of must match {expected_as_of.isoformat()}"
        raise AgentSchemaError(msg)
    normalized["as_of"] = actual_as_of.isoformat()

    claims = normalized.get("claims")
    if not isinstance(claims, list):
        msg = "claims must be a list"
        raise AgentSchemaError(msg)
    normalized["claims"] = [
        _validated_claim(claim, f"claims[{index}]")
        for index, claim in enumerate(claims)
    ]

    normalized["bear_case"] = [
        _validated_evidence_review_note(item, f"bear_case[{index}]")
        for index, item in enumerate(
            _required_list(normalized.get("bear_case"), "bear_case")
        )
    ]
    normalized["unresolved_conflicts"] = [
        _validated_evidence_review_note(item, f"unresolved_conflicts[{index}]")
        for index, item in enumerate(
            _required_list(
                normalized.get("unresolved_conflicts"),
                "unresolved_conflicts",
            )
        )
    ]
    if not isinstance(normalized.get("recommended_policy_downgrade"), bool):
        msg = "recommended_policy_downgrade must be a boolean"
        raise AgentSchemaError(msg)

    _reject_forbidden_execution_language(normalized)
    _raise_source_faithfulness_violations(normalized, evidence_packet)
    return normalized


def validate_skeptic_review_output(
    payload: Mapping[str, Any],
    *,
    ticker: str,
    as_of: datetime,
    evidence_packet: Mapping[str, Any],
) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        msg = "skeptic review payload must be a mapping"
        raise AgentSchemaError(msg)

    normalized = _json_safe_mapping(payload, "payload")
    _validate_output_envelope(
        normalized,
        ticker=ticker,
        as_of=as_of,
        schema_version=SKEPTIC_REVIEW_SCHEMA_VERSION,
    )

    bear_case = _required_list(normalized.get("bear_case"), "bear_case")
    normalized["bear_case"] = [
        _validated_skeptic_bear_case_item(item, f"bear_case[{index}]")
        for index, item in enumerate(bear_case)
    ]
    normalized["missing_evidence"] = _required_text_list(
        normalized.get("missing_evidence"),
        "missing_evidence",
    )
    normalized["contradictions"] = _required_text_list(
        normalized.get("contradictions"),
        "contradictions",
    )
    if not isinstance(normalized.get("recommended_policy_downgrade"), bool):
        msg = "recommended_policy_downgrade must be a boolean"
        raise AgentSchemaError(msg)
    normalized["manual_review_notes"] = _required_text(
        normalized.get("manual_review_notes"),
        "manual_review_notes",
    )

    _reject_forbidden_execution_language(normalized)
    _raise_source_faithfulness_violations(normalized, evidence_packet)
    return normalized


def validate_decision_card_draft_output(
    payload: Mapping[str, Any],
    *,
    ticker: str,
    as_of: datetime,
    evidence_packet: Mapping[str, Any],
) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        msg = "decision card draft payload must be a mapping"
        raise AgentSchemaError(msg)

    normalized = _json_safe_mapping(payload, "payload")
    _validate_output_envelope(
        normalized,
        ticker=ticker,
        as_of=as_of,
        schema_version=DECISION_CARD_DRAFT_SCHEMA_VERSION,
    )

    protected_fields = sorted(_PROTECTED_DECISION_CARD_FIELDS.intersection(normalized))
    if protected_fields:
        msg = (
            "decision card draft must not include deterministic fields: "
            f"{', '.join(protected_fields)}"
        )
        raise AgentSchemaError(msg)

    normalized["summary"] = _required_text(normalized.get("summary"), "summary")
    normalized["supporting_points"] = [
        _validated_decision_card_point(item, f"supporting_points[{index}]")
        for index, item in enumerate(
            _required_list(normalized.get("supporting_points"), "supporting_points")
        )
    ]
    normalized["risks"] = [
        _validated_decision_card_point(item, f"risks[{index}]")
        for index, item in enumerate(_required_list(normalized.get("risks"), "risks"))
    ]
    normalized["questions_for_human"] = _required_text_list(
        normalized.get("questions_for_human"),
        "questions_for_human",
    )
    if normalized.get("manual_review_only") is not True:
        msg = "manual_review_only must be true"
        raise AgentSchemaError(msg)

    _reject_forbidden_execution_language(normalized)
    _raise_source_faithfulness_violations(normalized, evidence_packet)
    return normalized


def _validated_claim(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise AgentSchemaError(msg)
    claim = _json_safe_mapping(value, field_name)
    source_id = _optional_text(claim.get("source_id"), f"{field_name}.source_id")
    computed_feature_id = _optional_text(
        claim.get("computed_feature_id"),
        f"{field_name}.computed_feature_id",
    )
    if not (source_id or computed_feature_id):
        msg = f"{field_name} must include source_id or computed_feature_id"
        raise AgentSchemaError(msg)
    if source_id is not None:
        claim["source_id"] = source_id
    if computed_feature_id is not None:
        claim["computed_feature_id"] = computed_feature_id

    for key in ("claim", "evidence_type", "uncertainty_notes"):
        claim[key] = _required_text(claim.get(key), f"{field_name}.{key}")
    claim["source_quality"] = _bounded_number(
        claim.get("source_quality"),
        f"{field_name}.source_quality",
        minimum=0.0,
        maximum=1.0,
    )
    claim["sentiment"] = _bounded_number(
        claim.get("sentiment"),
        f"{field_name}.sentiment",
        minimum=-1.0,
        maximum=1.0,
    )
    claim["confidence"] = _bounded_number(
        claim.get("confidence"),
        f"{field_name}.confidence",
        minimum=0.0,
        maximum=1.0,
    )
    return claim


def _validated_evidence_review_note(
    value: Any,
    field_name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise AgentSchemaError(msg)
    item = _json_safe_mapping(value, field_name)
    _normalize_source_link_fields(item, field_name)
    item["claim"] = _required_text(item.get("claim"), f"{field_name}.claim")
    if "confidence" in item and item["confidence"] is not None:
        item["confidence"] = _bounded_number(
            item.get("confidence"),
            f"{field_name}.confidence",
            minimum=0.0,
            maximum=1.0,
        )
    return item


def _validated_skeptic_bear_case_item(
    value: Any,
    field_name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise AgentSchemaError(msg)
    item = _json_safe_mapping(value, field_name)
    _normalize_source_link_fields(item, field_name)
    item["claim"] = _required_text(item.get("claim"), f"{field_name}.claim")
    item["why_it_matters"] = _required_text(
        item.get("why_it_matters"),
        f"{field_name}.why_it_matters",
    )
    severity = _required_text(item.get("severity"), f"{field_name}.severity").lower()
    if severity not in _SKEPTIC_SEVERITIES:
        msg = f"{field_name}.severity must be one of low, medium, high"
        raise AgentSchemaError(msg)
    item["severity"] = severity
    item["confidence"] = _bounded_number(
        item.get("confidence"),
        f"{field_name}.confidence",
        minimum=0.0,
        maximum=1.0,
    )
    return item


def _validated_decision_card_point(
    value: Any,
    field_name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise AgentSchemaError(msg)
    item = _json_safe_mapping(value, field_name)
    _normalize_source_link_fields(item, field_name)
    item["text"] = _required_text(item.get("text"), f"{field_name}.text")
    if "confidence" in item:
        item["confidence"] = _bounded_number(
            item.get("confidence"),
            f"{field_name}.confidence",
            minimum=0.0,
            maximum=1.0,
        )
    return item


def _normalize_source_link_fields(item: dict[str, Any], field_name: str) -> None:
    source_id = _optional_text(item.get("source_id"), f"{field_name}.source_id")
    computed_feature_id = _optional_text(
        item.get("computed_feature_id"),
        f"{field_name}.computed_feature_id",
    )
    if source_id is not None:
        item["source_id"] = source_id
    elif "source_id" in item:
        item.pop("source_id")
    if computed_feature_id is not None:
        item["computed_feature_id"] = computed_feature_id
    elif "computed_feature_id" in item:
        item.pop("computed_feature_id")


def _validate_output_envelope(
    normalized: dict[str, Any],
    *,
    ticker: str,
    as_of: datetime,
    schema_version: str,
) -> None:
    expected_ticker = _required_text(ticker, "ticker").upper()
    actual_ticker = _required_text(normalized.get("ticker"), "ticker").upper()
    if actual_ticker != expected_ticker:
        msg = f"ticker must match {expected_ticker}"
        raise AgentSchemaError(msg)
    normalized["ticker"] = actual_ticker

    expected_as_of = _require_aware_utc(as_of, "as_of")
    actual_as_of = _require_aware_utc(normalized.get("as_of"), "as_of")
    if actual_as_of != expected_as_of:
        msg = f"as_of must match {expected_as_of.isoformat()}"
        raise AgentSchemaError(msg)
    normalized["as_of"] = actual_as_of.isoformat()

    actual_schema_version = _required_text(
        normalized.get("schema_version"),
        "schema_version",
    )
    if actual_schema_version != schema_version:
        msg = f"schema_version must be {schema_version}"
        raise AgentSchemaError(msg)
    normalized["schema_version"] = actual_schema_version


def _required_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        msg = f"{field_name} must be a list"
        raise AgentSchemaError(msg)
    return value


def _required_text_list(value: Any, field_name: str) -> list[str]:
    return [
        _required_text(item, f"{field_name}[{index}]")
        for index, item in enumerate(_required_list(value, field_name))
    ]


def _raise_source_faithfulness_violations(
    normalized: Mapping[str, Any],
    evidence_packet: Mapping[str, Any],
) -> None:
    if not isinstance(evidence_packet, Mapping):
        msg = "evidence_packet must be a mapping"
        raise AgentSchemaError(msg)
    violations = source_faithfulness_violations(normalized, evidence_packet)
    if violations:
        raise AgentSchemaError("; ".join(violations))


def _reject_forbidden_execution_language(value: Mapping[str, Any]) -> None:
    forbidden = _first_forbidden_phrase(_walk_strings(value))
    if forbidden is not None:
        msg = f"payload contains forbidden execution wording: {forbidden!r}"
        raise AgentSchemaError(msg)


def _walk_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _walk_strings(item)
    elif isinstance(value, list | tuple):
        for item in value:
            yield from _walk_strings(item)


def _first_forbidden_phrase(values: Iterable[str]) -> str | None:
    for value in values:
        lowered = value.lower()
        for phrase in FORBIDDEN_EXECUTION_PHRASES:
            if phrase in lowered:
                return phrase
    return None


def _json_safe_mapping(value: Mapping[str, Any], field_name: str) -> dict[str, Any]:
    try:
        normalized = {
            str(key): _json_safe(item, f"{field_name}.{key}")
            for key, item in value.items()
        }
        json.dumps(normalized, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as exc:
        msg = f"{field_name} must be JSON-safe"
        raise AgentSchemaError(msg) from exc
    return normalized


def _json_safe(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe(item, f"{field_name}.{key}")
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [_json_safe(item, field_name) for item in value]
    if isinstance(value, datetime):
        return _require_aware_utc(value, field_name).isoformat()
    if value is None or isinstance(value, str | int | bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            msg = f"{field_name} must be finite"
            raise AgentSchemaError(msg)
        return value
    msg = f"{field_name} must be JSON-safe"
    raise AgentSchemaError(msg)


def _required_text(value: Any, field_name: str) -> str:
    text = _optional_text(value, field_name)
    if text is None:
        msg = f"{field_name} must not be blank"
        raise AgentSchemaError(msg)
    return text


def _optional_text(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        msg = f"{field_name} must be a string"
        raise AgentSchemaError(msg)
    text = value.strip()
    return text or None


def _require_aware_utc(value: Any, field_name: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            msg = f"{field_name} must be an ISO datetime"
            raise AgentSchemaError(msg) from exc
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise AgentSchemaError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise AgentSchemaError(msg)
    return value.astimezone(UTC)


def _finite_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        msg = f"{field_name} must be numeric"
        raise AgentSchemaError(msg)
    number = float(value)
    if not math.isfinite(number):
        msg = f"{field_name} must be finite"
        raise AgentSchemaError(msg)
    return number


def _bounded_number(
    value: Any,
    field_name: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    number = _finite_number(value, field_name)
    if number < minimum or number > maximum:
        msg = f"{field_name} must be between {minimum} and {maximum}"
        raise AgentSchemaError(msg)
    return number


__all__ = [
    "AgentSchemaError",
    "validate_decision_card_draft_output",
    "validate_evidence_review_output",
    "validate_skeptic_review_output",
]
