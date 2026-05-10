from __future__ import annotations

import json
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any


class AgentSchemaError(ValueError):
    pass


def validate_evidence_review_output(
    payload: Mapping[str, Any],
    *,
    ticker: str,
    as_of: datetime,
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

    if not isinstance(normalized.get("bear_case"), list):
        msg = "bear_case must be a list"
        raise AgentSchemaError(msg)
    if not isinstance(normalized.get("unresolved_conflicts"), list):
        msg = "unresolved_conflicts must be a list"
        raise AgentSchemaError(msg)
    if not isinstance(normalized.get("recommended_policy_downgrade"), bool):
        msg = "recommended_policy_downgrade must be a boolean"
        raise AgentSchemaError(msg)

    return normalized


def _validated_claim(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise AgentSchemaError(msg)
    claim = _json_safe_mapping(value, field_name)
    source_id = _optional_text(claim.get("source_id"))
    computed_feature_id = _optional_text(claim.get("computed_feature_id"))
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
    text = _optional_text(value)
    if text is None:
        msg = f"{field_name} must not be blank"
        raise AgentSchemaError(msg)
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
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


__all__ = ["AgentSchemaError", "validate_evidence_review_output"]
