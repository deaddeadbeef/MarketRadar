from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from catalyst_radar.alerts.models import Alert, AlertSuppression


@dataclass(frozen=True)
class AlertDigest:
    generated_at: datetime
    groups: Mapping[str, Sequence[Alert]]
    suppressed_count: int


def build_alert_digest(
    alerts: Sequence[Alert],
    suppressions: Sequence[AlertSuppression],
    generated_at: datetime,
) -> AlertDigest:
    generated_at_utc = _aware_utc(generated_at, "generated_at")
    grouped: dict[str, list[Alert]] = defaultdict(list)
    for alert in alerts:
        grouped[_group_key(alert)].append(alert)

    ordered_groups = {
        key: tuple(sorted(values, key=_alert_sort_key))
        for key, values in sorted(grouped.items(), key=lambda item: _group_sort_key(item[0]))
    }
    return AlertDigest(
        generated_at=generated_at_utc,
        groups=ordered_groups,
        suppressed_count=len(suppressions),
    )


def digest_payload(digest: AlertDigest) -> dict[str, object]:
    return {
        "generated_at": digest.generated_at.isoformat(),
        "group_count": len(digest.groups),
        "suppressed_count": digest.suppressed_count,
        "groups": [
            {
                "key": key,
                "route": _group_route(key),
                "priority": _group_priority(key),
                "alert_count": len(alerts),
                "alerts": [_alert_payload(alert) for alert in alerts],
            }
            for key, alerts in digest.groups.items()
        ],
    }


def _group_key(alert: Alert) -> str:
    return f"{_enum_value(alert.route)}:{_enum_value(alert.priority)}"


def _group_route(key: str) -> str:
    return key.split(":", 1)[0]


def _group_priority(key: str) -> str:
    return key.split(":", 1)[1]


def _group_sort_key(key: str) -> tuple[int, int, str]:
    route, priority = key.split(":", 1)
    return (_ROUTE_ORDER.get(route, 99), _PRIORITY_ORDER.get(priority, 99), key)


def _alert_sort_key(alert: Alert) -> tuple[str, str, str]:
    available_at = getattr(alert, "available_at", None)
    available_at_text = available_at.isoformat() if isinstance(available_at, datetime) else ""
    return (str(getattr(alert, "ticker", "")), available_at_text, str(getattr(alert, "id", "")))


def _alert_payload(alert: Alert) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": str(alert.id),
        "ticker": str(alert.ticker),
        "route": _enum_value(alert.route),
        "channel": _enum_value(alert.channel),
        "priority": _enum_value(alert.priority),
        "status": _enum_value(alert.status),
        "dedupe_key": str(alert.dedupe_key),
        "trigger_kind": str(alert.trigger_kind),
        "trigger_fingerprint": str(alert.trigger_fingerprint),
        "title": str(alert.title),
        "summary": str(alert.summary),
        "candidate_state_id": alert.candidate_state_id,
        "candidate_packet_id": alert.candidate_packet_id,
        "decision_card_id": alert.decision_card_id,
        "feedback_url": alert.feedback_url,
    }
    for attr in ("as_of", "source_ts", "available_at", "created_at", "sent_at"):
        value = getattr(alert, attr, None)
        payload[attr] = value.isoformat() if isinstance(value, datetime) else None
    return payload


def _enum_value(value: Any) -> str:
    enum_value = getattr(value, "value", None)
    return str(enum_value if enum_value is not None else value)


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must include timezone information"
        raise ValueError(msg)
    return value.astimezone(UTC)


_ROUTE_ORDER = {
    "immediate_manual_review": 0,
    "position_watch": 1,
    "warning_digest": 2,
    "daily_digest": 3,
}
_PRIORITY_ORDER = {"critical": 0, "high": 1, "normal": 2, "low": 3}
