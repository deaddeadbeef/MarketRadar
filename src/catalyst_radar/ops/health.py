from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from sqlalchemy import Engine, func, select

from catalyst_radar.core.models import ActionState
from catalyst_radar.ops.metrics import detect_score_drift, load_ops_metrics
from catalyst_radar.ops.runbooks import all_runbooks, provider_runbook
from catalyst_radar.ops.telemetry import TELEMETRY_PREFIX
from catalyst_radar.storage.schema import (
    audit_events,
    candidate_packets,
    candidate_states,
    data_quality_incidents,
    decision_cards,
    job_runs,
    provider_health,
    validation_runs,
)

UNHEALTHY_PROVIDER_STATUSES = {"stale", "unhealthy", "degraded", "down", "failed", "error"}
DISABLED_DEGRADED_STATES = (
    ActionState.WARNING,
    ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
    ActionState.THESIS_WEAKENING,
    ActionState.EXIT_INVALIDATE_REVIEW,
)


def load_ops_health(
    engine: Engine,
    now: datetime | None = None,
    stale_after: timedelta = timedelta(hours=36),
) -> dict[str, object]:
    resolved_now = _resolve_now(now)
    with engine.connect() as conn:
        providers = _latest_provider_rows(conn, available_at=resolved_now)
        jobs = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(job_runs)
                .where(job_runs.c.started_at <= resolved_now)
                .order_by(job_runs.c.started_at.desc(), job_runs.c.id.desc())
                .limit(25)
            )
        ]
        latest_candidate_as_of = _as_utc_datetime_or_none(
            conn.scalar(
                select(func.max(candidate_states.c.as_of)).where(
                    candidate_states.c.created_at <= resolved_now
                )
            )
        )
        database = {
            "status": "ok",
            "candidate_state_count": conn.scalar(
                select(func.count())
                .select_from(candidate_states)
                .where(candidate_states.c.created_at <= resolved_now)
            ),
            "candidate_packet_count": conn.scalar(
                select(func.count())
                .select_from(candidate_packets)
                .where(candidate_packets.c.available_at <= resolved_now)
            ),
            "decision_card_count": conn.scalar(
                select(func.count())
                .select_from(decision_cards)
                .where(decision_cards.c.available_at <= resolved_now)
            ),
            "validation_run_count": conn.scalar(
                select(func.count())
                .select_from(validation_runs)
                .where(validation_runs.c.created_at <= resolved_now)
            ),
            "latest_candidate_as_of": latest_candidate_as_of,
        }
        incidents = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(data_quality_incidents)
                .where(data_quality_incidents.c.detected_at <= resolved_now)
                .order_by(
                    data_quality_incidents.c.detected_at.desc(),
                    data_quality_incidents.c.id.desc(),
                )
                .limit(25)
            )
        ]
        telemetry_events = _latest_telemetry_rows(conn, available_at=resolved_now)

    stale_providers = [
        str(row["provider"])
        for row in providers
        if str(row.get("status") or "").lower() in UNHEALTHY_PROVIDER_STATUSES
    ]
    core_data_stale = _core_data_stale(
        latest_candidate_as_of,
        now=resolved_now,
        stale_after=stale_after,
    )
    degraded_enabled = core_data_stale or bool(stale_providers)

    payload = {
        "providers": providers,
        "jobs": jobs,
        "database": database,
        "stale_data": {
            "detected": degraded_enabled,
            "providers": stale_providers,
            "core_data": core_data_stale,
            "latest_candidate_as_of": latest_candidate_as_of,
            "stale_after_seconds": int(stale_after.total_seconds()),
        },
        "provider_banners": _provider_banners(providers),
        "degraded_mode": {
            "enabled": degraded_enabled,
            "max_action_state": ActionState.ADD_TO_WATCHLIST.value,
            "disabled_states": [state.value for state in DISABLED_DEGRADED_STATES],
            "reasons": _degraded_reasons(
                core_data_stale=core_data_stale,
                stale_providers=stale_providers,
            ),
        },
        "metrics": load_ops_metrics(engine, now=resolved_now),
        "score_drift": detect_score_drift(engine, now=resolved_now),
        "incidents": incidents,
        "telemetry": _telemetry_payload(telemetry_events),
        "runbooks": all_runbooks(),
    }
    serialized = _json_safe(payload)
    return serialized if isinstance(serialized, dict) else {}


def _latest_provider_rows(conn: Any, *, available_at: datetime) -> list[dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for row in conn.execute(
        select(provider_health)
        .where(provider_health.c.checked_at <= available_at)
        .order_by(
            provider_health.c.provider,
            provider_health.c.checked_at.desc(),
            provider_health.c.id.desc(),
        )
    ):
        values = _row_dict(row._mapping)
        rows.setdefault(str(values["provider"]), values)
    return [rows[key] for key in sorted(rows)]


def _latest_telemetry_rows(conn: Any, *, available_at: datetime) -> list[dict[str, object]]:
    return [
        _row_dict(row._mapping)
        for row in conn.execute(
            select(audit_events)
            .where(
                audit_events.c.event_type.like(f"{TELEMETRY_PREFIX}%"),
                audit_events.c.occurred_at <= available_at,
            )
            .order_by(
                audit_events.c.occurred_at.desc(),
                audit_events.c.created_at.desc(),
                audit_events.c.id.desc(),
            )
            .limit(25)
        )
    ]


def _telemetry_payload(events: list[dict[str, object]]) -> dict[str, object]:
    event_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for event in events:
        event_type = str(event.get("event_type") or "unknown")
        status = str(event.get("status") or "unknown")
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "event_count": len(events),
        "event_counts": dict(sorted(event_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "latest_event_at": events[0].get("occurred_at") if events else None,
        "events": events,
    }


def _resolve_now(now: datetime | None) -> datetime:
    if now is not None:
        return _as_utc_datetime(now)
    return datetime.now(UTC)


def _provider_banners(providers: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "provider": row.get("provider"),
            "status": row.get("status"),
            "reason": row.get("reason"),
            "runbook": provider_runbook(str(row.get("provider") or "")),
        }
        for row in providers
        if str(row.get("status") or "").lower() in UNHEALTHY_PROVIDER_STATUSES
    ]


def _core_data_stale(
    latest_candidate_as_of: datetime | None,
    *,
    now: datetime,
    stale_after: timedelta,
) -> bool:
    if latest_candidate_as_of is None:
        return False
    return now - latest_candidate_as_of > stale_after


def _degraded_reasons(
    *,
    core_data_stale: bool,
    stale_providers: list[str],
) -> list[str]:
    reasons = []
    if core_data_stale:
        reasons.append("stale_core_data")
    reasons.extend(f"provider:{provider}" for provider in stale_providers)
    return reasons


def _row_dict(row: Mapping[str, object] | None) -> dict[str, object]:
    if row is None:
        return {}
    return {str(key): _json_safe(value) for key, value in row.items()}


def _json_safe(value: object) -> object:
    if isinstance(value, datetime):
        return _as_utc_datetime(value).isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_safe(item) for item in value]
    return value


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _as_utc_datetime_or_none(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return _as_utc_datetime(value)


__all__ = ["DISABLED_DEGRADED_STATES", "UNHEALTHY_PROVIDER_STATUSES", "load_ops_health"]
