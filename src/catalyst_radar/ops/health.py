from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from sqlalchemy import Engine, func, select

from catalyst_radar.core.models import ActionState
from catalyst_radar.ops.metrics import detect_score_drift, load_ops_metrics
from catalyst_radar.ops.runbooks import all_runbooks, provider_runbook
from catalyst_radar.storage.schema import (
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
    resolved_now = _as_utc_datetime(now or datetime.now(UTC))
    with engine.connect() as conn:
        providers = _latest_provider_rows(conn)
        jobs = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(job_runs)
                .order_by(job_runs.c.started_at.desc(), job_runs.c.id.desc())
                .limit(25)
            )
        ]
        latest_candidate_as_of = _as_utc_datetime_or_none(
            conn.scalar(select(func.max(candidate_states.c.as_of)))
        )
        database = {
            "status": "ok",
            "candidate_state_count": conn.scalar(
                select(func.count()).select_from(candidate_states)
            ),
            "candidate_packet_count": conn.scalar(
                select(func.count()).select_from(candidate_packets)
            ),
            "decision_card_count": conn.scalar(
                select(func.count()).select_from(decision_cards)
            ),
            "validation_run_count": conn.scalar(
                select(func.count()).select_from(validation_runs)
            ),
            "latest_candidate_as_of": latest_candidate_as_of,
        }
        incidents = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(data_quality_incidents)
                .order_by(
                    data_quality_incidents.c.detected_at.desc(),
                    data_quality_incidents.c.id.desc(),
                )
                .limit(25)
            )
        ]

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

    return {
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
        "runbooks": all_runbooks(),
    }


def _latest_provider_rows(conn: Any) -> list[dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for row in conn.execute(
        select(provider_health).order_by(
            provider_health.c.provider,
            provider_health.c.checked_at.desc(),
            provider_health.c.id.desc(),
        )
    ):
        values = _row_dict(row._mapping)
        rows.setdefault(str(values["provider"]), values)
    return [rows[key] for key in sorted(rows)]


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
        return _as_utc_datetime(value)
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
