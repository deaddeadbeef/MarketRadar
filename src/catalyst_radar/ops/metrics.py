from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from math import isfinite
from typing import Any

from sqlalchemy import Engine, String, cast, func, or_, select

from catalyst_radar.storage.schema import (
    budget_ledger,
    candidate_states,
    data_quality_incidents,
    job_runs,
    useful_alert_labels,
    validation_runs,
)
from catalyst_radar.validation.reports import USEFUL_ALERT_LABELS

_SCHEMA_FAILURE_MARKERS = ("schema", "validation")
_UNSUPPORTED_CLAIM_SKIP_REASONS = (
    "schema_validation_failed",
    "source_faithfulness_failed",
    "unsupported_claim",
)
_UNSUPPORTED_CLAIM_MARKERS = (
    "source_faithfulness",
    "source faithfulness",
    "unsupported",
    "source_id",
    "computed_feature_id",
    "allowed_reference_ids",
    "source reference",
)


def load_ops_metrics(engine: Engine, now: datetime | None = None) -> dict[str, object]:
    resolved_now = _as_utc(now or datetime.now(UTC))
    with engine.connect() as conn:
        cost_summary = _cost_summary(conn, available_at=resolved_now)
        useful_alert_count = _useful_alert_count(conn, available_at=resolved_now)
        stale_incident_count = _incident_count(
            conn,
            available_at=resolved_now,
            markers=("stale", "freshness", "late", "delayed"),
        )
        schema_failure_count = _incident_count(
            conn,
            available_at=resolved_now,
            markers=_SCHEMA_FAILURE_MARKERS,
        )
        unsupported_claim_count = _unsupported_claim_count(
            conn,
            available_at=resolved_now,
        )
        total_budget_rows = _visible_budget_row_count(conn, available_at=resolved_now)
        latest_validation = _latest_validation_run(conn, available_at=resolved_now)
        stage_counts, candidate_state_counts, job_status_counts = _stage_counts(
            conn,
            available_at=resolved_now,
        )

    total_actual_cost = _finite_float(cost_summary.get("total_actual_cost_usd"))
    cost_per_useful_alert = (
        0.0
        if total_actual_cost <= 0
        else total_actual_cost / useful_alert_count
        if useful_alert_count > 0
        else None
    )
    cost_summary["cost_per_useful_alert"] = cost_per_useful_alert
    false_positive_rate = _false_positive_rate(
        latest_validation._mapping["metrics"] if latest_validation is not None else {}
    )

    return {
        "stage_counts": stage_counts,
        "candidate_state_counts": candidate_state_counts,
        "job_status_counts": job_status_counts,
        "cost": cost_summary,
        "useful_alert_count": useful_alert_count,
        "stale_incident_count": stale_incident_count,
        "schema_failure_count": schema_failure_count,
        "unsupported_claim_count": unsupported_claim_count,
        "unsupported_claim_rate": _safe_rate(unsupported_claim_count, total_budget_rows),
        "false_positive_rate": false_positive_rate,
        "latest_validation_run_id": (
            str(latest_validation._mapping["id"]) if latest_validation is not None else None
        ),
    }


def detect_score_drift(
    engine: Engine,
    *,
    now: datetime | None = None,
    mean_delta_threshold: float | None = None,
    count_delta_ratio: float | None = None,
    mean_shift_threshold: float | None = None,
    count_shift_ratio_threshold: float | None = None,
    min_count: int = 1,
) -> dict[str, object]:
    resolved_now = _as_utc(now or datetime.now(UTC))
    resolved_mean_threshold = (
        mean_delta_threshold
        if mean_delta_threshold is not None
        else mean_shift_threshold
        if mean_shift_threshold is not None
        else 15.0
    )
    resolved_count_threshold = (
        count_delta_ratio
        if count_delta_ratio is not None
        else count_shift_ratio_threshold
        if count_shift_ratio_threshold is not None
        else 0.5
    )
    with engine.connect() as conn:
        as_of_rows = [
            row[0]
            for row in conn.execute(
                select(candidate_states.c.as_of)
                .where(candidate_states.c.created_at <= resolved_now)
                .group_by(candidate_states.c.as_of)
                .order_by(candidate_states.c.as_of.desc())
                .limit(2)
            )
        ]
        if len(as_of_rows) < 2:
            latest = (
                _distribution(conn, as_of_rows[0], available_at=resolved_now)
                if as_of_rows
                else _empty_distribution()
            )
            return {
                "detected": False,
                "reason": "insufficient_history",
                "latest": latest,
                "previous": None,
                "mean_shift": 0.0,
                "count_shift_ratio": 0.0,
                "thresholds": _drift_thresholds(
                    resolved_mean_threshold,
                    resolved_count_threshold,
                    min_count,
                ),
            }
        latest = _distribution(conn, as_of_rows[0], available_at=resolved_now)
        previous = _distribution(conn, as_of_rows[1], available_at=resolved_now)

    mean_shift = latest["mean_score"] - previous["mean_score"]
    count_shift_ratio = (
        abs(int(latest["count"]) - int(previous["count"])) / max(int(previous["count"]), 1)
    )
    reasons = []
    if min(int(latest["count"]), int(previous["count"])) >= min_count:
        if abs(mean_shift) >= resolved_mean_threshold:
            reasons.append("mean_shift")
        if count_shift_ratio >= resolved_count_threshold:
            reasons.append("count_shift")

    return {
        "detected": bool(reasons),
        "reason": ",".join(reasons) if reasons else None,
        "latest": latest,
        "previous": previous,
        "mean_shift": round(mean_shift, 6),
        "count_shift_ratio": round(count_shift_ratio, 6),
        "thresholds": _drift_thresholds(
            resolved_mean_threshold,
            resolved_count_threshold,
            min_count,
        ),
    }


def _cost_summary(conn: Any, *, available_at: datetime) -> dict[str, object]:
    total = conn.execute(
        select(
            func.coalesce(func.sum(budget_ledger.c.actual_cost), 0.0),
            func.coalesce(func.sum(budget_ledger.c.estimated_cost), 0.0),
            func.count(),
            func.min(budget_ledger.c.currency),
        ).where(budget_ledger.c.available_at <= available_at)
    ).one()
    status_counts = {
        str(row[0]): int(row[1])
        for row in conn.execute(
            select(budget_ledger.c.status, func.count())
            .where(budget_ledger.c.available_at <= available_at)
            .group_by(budget_ledger.c.status)
        )
    }
    return {
        "currency": total[3] or "USD",
        "total_actual_cost_usd": _finite_float(total[0]),
        "total_estimated_cost_usd": _finite_float(total[1]),
        "attempt_count": int(total[2]),
        "status_counts": dict(sorted(status_counts.items())),
    }


def _useful_alert_count(conn: Any, *, available_at: datetime) -> int:
    return int(
        conn.scalar(
            select(func.count())
            .select_from(useful_alert_labels)
            .where(
                useful_alert_labels.c.created_at <= available_at,
                useful_alert_labels.c.label.in_(tuple(sorted(USEFUL_ALERT_LABELS))),
            )
        )
        or 0
    )


def _incident_count(
    conn: Any,
    *,
    available_at: datetime,
    markers: tuple[str, ...],
) -> int:
    return int(
        conn.scalar(
            select(func.count())
            .select_from(data_quality_incidents)
            .where(
                data_quality_incidents.c.detected_at <= available_at,
                _marker_filter(
                    markers,
                    data_quality_incidents.c.kind,
                    data_quality_incidents.c.reason,
                    data_quality_incidents.c.fail_closed_action,
                    cast(data_quality_incidents.c.payload, String),
                ),
            )
        )
        or 0
    )


def _unsupported_claim_count(conn: Any, *, available_at: datetime) -> int:
    return int(
        conn.scalar(
            select(func.count())
            .select_from(budget_ledger)
            .where(
                budget_ledger.c.available_at <= available_at,
                budget_ledger.c.status == "schema_rejected",
                or_(
                    budget_ledger.c.skip_reason.in_(_UNSUPPORTED_CLAIM_SKIP_REASONS),
                    _marker_filter(
                        _UNSUPPORTED_CLAIM_MARKERS,
                        cast(budget_ledger.c.payload, String),
                    ),
                ),
            )
        )
        or 0
    )


def _visible_budget_row_count(conn: Any, *, available_at: datetime) -> int:
    return int(
        conn.scalar(
            select(func.count())
            .select_from(budget_ledger)
            .where(budget_ledger.c.available_at <= available_at)
        )
        or 0
    )


def _latest_validation_run(conn: Any, *, available_at: datetime) -> Any:
    return conn.execute(
        select(validation_runs)
        .where(
            validation_runs.c.status == "success",
            validation_runs.c.finished_at.is_not(None),
            validation_runs.c.finished_at <= available_at,
        )
        .order_by(
            validation_runs.c.finished_at.desc(),
            validation_runs.c.started_at.desc(),
            validation_runs.c.created_at.desc(),
            validation_runs.c.id.desc(),
        )
        .limit(1)
    ).first()


def _stage_counts(
    conn: Any,
    *,
    available_at: datetime,
) -> tuple[dict[str, dict[str, int]], dict[str, int], dict[str, int]]:
    jobs_by_type: dict[str, dict[str, int]] = defaultdict(dict)
    job_statuses: Counter[str] = Counter()
    candidate_state_counts: Counter[str] = Counter()
    for row in conn.execute(
        select(job_runs.c.job_type, job_runs.c.status, func.count())
        .where(job_runs.c.started_at <= available_at)
        .group_by(job_runs.c.job_type, job_runs.c.status)
    ):
        job_type = str(row[0])
        status = str(row[1])
        count = int(row[2])
        jobs_by_type[job_type][status] = count
        job_statuses[status] += count
    latest_as_of = conn.scalar(
        select(func.max(candidate_states.c.as_of)).where(
            candidate_states.c.created_at <= available_at
        )
    )
    if latest_as_of is not None:
        for row in conn.execute(
            select(candidate_states.c.state, func.count())
            .where(
                candidate_states.c.as_of == latest_as_of,
                candidate_states.c.created_at <= available_at,
            )
            .group_by(candidate_states.c.state)
        ):
            candidate_state_counts[str(row[0])] = int(row[1])
    return (
        {key: dict(sorted(value.items())) for key, value in sorted(jobs_by_type.items())},
        dict(sorted(candidate_state_counts.items())),
        dict(sorted(job_statuses.items())),
    )


def _distribution(conn: Any, as_of: datetime, *, available_at: datetime) -> dict[str, object]:
    rows = [
        float(row[0])
        for row in conn.execute(
            select(candidate_states.c.final_score).where(
                candidate_states.c.as_of == as_of,
                candidate_states.c.created_at <= available_at,
            )
        )
        if row[0] is not None
    ]
    return {
        "as_of": _as_utc(as_of),
        "count": len(rows),
        "mean_score": round(sum(rows) / len(rows), 6) if rows else 0.0,
        "min_score": min(rows) if rows else None,
        "max_score": max(rows) if rows else None,
    }


def _empty_distribution() -> dict[str, object]:
    return {
        "as_of": None,
        "count": 0,
        "mean_score": 0.0,
        "min_score": None,
        "max_score": None,
    }


def _drift_thresholds(
    mean_shift_threshold: float,
    count_shift_ratio_threshold: float,
    min_count: int,
) -> dict[str, object]:
    return {
        "mean_delta": mean_shift_threshold,
        "mean_shift": mean_shift_threshold,
        "count_delta_ratio": count_shift_ratio_threshold,
        "count_shift_ratio": count_shift_ratio_threshold,
        "min_count": min_count,
    }


def _marker_filter(markers: tuple[str, ...], *columns: Any) -> Any:
    return or_(
        *[
            func.lower(column).like(f"%{marker}%")
            for column in columns
            for marker in markers
        ]
    )


def _false_positive_rate(metrics: object) -> float | None:
    if not isinstance(metrics, Mapping):
        return None
    for key in ("false_positive_rate", "false_positive_alert_rate"):
        value = metrics.get(key)
        if value is not None:
            return _finite_float(value)
    false_positive_count = metrics.get("false_positive_count")
    candidate_count = metrics.get("candidate_count") or metrics.get("alert_count")
    if false_positive_count is not None and candidate_count is not None:
        return _safe_rate(_finite_float(false_positive_count), _finite_float(candidate_count))
    precision = metrics.get("precision")
    if isinstance(precision, Mapping) and precision:
        values = [_finite_float(value) for value in precision.values()]
        return max(0.0, 1.0 - (sum(values) / len(values)))
    return None


def _safe_rate(numerator: float, denominator: float) -> float:
    return 0.0 if denominator <= 0 else round(numerator / denominator, 6)


def _finite_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if isfinite(number) else 0.0


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["detect_score_drift", "load_ops_metrics"]
