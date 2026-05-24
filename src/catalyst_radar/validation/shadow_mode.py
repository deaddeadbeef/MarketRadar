from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from math import isfinite

from sqlalchemy.engine import Engine

from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import ShadowModeRun, shadow_mode_run_id


def shadow_mode_run_payload(
    engine: Engine,
    config: AppConfig,
    *,
    run_date: date | None = None,
    as_of: date | None = None,
    available_at: datetime | None = None,
    execute: bool = False,
) -> dict[str, object]:
    cutoff = _to_utc(available_at or datetime.now(UTC), "available_at")
    snapshot = _local_shadow_snapshot(engine, config, available_at=cutoff)
    run = build_shadow_mode_run(
        snapshot,
        run_date=run_date or cutoff.date(),
        as_of=as_of,
        available_at=cutoff,
        db_writes_made=1 if execute else 0,
    )
    if execute:
        ValidationRepository(engine).upsert_shadow_mode_run(run)
    mode = "execute" if execute else "preview"
    readiness = _mapping(snapshot.get("shadow_readiness"))
    next_action = _shadow_mode_next_action(run)
    next_command = _shadow_mode_next_command(run, next_action)
    preview_command = _shadow_mode_run_command(
        run_date=run.run_date,
        as_of=run.as_of,
        available_at=cutoff,
        execute=False,
    )
    execute_command = _shadow_mode_run_command(
        run_date=run.run_date,
        as_of=run.as_of,
        available_at=cutoff,
        execute=True,
    )
    return {
        "schema_version": "shadow-mode-run-v1",
        "mode": mode,
        "status": run.status,
        "run": shadow_mode_run_to_payload(run),
        "provider_calls_planned": run.provider_calls_planned,
        "provider_calls_made": run.provider_calls_made,
        "db_writes_planned": run.db_writes_planned,
        "external_calls_required": 0,
        "external_calls_made": 0,
        "db_writes_required": 1,
        "db_writes_made": run.db_writes_made,
        "preview_command": preview_command,
        "execute_command": execute_command if not execute else None,
        "first_blocker": _shadow_mode_status_first_blocker(readiness),
        "first_gap_count": _shadow_mode_status_first_gap_count(readiness),
        "canonical_next_action": next_action,
        "canonical_next_command": next_command,
        "approval_required_unblock": _shadow_mode_approval_required_unblock(readiness),
        "next_action": next_action,
        "useful_definition": (
            "A useful shadow run records whether the current local scan is valid, "
            "partial, blocked, or setup-required without placing orders, delivering "
            "real alerts, calling providers, or treating output as investment advice."
        ),
    }


def shadow_mode_status_payload(
    engine: Engine,
    config: AppConfig,
    *,
    available_at: datetime | None = None,
    shadow_readiness: Mapping[str, object] | None = None,
) -> dict[str, object]:
    cutoff = _to_utc(available_at, "available_at") if available_at is not None else None
    latest = ValidationRepository(engine).latest_shadow_mode_run(available_at=cutoff)
    readiness = (
        dict(shadow_readiness)
        if isinstance(shadow_readiness, Mapping)
        else _local_shadow_readiness(engine, config)
    )
    latest_payload = shadow_mode_run_to_payload(latest) if latest is not None else None
    next_action = _shadow_mode_status_next_action(latest, readiness)
    next_command = _shadow_mode_status_next_command(latest, readiness, next_action)
    first_blocker = _shadow_mode_status_first_blocker(readiness)
    return {
        "schema_version": "shadow-mode-status-v1",
        "status": _shadow_mode_status_label(latest, readiness),
        "latest": latest_payload,
        "shadow_readiness_status": readiness.get("status") or "unknown",
        "ready_for_shadow_run": bool(readiness.get("ready")),
        "first_blocker": first_blocker,
        "first_gap_count": _shadow_mode_status_first_gap_count(readiness),
        "canonical_next_action": next_action,
        "canonical_next_command": next_command,
        "approval_required_unblock": _shadow_mode_approval_required_unblock(readiness),
        "next_action": next_action,
        "external_calls_required": 0,
        "db_writes_required": 0,
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def shadow_mode_latest_payload(
    engine: Engine,
    config: AppConfig | None = None,
    *,
    available_at: datetime | None = None,
) -> dict[str, object]:
    cutoff = _to_utc(available_at, "available_at") if available_at is not None else None
    latest = ValidationRepository(engine).latest_shadow_mode_run(available_at=cutoff)
    readiness = _local_shadow_readiness(engine, config) if config is not None else {}
    next_action = _shadow_mode_status_next_action(latest, readiness)
    next_command = _shadow_mode_status_next_command(latest, readiness, next_action)
    return {
        "schema_version": "shadow-mode-latest-v1",
        "run": shadow_mode_run_to_payload(latest) if latest is not None else None,
        "status": latest.status if latest is not None else "not_found",
        "shadow_readiness_status": readiness.get("status") or None,
        "ready_for_shadow_run": bool(readiness.get("ready")),
        "first_blocker": _shadow_mode_status_first_blocker(readiness),
        "first_gap_count": _shadow_mode_status_first_gap_count(readiness),
        "canonical_next_action": next_action,
        "canonical_next_command": next_command,
        "approval_required_unblock": _shadow_mode_approval_required_unblock(readiness),
        "next_action": next_action,
        "external_calls_required": 0,
        "db_writes_required": 0,
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def shadow_mode_list_payload(
    engine: Engine,
    *,
    available_at: datetime | None = None,
    limit: int = 30,
) -> dict[str, object]:
    cutoff = _to_utc(available_at, "available_at") if available_at is not None else None
    runs = ValidationRepository(engine).list_shadow_mode_runs(
        available_at=cutoff,
        limit=limit,
    )
    status_counts = Counter(run.status for run in runs)
    return {
        "schema_version": "shadow-mode-list-v1",
        "count": len(runs),
        "status_counts": dict(sorted(status_counts.items())),
        "runs": [shadow_mode_run_to_payload(run) for run in runs],
        "external_calls_required": 0,
        "db_writes_required": 0,
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def build_shadow_mode_run(
    snapshot: Mapping[str, object],
    *,
    run_date: date,
    as_of: date | None,
    available_at: datetime,
    db_writes_made: int,
) -> ShadowModeRun:
    shadow = _mapping(snapshot.get("shadow_readiness"))
    discovery = _mapping(snapshot.get("discovery_snapshot"))
    latest_run = _mapping(snapshot.get("latest_run"))
    discovered_yield = _mapping(discovery.get("yield"))
    freshness = _mapping(discovery.get("freshness"))
    snapshots = _mapping(shadow.get("snapshots"))
    scan_scope_payload = _mapping(snapshots.get("scan_scope"))
    latest_market_bar_check = _first_check(shadow, "latest_market_bars")
    latest_market_bar_metric = _mapping(latest_market_bar_check.get("metric"))
    call_boundary = _mapping(shadow.get("call_boundary"))
    planned_external_calls = _int(
        call_boundary.get("planned_run_external_call_count_max")
    )
    raw_scan_scope = str(scan_scope_payload.get("mode") or "unknown")
    candidate_rows = [
        row for row in _sequence(snapshot.get("candidate_rows")) if isinstance(row, Mapping)
    ]
    state_counts = Counter(
        str(row.get("state") or "")
        for row in candidate_rows
    )
    universe_size = _int(
        freshness.get("active_security_count"),
        _mapping(_mapping(snapshot.get("ops_health")).get("database")).get(
            "active_security_count"
        ),
    )
    requested = _int(discovered_yield.get("requested_securities"), universe_size)
    scanned = _int(discovered_yield.get("scanned_securities"))
    missing_bars = _first_int(
        freshness.get("missing_as_of_daily_bar_count"),
        latest_market_bar_metric.get("missing_as_of_daily_bar_count"),
        latest_market_bar_metric.get("missing_latest_daily_bar_count"),
    )
    candidate_count = _int(discovered_yield.get("candidate_states"), len(candidate_rows))
    blocker_count = len(
        [row for row in _sequence(shadow.get("blockers")) if isinstance(row, Mapping)]
    )
    scan_scope = _effective_shadow_scan_scope(
        shadow,
        raw_scan_scope=raw_scan_scope,
        candidate_count=candidate_count,
        scanned_securities=scanned,
        missing_market_bar_count=missing_bars,
        blocker_count=blocker_count,
    )
    resolved_as_of = as_of or _parse_date(latest_run.get("as_of")) or _parse_date(
        freshness.get("latest_daily_bar_date")
    )
    validation_status = _validation_status(shadow)
    status = classify_shadow_run_status(
        shadow_readiness_status=str(shadow.get("status") or "unknown"),
        scan_scope=scan_scope,
        candidate_count=candidate_count,
        scanned_securities=scanned,
        blocker_count=blocker_count,
    )
    now = datetime.now(UTC)
    run = ShadowModeRun(
        id=shadow_mode_run_id(run_date=run_date, available_at=available_at),
        run_date=run_date,
        as_of=resolved_as_of,
        available_at=available_at,
        status=status,
        validation_status=validation_status,
        scan_scope=scan_scope,
        universe_size=universe_size,
        requested_securities=requested,
        scanned_securities=scanned,
        missing_market_bar_count=missing_bars,
        candidate_count=candidate_count,
        warning_count=state_counts.get(ActionState.WARNING.value, 0),
        manual_review_count=state_counts.get(
            ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
            0,
        ),
        blocker_count=blocker_count,
        provider_calls_planned=planned_external_calls,
        provider_calls_made=0,
        db_writes_planned=1,
        db_writes_made=db_writes_made,
        estimated_cost_usd=0.0,
        actual_cost_usd=0.0,
        payload={
            "schema_version": "shadow-mode-run-payload-v1",
            "latest_radar_run": _json_value(latest_run),
            "shadow_readiness": _json_value(shadow),
            "discovery_snapshot": _json_value(discovery),
            "call_plan_external_call_count_max": _int(
                call_boundary.get("planned_run_external_call_count_max")
            ),
            "no_provider_calls_by_shadow_mode": True,
            "no_broker_orders": True,
            "no_real_alert_delivery": True,
            "decision_support_only": True,
        },
        created_at=now,
        updated_at=now,
    )
    return run


def classify_shadow_run_status(
    *,
    shadow_readiness_status: str,
    scan_scope: str,
    candidate_count: int,
    scanned_securities: int,
    blocker_count: int,
) -> str:
    readiness = shadow_readiness_status.strip().lower()
    scope = scan_scope.strip().lower()
    if readiness == "setup_required":
        return "setup_required"
    if readiness == "ready":
        if scope == "full_scan":
            return "valid_full_scan"
        return "valid_selected_universe_scan"
    if candidate_count > 0 or scanned_securities > 0:
        return "partial_scan"
    if blocker_count > 0:
        return "blocked_scan"
    return "blocked_scan"


def _effective_shadow_scan_scope(
    shadow_readiness: Mapping[str, object],
    *,
    raw_scan_scope: str,
    candidate_count: int,
    scanned_securities: int,
    missing_market_bar_count: int,
    blocker_count: int,
) -> str:
    readiness = str(shadow_readiness.get("status") or "").strip().lower()
    scope = raw_scan_scope.strip().lower() or "unknown"
    if readiness == "ready":
        return scope
    if (
        readiness == "setup_required"
        and missing_market_bar_count > 0
        and _shadow_mode_status_first_blocker(shadow_readiness)
        in {"market_bars", "latest_market_bars", "active_universe"}
    ):
        return "blocked_full_market_gate"
    if candidate_count > 0 or scanned_securities > 0:
        return scope
    if blocker_count > 0:
        return "blocked"
    return "setup_required" if readiness == "setup_required" else "unknown"


def shadow_mode_run_to_payload(run: ShadowModeRun | None) -> dict[str, object] | None:
    if run is None:
        return None
    return {
        "id": run.id,
        "run_date": run.run_date.isoformat(),
        "as_of": run.as_of.isoformat() if run.as_of is not None else None,
        "available_at": run.available_at.isoformat(),
        "status": run.status,
        "validation_status": run.validation_status,
        "scan_scope": run.scan_scope,
        "universe_size": run.universe_size,
        "requested_securities": run.requested_securities,
        "scanned_securities": run.scanned_securities,
        "missing_market_bar_count": run.missing_market_bar_count,
        "candidate_count": run.candidate_count,
        "warning_count": run.warning_count,
        "manual_review_count": run.manual_review_count,
        "eligible_for_manual_review_count": run.manual_review_count,
        "blocker_count": run.blocker_count,
        "provider_calls_planned": run.provider_calls_planned,
        "provider_calls_made": run.provider_calls_made,
        "db_writes_planned": run.db_writes_planned,
        "db_writes_made": run.db_writes_made,
        "estimated_cost_usd": run.estimated_cost_usd,
        "actual_cost_usd": run.actual_cost_usd,
        "payload": _json_value(thaw_json_value(run.payload)),
        "created_at": run.created_at.isoformat(),
        "updated_at": run.updated_at.isoformat(),
    }


def _local_shadow_snapshot(
    engine: Engine,
    config: AppConfig,
    *,
    available_at: datetime,
) -> dict[str, object]:
    from catalyst_radar.dashboard import data as dashboard_data

    ops_health = dashboard_data.load_ops_health(engine, now=available_at)
    latest_run = dashboard_data.load_radar_run_summary(engine)
    candidate_rows = (
        dashboard_data.load_radar_run_candidate_rows(
            engine,
            latest_run,
            include_post_run_artifacts=True,
        )
        if latest_run
        else dashboard_data.load_candidate_rows(engine, available_at=available_at)
    )
    discovery = dashboard_data.radar_discovery_snapshot_payload(
        engine,
        config,
        radar_run_summary=latest_run,
        ops_health=ops_health,
        candidate_rows=candidate_rows,
        available_at=available_at,
    )
    readiness = dashboard_data.radar_readiness_payload(
        engine,
        config,
        radar_run_summary=latest_run,
        candidate_rows=candidate_rows,
        ops_health=ops_health,
        discovery_snapshot=discovery,
    )
    shadow = dashboard_data.shadow_readiness_payload(
        engine,
        config,
        radar_readiness=readiness,
        ops_health=ops_health,
    )
    return {
        "ops_health": ops_health,
        "latest_run": latest_run,
        "candidate_rows": candidate_rows,
        "discovery_snapshot": discovery,
        "radar_readiness": readiness,
        "shadow_readiness": shadow,
    }


def _local_shadow_readiness(engine: Engine, config: AppConfig) -> dict[str, object]:
    from catalyst_radar.dashboard import data as dashboard_data

    return dashboard_data.shadow_readiness_payload(engine, config)


def _shadow_mode_next_action(run: ShadowModeRun) -> str:
    if run.status == "valid_full_scan":
        return "Record value-ledger entries for surfaced Warning or manual-review candidates."
    if run.status == "valid_selected_universe_scan":
        return "Use this as selected-universe shadow evidence, not a broad-market answer."
    canonical_action = _shadow_readiness_canonical_next_action(run)
    if canonical_action:
        return canonical_action
    if run.status == "partial_scan":
        return "Review blockers and treat this run as partial shadow evidence only."
    if run.status == "setup_required":
        return "Clear setup blockers before relying on daily shadow-mode evidence."
    return "Open shadow readiness and clear the first blocker before rerunning."


def _shadow_mode_next_command(run: ShadowModeRun, next_action: str) -> str | None:
    if run.status in {"valid_full_scan", "valid_selected_universe_scan"}:
        return None
    return _shadow_readiness_canonical_next_command(run) or _canonical_command(
        next_action
    )


def _shadow_mode_status_next_action(
    latest: ShadowModeRun | None,
    readiness: Mapping[str, object],
) -> str:
    readiness_status = str(readiness.get("status") or "").strip().lower()
    if readiness_status != "ready":
        action = readiness.get("canonical_next_action")
        if isinstance(action, str) and action.strip():
            return action.strip()
    if latest is not None:
        return _shadow_mode_next_action(latest)
    return "Run `catalyst-radar shadow-mode run --preview` to inspect the daily audit row."


def _shadow_mode_status_next_command(
    latest: ShadowModeRun | None,
    readiness: Mapping[str, object],
    next_action: str,
) -> str | None:
    readiness_status = str(readiness.get("status") or "").strip().lower()
    if readiness_status != "ready":
        command = readiness.get("canonical_next_command")
        if isinstance(command, str) and command.strip():
            return command.strip()
        return _canonical_command(next_action)
    if latest is not None:
        return _shadow_mode_next_command(latest, next_action)
    return _canonical_command(next_action)


def _shadow_mode_approval_required_unblock(
    readiness: Mapping[str, object],
) -> dict[str, object] | None:
    approval = readiness.get("approval_required_unblock")
    return dict(approval) if isinstance(approval, Mapping) else None


def _shadow_mode_status_label(
    latest: ShadowModeRun | None,
    readiness: Mapping[str, object],
) -> str:
    if latest is None:
        return "no_shadow_run"
    if bool(readiness.get("ready")):
        return latest.status
    status = readiness.get("status")
    if isinstance(status, str) and status.strip():
        return status.strip()
    return latest.status


def _shadow_mode_status_first_blocker(readiness: Mapping[str, object]) -> str | None:
    if bool(readiness.get("ready")):
        return None
    blocker = readiness.get("first_blocker")
    if isinstance(blocker, str) and blocker.strip():
        return blocker.strip()
    blockers = readiness.get("blockers")
    if isinstance(blockers, Sequence) and not isinstance(blockers, str):
        for row in blockers:
            if not isinstance(row, Mapping):
                continue
            code = row.get("code")
            if isinstance(code, str) and code.strip():
                return code.strip()
    return None


def _shadow_mode_status_first_gap_count(readiness: Mapping[str, object]) -> int:
    if bool(readiness.get("ready")):
        return 0
    return _first_int(readiness.get("first_gap_count"))


def _canonical_command(action: object) -> str | None:
    if not isinstance(action, str):
        return None
    text = action.strip()
    return text if text.startswith("catalyst-radar ") else None


def _shadow_mode_run_command(
    *,
    run_date: date | None,
    as_of: date | None,
    available_at: datetime,
    execute: bool,
) -> str:
    parts = [
        "catalyst-radar",
        "shadow-mode",
        "run",
    ]
    if run_date is not None:
        parts.extend(["--run-date", run_date.isoformat()])
    if as_of is not None:
        parts.extend(["--as-of", as_of.isoformat()])
    parts.extend(["--available-at", available_at.isoformat()])
    parts.append("--execute" if execute else "--preview")
    parts.append("--json")
    return " ".join(parts)


def _shadow_readiness_canonical_next_action(run: ShadowModeRun) -> str | None:
    payload = _mapping(thaw_json_value(run.payload))
    readiness = _mapping(payload.get("shadow_readiness"))
    action = readiness.get("canonical_next_action")
    if isinstance(action, str) and action.strip():
        return action.strip()
    return None


def _shadow_readiness_canonical_next_command(run: ShadowModeRun) -> str | None:
    payload = _mapping(thaw_json_value(run.payload))
    readiness = _mapping(payload.get("shadow_readiness"))
    command = readiness.get("canonical_next_command")
    if isinstance(command, str) and command.strip():
        return command.strip()
    return None


def _validation_status(shadow_readiness: Mapping[str, object]) -> str:
    for row in _sequence(shadow_readiness.get("checks")):
        if not isinstance(row, Mapping):
            continue
        if row.get("code") == "validation_ready":
            return "ready" if row.get("status") == "ready" else "blocked"
    return "unknown"


def _first_check(payload: Mapping[str, object], code: str) -> dict[str, object]:
    for row in _sequence(payload.get("checks")):
        if isinstance(row, Mapping) and row.get("code") == code:
            return dict(row)
    return {}


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sequence(value: object) -> Sequence[object]:
    return value if isinstance(value, Sequence) and not isinstance(value, str) else ()


def _int(value: object, fallback: object = 0) -> int:
    for candidate in (value, fallback):
        try:
            number = float(candidate)
        except (TypeError, ValueError):
            continue
        if isfinite(number):
            return max(0, int(number))
    return 0


def _first_int(*values: object) -> int:
    for value in values:
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if isfinite(number):
            return max(0, int(number))
    return 0


def _parse_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def _to_utc(value: datetime | None, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _json_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [_json_value(item) for item in value]
    return value
