from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from catalyst_radar.connectors.polygon_fixture import (
    preview_polygon_grouped_daily_fixture,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.market.manual_bars import (
    MANUAL_BAR_COMPANY_LIKE_TYPES,
    manual_market_bars_repair_plan,
)
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import daily_bars, securities


def market_bars_status_payload(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date | None = None,
    stocks_only: bool = False,
):
    """Summarize the current market-bar unblock state without provider calls."""

    try:
        resolved_expected_as_of = _resolve_expected_as_of(engine, expected_as_of)
    except ValueError as exc:
        active_count = _active_security_count(engine, stocks_only=stocks_only)
        if active_count > 0:
            return _expected_as_of_required_status_payload(
                expected_as_of_source="not_available",
                stocks_only=stocks_only,
                reason=str(exc),
                active_security_count=active_count,
            )
        return _setup_required_status_payload(
            config,
            expected_as_of=None,
            expected_as_of_source="not_available",
            stocks_only=stocks_only,
            reason=str(exc),
        )
    try:
        repair = _repair_payload(
            engine,
            config,
            expected_as_of=resolved_expected_as_of,
            stocks_only=stocks_only,
        )
    except ValueError as exc:
        return _setup_required_status_payload(
            config,
            expected_as_of=resolved_expected_as_of,
            expected_as_of_source="argument" if expected_as_of else "latest_daily_bar",
            stocks_only=stocks_only,
            reason=str(exc),
        )
    stock_scope = None
    if not stocks_only:
        stock_scope = _stock_scope_payload(
            engine,
            config,
            expected_as_of=resolved_expected_as_of,
            full_repair=repair,
        )
    missing = int(repair.get("missing_as_of_bar_count") or 0)
    configured_universe_scope = None
    if not stocks_only:
        configured_universe_scope = _configured_universe_scope_payload(
            engine,
            config,
            expected_as_of=resolved_expected_as_of,
            active_universe_missing=missing,
        )
    missing_any = missing != 0
    local_preview = _mapping(repair.get("local_template_preview"))
    fill_progress = _mapping(local_preview.get("fill_progress"))
    operator_step = _mapping(repair.get("operator_step"))
    approval_packet = _mapping(
        repair.get("provider_saved_file_capture_approval_packet")
    )
    status = "blocked" if missing_any else "ready"
    saved_file_path = repair.get("provider_saved_file_path")
    saved_file_status = repair.get("provider_saved_file_status")
    import_command = repair.get("provider_saved_file_import_command")
    saved_file_projection = _saved_file_projection(
        engine,
        config,
        expected_as_of=resolved_expected_as_of,
        saved_file_path=saved_file_path,
        stocks_only=stocks_only,
    )
    recommended_action = _recommended_unblock_action(
        expected_as_of=resolved_expected_as_of,
        missing=missing,
        repair=repair,
        operator_step=operator_step,
        approval_packet=approval_packet,
        saved_file_path=saved_file_path,
        saved_file_status=saved_file_status,
        saved_file_projection=saved_file_projection,
    )
    next_action = str(
        recommended_action.get("reason")
        or operator_step.get("action")
        or approval_packet.get("next_action")
        or repair.get("next_action")
        or ""
    ).strip()
    unblock_checklist = _market_bar_unblock_checklist(
        expected_as_of=resolved_expected_as_of,
        missing=missing,
        repair=repair,
        approval_packet=approval_packet,
        saved_file_path=saved_file_path,
        saved_file_status=saved_file_status,
        saved_file_projection=saved_file_projection,
        import_command=import_command,
        recommended_action=recommended_action,
        stocks_only=stocks_only,
    )
    after_clear = _post_market_bars_clear_payload(
        engine,
        config,
        missing=missing,
        stocks_only=stocks_only,
    )
    return {
        "schema_version": "market-bars-status-v1",
        "status": status,
        "first_blocker": "market_bars" if missing_any else None,
        "expected_as_of": resolved_expected_as_of.isoformat(),
        "expected_as_of_source": "argument" if expected_as_of else "latest_daily_bar",
        "stocks_only": bool(stocks_only),
        "coverage_scope": repair.get("coverage_scope"),
        "active_security_count": repair.get("active_security_count"),
        "existing_as_of_bar_count": repair.get("existing_as_of_bar_count"),
        "missing_as_of_bar_count": missing,
        "missing_as_of_bar_ticker_sample": repair.get(
            "missing_as_of_bar_ticker_sample",
        )
        or [],
        "missing_as_of_bar_ticker_more": repair.get(
            "missing_as_of_bar_ticker_more",
        )
        or 0,
        "missing_security_type_counts": repair.get("missing_security_type_counts")
        or {},
        "missing_universe_diagnostic": repair.get("missing_universe_diagnostic")
        or {},
        "stock_scope": stock_scope,
        "configured_universe_scope": configured_universe_scope,
        "manual": {
            "status": operator_step.get("status"),
            "action": operator_step.get("action"),
            "command": operator_step.get("command"),
            "after_manual_command": operator_step.get("after_manual_command"),
            "template_command": repair.get("manual_template_command"),
            "import_preview_command": repair.get("manual_import_preview_command"),
            "import_execute_command": repair.get("manual_import_execute_command"),
            "incremental_preview_command": repair.get(
                "manual_incremental_import_preview_command"
            ),
            "local_template_path": repair.get("local_template_path"),
            "local_template_exists": repair.get("local_template_exists"),
            "local_template_status": local_preview.get("status"),
            "fill_progress": dict(fill_progress),
            "external_calls_made": 0,
        },
        "saved_capture": {
            "status": approval_packet.get("status"),
            "approval_required": bool(approval_packet.get("approval_required")),
            "provider": approval_packet.get("provider") or repair.get("provider"),
            "provider_label": approval_packet.get("provider_label")
            or repair.get("provider_label"),
            "provider_key_configured": bool(
                approval_packet.get(
                    "provider_key_configured",
                    repair.get("provider_key_configured"),
                )
            ),
            "saved_file_path": approval_packet.get("saved_file_path")
            or saved_file_path,
            "saved_file_status": approval_packet.get("saved_file_status")
            or saved_file_status,
            "external_calls_without_approval": int(
                approval_packet.get("external_calls_without_approval") or 0
            ),
            "external_calls_if_approved": int(
                approval_packet.get("external_calls_if_approved") or 0
            ),
            "db_writes_during_capture": int(
                approval_packet.get("db_writes_during_capture") or 0
            ),
            "capture_cli_command": approval_packet.get("capture_cli_command"),
            "capture_api": approval_packet.get("capture_api")
            or repair.get("provider_saved_file_capture_api"),
            "capture_request_body": approval_packet.get("capture_request_body"),
            "capture_confirm_request_body": approval_packet.get(
                "capture_confirm_request_body"
            ),
            "next_action": approval_packet.get("next_action"),
            "guardrails": approval_packet.get("guardrails") or [],
        },
        "saved_file": {
            "status": saved_file_status,
            "path": saved_file_path,
            "validate_command": repair.get("provider_saved_file_validate_command"),
            "validate_api": repair.get("provider_saved_file_validate_api"),
            "validate_request_body": repair.get(
                "provider_saved_file_validate_request_body"
            ),
            "import_preview_command": import_command,
            "import_execute_command": f"{import_command} --execute"
            if import_command
            else None,
            "import_api": repair.get("provider_saved_file_import_api"),
            "import_preview_request_body": repair.get(
                "provider_saved_file_import_preview_request_body"
            ),
            "import_request_body": repair.get(
                "provider_saved_file_import_request_body"
            ),
            "projection": saved_file_projection,
            "external_calls_made": 0,
        },
        "unblock_checklist": unblock_checklist,
        "repair_plan": repair,
        "recommended_action": recommended_action,
        "after_market_bars_clear": after_clear,
        "next_action": next_action,
        "zero_call_boundary": (
            "Status reads local database/provider-health metadata only and makes "
            "0 provider calls and 0 database writes."
        ),
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def market_bars_residual_review_payload(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date | None = None,
    stocks_only: bool = False,
):
    """Review residual missing bars without clearing the market-bar gate."""

    status_payload = market_bars_status_payload(
        engine,
        config,
        expected_as_of=expected_as_of,
        stocks_only=stocks_only,
    )
    missing = _int_payload_value(status_payload.get("missing_as_of_bar_count"))
    first_blocker = status_payload.get("first_blocker")
    if first_blocker == "universe":
        status = "setup_required"
    elif missing <= 0:
        status = "ready"
    elif _status_payload_has_universe_quality_residual(status_payload, missing):
        status = "universe_review_required"
    elif _status_payload_has_saved_file_residual_gap(status_payload, missing):
        status = "residual_review_required"
    else:
        status = "repair_required"

    stock_scope = _mapping(status_payload.get("stock_scope"))
    if stocks_only:
        stock_like_missing = missing
        non_stock_missing = 0
    else:
        stock_like_missing = _int_payload_value(
            stock_scope.get("stock_like_missing_as_of_bar")
        )
        non_stock_missing = max(0, missing - stock_like_missing)
    configured_scope = _mapping(status_payload.get("configured_universe_scope"))
    repair = _mapping(status_payload.get("repair_plan"))
    diagnostic = _mapping(status_payload.get("missing_universe_diagnostic"))
    saved_file = _mapping(status_payload.get("saved_file"))
    projection = _mapping(saved_file.get("projection"))
    expected_text = str(status_payload.get("expected_as_of") or "").strip()
    manual = _mapping(status_payload.get("manual"))
    recommended = _mapping(status_payload.get("recommended_action"))
    if status == "ready":
        next_action = "Market bars already cover this scope; rerun the priced-in answer."
    elif status == "setup_required":
        next_action = str(status_payload.get("next_action") or "")
    elif status == "repair_required":
        next_action = str(
            recommended.get("reason")
            or status_payload.get("next_action")
            or "Repair missing market bars before relying on the priced-in answer."
        )
    else:
        next_action = (
            "Review the residual evidence before filling bars. If the rows are "
            "real tradable securities, use the manual repair path. If they are "
            "stale or unsupported active-universe rows, fix the universe source "
            "and rerun status. Until then, the full-market answer remains blocked."
        )
    return {
        "schema_version": "market-bars-residual-review-v1",
        "status": status,
        "expected_as_of": status_payload.get("expected_as_of"),
        "expected_as_of_source": status_payload.get("expected_as_of_source"),
        "stocks_only": bool(stocks_only),
        "coverage_scope": status_payload.get("coverage_scope"),
        "first_blocker": first_blocker,
        "clears_market_bar_gate": False,
        "trusted_answer_ready": False,
        "active_security_count": status_payload.get("active_security_count"),
        "existing_as_of_bar_count": status_payload.get("existing_as_of_bar_count"),
        "missing_as_of_bar_count": missing,
        "stock_like_missing_as_of_bar_count": stock_like_missing,
        "non_stock_missing_as_of_bar_count": non_stock_missing,
        "missing_security_type_counts": status_payload.get(
            "missing_security_type_counts"
        )
        or {},
        "missing_as_of_bar_ticker_sample": status_payload.get(
            "missing_as_of_bar_ticker_sample"
        )
        or [],
        "missing_as_of_bar_ticker_more": status_payload.get(
            "missing_as_of_bar_ticker_more"
        )
        or 0,
        "residual_evidence": {
            "schema_version": "market-bars-residual-evidence-v1",
            "missing_count": missing,
            "zero_avg_dollar_volume_20d_count": _int_payload_value(
                diagnostic.get("zero_avg_dollar_volume_20d_count")
            ),
            "zero_market_cap_count": _int_payload_value(
                diagnostic.get("zero_market_cap_count")
            ),
            "no_composite_figi_count": _int_payload_value(
                diagnostic.get("no_composite_figi_count")
            ),
            "no_options_count": _int_payload_value(diagnostic.get("no_options_count")),
            "unknown_sector_count": _int_payload_value(
                diagnostic.get("unknown_sector_count")
            ),
            "acquisition_or_spac_name_count": _int_payload_value(
                diagnostic.get("acquisition_or_spac_name_count")
            ),
            "missing_without_local_history_count": _int_payload_value(
                repair.get("missing_without_local_history_count")
            ),
            "missing_with_local_history_count": _int_payload_value(
                repair.get("missing_with_local_history_count")
            ),
            "summary": diagnostic.get("summary"),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "saved_file_projection": dict(projection),
        "configured_universe_scope": dict(configured_scope),
        "manual_repair": {
            "status": manual.get("status"),
            "template_command": manual.get("template_command"),
            "import_preview_command": manual.get("import_preview_command"),
            "incremental_preview_command": manual.get("incremental_preview_command"),
            "local_template_path": manual.get("local_template_path"),
            "local_template_exists": bool(manual.get("local_template_exists")),
            "fill_progress": dict(_mapping(manual.get("fill_progress"))),
            "external_calls_made": 0,
        },
        "decision_options": _residual_review_decision_options(
            status_payload,
            expected_as_of_text=expected_text,
            stocks_only=stocks_only,
        ),
        "safe_default": (
            "Keep market_bars blocked; do not treat the full-market priced-in "
            "answer as trusted until bars are repaired or the active universe is "
            "fixed through an explicit operator-approved path."
        ),
        "next_action": next_action,
        "zero_call_boundary": (
            "Residual review reads local database state and saved-file metadata "
            "only; it makes 0 provider calls and 0 database writes."
        ),
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def market_bars_import_verification_payload(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date | None,
    stocks_only: bool = False,
    executed: bool,
    source: str,
    db_changes_made: int = 0,
    projected_missing_after_import_count: int | None = None,
    projected_db_changes_made: int | None = None,
):
    """Verify whether a market-bar import cleared the trusted-answer blocker."""

    resolved_expected_as_of = _resolve_expected_as_of(engine, expected_as_of)
    status_payload = market_bars_status_payload(
        engine,
        config,
        expected_as_of=resolved_expected_as_of,
        stocks_only=stocks_only,
    )
    missing = int(status_payload.get("missing_as_of_bar_count") or 0)
    projected_missing = (
        max(0, int(projected_missing_after_import_count))
        if projected_missing_after_import_count is not None
        else None
    )
    if projected_missing is None:
        preview_projection_status = "unknown"
    elif projected_missing == 0:
        preview_projection_status = "would_clear_market_bars"
    else:
        preview_projection_status = "would_still_block_market_bars"
    if not executed:
        verification_status = "preview_only"
    elif missing == 0:
        verification_status = "market_bars_cleared"
    else:
        verification_status = "market_bars_still_blocked"
    answer_summary = (
        _post_import_priced_in_answer_summary(
            engine,
            config,
            stocks_only=stocks_only,
        )
        if executed and missing == 0
        else {}
    )
    next_blocker = answer_summary.get("first_blocker")
    if executed and missing == 0 and next_blocker:
        next_action = (
            f"Market bars are clear; inspect the next blocker `{next_blocker}` "
            "before running provider source chunks."
        )
    elif executed and missing == 0:
        next_action = (
            "Market bars are clear; rerun priced-in-answer for the trusted answer."
        )
    elif executed:
        next_action = (
            "Import completed, but market bars are still incomplete. Fill the "
            "remaining rows before treating the priced-in answer as trusted."
        )
    elif preview_projection_status == "would_clear_market_bars":
        next_action = (
            "Preview covers the current market-bar gap. Execute only after "
            "review; then rerun the priced-in answer."
        )
    elif preview_projection_status == "would_still_block_market_bars":
        next_action = (
            f"Preview would still leave {projected_missing} market-bar row(s) "
            "missing. Fill or replace the import before relying on it."
        )
    else:
        next_action = (
            "Preview only. Execute the import only after coverage matches intent, "
            "then verify this same post-import status."
        )
    priced_in_command = "catalyst-radar priced-in-answer --limit 5"
    if stocks_only:
        priced_in_command += " --stocks-only"
    return {
        "schema_version": "market-bars-post-import-verification-v1",
        "status": verification_status,
        "source": source,
        "executed": bool(executed),
        "expected_as_of": resolved_expected_as_of.isoformat(),
        "stocks_only": bool(stocks_only),
        "coverage_scope": status_payload.get("coverage_scope"),
        "active_security_count": status_payload.get("active_security_count"),
        "existing_as_of_bar_count": status_payload.get("existing_as_of_bar_count"),
        "missing_as_of_bar_count": missing,
        "projected_missing_after_import_count": projected_missing,
        "preview_projection_status": preview_projection_status,
        "preview_would_clear_market_bars": bool(
            not executed and projected_missing == 0
        ),
        "preview_would_still_block_market_bars": bool(
            not executed
            and projected_missing is not None
            and projected_missing != 0
        ),
        "projected_db_changes_made": projected_db_changes_made,
        "market_bars_first_blocker": status_payload.get("first_blocker"),
        "trusted_answer_status": answer_summary.get("status"),
        "trusted_answer_ready": bool(answer_summary.get("trusted_answer_ready")),
        "next_blocker": next_blocker,
        "next_blocker_action": answer_summary.get("next_action"),
        "next_blocker_command": answer_summary.get("command"),
        "priced_in_answer_command": priced_in_command,
        "priced_in_answer_api": "GET /api/radar/priced-in/answer",
        "external_calls_made": 0,
        "db_changes_made": db_changes_made,
        "next_action": next_action,
    }


def market_bars_post_capture_verification_payload(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date | None,
    capture_payload: Mapping[str, object],
    stocks_only: bool = False,
):
    """Project whether a saved provider capture would clear market-bar gaps."""

    preview = capture_payload.get("post_capture_preview")
    coverage = preview.get("coverage") if isinstance(preview, Mapping) else {}
    projected_missing = (
        int(coverage.get("missing_after_import_count") or 0)
        if isinstance(coverage, Mapping)
        else None
    )
    return market_bars_import_verification_payload(
        engine,
        config,
        expected_as_of=expected_as_of,
        stocks_only=stocks_only,
        executed=False,
        source="saved_provider_capture",
        db_changes_made=0,
        projected_missing_after_import_count=projected_missing,
        projected_db_changes_made=1 if projected_missing is not None else None,
    )


def _post_import_priced_in_answer_summary(
    engine: Engine,
    config: AppConfig,
    *,
    stocks_only: bool,
):
    try:
        from catalyst_radar.dashboard.data import priced_in_answer_payload

        answer = priced_in_answer_payload(
            engine,
            config,
            limit=1,
            stocks_only=stocks_only,
        )
    except (SQLAlchemyError, ValueError):
        command = "catalyst-radar priced-in-answer --limit 5"
        if stocks_only:
            command += " --stocks-only"
        return {
            "status": "unavailable",
            "next_action": "Rerun priced-in-answer to inspect the next blocker.",
            "command": command,
        }
    gate = _mapping(answer.get("full_market_trust_gate"))
    action = _mapping(gate.get("recommended_action"))
    command = (
        action.get("cli_command")
        or action.get("command")
        or "catalyst-radar priced-in-answer --limit 5"
    )
    if stocks_only and command == "catalyst-radar priced-in-answer --limit 5":
        command = f"{command} --stocks-only"
    return {
        "status": gate.get("status"),
        "trusted_answer_ready": bool(gate.get("trusted_full_market_answer")),
        "first_blocker": gate.get("first_blocker"),
        "next_action": action.get("action") or gate.get("next_action"),
        "command": command,
    }


def _market_bar_unblock_checklist(
    *,
    expected_as_of,
    missing,
    repair,
    approval_packet,
    saved_file_path,
    saved_file_status,
    saved_file_projection,
    import_command,
    recommended_action,
    stocks_only,
):
    scope = str(repair.get("coverage_scope") or "active_universe")
    expected_text = expected_as_of.isoformat()
    saved_ready = str(saved_file_status or "").strip() == "available"
    projection = _mapping(saved_file_projection)
    projected_covered = int(projection.get("missing_covered_by_fixture_count") or 0)
    saved_file_can_advance = bool(
        saved_ready
        and (
            not projection
            or str(projection.get("status") or "") == "unavailable"
            or projected_covered > 0
        )
    )
    approval_required = bool(approval_packet.get("approval_required"))
    if missing <= 0:
        status = "ready"
        next_step = 6
    elif saved_file_can_advance:
        status = "saved_file_available"
        next_step = 3
    elif saved_ready:
        status = "saved_file_residual_gap"
        next_step = 1
    elif approval_required:
        status = "approval_required"
        next_step = 2
    else:
        status = str(approval_packet.get("status") or "blocked")
        next_step = 1
    capture_command = approval_packet.get("capture_cli_command") or repair.get(
        "provider_saved_file_capture_command"
    )
    import_execute = f"{import_command} --execute" if import_command else None
    stock_flag = " --stocks-only" if stocks_only else ""
    steps = [
        {
            "order": 1,
            "label": "Review current gap",
            "command": (
                "catalyst-radar market-bars status "
                f"--expected-as-of {expected_text}{stock_flag}"
            ),
            "external_calls_required": 0,
            "db_changes_required": 0,
        },
        {
            "order": 2,
            "label": "Capture saved provider file",
            "command": capture_command,
            "tui_command": approval_packet.get("tui_confirm_command"),
            "external_calls_required": int(
                approval_packet.get("external_calls_if_approved") or 0
            ),
            "db_changes_required": int(
                approval_packet.get("db_writes_during_capture") or 0
            ),
            "approval_required": approval_required,
        },
        {
            "order": 3,
            "label": "Validate saved file",
            "command": repair.get("provider_saved_file_validate_command"),
            "external_calls_required": 0,
            "db_changes_required": 0,
        },
        {
            "order": 4,
            "label": "Preview saved import",
            "command": import_command,
            "external_calls_required": 0,
            "db_changes_required": 0,
        },
        {
            "order": 5,
            "label": "Execute saved import",
            "command": import_execute,
            "external_calls_required": 0,
            "db_changes_required": 1,
        },
        {
            "order": 6,
            "label": "Rerun priced-in answer",
            "command": f"catalyst-radar priced-in-answer --limit 5{stock_flag}",
            "external_calls_required": 0,
            "db_changes_required": 0,
        },
    ]
    return {
        "schema_version": "market-bars-unblock-checklist-v1",
        "status": status,
        "next_step_order": next_step,
        "expected_as_of": expected_text,
        "coverage_scope": scope,
        "active_security_count": repair.get("active_security_count"),
        "existing_as_of_bar_count": repair.get("existing_as_of_bar_count"),
        "missing_as_of_bar_count": missing,
        "saved_file_status": saved_file_status,
        "saved_file_path": saved_file_path,
        "saved_file_projection": dict(projection),
        "recommended_action_kind": recommended_action.get("kind"),
        "steps": steps,
        "external_calls_made": 0,
        "db_changes_made": 0,
    }


def _post_market_bars_clear_payload(
    engine: Engine,
    config: AppConfig,
    *,
    missing: int,
    stocks_only: bool,
):
    if missing <= 0:
        return {
            "schema_version": "market-bars-after-clear-v1",
            "status": "ready",
            "current_blocker": None,
            "current_gap_count": 0,
            "next_action": "Rerun the priced-in answer against the repaired scan.",
            "plan_command": "catalyst-radar priced-in-answer --limit 5",
            "plan_api": "GET /api/radar/priced-in/answer",
            "operator_note": (
                "Market bars already cover this scope. This preview makes 0 "
                "provider calls and does not run source fills."
            ),
            "external_calls_made": 0,
        }
    try:
        from catalyst_radar.dashboard.data import priced_in_answer_payload

        answer = priced_in_answer_payload(
            engine,
            config,
            limit=1,
            stocks_only=stocks_only,
        )
    except (SQLAlchemyError, ValueError):
        return {
            "schema_version": "market-bars-after-clear-v1",
            "status": "unavailable",
            "current_blocker": "market_bars",
            "current_gap_count": missing,
            "reason": (
                "The post-bar priced-in source preview could not be built from "
                "the local database state."
            ),
            "operator_note": (
                "Clear market bars first, then rerun priced-in-answer or "
                "priced-in-source-batches --source all."
            ),
            "external_calls_made": 0,
        }
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    after_current = _mapping(trust_gate.get("after_current_blocker"))
    if str(after_current.get("current_blocker") or "") != "market_bars":
        return {
            "schema_version": "market-bars-after-clear-v1",
            "status": "unavailable",
            "current_blocker": "market_bars",
            "current_gap_count": missing,
            "reason": "No downstream blocker preview is currently available.",
            "operator_note": (
                "Clear market bars first, then rerun priced-in-answer or "
                "priced-in-source-batches --source all."
            ),
            "external_calls_made": 0,
        }
    plan = _mapping(after_current.get("next_source_plan"))
    payload: dict[str, object] = {
        "schema_version": "market-bars-after-clear-v1",
        "status": "preview",
        "current_blocker": "market_bars",
        "current_gap_count": int(after_current.get("current_gap_count") or missing),
        "next_source": after_current.get("next_source"),
        "next_status": after_current.get("next_status"),
        "next_gap_count": int(after_current.get("next_gap_count") or 0),
        "why_it_matters": after_current.get("why_it_matters"),
        "next_action": after_current.get("next_action"),
        "plan_command": after_current.get("plan_command"),
        "plan_api": after_current.get("plan_api"),
        "execute_next_command": after_current.get("execute_next_command"),
        "execute_next_api": after_current.get("execute_next_api"),
        "execute_next_request_body": after_current.get("execute_next_request_body"),
        "operator_note": (
            "Preview only. This is what to inspect after market bars clear; "
            "do not run it until the market-bar blocker is gone and the "
            "provider call budget is intentional."
        ),
        "external_calls_made": 0,
    }
    if plan:
        payload["next_source_plan"] = dict(plan)
    return payload


def _repair_payload(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date,
    stocks_only: bool,
):
    provider_health = ProviderRepository(engine).latest_health("polygon")
    return manual_market_bars_repair_plan(
        engine,
        expected_as_of=expected_as_of,
        stocks_only=stocks_only,
        provider_key_configured=config.polygon_api_key_configured,
        provider_health_status=(
            provider_health.status.value if provider_health is not None else None
        ),
        provider_health_reason=(
            provider_health.reason if provider_health is not None else None
        ),
        provider_health_checked_at=(
            provider_health.checked_at if provider_health is not None else None
        ),
    ).as_payload()


def _stock_scope_payload(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date,
    full_repair: Mapping[str, object],
):
    try:
        stock_repair = _repair_payload(
            engine,
            config,
            expected_as_of=expected_as_of,
            stocks_only=True,
        )
    except ValueError:
        return None
    active = int(stock_repair.get("active_security_count") or 0)
    with_bar = int(stock_repair.get("existing_as_of_bar_count") or 0)
    missing = int(stock_repair.get("missing_as_of_bar_count") or 0)
    full_missing = int(full_repair.get("missing_as_of_bar_count") or 0)
    sample = list(stock_repair.get("missing_as_of_bar_ticker_sample") or [])
    coverage_pct = round((with_bar / active) * 100, 2) if active else None
    operator_step = _mapping(stock_repair.get("operator_step"))
    return {
        "schema_version": "market-bars-stock-scope-v1",
        "status": "blocked" if missing else "ready",
        "expected_as_of": expected_as_of.isoformat(),
        "stock_like_active": active,
        "stock_like_with_as_of_bar": with_bar,
        "stock_like_missing_as_of_bar": missing,
        "stock_like_coverage_pct": coverage_pct,
        "sample_missing_stock_like_tickers": sample,
        "sample_missing_stock_like_more": int(
            stock_repair.get("missing_as_of_bar_ticker_more") or 0
        ),
        "missing_security_type_counts": stock_repair.get(
            "missing_security_type_counts"
        )
        or {},
        "non_stock_missing_as_of_bar": max(0, full_missing - missing),
        "manual_template_command": stock_repair.get("manual_template_command"),
        "manual_import_preview_command": stock_repair.get(
            "manual_import_preview_command"
        ),
        "manual_import_execute_command": stock_repair.get(
            "manual_import_execute_command"
        ),
        "saved_capture_command": stock_repair.get(
            "provider_saved_file_capture_command"
        ),
        "operator_step": dict(operator_step),
        "next_action": stock_repair.get("next_action"),
        "answer_boundary": (
            "A stocks-only priced-in answer remains blocked until these "
            "stock-like bars are filled."
            if missing
            else "Stock-like market bars are complete for the stocks-only answer."
        ),
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def _configured_universe_scope_payload(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date,
    active_universe_missing: int,
):
    universe_name = str(config.universe_name or "").strip()
    if not universe_name:
        return {
            "schema_version": "market-bars-configured-universe-scope-v1",
            "status": "not_configured",
            "external_calls_made": 0,
            "db_writes_made": 0,
        }
    provider_repo = ProviderRepository(engine)
    snapshot = provider_repo.latest_universe_snapshot(
        name=universe_name,
        as_of=datetime.combine(expected_as_of, time.max, tzinfo=UTC),
        available_at=datetime.now(UTC),
    )
    if snapshot is None:
        return {
            "schema_version": "market-bars-configured-universe-scope-v1",
            "status": "not_configured",
            "universe": universe_name,
            "expected_as_of": expected_as_of.isoformat(),
            "next_action": (
                "Build the configured universe before treating it as a scan boundary."
            ),
            "external_calls_made": 0,
            "db_writes_made": 0,
        }
    member_rows = provider_repo.list_universe_member_rows(snapshot.id)
    members = tuple(
        sorted(
            {
                str(row.ticker or "").strip().upper()
                for row in member_rows
                if str(row.ticker or "").strip()
            }
        )
    )
    member_set = set(members)
    with_bar = _bar_tickers_for_date(engine, expected_as_of) & member_set
    missing = tuple(sorted(member_set - with_bar))
    member_count = len(members)
    with_bar_count = len(with_bar)
    coverage_pct = (
        round((with_bar_count / member_count) * 100, 2) if member_count else None
    )
    status = "ready" if member_count > 0 and not missing else "blocked"
    if member_count == 0:
        status = "empty"
    return {
        "schema_version": "market-bars-configured-universe-scope-v1",
        "status": status,
        "universe": universe_name,
        "snapshot_id": snapshot.id,
        "snapshot_as_of": snapshot.as_of.date().isoformat(),
        "snapshot_available_at": snapshot.available_at.isoformat(),
        "provider": snapshot.provider,
        "expected_as_of": expected_as_of.isoformat(),
        "member_count": member_count,
        "with_as_of_bar_count": with_bar_count,
        "missing_as_of_bar_count": len(missing),
        "coverage_pct": coverage_pct,
        "sample_missing_tickers": list(missing[:12]),
        "sample_missing_ticker_more": max(0, len(missing) - 12),
        "active_universe_missing_as_of_bar_count": max(0, int(active_universe_missing)),
        "answer_boundary": (
            "Configured-universe bar coverage is a separate investable-universe "
            "readiness signal. It does not clear the all-active market-bar gate."
        ),
        "next_action": (
            "Run a configured-universe shadow scan only if this scope matches the "
            "intended review universe; keep all-active status separate."
        )
        if status == "ready"
        else "Fill configured-universe market bars before relying on this scan boundary.",
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def _resolve_expected_as_of(engine: Engine, expected_as_of: date | None) -> date:
    if expected_as_of is not None:
        return expected_as_of
    with engine.connect() as connection:
        latest = connection.execute(select(func.max(daily_bars.c.date))).scalar_one()
    if latest is None:
        raise ValueError(
            "expected_as_of is required when no daily bars are stored; "
            "pass --expected-as-of YYYY-MM-DD."
        )
    if isinstance(latest, date):
        return latest
    return date.fromisoformat(str(latest))


def _active_security_count(engine: Engine, *, stocks_only: bool) -> int:
    with engine.connect() as connection:
        rows = connection.execute(
            select(securities.c.metadata).where(securities.c.is_active.is_(True))
        ).all()
    if not stocks_only:
        return len(rows)
    count = 0
    for row in rows:
        metadata = row._mapping["metadata"]
        if not isinstance(metadata, Mapping):
            metadata = {}
        security_type = str(metadata.get("type") or "").strip().upper()
        if security_type in MANUAL_BAR_COMPANY_LIKE_TYPES:
            count += 1
    return count


def _bar_tickers_for_date(engine: Engine, as_of_date: date) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row._mapping["ticker"]).strip().upper()
            for row in connection.execute(
                select(daily_bars.c.ticker).where(daily_bars.c.date == as_of_date)
            )
            if str(row._mapping["ticker"]).strip()
        }


def _expected_as_of_required_status_payload(
    *,
    expected_as_of_source: str,
    stocks_only: bool,
    reason: str,
    active_security_count: int,
):
    scope = "stock_like" if stocks_only else "active_universe"
    status_command = "catalyst-radar market-bars status --expected-as-of YYYY-MM-DD"
    if stocks_only:
        status_command = f"{status_command} --stocks-only"
    action = (
        "Choose the target trading date, then rerun market-bars status with "
        "`--expected-as-of YYYY-MM-DD` before repairing or importing bars."
    )
    recommended_action = _recommended_action_payload(
        kind="provide_expected_as_of",
        label="Set market-bar date",
        status="blocked",
        reason=action,
        command=status_command,
        tui_command="bars status --expected-as-of YYYY-MM-DD",
        api="GET /api/radar/market-bars/status",
        request_body={"expected_as_of": "YYYY-MM-DD", "stocks_only": bool(stocks_only)},
    )
    return {
        "schema_version": "market-bars-status-v1",
        "status": "blocked",
        "first_blocker": "market_bars",
        "expected_as_of": None,
        "expected_as_of_source": expected_as_of_source,
        "stocks_only": bool(stocks_only),
        "coverage_scope": scope,
        "active_security_count": active_security_count,
        "existing_as_of_bar_count": 0,
        "missing_as_of_bar_count": active_security_count,
        "missing_as_of_bar_ticker_sample": [],
        "missing_security_type_counts": {},
        "missing_universe_diagnostic": {},
        "stock_scope": None,
        "manual": {
            "status": "expected_as_of_required",
            "action": action,
            "command": status_command,
            "external_calls_made": 0,
        },
        "saved_capture": {"status": "not_available"},
        "saved_file": {"status": "not_available"},
        "unblock_checklist": {
            "schema_version": "market-bars-unblock-checklist-v1",
            "status": "blocked",
            "next_step_order": 1,
            "coverage_scope": scope,
            "active_security_count": active_security_count,
            "existing_as_of_bar_count": 0,
            "missing_as_of_bar_count": active_security_count,
            "steps": [
                {
                    "order": 1,
                    "step": "set_expected_as_of",
                    "label": "Choose target trading date",
                    "command": status_command,
                    "api": "GET /api/radar/market-bars/status",
                    "request_body": {
                        "expected_as_of": "YYYY-MM-DD",
                        "stocks_only": bool(stocks_only),
                    },
                    "external_calls_required": 0,
                    "db_changes_required": 0,
                }
            ],
            "external_calls_made": 0,
            "db_changes_made": 0,
        },
        "repair_plan": {},
        "recommended_action": recommended_action,
        "after_market_bars_clear": {},
        "next_action": action,
        "zero_call_boundary": (
            "Status reads local metadata and makes 0 provider calls and "
            "0 database writes."
        ),
        "reason": reason,
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def _setup_required_status_payload(
    config: AppConfig,
    *,
    expected_as_of: date | None,
    expected_as_of_source: str,
    stocks_only: bool,
    reason: str,
):
    setup = _universe_setup_action(config)
    action = str(setup["action"])
    command = str(setup["command"])
    recommended_action = _recommended_action_payload(
        kind="seed_universe",
        label="Seed active universe",
        status="setup_required",
        reason=action,
        command=command,
        tui_command="1",
        api=setup.get("api"),
        request_body=setup.get("request_body"),
        approval_required=bool(setup["approval_required"]),
        external_calls_required=int(setup["external_calls_required"]),
        db_writes_required=int(setup["db_writes_required"]),
    )
    return {
        "schema_version": "market-bars-status-v1",
        "status": "setup_required",
        "first_blocker": "universe",
        "expected_as_of": expected_as_of.isoformat() if expected_as_of else None,
        "expected_as_of_source": expected_as_of_source,
        "stocks_only": bool(stocks_only),
        "coverage_scope": "stock_like" if stocks_only else "active_universe",
        "active_security_count": 0,
        "existing_as_of_bar_count": 0,
        "missing_as_of_bar_count": 0,
        "missing_as_of_bar_ticker_sample": [],
        "missing_security_type_counts": {},
        "missing_universe_diagnostic": {
            "schema_version": "market-bars-setup-required-v1",
            "status": "setup_required",
            "reason": reason,
            "next_action": action,
            "command": command,
            "api": setup.get("api"),
            "request_body": setup.get("request_body"),
            "approval_required": bool(setup["approval_required"]),
            "external_calls_required": int(setup["external_calls_required"]),
            "db_writes_required": int(setup["db_writes_required"]),
            "provider": setup["provider"],
            "external_calls_made": 0,
        },
        "stock_scope": None,
        "manual": {"status": "not_available", "command": None},
        "saved_capture": {"status": "not_available"},
        "saved_file": {"status": "not_available"},
        "unblock_checklist": {
            "schema_version": "market-bars-unblock-checklist-v1",
            "status": "setup_required",
            "next_step_order": 1,
            "steps": [
                {
                    "order": 1,
                    "step": "seed_universe",
                    "label": "Seed active universe",
                    "command": command,
                    "api": setup.get("api"),
                    "request_body": setup.get("request_body"),
                    "approval_required": bool(setup["approval_required"]),
                    "external_calls_required": int(setup["external_calls_required"]),
                    "db_changes_required": int(setup["db_writes_required"]),
                },
                {
                    "order": 2,
                    "step": "rerun_status",
                    "label": "Rerun market-bar status",
                    "command": "catalyst-radar market-bars status",
                    "external_calls_required": 0,
                    "db_changes_required": 0,
                },
            ],
            "external_calls_made": 0,
        },
        "repair_plan": {},
        "recommended_action": recommended_action,
        "after_market_bars_clear": {},
        "next_action": action,
        "zero_call_boundary": "Status reads local metadata and makes 0 provider calls.",
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def _universe_setup_action(config: AppConfig) -> dict[str, object]:
    provider = str(config.daily_market_provider or config.market_provider or "csv").strip().lower()
    if provider == "polygon":
        max_pages = max(1, int(config.polygon_tickers_max_pages))
        return {
            "provider": "polygon",
            "action": "Seed the active universe from Polygon/Massive before market-bar repair.",
            "command": (
                "catalyst-radar ingest-polygon tickers "
                f"--max-pages {max_pages} --confirm-external-call"
            ),
            "api": "POST /api/radar/universe/seed",
            "request_body": {"provider": "polygon", "max_pages": max_pages},
            "approval_required": True,
            "external_calls_required": max_pages,
            "db_writes_required": 1,
        }
    command = (
        "catalyst-radar ingest-csv "
        f"--securities {config.csv_securities_path} "
        f"--daily-bars {config.csv_daily_bars_path}"
    )
    if config.csv_holdings_path:
        command = f"{command} --holdings {config.csv_holdings_path}"
    return {
        "provider": provider or "csv",
        "action": "Load the CSV securities and daily bars before market-bar repair.",
        "command": command,
        "api": None,
        "request_body": None,
        "approval_required": False,
        "external_calls_required": 0,
        "db_writes_required": 1,
    }


def _recommended_unblock_action(
    *,
    expected_as_of: date,
    missing: int,
    repair: Mapping[str, object],
    operator_step: Mapping[str, object],
    approval_packet: Mapping[str, object],
    saved_file_path: object,
    saved_file_status: object,
    saved_file_projection: Mapping[str, object],
):
    if missing <= 0:
        return _recommended_action_payload(
            kind="rerun_priced_in_answer",
            label="Rerun priced-in answer",
            status="ready",
            reason="Scan-date market bars already cover this scope.",
            command="catalyst-radar priced-in-answer --limit 5",
            tui_command="refresh",
            api="GET /api/radar/priced-in/answer",
        )
    if str(saved_file_status or "").strip() == "available":
        projection = _mapping(saved_file_projection)
        projected_status = str(projection.get("status") or "").strip()
        covered = int(projection.get("missing_covered_by_fixture_count") or 0)
        missing_after = int(projection.get("missing_after_import_count") or 0)
        if projected_status == "invalid":
            return _recommended_action_payload(
                kind="saved_file_validate",
                label="Fix saved provider file",
                status="blocked",
                reason=(
                    "The saved grouped-daily file is present but invalid; validate "
                    "or replace it before using the saved-file path."
                ),
                command=repair.get("provider_saved_file_validate_command"),
                tui_command="bars saved validate",
                api=repair.get("provider_saved_file_validate_api"),
                request_body=repair.get("provider_saved_file_validate_request_body"),
                saved_file_path=saved_file_path,
                expected_as_of=expected_as_of.isoformat(),
            )
        if projected_status and projected_status != "unavailable" and covered <= 0:
            if _repair_payload_has_universe_quality_residual(repair, missing):
                return _recommended_action_payload(
                    kind="residual_universe_review",
                    label="Review residual universe",
                    status="blocked",
                    reason=(
                        "The saved grouped-daily file covers no remaining missing "
                        "active tickers, and the residual set looks like a "
                        "zero-liquidity universe-quality gap. Review the residual "
                        "rows before filling bars or changing scan scope."
                    ),
                    command=_residual_review_command(
                        expected_as_of,
                        stocks_only=bool(repair.get("stocks_only")),
                    ),
                    tui_command="bars residual review",
                    api="GET /api/radar/market-bars/residual-review",
                    request_body={
                        "expected_as_of": expected_as_of.isoformat(),
                        "stocks_only": bool(repair.get("stocks_only")),
                    },
                    saved_file_path=saved_file_path,
                    expected_as_of=expected_as_of.isoformat(),
                    projected_missing_after_import_count=missing_after,
                )
            return _recommended_action_payload(
                kind="manual_csv",
                label="Manual residual repair",
                status="attention",
                reason=(
                    "The saved grouped-daily file covers no remaining missing active "
                    "tickers; generate and fill the manual CSV or review the residual "
                    "universe-quality gap instead of reimporting the same file."
                ),
                command=repair.get("manual_template_command"),
                tui_command=repair.get("dashboard_manual_template_command")
                or "bars manual template",
                api=repair.get("manual_template_api"),
                request_body={
                    "expected_as_of": expected_as_of.isoformat(),
                    "output_path": repair.get("local_template_path"),
                    "missing_only": True,
                    "stocks_only": bool(repair.get("stocks_only")),
                },
                saved_file_path=saved_file_path,
                expected_as_of=expected_as_of.isoformat(),
                projected_missing_after_import_count=missing_after,
            )
        if covered > 0:
            reason = (
                f"The saved grouped-daily file covers {covered} missing active "
                "ticker(s); preview the import before executing."
            )
            if missing_after > 0:
                reason = (
                    f"The saved grouped-daily file covers {covered} missing active "
                    f"ticker(s) but would still leave {missing_after}; use it only "
                    "as an incremental repair, then fix the residual gap."
                )
            return _recommended_action_payload(
                kind="saved_file_import_preview",
                label="Preview saved import",
                status="attention" if missing_after > 0 else "ready",
                reason=reason,
                command=repair.get("provider_saved_file_import_command"),
                tui_command="bars saved import",
                api=repair.get("provider_saved_file_import_api"),
                request_body=repair.get("provider_saved_file_import_preview_request_body"),
                saved_file_path=saved_file_path,
                expected_as_of=expected_as_of.isoformat(),
                projected_missing_after_import_count=missing_after,
            )
        return _recommended_action_payload(
            kind="saved_file_validate",
            label="Validate saved provider file",
            status="ready",
            reason="A saved grouped-daily file already exists; validate it before import.",
            command=repair.get("provider_saved_file_validate_command"),
            tui_command="bars saved validate",
            api=repair.get("provider_saved_file_validate_api"),
            request_body=repair.get("provider_saved_file_validate_request_body"),
        )
    if approval_packet.get("status") == "approval_required":
        return _recommended_action_payload(
            kind="saved_provider_capture",
            label="Capture saved provider file",
            status="approval_required",
            reason=(
                "The manual CSV is not complete and a provider key is configured; "
                "one saved capture can produce the file needed for zero-call validation."
            ),
            command=approval_packet.get("capture_cli_command")
            or repair.get("provider_saved_file_capture_command"),
            tui_command=approval_packet.get("tui_confirm_command"),
            api=approval_packet.get("capture_api")
            or repair.get("provider_saved_file_capture_api"),
            request_body=approval_packet.get("capture_confirm_request_body"),
            approval_required=True,
            external_calls_required=int(
                approval_packet.get("external_calls_if_approved") or 0
            ),
            db_writes_required=int(
                approval_packet.get("db_writes_during_capture") or 0
            ),
            saved_file_path=saved_file_path,
            expected_as_of=expected_as_of.isoformat(),
        )
    command = operator_step.get("command") or operator_step.get("after_manual_command")
    return _recommended_action_payload(
        kind=str(operator_step.get("kind") or "manual_csv"),
        label="Manual market-bar repair",
        status=operator_step.get("status") or "attention",
        reason=operator_step.get("action") or repair.get("next_action"),
        command=command or repair.get("manual_template_command"),
        tui_command=repair.get("dashboard_manual_import_preview_command")
        or repair.get("dashboard_manual_template_command"),
        api=repair.get("manual_import_api") or repair.get("manual_template_api"),
    )


def _recommended_action_payload(
    *,
    kind: str,
    label: str,
    status: object,
    reason: object,
    command: object,
    tui_command: object,
    api: object,
    request_body: object = None,
    approval_required: bool = False,
    external_calls_required: int = 0,
    db_writes_required: int = 0,
    **extra: object,
):
    return {
        "schema_version": "market-bars-recommended-action-v1",
        "kind": kind,
        "label": label,
        "status": status,
        "reason": reason,
        "command": command,
        "tui_command": tui_command,
        "api": api,
        "request_body": request_body,
        "approval_required": approval_required,
        "external_calls_required": external_calls_required,
        "db_writes_required": db_writes_required,
        "external_calls_made": 0,
        **extra,
    }


def _residual_review_decision_options(
    status_payload: Mapping[str, object],
    *,
    expected_as_of_text: str,
    stocks_only: bool,
) -> list[dict[str, object]]:
    manual = _mapping(status_payload.get("manual"))
    configured = _mapping(status_payload.get("configured_universe_scope"))
    options: list[dict[str, object]] = []
    if manual.get("template_command"):
        options.append(
            {
                "kind": "manual_bar_repair",
                "label": "Fill real residual bars",
                "when": (
                    "Use only if the residual tickers are truly active tradable "
                    "securities with real OHLCV/VWAP bars for the scan date."
                ),
                "command": manual.get("template_command"),
                "next_command": manual.get("import_preview_command"),
                "external_calls_required": 0,
                "db_writes_required": 0,
            }
        )
    options.append(
        {
            "kind": "active_universe_repair",
            "label": "Fix active-universe source",
            "when": (
                "Use when residual rows are stale, unsupported, or not part of "
                "the intended stock-market scan boundary."
            ),
            "command": None,
            "external_calls_required": 0,
            "db_writes_required": 0,
            "note": "No automatic mutation is provided here; keep the gate blocked.",
        }
    )
    if configured.get("status") == "ready":
        options.append(
            {
                "kind": "configured_universe_shadow",
                "label": "Use configured universe explicitly",
                "when": (
                    "Use only as a selected/configured-universe shadow scan; it "
                    "does not clear the all-active full-market gate."
                ),
                "universe": configured.get("universe"),
                "member_count": configured.get("member_count"),
                "missing_as_of_bar_count": configured.get(
                    "missing_as_of_bar_count"
                ),
                "external_calls_required": 0,
                "db_writes_required": 0,
            }
        )
    command_date = date.fromisoformat(expected_as_of_text) if expected_as_of_text else None
    options.append(
        {
            "kind": "keep_blocked",
            "label": "Keep full-market gate blocked",
            "when": "Use when the residual rows are unresolved.",
            "command": _residual_review_command(command_date, stocks_only=stocks_only),
            "external_calls_required": 0,
            "db_writes_required": 0,
        }
    )
    return options


def _residual_review_command(expected_as_of: date | None, *, stocks_only: bool) -> str:
    parts = ["catalyst-radar market-bars residual-review"]
    if expected_as_of is not None:
        parts.append(f"--expected-as-of {expected_as_of.isoformat()}")
    if stocks_only:
        parts.append("--stocks-only")
    return " ".join(parts)


def _repair_payload_has_universe_quality_residual(
    repair: Mapping[str, object],
    missing: int,
) -> bool:
    diagnostic = _mapping(repair.get("missing_universe_diagnostic"))
    if missing <= 0:
        return False
    zero_volume = _int_payload_value(diagnostic.get("zero_avg_dollar_volume_20d_count"))
    zero_market_cap = _int_payload_value(diagnostic.get("zero_market_cap_count"))
    missing_without_history = _int_payload_value(
        repair.get("missing_without_local_history_count")
    )
    return (
        zero_volume >= missing
        and zero_market_cap >= missing
        and (missing_without_history <= 0 or missing_without_history >= missing)
    )


def _status_payload_has_universe_quality_residual(
    status_payload: Mapping[str, object],
    missing: int,
) -> bool:
    return _status_payload_has_saved_file_residual_gap(status_payload, missing) and (
        _repair_payload_has_universe_quality_residual(
            _mapping(status_payload.get("repair_plan")),
            missing,
        )
    )


def _status_payload_has_saved_file_residual_gap(
    status_payload: Mapping[str, object],
    missing: int,
) -> bool:
    if missing <= 0:
        return False
    saved_file = _mapping(status_payload.get("saved_file"))
    projection = _mapping(saved_file.get("projection"))
    if not projection:
        return False
    projected_status = str(projection.get("status") or "").strip()
    covered = _int_payload_value(projection.get("missing_covered_by_fixture_count"))
    missing_after = _int_payload_value(projection.get("missing_after_import_count"))
    return bool(
        str(saved_file.get("status") or "").strip() == "available"
        and projected_status
        and projected_status != "unavailable"
        and covered <= 0
        and missing_after >= missing
    )


def _int_payload_value(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _saved_file_projection(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date,
    saved_file_path: object,
    stocks_only: bool,
):
    path_text = str(saved_file_path or "").strip()
    if not path_text:
        return {}
    path = Path(path_text)
    if not path.exists():
        return {}
    try:
        preview = preview_polygon_grouped_daily_fixture(
            config=config,
            market_repo=MarketRepository(engine),
            date_value=expected_as_of,
            fixture_path=path,
        )
    except Exception as exc:  # fail closed for status planning only
        return {
            "schema_version": "market-bars-saved-file-projection-v1",
            "status": "invalid",
            "path": str(path),
            "reason": str(exc),
            "external_calls_made": 0,
            "db_writes_made": 0,
        }
    coverage = _mapping(preview.get("coverage"))
    missing_after_key = (
        "stock_like_missing_after_import_count"
        if stocks_only
        else "missing_after_import_count"
    )
    covered_key = (
        "stock_like_covered_by_fixture_count"
        if stocks_only
        else "missing_covered_by_fixture_count"
    )
    return {
        "schema_version": "market-bars-saved-file-projection-v1",
        "status": preview.get("status") or "unknown",
        "path": str(path),
        "coverage_scope": "stock_like" if stocks_only else "active_universe",
        "missing_covered_by_fixture_count": int(coverage.get(covered_key) or 0),
        "missing_after_import_count": int(coverage.get(missing_after_key) or 0),
        "fixture_active_match_count": int(
            coverage.get("fixture_active_match_count") or 0
        ),
        "next_action": preview.get("next_action"),
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def _mapping(value: object):
    return value if isinstance(value, Mapping) else {}
