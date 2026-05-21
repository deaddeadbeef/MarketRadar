from __future__ import annotations

from collections.abc import Mapping
from datetime import date

from sqlalchemy.engine import Engine

from catalyst_radar.core.config import AppConfig
from catalyst_radar.market.manual_bars import manual_market_bars_repair_plan
from catalyst_radar.storage.provider_repositories import ProviderRepository


def market_bars_status_payload(
    engine: Engine,
    config: AppConfig,
    *,
    expected_as_of: date,
    stocks_only: bool = False,
):
    """Summarize the current market-bar unblock state without provider calls."""

    repair = _repair_payload(
        engine,
        config,
        expected_as_of=expected_as_of,
        stocks_only=stocks_only,
    )
    missing = int(repair.get("missing_as_of_bar_count") or 0)
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
    next_action = str(
        operator_step.get("action")
        or approval_packet.get("next_action")
        or repair.get("next_action")
        or ""
    ).strip()
    import_command = repair.get("provider_saved_file_import_command")
    return {
        "schema_version": "market-bars-status-v1",
        "status": status,
        "first_blocker": "market_bars" if missing_any else None,
        "expected_as_of": expected_as_of.isoformat(),
        "stocks_only": bool(stocks_only),
        "coverage_scope": repair.get("coverage_scope"),
        "active_security_count": repair.get("active_security_count"),
        "existing_as_of_bar_count": repair.get("existing_as_of_bar_count"),
        "missing_as_of_bar_count": missing,
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
            "external_calls_made": 0,
        },
        "repair_plan": repair,
        "next_action": next_action,
        "zero_call_boundary": (
            "Status reads local database/provider-health metadata only and makes "
            "0 provider calls and 0 database writes."
        ),
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


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


def _mapping(value: object):
    return value if isinstance(value, Mapping) else {}
