from __future__ import annotations

from catalyst_radar.ops.reports import (
    build_ops_run_report_payload,
    render_ops_run_report_html,
)


def test_build_ops_run_report_payload_normalizes_machine_contract() -> None:
    payload = build_ops_run_report_payload(
        result=_result(),
        snapshot=_snapshot(),
        terminal_text="┌ MARKETRADAR ┐\nMSFT inspect\n",
    )

    assert payload["schema_version"] == "ops-run-report-v1"
    assert payload["run"]["run_id"] == "20260530T000000Z-12345678"
    assert payload["summary"]["external_calls_made"] == 0
    assert payload["summary"]["status"] == "setup_required"
    assert payload["next_steps"]["command"] == "catalyst-radar next"
    assert payload["rows"][0]["ticker"] == "MSFT"
    assert payload["artifacts"][0]["name"] == "result.json"
    assert payload["terminal_preview"][0] == "┌ MARKETRADAR ┐"


def test_render_ops_run_report_html_is_standalone_and_json_backed() -> None:
    payload = build_ops_run_report_payload(
        result=_result(),
        snapshot=_snapshot(),
        terminal_text="┌ MARKETRADAR ┐\nMSFT inspect\n",
    )

    html = render_ops_run_report_html(payload)

    assert "<!doctype html>" in html
    assert "MarketRadar Ops Report" in html
    assert "MSFT" in html
    assert '<script type="application/json" id="ops-report-data">' in html
    assert "terminal.png" in html
    assert "Provider Calls" in html


def _result() -> dict[str, object]:
    return {
        "schema_version": "ops-run-v1",
        "run_id": "20260530T000000Z-12345678",
        "action": "radar-dashboard",
        "page": "overview",
        "status": "completed",
        "started_at": "2026-05-30T00:00:00+00:00",
        "finished_at": "2026-05-30T00:00:02+00:00",
        "elapsed_ms": 2000,
        "capture_mode": "headless-terminal-frame",
        "renderer": "rust",
        "requested_renderer": "auto",
        "run_dir": "C:/runs/20260530T000000Z-12345678",
        "summary": {
            "dashboard_status": "setup_required",
            "snapshot_mode": "fast_view",
            "external_calls_made": 0,
            "row_count": 1,
        },
        "artifacts": [
            {"name": "result.json", "kind": "ops-run-result", "path": "C:/runs/result.json"},
            {"name": "terminal.png", "kind": "terminal-image", "path": "C:/runs/terminal.png"},
        ],
    }


def _snapshot() -> dict[str, object]:
    return {
        "schema_version": "dashboard-cli-snapshot-v1",
        "status": "setup_required",
        "snapshot_mode": "fast_view",
        "external_calls_made": 0,
        "next_action": "Review residual rows",
        "next_command": "catalyst-radar next",
        "priced_in_queue": {
            "rows": [
                {
                    "ticker": "MSFT",
                    "state": "AddToWatchlist",
                    "signal": "filings_catalyst",
                    "next_action": "inspect",
                    "score": 77.5,
                }
            ]
        },
    }
