from __future__ import annotations

from catalyst_radar.deprecation import (
    ACTIVE_DESKTOP_PAGES,
    DEPRECATED_DESKTOP_PAGES,
    cli_command_status,
    desktop_page_status,
    package_status,
    product_scope_payload,
    warn_if_deprecated_cli,
)


def test_product_scope_payload_lists_event_first_core() -> None:
    payload = product_scope_payload()
    assert payload["scope_version"] == "event-first-discovery-v1"
    assert "discovery" in payload["packages"]["active"]
    assert "world-events" in payload["desktop_pages"]["active"]
    assert "discovery-brief" in payload["cli_commands"]["active"]
    assert "broker" in payload["desktop_pages"]["deprecated"]
    assert payload["investment_advice"] is False
    phases = {row["id"]: row["status"] for row in payload["removal_phases"]}
    assert phases["D1"] == "done"
    assert phases["D2"] == "done"
    assert phases["D3"] == "done"
    assert phases["D4"] == "done"
    assert phases["D5"] == "in_progress"
    assert "discovery-outcomes" in payload["cli_commands"]["active"]
    assert "assert-discovery-ready" in payload["cli_commands"]["active"]
    assert "discovery-from-posts" in payload["cli_commands"]["active"]
    assert "discovery-bars" in payload["cli_commands"]["active"]
    assert "discovery-insights" in payload["cli_commands"]["active"]


def test_page_and_package_status() -> None:
    assert desktop_page_status("world-events") == "active"
    assert desktop_page_status("broker") == "deprecated"
    assert package_status("discovery") == "active"
    assert package_status("brokers") == "deprecated"
    assert package_status("scoring") == "supporting"
    assert "world-events" in ACTIVE_DESKTOP_PAGES
    assert "ipo" in DEPRECATED_DESKTOP_PAGES


def test_cli_deprecation_warning() -> None:
    assert cli_command_status("discovery-label") == "active"
    assert cli_command_status("schwab-market-sync") == "deprecated"
    warning = warn_if_deprecated_cli("agent-brief")
    assert warning is not None
    assert "DEPRECATED" in warning
    assert warn_if_deprecated_cli("discovery-brief") is None
