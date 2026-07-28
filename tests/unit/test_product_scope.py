from __future__ import annotations

from catalyst_radar.deprecation import (
    ACTIVE_CLI_COMMANDS,
    ACTIVE_DESKTOP_PAGES,
    DEPRECATED_DESKTOP_PAGES,
    LEGACY_WORKBENCH_ENV,
    REMOVAL_PHASES,
    block_deprecated_cli,
    cli_command_status,
    desktop_page_status,
    legacy_workbench_enabled,
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
    assert "discovery-from-x" in payload["cli_commands"]["active"]
    assert "broker" in payload["desktop_pages"]["deprecated"]
    assert payload["investment_advice"] is False
    assert payload["legacy_env"] == LEGACY_WORKBENCH_ENV
    assert "default_nav" in payload["desktop_pages"]
    assert set(payload["desktop_pages"]["default_nav"]) == set(ACTIVE_DESKTOP_PAGES)


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
    assert cli_command_status("discovery-from-x") == "active"
    assert cli_command_status("schwab-market-sync") == "deprecated"
    warning = warn_if_deprecated_cli("agent-brief")
    assert warning is not None
    assert "DEPRECATED" in warning
    assert warn_if_deprecated_cli("discovery-brief") is None


def test_phases_d2_d3_done() -> None:
    by_id = {row["id"]: row for row in REMOVAL_PHASES}
    assert by_id["D2"]["status"] == "done"
    assert by_id["D3"]["status"] == "done"
    assert by_id["D4"]["status"] == "planned"


def test_discovery_from_x_in_active_commands() -> None:
    assert "discovery-from-x" in ACTIVE_CLI_COMMANDS
    assert cli_command_status("discovery-from-x") == "active"


def test_block_deprecated_cli_when_legacy_off(monkeypatch) -> None:
    monkeypatch.delenv(LEGACY_WORKBENCH_ENV, raising=False)
    assert legacy_workbench_enabled() is False
    blocked = block_deprecated_cli("agent-brief")
    assert blocked is not None
    assert "BLOCKED" in blocked
    assert block_deprecated_cli("discovery-brief") is None
    assert block_deprecated_cli("discovery-from-x") is None
    payload = product_scope_payload()
    assert payload["legacy_workbench_enabled"] is False
    assert payload["cli_commands"]["deprecated_default_reachable"] is False


def test_block_deprecated_cli_allows_when_legacy_on(monkeypatch) -> None:
    monkeypatch.setenv(LEGACY_WORKBENCH_ENV, "1")
    assert legacy_workbench_enabled() is True
    assert block_deprecated_cli("agent-brief") is None
    assert warn_if_deprecated_cli("agent-brief") is not None
    payload = product_scope_payload()
    assert payload["legacy_workbench_enabled"] is True
    assert payload["cli_commands"]["deprecated_default_reachable"] is True


def test_main_agent_brief_returns_2_when_legacy_off(monkeypatch) -> None:
    monkeypatch.delenv(LEGACY_WORKBENCH_ENV, raising=False)
    from catalyst_radar.cli import main

    code = main(["agent-brief", "--json"])
    assert code == 2


def test_main_discovery_brief_still_active(monkeypatch) -> None:
    monkeypatch.delenv(LEGACY_WORKBENCH_ENV, raising=False)
    from catalyst_radar.cli import main

    code = main(
        [
            "discovery-brief",
            "--events",
            "data/sample/world_events.json",
            "--no-db",
            "--json",
        ]
    )
    assert code == 0
