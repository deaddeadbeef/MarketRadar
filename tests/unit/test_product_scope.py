from __future__ import annotations

import json

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
    assert payload["scope_version"] == "event-first-cli-v1"
    assert payload["primary_surface"] == "cli"
    assert payload["receive_surface"] == "cli"
    assert "discovery" in payload["packages"]["active"]
    assert payload["desktop_pages"]["active"] == []
    assert "world-events" in payload["desktop_pages"]["deprecated"]
    assert "discovery-brief" in payload["cli_commands"]["active"]
    assert "brief" in payload["cli_commands"]["active"]
    assert "convert" in payload["cli_commands"]["active"]
    assert "bars" in payload["cli_commands"]["active"]
    assert "ready" in payload["cli_commands"]["active"]
    assert "hunt" in payload["cli_commands"]["active"]
    assert "product-scope" in payload["cli_commands"]["active"]
    assert "broker" in payload["desktop_pages"]["deprecated"]
    assert payload["investment_advice"] is False
    phases = {row["id"]: row["status"] for row in payload["removal_phases"]}
    assert phases["D1"] == "done"
    assert phases["D2"] == "done"
    assert phases["D3"] == "done"
    assert phases["D4"] == "done"
    assert phases["D5"] == "in_progress"
    assert phases["D6"] == "done"
    assert "discovery-outcomes" in payload["cli_commands"]["active"]
    assert "assert-discovery-ready" in payload["cli_commands"]["active"]
    assert "discovery-from-posts" in payload["cli_commands"]["active"]
    assert "discovery-bars" in payload["cli_commands"]["active"]
    assert "discovery-insights" in payload["cli_commands"]["active"]
    assert payload["docs"]["spec"] == "docs/designs/2026-08-15-marketradar-product-spec.md"
    assert payload["docs"]["scope"] == "docs/PRODUCT_SCOPE.md"
    assert payload["docs"]["deprecation"] == "docs/DEPRECATION.md"
    assert "World Events UI" not in payload["product"]
    assert "cli" in payload["product"].casefold()


def test_page_and_package_status() -> None:
    assert desktop_page_status("world-events") == "deprecated"
    assert desktop_page_status("broker") == "deprecated"
    assert package_status("discovery") == "active"
    assert package_status("brokers") == "deprecated"
    assert package_status("scoring") == "supporting"
    assert ACTIVE_DESKTOP_PAGES == frozenset()
    assert "world-events" in DEPRECATED_DESKTOP_PAGES
    assert "ipo" in DEPRECATED_DESKTOP_PAGES


def test_cli_deprecation_warning() -> None:
    assert cli_command_status("discovery-label") == "active"
    assert cli_command_status("brief") == "active"
    assert cli_command_status("schwab-market-sync") == "deprecated"
    warning = warn_if_deprecated_cli("agent-brief")
    assert warning is not None
    assert "DEPRECATED" in warning
    assert "World Events UI" not in warning
    assert warn_if_deprecated_cli("discovery-brief") is None
    assert warn_if_deprecated_cli("hunt") is None


def test_cli_product_aliases_parse() -> None:
    from catalyst_radar.cli import _canonical_cli_command, build_parser

    parser = build_parser()
    assert parser.parse_args(["brief"]).command == "brief"
    assert _canonical_cli_command("brief") == "discovery-brief"
    assert parser.parse_args(["brief"]).json is True
    assert parser.parse_args(["convert", "--posts", "x.json"]).command == "convert"
    assert _canonical_cli_command("convert") == "discovery-from-posts"
    assert parser.parse_args(["bars"]).command == "bars"
    assert _canonical_cli_command("bars") == "discovery-bars"
    assert parser.parse_args(["ready"]).command == "ready"
    assert _canonical_cli_command("ready") == "assert-discovery-ready"
    assert parser.parse_args(["hunt"]).command == "hunt"
    assert parser.parse_args(["product-scope"]).json is True
    assert parser.parse_args(["product-scope", "--human"]).json is False


def test_hunt_cli_prints_json(capsys) -> None:
    from catalyst_radar.cli import main

    assert main(["hunt"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "discovery-hunt-v1"
    assert payload["receive_surface"] == "cli"
    assert payload["investment_advice"] is False
    assert payload["external_calls_made"] == 0


def test_hunt_status_does_not_search_x(tmp_path) -> None:
    from catalyst_radar.discovery.hunt import build_hunt_status

    payload = build_hunt_status(root=tmp_path)
    assert payload["schema_version"] == "discovery-hunt-v1"
    assert payload["investment_advice"] is False
    assert payload["receive_surface"] == "cli"
    assert payload["status"] == "skill_required"
    assert payload["external_calls_made"] == 0
    assert "grok -p" in str(payload["headless_command"])
    assert payload["today_posts_present"] is False
