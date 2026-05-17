from __future__ import annotations

import json

from catalyst_radar.agents.sdk_orchestrator import (
    agent_sdk_gate_payload,
    redacted_operator_snapshot,
    run_market_radar_agents,
)
from catalyst_radar.core.config import AppConfig


def test_agent_sdk_dry_run_brief_is_multi_agent_and_zero_call() -> None:
    payload = _dashboard_payload()
    brief = run_market_radar_agents(payload, AppConfig.from_env({}))

    assert brief["schema_version"] == "market-radar-agent-brief-v1"
    assert brief["mode"] == "dry_run"
    assert brief["status"] == "dry_run"
    assert [agent["agent"] for agent in brief["agents"]] == [
        "Data Sentinel",
        "Catalyst Analyst",
        "Risk Officer",
        "Operator",
    ]
    assert brief["external_calls_made"] == {
        "openai": 0,
        "market_data": 0,
        "broker": 0,
    }
    blocked = " ".join(brief["blocked_operations"])
    assert "Schwab" in blocked
    assert "order" in blocked
    assert "shell" in blocked


def test_agent_sdk_real_mode_gate_fails_closed_without_secret_leak() -> None:
    config = AppConfig.from_env({"OPENAI_API_KEY": "sk-test-secret"})

    gate = agent_sdk_gate_payload(config)
    brief = run_market_radar_agents(_dashboard_payload(), config, real=True)

    gate_text = json.dumps(gate, sort_keys=True)
    brief_text = json.dumps(brief, sort_keys=True)
    assert gate["status"] == "blocked"
    assert "CATALYST_ENABLE_AGENT_SDK=true" in gate["missing_env"]
    assert "CATALYST_AGENT_SDK_MODEL" in gate["missing_env"]
    assert "sk-test-secret" not in gate_text
    assert brief["mode"] == "blocked"
    assert brief["status"] == "blocked"
    assert brief["external_calls_made"]["openai"] == 0
    assert "sk-test-secret" not in brief_text


def test_redacted_operator_snapshot_allowlists_dashboard_fields() -> None:
    snapshot = redacted_operator_snapshot(
        {
            **_dashboard_payload(),
            "alerts": {
                "count": 1,
                "rows": [
                    {
                        "id": "alert-1",
                        "ticker": "ACME",
                        "title": "ACME alert",
                        "payload": {"api_key": "secret-polygon"},
                    }
                ],
            },
            "broker": {
                "snapshot": {"broker": "schwab", "connection_status": "connected"},
                "exposure": {
                    "read_only": True,
                    "order_submission_enabled": False,
                    "access_token": "secret-token",
                },
            },
        }
    )

    serialized = json.dumps(snapshot, sort_keys=True)
    assert snapshot["schema_version"] == "market-radar-agent-snapshot-v1"
    assert snapshot["alerts"]["rows"][0] == {
        "id": "alert-1",
        "ticker": "ACME",
        "title": "ACME alert",
    }
    assert "payload" not in serialized
    assert "secret-polygon" not in serialized
    assert "secret-token" not in serialized


def _dashboard_payload() -> dict[str, object]:
    return {
        "schema_version": "dashboard-cli-snapshot-v1",
        "controls": {"ticker": "ACME", "available_at": "2026-05-17T00:00:00Z"},
        "runtime_context": {
            "daily_market_provider": "polygon",
            "daily_event_provider": "sec",
            "polygon_key_configured": True,
            "sec_live_enabled": True,
            "schwab_credentials_configured": True,
            "openai_key_configured": False,
        },
        "readiness": {
            "status": "research_only",
            "safe_to_make_investment_decision": False,
            "next_action": "Review candidates manually.",
            "market_radar_usefulness": {
                "status": "research_useful",
                "next_action": "Work the queue.",
            },
            "readiness_checklist": [
                {
                    "area": "Order safety",
                    "status": "safe",
                    "finding": "Real order submission is disabled.",
                    "next_action": "Keep the kill switch off.",
                }
            ],
        },
        "operator_next_step": {
            "status": "blocked",
            "priority": "must_fix",
            "area": "Candidate",
            "item": "ACME needs review",
            "ticker": "ACME",
            "action": "Open candidate ACME before acting.",
        },
        "operator_work_queue": {
            "status": "research",
            "headline": "1 candidate needs research.",
            "next_action": "Open ACME.",
            "safe_to_make_investment_decision": False,
            "rows": [
                {
                    "priority": "research",
                    "area": "Candidate",
                    "item": "ACME: catalyst review",
                    "status": "research_now",
                    "ticker": "ACME",
                    "next_action": "Open candidate detail.",
                    "evidence": "fresh catalyst",
                }
            ],
        },
        "call_plan": {
            "status": "live_calls_planned",
            "headline": "Radar run may make up to 3 external calls.",
            "next_action": "Run only if this matches your intent.",
            "will_call_external_providers": True,
            "max_external_call_count": 3,
            "rows": [
                {
                    "layer": "Market data",
                    "provider": "polygon",
                    "status": "live_call_planned",
                    "external_call_count_max": 1,
                    "next_action": "Check provider health.",
                }
            ],
        },
        "candidates": {
            "count": 1,
            "rows": [
                {
                    "ticker": "ACME",
                    "state": "Blocked",
                    "final_score": 80.0,
                    "hard_blocks": ["data_stale"],
                    "setup_type": "filings_catalyst",
                    "top_event_title": "ACME files S-1 update",
                    "research_brief": {"external_export_blocked": True},
                }
            ],
        },
        "alerts": {"count": 0, "rows": []},
        "broker": {
            "snapshot": {"broker": "schwab", "connection_status": "connected"},
            "exposure": {"read_only": True, "order_submission_enabled": False},
        },
        "ops_health": {
            "database": {"latest_daily_bar_date": "2026-05-15"},
            "providers": [{"provider": "polygon", "status": "healthy"}],
        },
        "telemetry": {"status": "ready", "headline": "Telemetry is healthy."},
        "external_calls_made": 0,
    }
