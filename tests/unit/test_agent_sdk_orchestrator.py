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
    assert brief["runtime"] == {
        "schema_version": "market-radar-agent-runtime-v1",
        "orchestrator": "openai_agents_sdk",
        "provider": "openai",
        "mode": "dry_run",
        "real_mode_gate_status": "blocked",
        "tool_surface": "specialist_agents_only",
        "copilot_dependency": "absent",
        "external_market_tools": False,
        "broker_tools": False,
        "shell_tools": False,
        "filesystem_tools": False,
        "web_tools": False,
        "max_turns": 6,
    }
    blocked = " ".join(brief["blocked_operations"])
    assert "Schwab" in blocked
    assert "order" in blocked
    assert "shell" in blocked
    assert any(
        check["name"] == "Agent runtime"
        and "openai_agents_sdk" in check["detail"]
        and "copilot_dependency=absent" in check["detail"]
        for check in brief["security_checks"]
    )
    assert any("Priced-in answer is research_only" in insight for insight in brief["insights"])
    assert any("Priced-in queue is ready" in insight for insight in brief["insights"])
    assert any("Priced-in evidence plan is attention" in insight for insight in brief["insights"])
    assert any("Priced-in source workflow is attention" in insight for insight in brief["insights"])
    assert "Review the full-scan source batch plan." in brief["next_actions"]
    assert "Plan options batches." in brief["next_actions"]
    assert "catalyst-radar priced-in-source-batches --source options" in brief["next_actions"]
    assert "Start with options; inspect the first safe chunk." in brief["next_actions"]
    assert (
        "catalyst-radar priced-in-source-batches --source options --execute-next"
        in brief["next_actions"]
    )
    assert "Review full scan source batches." in brief["next_actions"]


def test_agent_brief_includes_market_bar_unblock_options() -> None:
    payload = {
        **_dashboard_payload(),
        "priced_in_audit": {
            "market_bars": {
                "repair": {
                    "dashboard_manual_template_command": "bars manual template",
                    "dashboard_manual_import_preview_command": "bars manual import",
                    "provider_fill_plan": {
                        "provider_saved_file_capture_approval_packet": {
                            "status": "approval_required",
                            "approval_required": True,
                            "external_calls_if_approved": 1,
                            "db_writes_during_capture": 0,
                            "tui_confirm_command": "bars saved capture confirm",
                            "question": (
                                "Approve one Polygon/Massive grouped-daily call "
                                "for 2026-05-15?"
                            ),
                            "saved_file_status": "missing",
                            "post_capture_zero_call_steps": [
                                {
                                    "step": "validate_saved_file",
                                    "tui_command": "bars saved validate",
                                    "external_calls_made": 0,
                                    "db_writes_made": 0,
                                },
                                {
                                    "step": "preview_import",
                                    "tui_command": "bars saved import",
                                    "external_calls_made": 0,
                                    "db_writes_made": 0,
                                },
                            ],
                        }
                    },
                }
            }
        },
    }

    snapshot = redacted_operator_snapshot(payload)
    options = snapshot["priced_in"]["market_bar_unblock_options"]
    brief = run_market_radar_agents(payload, AppConfig.from_env({}))

    assert [option["kind"] for option in options] == [
        "manual_csv",
        "saved_provider_capture",
        "validate_saved_file",
        "preview_import",
    ]
    assert options[1]["external_calls_required"] == 1
    assert options[1]["db_writes_during_step"] == 0
    assert options[1]["command"] == "bars saved capture confirm"
    assert brief["external_calls_made"] == {
        "openai": 0,
        "market_data": 0,
        "broker": 0,
    }
    assert any(
        "Market-bar unblock options" in insight
        and "bars saved capture confirm" in insight
        for insight in brief["insights"]
    )
    assert (
        "Approve bars saved capture confirm only if one market-data call and 0 "
        "DB writes during capture match your intent."
    ) in brief["next_actions"]


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
    assert brief["runtime"]["orchestrator"] == "openai_agents_sdk"
    assert brief["runtime"]["copilot_dependency"] == "absent"
    assert brief["runtime"]["real_mode_gate_status"] == "blocked"
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
    assert snapshot["priced_in"]["total_count"] == 12087
    assert snapshot["priced_in"]["rows"][0]["ticker"] == "ACME"
    assert snapshot["priced_in"]["answer"] == {
        "schema_version": "priced-in-answer-v1",
        "status": "research_only",
        "decision_ready": False,
        "question": "Has price fully matched market expectations?",
        "answer": "Not fully priced for 5 research lead(s), but none are decision-ready yet.",
        "headline": "5 research-useful not-priced-in lead(s), 12087 scanned row(s).",
        "next_action": "Review the full-scan source batch plan.",
        "next_command": (
            "catalyst-radar priced-in-source-batches --source options --all --json"
        ),
        "external_calls_made": 0,
        "counts": {
            "total_rows": 12087,
            "research_lead_rows": 5,
            "decision_ready_rows": 0,
        },
        "decision_readiness": {},
        "trust_blockers": ["options coverage missing"],
    }
    assert snapshot["priced_in"]["source_coverage"]["actions"][0] == {
        "source": "options",
        "status": "missing",
        "coverage_pct": 0.0,
        "gap_count": 12080,
        "next_action": "Plan source batches.",
        "batch_plan_command": "catalyst-radar priced-in-source-batches --source options",
        "full_scan_gap_review_command": (
            "catalyst-radar priced-in-queue --source-gap options --limit 50"
        ),
        "full_scan_export_command": (
            "catalyst-radar priced-in-queue --source-gap options --all --json"
        ),
    }
    assert snapshot["priced_in"]["evidence_plan"] == {
        "schema_version": "priced-in-evidence-plan-v1",
        "status": "attention",
        "headline": "2 source steps need attention.",
        "next_action": "Plan options batches.",
        "next_command": "catalyst-radar priced-in-source-batches --source options",
        "external_calls_made": 0,
        "steps": [
            {
                "priority": 1,
                "area": "options",
                "status": "attention",
                "depends_on": ["market_bars", "catalyst_events", "local_text"],
                "action": "Plan options batches.",
                "command": "catalyst-radar priced-in-source-batches --source options",
                "api": "GET /api/radar/priced-in/source-batches?source=options",
            }
        ],
    }
    assert snapshot["priced_in"]["source_workflow"] == {
        "schema_version": "priced-in-source-workflow-v1",
        "status": "attention",
        "coverage_first_action": "Plan options batches.",
        "coverage_first_command": (
            "catalyst-radar priced-in-source-batches --source options --all --json"
        ),
        "decision_shortcut_action": "Start with options; inspect the first safe chunk.",
        "decision_shortcut_command": (
            "catalyst-radar priced-in-source-batches --source options --execute-next"
        ),
        "priority_scope": "full_scan_coverage",
        "decision_priority_scope": "visible_priced_in_rows",
        "overview_command": "catalyst-radar priced-in-source-batches --source all",
        "overview_api": "GET /api/radar/priced-in/source-batches?source=all",
        "external_calls_made": 0,
        "steps": [
            {
                "priority": 1,
                "source": "options",
                "status": "attention",
                "action": "Plan options batches.",
                "command": (
                    "catalyst-radar priced-in-source-batches --source options --all --json"
                ),
                "api": "GET /api/radar/priced-in/source-batches?source=options",
                "decision_useful_gap_rows": 1,
                "actionable_gap_rows": 1,
                "priority_sample_tickers": ["ACME"],
            }
        ],
    }
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
        "priced_in_queue": {
            "schema_version": "priced-in-queue-v1",
            "status": "ready",
            "headline": "Latest full scan ranked 12087 priced-in row(s); showing 1-1.",
            "next_action": "Review full scan source batches.",
            "total_count": 12087,
            "returned_count": 1,
            "count": 1,
            "has_more": True,
            "offset": 0,
            "filters": {"status": "all", "limit": 1, "offset": 0},
            "scan": {"requested_securities": 12104, "scanned_securities": 12087},
            "status_counts": {"bullish_not_priced_in": 5, "neutral": 12082},
            "usefulness_counts": {"research_useful": 5, "monitor_only": 12082},
            "rows": [
                {
                    "ticker": "ACME",
                    "priced_in_status": "bullish_not_priced_in",
                    "priced_in_direction": "bullish",
                    "emotion_reaction_gap": 49.0,
                    "emotion_score": 72.0,
                    "reaction_score": 23.0,
                    "priced_in_score": 36.36,
                    "score": 80.0,
                    "blocked": False,
                    "next_step": "Build a candidate packet.",
                    "usefulness": {
                        "status": "research_useful",
                        "label": "Research-useful mismatch",
                        "decision_ready": False,
                        "missing_for_decision": ["decision_card", "options"],
                        "next_action": "Build a Candidate Packet.",
                    },
                    "data_sources": {
                        "available": ["market_bars", "catalyst_events", "local_text"],
                        "missing": ["options", "broker_context"],
                        "summary": "available: market_bars; missing: options",
                    },
                }
            ],
        },
        "priced_in_source_coverage": {
            "schema_version": "priced-in-source-coverage-v1",
            "row_count": 12087,
            "summary": "options 0/12087",
            "weak_sources": ["options"],
            "actions": [
                {
                    "source": "options",
                    "status": "missing",
                    "coverage_pct": 0.0,
                    "gap_count": 12080,
                    "next_action": "Plan source batches.",
                    "batch_plan_command": (
                        "catalyst-radar priced-in-source-batches --source options"
                    ),
                    "full_scan_gap_review_command": (
                        "catalyst-radar priced-in-queue --source-gap options --limit 50"
                    ),
                    "full_scan_export_command": (
                        "catalyst-radar priced-in-queue --source-gap options --all --json"
                    ),
                    "api_payload": {"api_key": "secret-polygon"},
                }
            ],
        },
        "priced_in_answer": {
            "schema_version": "priced-in-answer-v1",
            "status": "research_only",
            "decision_ready": False,
            "question": "Has price fully matched market expectations?",
            "answer": (
                "Not fully priced for 5 research lead(s), but none are "
                "decision-ready yet."
            ),
            "headline": "5 research-useful not-priced-in lead(s), 12087 scanned row(s).",
            "next_action": "Review the full-scan source batch plan.",
            "next_command": (
                "catalyst-radar priced-in-source-batches --source options --all --json"
            ),
            "counts": {
                "total_rows": 12087,
                "research_lead_rows": 5,
                "decision_ready_rows": 0,
            },
            "trust_blockers": ["options coverage missing"],
            "external_calls_made": 0,
            "payload": {"api_key": "secret-polygon"},
        },
        "priced_in_preflight": {
            "status": "attention",
            "headline": "Source gaps need review.",
            "next_action": "Plan batches.",
            "scan_status": "ready",
            "evidence_plan": {
                "schema_version": "priced-in-evidence-plan-v1",
                "status": "attention",
                "headline": "2 source steps need attention.",
                "next_action": "Plan options batches.",
                "next_command": (
                    "catalyst-radar priced-in-source-batches --source options"
                ),
                "external_calls_made": 0,
                "steps": [
                    {
                        "priority": 1,
                        "area": "options",
                        "status": "attention",
                        "depends_on": [
                            "market_bars",
                            "catalyst_events",
                            "local_text",
                        ],
                        "action": "Plan options batches.",
                        "command": (
                            "catalyst-radar priced-in-source-batches --source options"
                        ),
                        "api": "GET /api/radar/priced-in/source-batches?source=options",
                    }
                ],
            },
        },
        "priced_in_source_workflow": {
            "schema_version": "priced-in-source-workflow-v1",
            "status": "attention",
            "coverage_first_action": "Plan options batches.",
            "coverage_first_command": (
                "catalyst-radar priced-in-source-batches --source options --all --json"
            ),
            "decision_shortcut_action": (
                "Start with options; inspect the first safe chunk."
            ),
            "decision_shortcut_command": (
                "catalyst-radar priced-in-source-batches --source options --execute-next"
            ),
            "priority_scope": "full_scan_coverage",
            "decision_priority_scope": "visible_priced_in_rows",
            "overview_command": "catalyst-radar priced-in-source-batches --source all",
            "overview_api": "GET /api/radar/priced-in/source-batches?source=all",
            "external_calls_made": 0,
            "steps": [
                {
                    "priority": 1,
                    "source": "options",
                    "status": "attention",
                    "action": "Plan options batches.",
                    "command": (
                        "catalyst-radar priced-in-source-batches "
                        "--source options --all --json"
                    ),
                    "api": "GET /api/radar/priced-in/source-batches?source=options",
                    "decision_useful_gap_rows": 1,
                    "actionable_gap_rows": 1,
                    "priority_sample_tickers": ["ACME"],
                    "payload": {"api_key": "secret-polygon"},
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
