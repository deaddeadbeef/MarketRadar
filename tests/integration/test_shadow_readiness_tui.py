from __future__ import annotations

from catalyst_radar.dashboard.tui import render_dashboard_tui


def test_readiness_page_renders_trial_gate_contract() -> None:
    text = render_dashboard_tui(
        {
            "readiness": {
                "status": "research_only",
                "decision_mode": "research_only",
                "headline": "Research only.",
                "next_action": "Open the dry-run gate.",
                "evidence": "local snapshot",
                "readiness_checklist": [],
            },
            "shadow_readiness": {
                "status": "blocked",
                "ready": False,
                "canonical_next_action": "Seed the scan universe.",
                "useful_definition": "Useful means shadow alerts can be judged.",
                "call_boundary": {
                    "planned_run_external_call_count_max": 1,
                },
                "checks": [
                    {
                        "code": "active_universe",
                        "status": "blocked",
                        "finding": "No active universe is loaded.",
                        "next_action": "Seed the scan universe.",
                    }
                ],
            },
            "operator_work_queue": {
                "status": "blocked",
                "headline": "Setup blocked.",
                "rows": [],
            },
        },
        page="readiness",
        width=140,
    )

    assert "Trial gate" in text
    assert "dry-run alerts" in text
    assert "trial run max=1" in text
    assert "active universe" in text
