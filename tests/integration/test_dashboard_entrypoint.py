from __future__ import annotations

import ast
import importlib.util
from pathlib import Path


def test_streamlit_dashboard_is_single_command_center_entrypoint() -> None:
    page_modules = sorted(Path("apps/dashboard/pages").glob("*.py"))

    assert Path("apps/dashboard/Home.py").is_file()
    assert page_modules == []


def test_dashboard_wires_manual_review_gate_as_display_only() -> None:
    tree = ast.parse(Path("apps/dashboard/Home.py").read_text(encoding="utf-8"))
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }

    gate = functions["_show_decision_contract"]
    overview = functions["_show_overview"]
    gate_calls = [
        node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
        for node in ast.walk(gate)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute | ast.Name)
    ]
    overview_calls = [
        node.func.id
        for node in ast.walk(overview)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_show_decision_contract"
    ]
    allowed_display_calls = {
        "bool",
        "columns",
        "get",
        "_manual_review_gate_rows",
        "metric",
        "_show_records",
        "str",
        "subheader",
        "success",
        "warning",
    }

    assert overview_calls == ["_show_decision_contract"]
    assert set(gate_calls) <= allowed_display_calls
    assert {"subheader", "success", "warning", "columns", "metric"}.issubset(
        gate_calls
    )
    assert "_manual_review_gate_rows" in gate_calls
    assert "_show_records" in gate_calls


def test_dashboard_shows_radar_call_plan_before_run_post() -> None:
    tree = ast.parse(Path("apps/dashboard/Home.py").read_text(encoding="utf-8"))
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    controls = functions["_show_radar_run_controls"]
    calls = [
        (node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id, node.lineno)
        for node in ast.walk(controls)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute | ast.Name)
    ]
    call_names = [name for name, _line in calls]

    assert "radar_run_call_plan_payload" in call_names
    assert "_show_radar_call_plan" in call_names
    assert "_api_post" in call_names
    plan_line = next(line for name, line in calls if name == "radar_run_call_plan_payload")
    post_line = next(line for name, line in calls if name == "_api_post")
    assert plan_line < post_line


def test_dashboard_manual_radar_run_uses_default_scope_payload() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    controls_source = ast.get_source_segment(source, functions["_show_radar_run_controls"])

    assert controls_source is not None
    assert "radar_run_default_scope_payload" in controls_source
    assert "default_run_scope_payload" in controls_source
    assert "**run_scope_payload" in controls_source


def test_dashboard_manual_radar_run_defaults_agent_review_dry_run_on() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    controls = functions["_show_radar_run_controls"]
    checkboxes = [
        node
        for node in ast.walk(controls)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "checkbox"
    ]
    llm_checkbox = next(
        node
        for node in checkboxes
        if any(
            keyword.arg == "key"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "run_radar_llm_dry_run"
            for keyword in node.keywords
        )
    )

    assert isinstance(llm_checkbox.args[0], ast.Constant)
    assert llm_checkbox.args[0].value == "Agent review dry run"
    assert any(
        keyword.arg == "value"
        and isinstance(keyword.value, ast.Constant)
        and keyword.value.value is True
        for keyword in llm_checkbox.keywords
    )


def test_dashboard_manual_radar_run_reruns_after_successful_post() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    controls_source = ast.get_source_segment(source, functions["_show_radar_run_controls"])

    assert controls_source is not None
    assert 'st.session_state["manual_radar_run_result"] = result' in controls_source
    assert "st.rerun()" in controls_source
    assert controls_source.index("_api_post") < controls_source.index("st.rerun()")


def test_dashboard_manual_radar_run_displays_flash_before_latest_summary() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    controls_source = ast.get_source_segment(source, functions["_show_radar_run_controls"])

    assert controls_source is not None
    assert 'st.session_state.pop("manual_radar_run_result", None)' in controls_source
    assert controls_source.index("_show_radar_run_result_notice") < controls_source.index(
        "_show_radar_run_summary"
    )


def test_dashboard_radar_run_summary_uses_operator_skip_labels() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }

    summary_source = ast.get_source_segment(source, functions["_show_radar_run_summary"])
    sections_source = ast.get_source_segment(
        source,
        functions["_show_radar_operator_sections"],
    )
    optional_source = ast.get_source_segment(
        source,
        functions["_operator_optional_rows"],
    )

    assert summary_source is not None
    assert "Telemetry Rows" in summary_source
    assert "Optional Gates" in summary_source
    assert "Raw Skips Retained" in summary_source
    assert "Tracked Stages" not in summary_source
    assert "Raw Records" not in summary_source
    assert "optional_expected_gate_count" in summary_source
    assert "required_incomplete_count" in summary_source
    assert sections_source is not None
    assert "Optional gates not triggered" in sections_source
    assert "Audit-only raw telemetry" in sections_source
    assert optional_source is not None
    assert "_operator_optional_outcome_label" in optional_source
    assert "Not triggered (expected)" in source
    assert "Runs When" in optional_source
    notice_source = ast.get_source_segment(
        source,
        functions["_show_radar_run_result_notice"],
    )
    assert notice_source is not None
    assert "Radar run completed with expected gates" in notice_source
    assert "st.info" in notice_source


def test_dashboard_wires_operator_work_queue_before_activation_sections() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    overview_source = ast.get_source_segment(source, functions["_show_overview"])
    helper_source = ast.get_source_segment(source, functions["_show_operator_work_queue"])

    assert "_show_operator_work_queue" in functions
    assert "_visible_operator_queue_rows" in functions
    assert overview_source is not None
    assert helper_source is not None
    assert "operator_work_queue_payload" in helper_source
    assert "Priority Queue" in helper_source
    assert overview_source.index("_show_operator_work_queue") < overview_source.index(
        "_show_activation_summary"
    )


def test_dashboard_wires_agent_review_summary_near_radar_run() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    overview_source = ast.get_source_segment(source, functions["_show_overview"])
    helper_source = ast.get_source_segment(source, functions["_show_agent_review_summary"])

    assert "_show_agent_review_summary" in functions
    assert overview_source is not None
    assert helper_source is not None
    assert "agent_review_summary_payload" in helper_source
    assert "candidate_rows" in helper_source
    assert "Agent Review" in helper_source
    assert "Reviewed Candidate Context" in helper_source
    assert overview_source.index("_show_radar_run_controls") < overview_source.index(
        "_show_agent_review_summary"
    )
    assert overview_source.index("_show_agent_review_summary") < overview_source.index(
        "_show_discovery_snapshot"
    )


def test_dashboard_candidate_queue_surfaces_blocker_diagnostics() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    rows_helper_source = ast.get_source_segment(
        source,
        functions["_candidate_rows_with_labels"],
    )
    overview_source = ast.get_source_segment(source, functions["_show_overview"])

    assert rows_helper_source is not None
    assert "blocker_summary" in rows_helper_source
    assert "_candidate_blocker_summary" in rows_helper_source
    assert "_candidate_blocker_rows" in functions
    assert "_candidate_decision_brief_rows" in functions
    assert overview_source is not None
    assert "Risk / Blocker" in overview_source
    assert "Readiness Gate" in overview_source
    assert "Decision Brief" in overview_source
    assert "Blocker Diagnostics" in overview_source
    assert "Schwab Price" in overview_source
    assert "Schwab RVOL" in overview_source
    assert "hard_blocks" in overview_source
    assert "transition_reasons" in overview_source


def test_dashboard_overview_wires_candidate_opportunity_actions() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    overview_source = ast.get_source_segment(source, functions["_show_overview"])

    assert "_latest_opportunity_action_rows" in functions
    assert "_show_candidate_opportunity_action_form" in functions
    assert overview_source is not None
    assert "Saved Candidate Actions" in overview_source
    assert "_show_candidate_opportunity_action_form" in overview_source
    assert overview_source.index("_show_candidate_opportunity_action_form") < (
        overview_source.index("Saved Candidate Actions")
    )


def test_latest_opportunity_action_rows_queries_repository_by_selected_ticker(
    monkeypatch,
) -> None:
    module = _load_dashboard_module()
    captured = {}

    class FakeRepo:
        def __init__(self, engine) -> None:
            captured["engine"] = engine

        def list_opportunity_actions(self, *, ticker=None, limit=100):
            captured["ticker"] = ticker
            captured["limit"] = limit
            return [
                {"ticker": ticker, "action": "watch"},
                {"ticker": ticker, "action": "ready"},
            ]

    monkeypatch.setattr(module, "BrokerRepository", FakeRepo)
    monkeypatch.setattr(module, "opportunity_action_payload", lambda row: row)
    engine = object()

    rows = module._latest_opportunity_action_rows(engine, "msft")  # noqa: SLF001

    assert [row["action"] for row in rows] == ["watch", "ready"]
    assert captured == {"engine": engine, "ticker": "MSFT", "limit": 3}


def test_candidate_opportunity_action_form_requires_analyst_role() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    helper_source = ast.get_source_segment(
        source,
        functions["_show_candidate_opportunity_action_form"],
    )

    assert helper_source is not None
    assert "role_allows(dashboard_role, Role.ANALYST)" in helper_source
    assert "record_opportunity_action" in helper_source
    assert 'actor_source="dashboard"' in helper_source
    assert "actor_role=dashboard_role.value" in helper_source


def test_candidate_schwab_context_refresh_is_explicit_and_rate_guarded() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    overview_source = ast.get_source_segment(source, functions["_show_overview"])
    helper_source = ast.get_source_segment(
        source,
        functions["_show_candidate_schwab_context_refresh"],
    )

    assert overview_source is not None
    assert "_show_candidate_schwab_context_refresh" in overview_source
    assert helper_source is not None
    assert "role_allows(dashboard_role, Role.ANALYST)" in helper_source
    assert "Refresh Schwab Context" in helper_source
    assert '"/api/brokers/schwab/market-sync"' in helper_source
    assert '"include_history": True' in helper_source
    assert '"include_options": True' in helper_source
    assert "market-sync cooldown" in helper_source
    assert 'st.session_state["candidate_schwab_refresh_message"]' in helper_source
    assert "st.rerun()" in helper_source


def test_broker_write_controls_require_analyst_role() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }

    for name in [
        "_show_broker_controls",
        "_show_opportunity_action_form",
        "_show_candidate_schwab_context_refresh",
        "_show_trigger_form",
        "_show_order_ticket_form",
    ]:
        helper_source = ast.get_source_segment(source, functions[name])
        assert helper_source is not None
        assert "role_allows(dashboard_role, Role.ANALYST)" in helper_source

    action_source = ast.get_source_segment(source, functions["_show_opportunity_action_form"])
    trigger_source = ast.get_source_segment(source, functions["_show_trigger_form"])
    ticket_source = ast.get_source_segment(source, functions["_show_order_ticket_form"])
    assert action_source is not None
    assert trigger_source is not None
    assert ticket_source is not None
    assert 'actor_source="dashboard"' in action_source
    assert 'actor_source="dashboard"' in trigger_source
    assert 'actor_source="dashboard"' in ticket_source

    broker_source = ast.get_source_segment(source, functions["_show_broker_layer"])
    assert broker_source is not None
    assert "dashboard_role: Role" in broker_source
    assert "dashboard_role=dashboard_role" in broker_source


def test_dashboard_wires_candidate_delta_before_actionability() -> None:
    tree = ast.parse(Path("apps/dashboard/Home.py").read_text(encoding="utf-8"))
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    overview = functions["_show_overview"]
    calls = [
        (node.func.id, node.lineno)
        for node in ast.walk(overview)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    line_by_call = {name: line for name, line in calls}

    assert "_show_candidate_delta" in line_by_call
    assert (
        line_by_call["_show_research_shortlist"]
        < line_by_call["_show_candidate_delta"]
        < line_by_call["_show_actionability_breakdown"]
    )


def test_candidate_blocker_summary_uses_transition_reasons() -> None:
    module = _load_dashboard_module()

    assert module._candidate_blocker_summary(  # noqa: SLF001
        {
            "hard_blocks": [],
            "portfolio_hard_blocks": [],
            "transition_reasons": ["candidate data is stale"],
        }
    ) == "candidate data is stale"


def test_manual_review_gate_rows_explain_high_score_blockers() -> None:
    module = _load_dashboard_module()

    rows = module._manual_review_gate_rows(  # noqa: SLF001
        [
            {
                "ticker": "AAA",
                "state": "Warning",
                "final_score": 100,
                "decision_card_id": None,
                "hard_blocks": [],
                "portfolio_hard_blocks": [],
                "transition_reasons": ["trade_plan_required"],
                "risk_or_gap": "Trade plan is incomplete",
            },
            {
                "ticker": "BBB",
                "state": "EligibleForManualBuyReview",
                "final_score": 91,
                "decision_card_id": "",
                "transition_reasons": [],
            },
            {
                "ticker": "CCC",
                "state": "EligibleForManualBuyReview",
                "final_score": 92,
                "decision_card_id": "card-ccc",
                "transition_reasons": [],
            },
        ]
    )

    assert rows[0]["Ticker"] == "AAA"
    assert rows[0]["Gate Status"] == "blocked"
    assert rows[0]["Why Not Ready"] == "trade_plan_required"
    assert rows[1]["Gate Status"] == "needs decision card"
    assert rows[1]["Why Not Ready"] == "Decision card is required."
    assert rows[2]["Gate Status"] == "ready"


def test_dashboard_wires_research_shortlist_after_manual_review_gate() -> None:
    tree = ast.parse(Path("apps/dashboard/Home.py").read_text(encoding="utf-8"))
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    overview = functions["_show_overview"]
    calls = [
        (node.func.id, node.lineno)
        for node in ast.walk(overview)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    line_by_call = {name: line for name, line in calls}

    assert "_show_research_shortlist" in line_by_call
    assert (
        line_by_call["_show_decision_contract"]
        < line_by_call["_show_research_shortlist"]
        < line_by_call["_show_actionability_breakdown"]
    )


def test_dashboard_research_shortlist_strips_restricted_audit_from_visible_rows() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    helper = functions["_visible_shortlist_rows"]

    assert "audit" in ast.get_source_segment(source, helper)
    assert any(
        isinstance(node, ast.Compare)
        and isinstance(node.left, ast.Name)
        and node.left.id == "key"
        and any(isinstance(operator, ast.NotEq) for operator in node.ops)
        and any(
            isinstance(comparator, ast.Constant) and comparator.value == "audit"
            for comparator in node.comparators
        )
        for node in ast.walk(helper)
    )


def test_dashboard_wires_live_data_activation_contract_after_plan() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    overview = functions["_show_overview"]
    calls = [
        (node.func.id, node.lineno)
        for node in ast.walk(overview)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    line_by_call = {name: line for name, line in calls}

    assert "_show_live_data_activation_contract" in line_by_call
    assert (
        line_by_call["_show_live_activation_plan"]
        < line_by_call["_show_live_data_activation_contract"]
        < line_by_call["_show_telemetry_tape"]
    )
    contract_source = ast.get_source_segment(
        source,
        functions["_show_live_data_activation_contract"],
    )
    assert contract_source is not None
    assert "minimum_env_lines" in contract_source
    assert "Minimum .env.local block" in contract_source
    assert "Worker automation handoff" in contract_source
    assert "worker_env_lines" in contract_source
    assert "worker_commands" in contract_source


def test_dashboard_wires_alert_planning_diagnostics_after_readiness() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    overview_source = ast.get_source_segment(source, functions["_show_overview"])
    helper_source = ast.get_source_segment(
        source,
        functions["_show_alert_planning_diagnostics"],
    )

    assert overview_source is not None
    assert helper_source is not None
    assert "_show_alert_planning_diagnostics" in functions
    assert "alert_planning_diagnostics_payload" in helper_source
    assert overview_source.index("readiness_checklist_payload") < overview_source.index(
        "_show_alert_planning_diagnostics"
    )


def _load_dashboard_module():
    path = Path("apps/dashboard/Home.py")
    spec = importlib.util.spec_from_file_location("dashboard_home_for_test", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module
