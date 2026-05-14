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
        "metric",
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

    assert summary_source is not None
    assert "Run Steps" in summary_source
    assert "Expected Skips" in summary_source
    assert "Skipped Raw" in summary_source
    assert "Tracked Stages" not in summary_source
    assert "Raw Records" not in summary_source
    assert "optional_expected_gate_count" in summary_source
    assert "required_incomplete_count" in summary_source
    assert sections_source is not None
    assert "Expected skipped gates" in sections_source


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
    assert overview_source is not None
    assert "Risk / Blocker" in overview_source
    assert "Blocker Diagnostics" in overview_source
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


def test_broker_write_controls_require_analyst_role() -> None:
    source = Path("apps/dashboard/Home.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }

    for name in [
        "_show_broker_controls",
        "_show_opportunity_action_form",
        "_show_trigger_form",
        "_show_order_ticket_form",
    ]:
        helper_source = ast.get_source_segment(source, functions[name])
        assert helper_source is not None
        assert "role_allows(dashboard_role, Role.ANALYST)" in helper_source

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

    assert "_show_live_data_activation_contract" in line_by_call
    assert (
        line_by_call["_show_live_activation_plan"]
        < line_by_call["_show_live_data_activation_contract"]
        < line_by_call["_show_telemetry_tape"]
    )


def _load_dashboard_module():
    path = Path("apps/dashboard/Home.py")
    spec = importlib.util.spec_from_file_location("dashboard_home_for_test", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module
