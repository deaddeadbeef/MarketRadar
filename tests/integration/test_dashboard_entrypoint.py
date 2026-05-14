from __future__ import annotations

import ast
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
