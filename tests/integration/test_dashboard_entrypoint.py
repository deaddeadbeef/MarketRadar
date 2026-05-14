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
