from __future__ import annotations

from pathlib import Path

from catalyst_radar.dashboard.design import DASHBOARD_STYLE


def test_design_contract_has_canonical_sections_in_order() -> None:
    text = Path("DESIGN.md").read_text(encoding="utf-8")
    sections = [
        line.removeprefix("## ").strip()
        for line in text.splitlines()
        if line.startswith("## ")
    ]

    assert sections == [
        "Overview",
        "Colors",
        "Typography",
        "Layout",
        "Elevation & Depth",
        "Shapes",
        "Components",
        "Do's and Don'ts",
    ]


def test_dashboard_style_uses_design_contract_tokens() -> None:
    for token_value in (
        "#191C1F",
        "#F7F8FA",
        "#FFFFFF",
        "#D9DEE5",
        "#0B7A53",
        "#A16207",
        "#B42318",
    ):
        assert token_value in DASHBOARD_STYLE
