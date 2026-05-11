from __future__ import annotations

from pathlib import Path


def test_streamlit_dashboard_is_single_command_center_entrypoint() -> None:
    page_modules = sorted(Path("apps/dashboard/pages").glob("*.py"))

    assert Path("apps/dashboard/Home.py").is_file()
    assert page_modules == []
