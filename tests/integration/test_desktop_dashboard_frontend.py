from __future__ import annotations

from pathlib import Path


def test_tauri_dashboard_json_command_targets_focusable_output() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert 'id="snapshot-json-output"' in source
    assert 'data-testid="snapshot-json-output"' in source
    assert 'tabindex="0"' in source
    assert 'role="textbox"' in source
    assert 'aria-readonly="true"' in source
    assert "const shouldFocusCommand = await applyCommand(raw);" in source
    assert "if (shouldFocusCommand !== false) input.focus();" in source
    assert "qs('#snapshot-json-output')?.focus?.()" in source
    assert "return false;" in source


def test_tauri_dashboard_exposes_cli_command_reference_families() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "data-testid=\"command-reference\"" in source
    assert "['v', 'costs']" in source
    assert "renderCosts" in source
    for command in (
        "export full / export current",
        "batch SOURCE / batch SOURCE execute",
        "bars manual template/import",
        "options template/validate/import",
        "cik template/validate/import",
        "ledger coverage / record",
        "outcome coverage / update",
    ):
        assert command in source
