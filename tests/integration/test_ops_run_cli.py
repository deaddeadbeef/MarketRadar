from __future__ import annotations

import json
from pathlib import Path

from catalyst_radar.cli import main


def test_ops_run_cli_outputs_json_and_artifacts(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'ops-run-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))

    assert (
        main(
            [
                "ops",
                "run",
                "radar-dashboard",
                "--page",
                "overview",
                "--renderer",
                "python",
                "--frame-width",
                "100",
                "--frame-height",
                "30",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "completed"
    assert payload["summary"]["external_calls_made"] == 0
    assert Path(payload["run_dir"], "terminal.png").exists()
    assert Path(payload["run_dir"], "report.html").exists()
    assert any(artifact["name"] == "report.json" for artifact in payload["artifacts"])


def test_ops_show_cli_outputs_existing_run(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'ops-show-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))

    assert main(["ops", "run", "radar-dashboard", "--renderer", "python"]) == 0
    created = json.loads(capsys.readouterr().out)

    assert main(["ops", "show", created["run_id"]]) == 0
    shown = json.loads(capsys.readouterr().out)

    assert shown["run_id"] == created["run_id"]


def test_ops_capabilities_cli_outputs_catalog_by_default(capsys) -> None:
    assert main(["ops", "capabilities"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == "ops-capability-catalog-v1"
    assert any(action["id"] == "radar-dashboard" for action in payload["actions"])


def test_ops_run_cli_human_mode_outputs_summary(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'ops-run-human-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))

    assert main(["ops", "run", "radar-dashboard", "--renderer", "python", "--human"]) == 0

    assert "ops_run id=" in capsys.readouterr().out
