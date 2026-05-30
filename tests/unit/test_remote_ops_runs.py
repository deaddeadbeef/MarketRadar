from __future__ import annotations

import json
from pathlib import Path

import pytest

from catalyst_radar.ops import remote_runs as remote_runs_module
from catalyst_radar.ops.remote_runs import (
    OpsRunError,
    create_ops_run,
    load_ops_run,
    resolve_ops_artifact,
)


def test_run_allowlisted_dashboard_creates_artifacts(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'ops-run.db').as_posix()}"
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))

    result = create_ops_run(
        action="radar-dashboard",
        page="overview",
        renderer="python",
        frame_width=100,
        frame_height=30,
        database_url=database_url,
    )

    assert result["schema_version"] == "ops-run-v1"
    assert result["status"] == "completed"
    assert result["action"] == "radar-dashboard"
    assert result["page"] == "overview"
    assert result["summary"]["external_calls_made"] == 0
    artifact_names = {artifact["name"] for artifact in result["artifacts"]}
    assert {"result.json", "snapshot.json", "terminal.txt", "terminal.png"} <= artifact_names
    assert Path(result["run_dir"]).is_dir()
    assert Path(result["run_dir"], "terminal.png").read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


def test_load_ops_run_reads_persisted_result(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'ops-run.db').as_posix()}"
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    created = create_ops_run(
        action="radar-dashboard",
        page="overview",
        renderer="python",
        database_url=database_url,
    )

    loaded = load_ops_run(str(created["run_id"]))

    assert loaded["run_id"] == created["run_id"]
    assert loaded["status"] == "completed"


def test_run_rejects_unapproved_action(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))

    with pytest.raises(OpsRunError, match="unsupported ops action"):
        create_ops_run(action="powershell", page="overview", renderer="python")


def test_run_rejects_unapproved_renderer(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))

    with pytest.raises(OpsRunError, match="unsupported renderer"):
        create_ops_run(action="radar-dashboard", page="overview", renderer="shell")


def test_resolve_artifact_rejects_path_traversal(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'ops-run.db').as_posix()}"
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    result = create_ops_run(
        action="radar-dashboard",
        page="overview",
        renderer="python",
        database_url=database_url,
    )

    with pytest.raises(OpsRunError, match="invalid artifact name"):
        resolve_ops_artifact(str(result["run_id"]), "../snapshot.json")


def test_resolve_artifact_returns_known_artifact(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'ops-run.db').as_posix()}"
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    result = create_ops_run(
        action="radar-dashboard",
        page="overview",
        renderer="python",
        database_url=database_url,
    )

    artifact = resolve_ops_artifact(str(result["run_id"]), "snapshot.json")

    assert artifact.name == "snapshot.json"
    assert json.loads(artifact.read_text(encoding="utf-8"))["schema_version"]


def test_copy_to_onedrive_records_destination(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'ops-run.db').as_posix()}"
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("OneDrive", str(tmp_path / "OneDrive"))

    result = create_ops_run(
        action="radar-dashboard",
        page="overview",
        renderer="python",
        copy_to_onedrive=True,
        database_url=database_url,
    )

    assert result["onedrive"]["status"] == "copied"
    onedrive_dir = Path(result["onedrive"]["path"])
    assert onedrive_dir.name == result["run_id"]
    assert (onedrive_dir / "terminal.png").exists()


def test_rust_renderer_decodes_subprocess_output_as_utf8(tmp_path: Path, monkeypatch) -> None:
    exe = tmp_path / "target" / "release" / "radar-tui.exe"
    exe.parent.mkdir(parents=True)
    exe.write_text("", encoding="utf-8")
    captured: dict[str, object] = {}

    class Completed:
        returncode = 0
        stdout = "┌ MARKETRADAR ┐\n"
        stderr = ""

    def fake_run(*_args, **kwargs):
        captured.update(kwargs)
        return Completed()

    monkeypatch.setattr(remote_runs_module, "_repo_root", lambda: tmp_path)
    monkeypatch.setattr(remote_runs_module.subprocess, "run", fake_run)

    result = remote_runs_module._render_with_rust(
        page="overview",
        frame_width=100,
        frame_height=30,
        database_url="sqlite:///:memory:",
    )

    assert result.text == "┌ MARKETRADAR ┐\n"
    assert captured["encoding"] == "utf-8"
    assert captured["errors"] == "replace"
