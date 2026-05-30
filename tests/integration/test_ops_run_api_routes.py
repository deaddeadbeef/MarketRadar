from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from apps.api.main import create_app
from catalyst_radar.storage.db import create_schema, engine_from_url


def test_get_ops_capabilities_returns_ai_first_catalog(tmp_path: Path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "ops-capabilities.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.get("/api/ops/capabilities")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "ops-capability-catalog-v1"
    assert payload["external_calls_made"] == 0
    assert payload["safety"]["arbitrary_shell"] is False
    assert any(
        operation["path"] == "/api/dashboard/snapshot"
        for operation in payload["operations"]
    )


def test_get_ops_actions_returns_allowlisted_actions(tmp_path: Path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "ops-actions.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.get("/api/ops/actions")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "ops-action-catalog-v1"
    assert [action["id"] for action in payload["actions"]] == ["radar-dashboard"]


def test_post_ops_run_creates_artifacts_and_downloads_them(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "ops-run-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.post(
        "/api/ops/runs",
        json={
            "action": "radar-dashboard",
            "page": "overview",
            "renderer": "python",
            "frame_width": 100,
            "frame_height": 30,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["summary"]["external_calls_made"] == 0
    assert {artifact["name"] for artifact in payload["artifacts"]} >= {
        "result.json",
        "snapshot.json",
        "terminal.txt",
        "terminal.png",
    }

    show = client.get(f"/api/ops/runs/{payload['run_id']}")
    assert show.status_code == 200
    assert show.json()["run_id"] == payload["run_id"]

    text_artifact = client.get(f"/api/ops/runs/{payload['run_id']}/artifacts/terminal.txt")
    assert text_artifact.status_code == 200
    assert "MarketRadar" in text_artifact.text or "MARKET" in text_artifact.text

    png_artifact = client.get(f"/api/ops/runs/{payload['run_id']}/artifacts/terminal.png")
    assert png_artifact.status_code == 200
    assert png_artifact.content.startswith(b"\x89PNG\r\n\x1a\n")


def test_post_ops_run_rejects_unapproved_action(tmp_path: Path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "ops-run-api-bad-action.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.post(
        "/api/ops/runs",
        json={"action": "powershell", "renderer": "python"},
    )

    assert response.status_code == 400
    assert "unsupported ops action" in response.json()["detail"]


def test_get_ops_artifact_rejects_path_traversal(tmp_path: Path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "ops-run-api-traversal.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    _create_database(database_url)
    client = TestClient(create_app())
    created = client.post(
        "/api/ops/runs",
        json={"action": "radar-dashboard", "renderer": "python"},
    ).json()

    response = client.get(f"/api/ops/runs/{created['run_id']}/artifacts/..%2Fsnapshot.json")

    assert response.status_code in {400, 404}


def _database_url(tmp_path: Path, filename: str) -> str:
    return f"sqlite:///{(tmp_path / filename).as_posix()}"


def _create_database(database_url: str):
    engine = engine_from_url(database_url)
    create_schema(engine)
    return engine
