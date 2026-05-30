from __future__ import annotations

from catalyst_radar.ops.capabilities import ops_capability_catalog


def test_ops_capability_catalog_is_ai_first_and_read_only() -> None:
    catalog = ops_capability_catalog()

    assert catalog["schema_version"] == "ops-capability-catalog-v1"
    assert catalog["external_calls_made"] == 0
    assert catalog["safety"]["arbitrary_shell"] is False
    assert catalog["safety"]["default_write_boundary"] == "read_only_or_artifact_write"
    assert "report.json" in catalog["agent_contract"]["evidence"]


def test_ops_capability_catalog_exposes_existing_api_surface() -> None:
    catalog = ops_capability_catalog()
    operation_paths = {operation["path"] for operation in catalog["operations"]}

    assert "/api/dashboard/snapshot" in operation_paths
    assert "/api/radar/candidates" in operation_paths
    assert "/api/radar/runs/call-plan" in operation_paths
    assert "/api/ops/runs" in operation_paths


def test_ops_capability_catalog_exposes_allowlisted_actions_only() -> None:
    catalog = ops_capability_catalog()
    actions = {action["id"]: action for action in catalog["actions"]}

    assert set(actions) == {"radar-dashboard"}
    assert actions["radar-dashboard"]["endpoint"] == "/api/ops/runs"
    assert actions["radar-dashboard"]["external_calls_made"] == 0
    assert "report.json" in actions["radar-dashboard"]["artifacts"]
    assert "report.html" in actions["radar-dashboard"]["artifacts"]
    assert "terminal.png" in actions["radar-dashboard"]["artifacts"]
