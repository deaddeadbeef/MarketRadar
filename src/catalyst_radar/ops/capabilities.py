from __future__ import annotations

from datetime import UTC, datetime


def ops_capability_catalog() -> dict[str, object]:
    return {
        "schema_version": "ops-capability-catalog-v1",
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "external_calls_made": 0,
        "system": {
            "name": "MarketRadar operations API",
            "purpose": (
                "AI-first control surface for read-only market radar review, "
                "safe run planning, dashboard evidence capture, and artifact retrieval."
            ),
        },
        "safety": {
            "arbitrary_shell": False,
            "default_write_boundary": "read_only_or_artifact_write",
            "artifact_root": "CATALYST_OPS_RUN_DIR or .state/ops-runs",
            "provider_calls": (
                "Catalog and dashboard artifact runs make zero provider calls. "
                "Routes that can call providers are marked explicitly."
            ),
        },
        "actions": _actions(),
        "operations": _operations(),
        "agent_contract": {
            "discovery": "Call GET /api/ops/capabilities before choosing a route.",
            "execution": (
                "Use POST /api/ops/runs for allowlisted artifact-producing actions. "
                "Do not pass raw shell commands through the API."
            ),
            "evidence": (
                "Prefer report.json for aggregation, result.json for run metadata, "
                "snapshot.json for raw dashboard state, report.html for human review, "
                "terminal.txt for transcript review, and terminal.png for a shareable "
                "visual artifact."
            ),
            "roles": {
                "viewer": "read-only status, telemetry, snapshots, and artifacts",
                "analyst": "approved run creation and plan execution endpoints",
                "admin": "all analyst and viewer operations",
            },
        },
    }


def ops_action_catalog() -> dict[str, object]:
    return {
        "schema_version": "ops-action-catalog-v1",
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "external_calls_made": 0,
        "actions": _actions(),
    }


def _actions() -> list[dict[str, object]]:
    return [
        {
            "id": "radar-dashboard",
            "label": "Capture dashboard evidence",
            "endpoint": "/api/ops/runs",
            "method": "POST",
            "role": "analyst",
            "safety": "read_only_artifact_write",
            "external_calls_made": 0,
            "description": (
                "Creates a headless dashboard run for an approved page and writes "
                "aggregation JSON, report HTML, terminal transcript, and PNG artifacts."
            ),
            "input_schema": {
                "type": "object",
                "required": ["action"],
                "properties": {
                    "action": {"const": "radar-dashboard"},
                    "page": {
                        "type": "string",
                        "default": "overview",
                        "description": (
                            "Dashboard page alias such as overview, run, ops, or telemetry."
                        ),
                    },
                    "renderer": {
                        "type": "string",
                        "enum": ["auto", "rust", "python"],
                        "default": "auto",
                    },
                    "frame_width": {"type": "integer", "minimum": 80, "maximum": 240},
                    "frame_height": {"type": "integer", "minimum": 24, "maximum": 80},
                    "copy_to_onedrive": {"type": "boolean", "default": False},
                },
            },
            "artifacts": [
                "result.json",
                "report.json",
                "report.html",
                "snapshot.json",
                "terminal.txt",
                "terminal.png",
            ],
            "next_step": "GET /api/ops/runs/{run_id} or download artifacts by name.",
        }
    ]


def _operations() -> list[dict[str, object]]:
    return [
        {
            "id": "ops.capabilities",
            "method": "GET",
            "path": "/api/ops/capabilities",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "Discover the available MarketRadar API operations and action contracts.",
            "external_calls_possible": False,
        },
        {
            "id": "ops.actions",
            "method": "GET",
            "path": "/api/ops/actions",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "List executable allowlisted ops actions.",
            "external_calls_possible": False,
        },
        {
            "id": "ops.runs.create",
            "method": "POST",
            "path": "/api/ops/runs",
            "role": "analyst",
            "safety": "read_only_artifact_write",
            "ai_use": "Create a dashboard evidence run and get durable artifacts.",
            "external_calls_possible": False,
        },
        {
            "id": "ops.runs.show",
            "method": "GET",
            "path": "/api/ops/runs/{run_id}",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "Load a previously captured run result.",
            "external_calls_possible": False,
        },
        {
            "id": "ops.runs.artifact",
            "method": "GET",
            "path": "/api/ops/runs/{run_id}/artifacts/{artifact_name}",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "Download a run artifact such as terminal.png or snapshot.json.",
            "external_calls_possible": False,
        },
        {
            "id": "dashboard.snapshot",
            "method": "GET",
            "path": "/api/dashboard/snapshot",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "Fetch current dashboard state as structured JSON.",
            "external_calls_possible": False,
        },
        {
            "id": "dashboard.manifest",
            "method": "GET",
            "path": "/api/dashboard/manifest",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": (
                "Discover Tauri/Rust dashboard pages, automation landmarks, "
                "and snapshot contract."
            ),
            "external_calls_possible": False,
        },
        {
            "id": "radar.candidates",
            "method": "GET",
            "path": "/api/radar/candidates",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "List latest candidate queue rows for review.",
            "external_calls_possible": False,
        },
        {
            "id": "radar.candidate_detail",
            "method": "GET",
            "path": "/api/radar/candidates/{ticker}",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "Inspect one ticker with redacted evidence payloads.",
            "external_calls_possible": False,
        },
        {
            "id": "radar.call_plan",
            "method": "POST",
            "path": "/api/radar/runs/call-plan",
            "role": "viewer",
            "safety": "plan_only",
            "ai_use": "Estimate a safe radar run plan before executing provider or model work.",
            "external_calls_possible": False,
        },
        {
            "id": "radar.run",
            "method": "POST",
            "path": "/api/radar/runs",
            "role": "analyst",
            "safety": "stateful_guarded_execution",
            "ai_use": "Run the guarded radar scheduler after inspecting readiness and call plan.",
            "external_calls_possible": True,
        },
        {
            "id": "ops.health",
            "method": "GET",
            "path": "/api/ops/health",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "Check provider, job, and runtime health before selecting a workflow.",
            "external_calls_possible": False,
        },
        {
            "id": "ops.telemetry",
            "method": "GET",
            "path": "/api/ops/telemetry",
            "role": "viewer",
            "safety": "read_only",
            "ai_use": "Review recent audit and telemetry summaries.",
            "external_calls_possible": False,
        },
        {
            "id": "priced_in.source_batches",
            "method": "GET",
            "path": "/api/radar/priced-in/source-batches",
            "role": "viewer",
            "safety": "plan_only",
            "ai_use": "Plan missing source-fill batches without making provider calls.",
            "external_calls_possible": False,
        },
        {
            "id": "broker.market_context",
            "method": "GET",
            "path": "/api/market/context",
            "role": "viewer",
            "safety": "read_only_broker_context",
            "ai_use": "Read broker context for portfolio-aware candidate review.",
            "external_calls_possible": False,
        },
    ]
