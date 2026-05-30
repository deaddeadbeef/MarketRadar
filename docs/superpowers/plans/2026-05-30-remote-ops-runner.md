# Remote Ops Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a robust AI-first ops API layer that consolidates MarketRadar operations, can run approved dashboard validations remotely, persists real terminal-style artifacts, and returns JSON/text/PNG outputs through CLI and API.

**Architecture:** Add a focused ops service that owns allowlisted run definitions, state directory layout, artifact generation, and OneDrive copying. Add an AI-first capability catalog that describes the existing dashboard, radar, telemetry, safe-run, and artifact APIs as one operational system, then extend the existing `/api/ops` router with discovery, run creation, run lookup, and artifact download endpoints. The default renderer is the Rust TUI static frame path, with a Python text fallback so remote API calls remain useful when Rust is not installed or not built.

**Tech Stack:** Python 3.11, FastAPI, Pydantic, SQLAlchemy engine creation, existing dashboard snapshot helpers, Rust `radar-tui --render-frame`, Pillow for PNG artifact rendering, pytest.

---

### File Structure

- Create `src/catalyst_radar/ops/remote_runs.py` for allowlisted run specs, run state layout, Rust/Python rendering, PNG writing, and OneDrive copying.
- Create `src/catalyst_radar/ops/capabilities.py` for the AI-first API catalog and executable action manifest.
- Modify `src/catalyst_radar/api/routes/ops.py` to add `GET /api/ops/capabilities`, `GET /api/ops/actions`, `POST /api/ops/runs`, `GET /api/ops/runs/{run_id}`, and `GET /api/ops/runs/{run_id}/artifacts/{artifact_name}`.
- Modify `src/catalyst_radar/cli.py` to add `ops run radar-dashboard` and `ops show` commands.
- Modify `pyproject.toml` to declare Pillow explicitly because the PNG artifact path imports it directly.
- Add `tests/unit/test_remote_ops_runs.py` for path safety, allowlist behavior, artifact generation, and OneDrive copy metadata.
- Add `tests/unit/test_ops_capabilities.py` for the AI-first capability contract.
- Add `tests/integration/test_ops_run_api_routes.py` for capability discovery, route behavior, and artifact downloads.
- Add `tests/integration/test_ops_run_cli.py` for CLI JSON output, capability discovery, and artifact creation.
- Modify `tests/integration/test_security_boundaries.py` to include the three new allowlisted API routes.
- Modify `README.md` with terminal instructions for local and remote-friendly artifact capture.

### Task 1: Core Ops Run Service

**Files:**
- Create: `src/catalyst_radar/ops/remote_runs.py`
- Test: `tests/unit/test_remote_ops_runs.py`

- [ ] **Step 1: Write the service tests**

Add tests that create a temp SQLite database, monkeypatch `CATALYST_OPS_RUN_DIR`, and call the service with `renderer="python"` so the tests do not need Rust:

```python
def test_run_allowlisted_dashboard_creates_artifacts(tmp_path, monkeypatch):
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    result = create_ops_run(
        action="radar-dashboard",
        page="overview",
        renderer="python",
        frame_width=100,
        frame_height=30,
    )
    assert result["schema_version"] == "ops-run-v1"
    assert result["status"] == "completed"
    assert result["action"] == "radar-dashboard"
    assert result["page"] == "overview"
    assert result["summary"]["external_calls_made"] == 0
    artifact_names = {artifact["name"] for artifact in result["artifacts"]}
    assert {"result.json", "snapshot.json", "terminal.txt", "terminal.png"} <= artifact_names
    assert Path(result["run_dir"]).is_dir()
```

Add a rejection test:

```python
def test_run_rejects_unapproved_action(tmp_path, monkeypatch):
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    with pytest.raises(OpsRunError, match="unsupported ops action"):
        create_ops_run(action="powershell", page="overview", renderer="python")
```

Add an artifact lookup test:

```python
def test_resolve_artifact_rejects_path_traversal(tmp_path, monkeypatch):
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    result = create_ops_run(action="radar-dashboard", page="overview", renderer="python")
    with pytest.raises(OpsRunError, match="invalid artifact name"):
        resolve_ops_artifact(result["run_id"], "../snapshot.json")
```

- [ ] **Step 2: Run the new tests and confirm they fail**

Run:

```powershell
pytest tests/unit/test_remote_ops_runs.py -q
```

Expected: import failure for `catalyst_radar.ops.remote_runs`.

- [ ] **Step 3: Implement `remote_runs.py`**

Implement these public functions and types:

```python
class OpsRunError(ValueError):
    pass

def create_ops_run(
    *,
    action: str,
    page: str = "overview",
    renderer: str = "auto",
    frame_width: int = 140,
    frame_height: int = 42,
    copy_to_onedrive: bool = False,
    database_url: str | None = None,
) -> dict[str, object]:
    ...

def load_ops_run(run_id: str) -> dict[str, object]:
    ...

def resolve_ops_artifact(run_id: str, artifact_name: str) -> Path:
    ...
```

Implementation details:
- Accept only action `radar-dashboard`.
- Normalize page through existing dashboard aliases by calling `dashboard_filters_for_page(DashboardFilters(), page)` and preserving the original display page in the result.
- Create run ids as `YYYYMMDDTHHMMSSZ-<8 hex chars>`.
- Use `CATALYST_OPS_RUN_DIR` when set, otherwise `.state/ops-runs`.
- Write `snapshot.json`, `terminal.txt`, `terminal.png`, and `result.json` under the run directory.
- Build `snapshot.json` from `dashboard_snapshot_payload(engine=..., config=..., dotenv_loaded=True, filters=..., fast_view=True)`.
- For `renderer="auto"` or `renderer="rust"`, try the Rust `target/release/radar-tui.exe` or `cargo run -p radar-tui --release -- --render-frame` path with an allowlisted snapshot command. For `renderer="auto"`, fall back to Python text rendering if Rust fails. For `renderer="rust"`, return a failed run if Rust fails.
- For `renderer="python"`, use `render_dashboard_tui(payload, page=page)`.
- Render `terminal.png` with Pillow using a dark background, Consolas or DejaVu Sans Mono if available, and the exact terminal text content.
- Copy artifacts to `$OneDrive\MarketRadar\ops-runs\<run_id>` only when `copy_to_onedrive=True`; record the destination path or an unavailable status without deleting the canonical run directory.

- [ ] **Step 4: Run service tests**

Run:

```powershell
pytest tests/unit/test_remote_ops_runs.py -q
```

Expected: all tests pass.

### Task 2: AI-First API Catalog And Routes

**Files:**
- Create: `src/catalyst_radar/ops/capabilities.py`
- Modify: `src/catalyst_radar/api/routes/ops.py`
- Test: `tests/unit/test_ops_capabilities.py`
- Test: `tests/integration/test_ops_run_api_routes.py`
- Modify: `tests/integration/test_security_boundaries.py`

- [ ] **Step 1: Write API route tests**

Add a unit test that asserts the catalog exposes the existing API as a single AI-first operations system:

```python
catalog = ops_capability_catalog()
assert catalog["schema_version"] == "ops-capability-catalog-v1"
assert catalog["external_calls_made"] == 0
assert catalog["safety"]["arbitrary_shell"] is False
paths = {operation["path"] for operation in catalog["operations"]}
assert "/api/dashboard/snapshot" in paths
assert "/api/radar/candidates" in paths
assert "/api/radar/runs/call-plan" in paths
assert "/api/ops/runs" in paths
```

Add tests that use `TestClient(create_app())`, monkeypatch `CATALYST_OPS_RUN_DIR`, first verify discovery routes, and then post:

```python
capabilities = client.get("/api/ops/capabilities")
assert capabilities.status_code == 200
assert capabilities.json()["schema_version"] == "ops-capability-catalog-v1"

actions = client.get("/api/ops/actions")
assert actions.status_code == 200
assert [action["id"] for action in actions.json()["actions"]] == ["radar-dashboard"]
```

```python
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
```

Then fetch:

```python
show = client.get(f"/api/ops/runs/{payload['run_id']}")
assert show.status_code == 200
artifact = client.get(f"/api/ops/runs/{payload['run_id']}/artifacts/terminal.txt")
assert artifact.status_code == 200
assert "MarketRadar" in artifact.text or "MARKET" in artifact.text
```

Add a bad artifact test:

```python
bad = client.get(f"/api/ops/runs/{payload['run_id']}/artifacts/..%2Fsnapshot.json")
assert bad.status_code in {400, 404}
```

- [ ] **Step 2: Run API tests and confirm they fail**

Run:

```powershell
pytest tests/integration/test_ops_run_api_routes.py -q
```

Expected: 404 for `/api/ops/runs`.

- [ ] **Step 3: Add catalog, route models, and endpoints**

In `src/catalyst_radar/ops/capabilities.py`, add `ops_capability_catalog()` and `ops_action_catalog()` with these properties:
- `external_calls_made=0`
- `safety.arbitrary_shell=False`
- executable action `radar-dashboard`
- operation entries for existing dashboard snapshot, radar candidates, radar call-plan, radar run, ops health, telemetry, and artifact endpoints

In `src/catalyst_radar/api/routes/ops.py`, add:

```python
class OpsRunRequest(BaseModel):
    action: str = Field(default="radar-dashboard")
    page: str = Field(default="overview")
    renderer: str = Field(default="auto")
    frame_width: int = Field(default=140, ge=80, le=240)
    frame_height: int = Field(default=42, ge=24, le=80)
    copy_to_onedrive: bool = False

@router.get("/capabilities", dependencies=[Depends(require_role(Role.VIEWER))])
def ops_capabilities() -> dict[str, object]:
    ...

@router.get("/actions", dependencies=[Depends(require_role(Role.VIEWER))])
def ops_actions() -> dict[str, object]:
    ...

@router.post("/runs", dependencies=[Depends(require_role(Role.ANALYST))])
def ops_run_create(request: OpsRunRequest) -> dict[str, object]:
    ...

@router.get("/runs/{run_id}", dependencies=[Depends(require_role(Role.VIEWER))])
def ops_run_show(run_id: str) -> dict[str, object]:
    ...

@router.get("/runs/{run_id}/artifacts/{artifact_name}", dependencies=[Depends(require_role(Role.VIEWER))])
def ops_run_artifact(run_id: str, artifact_name: str) -> FileResponse:
    ...
```

Translate `OpsRunError` into `HTTPException(status_code=400)` for validation failures and `HTTPException(status_code=404)` for missing runs or artifacts.

- [ ] **Step 4: Update the API allowlist**

Add these entries to `EXPECTED_API_ROUTES`:

```python
("GET", "/api/ops/capabilities"): ("catalyst_radar.api.routes.ops", "ops_capabilities", ("ops",)),
("GET", "/api/ops/actions"): ("catalyst_radar.api.routes.ops", "ops_actions", ("ops",)),
("POST", "/api/ops/runs"): ("catalyst_radar.api.routes.ops", "ops_run_create", ("ops",)),
("GET", "/api/ops/runs/{run_id}"): ("catalyst_radar.api.routes.ops", "ops_run_show", ("ops",)),
("GET", "/api/ops/runs/{run_id}/artifacts/{artifact_name}"): ("catalyst_radar.api.routes.ops", "ops_run_artifact", ("ops",)),
```

- [ ] **Step 5: Run API and security tests**

Run:

```powershell
pytest tests/unit/test_ops_capabilities.py tests/integration/test_ops_run_api_routes.py tests/integration/test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
```

Expected: all tests pass.

### Task 3: CLI Entry Point

**Files:**
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_ops_run_cli.py`

- [ ] **Step 1: Write CLI tests**

Add:

```python
def test_ops_capabilities_cli_outputs_catalog(capsys):
    assert main(["ops", "capabilities", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "ops-capability-catalog-v1"
    assert any(action["id"] == "radar-dashboard" for action in payload["actions"])

def test_ops_run_cli_outputs_json_and_artifacts(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    assert main([
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
        "--json",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "completed"
    assert Path(payload["run_dir"], "terminal.png").exists()
```

Add:

```python
def test_ops_show_cli_outputs_existing_run(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CATALYST_OPS_RUN_DIR", str(tmp_path / "runs"))
    assert main(["ops", "run", "radar-dashboard", "--renderer", "python", "--json"]) == 0
    created = json.loads(capsys.readouterr().out)
    assert main(["ops", "show", created["run_id"], "--json"]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["run_id"] == created["run_id"]
```

- [ ] **Step 2: Run CLI tests and confirm they fail**

Run:

```powershell
pytest tests/integration/test_ops_run_cli.py -q
```

Expected: parser error because the `ops` command does not exist.

- [ ] **Step 3: Add parser commands and dispatch**

In `build_parser()`, add:

```python
ops = subparsers.add_parser("ops")
ops_sub = ops.add_subparsers(dest="ops_command", required=True)
ops_capabilities = ops_sub.add_parser("capabilities")
ops_capabilities.add_argument("--json", action="store_true")
ops_run = ops_sub.add_parser("run")
ops_run.add_argument("action", choices=["radar-dashboard"])
ops_run.add_argument("--page", default="overview")
ops_run.add_argument("--renderer", choices=["auto", "rust", "python"], default="auto")
ops_run.add_argument("--frame-width", type=int, default=140)
ops_run.add_argument("--frame-height", type=int, default=42)
ops_run.add_argument("--copy-to-onedrive", action="store_true")
ops_run.add_argument("--json", action="store_true")
ops_show = ops_sub.add_parser("show")
ops_show.add_argument("run_id")
ops_show.add_argument("--json", action="store_true")
```

In `main()`, dispatch before commands that require an engine:

```python
if args.command == "ops":
    if args.ops_command == "run":
        payload = create_ops_run(...)
    elif args.ops_command == "show":
        payload = load_ops_run(args.run_id)
    ...
```

Human output should print run id, status, summary status, artifact paths, and OneDrive path when present.

- [ ] **Step 4: Run CLI tests**

Run:

```powershell
pytest tests/integration/test_ops_run_cli.py -q
```

Expected: all tests pass.

### Task 4: Documentation and Dependency Declaration

**Files:**
- Modify: `pyproject.toml`
- Modify: `README.md`
- Test: `tests/integration/test_local_scripts.py`

- [ ] **Step 1: Add Pillow dependency**

Add:

```toml
"Pillow>=10",
```

to `[project].dependencies`.

- [ ] **Step 2: Document terminal and API usage**

Add README examples:

```powershell
catalyst-radar ops run radar-dashboard --page overview --renderer auto --copy-to-onedrive --json
catalyst-radar ops show <run-id> --json
```

Add API examples:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/ops/runs -Headers @{"x-catalyst-role"="analyst"} -Body (@{action="radar-dashboard"; page="overview"; renderer="auto"; copy_to_onedrive=$true} | ConvertTo-Json) -ContentType "application/json"
```

Document that `terminal.png` is a headless artifact generated from the real dashboard frame, not a desktop screenshot.

- [ ] **Step 3: Run README/local script tests**

Run:

```powershell
pytest tests/integration/test_local_scripts.py -q
```

Expected: all tests pass.

### Task 5: End-to-End Validation and Branch Completion

**Files:**
- No new files unless verification exposes a defect.

- [ ] **Step 1: Run focused Python tests**

Run:

```powershell
pytest tests/unit/test_remote_ops_runs.py tests/unit/test_ops_capabilities.py tests/integration/test_ops_run_api_routes.py tests/integration/test_ops_run_cli.py tests/integration/test_dashboard_api_routes.py tests/integration/test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
```

Expected: all tests pass.

- [ ] **Step 2: Run the real terminal artifact command**

Run:

```powershell
catalyst-radar ops run radar-dashboard --page overview --renderer auto --copy-to-onedrive --json
```

Expected: JSON includes `status=completed`, `summary.external_calls_made=0`, `terminal.txt`, `terminal.png`, and an `onedrive_dir` when OneDrive is available.

- [ ] **Step 3: Smoke the API route locally**

Run:

```powershell
python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8000
```

In a second terminal:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/ops/runs -Body (@{action="radar-dashboard"; page="overview"; renderer="python"} | ConvertTo-Json) -ContentType "application/json"
```

Expected: a completed ops run response with artifact URLs or paths.

- [ ] **Step 4: Commit, push, PR, and merge**

Run:

```powershell
git status --short
git add docs/superpowers/plans/2026-05-30-remote-ops-runner.md src/catalyst_radar/ops/remote_runs.py src/catalyst_radar/api/routes/ops.py src/catalyst_radar/cli.py pyproject.toml README.md tests/unit/test_remote_ops_runs.py tests/integration/test_ops_run_api_routes.py tests/integration/test_ops_run_cli.py tests/integration/test_security_boundaries.py
git commit -m "feat: add remote ops artifact runner"
git push -u origin codex/remote-ops-runner
```

Then create a PR to `main`, verify checks, and rebase-merge if clean.

### Self-Review

- Spec coverage: The plan covers allowlisted remote execution, API creation, CLI access, artifact persistence, terminal text, JSON, PNG output, OneDrive copy, and honest headless artifact semantics.
- Placeholder scan: No placeholder tasks remain; every task names files, commands, and expected outcomes.
- Type consistency: The public service functions, route function names, CLI command names, and test assertions use the same `radar-dashboard`, `ops-run-v1`, and artifact field names throughout.
