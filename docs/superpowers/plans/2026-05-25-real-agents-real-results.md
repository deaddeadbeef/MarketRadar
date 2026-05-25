# Real Agents SDK And Real Results Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the OpenAI Agents SDK and agent APIs to explain real MarketRadar scan results only, with no canned/demo/fake data in production dashboard or agent-review flows.

**Architecture:** The deterministic priced-in scan remains the source of truth for ticker ranking, gap, score, and status. The real Agents SDK layer consumes a redacted, provenance-checked snapshot from the local database, produces source-linked explanations and missing-evidence guidance, and persists an audit record; it never silently changes scan scores or makes provider/broker calls from dashboard render. If real data is missing, the product shows an explicit "no real result yet" state instead of substituting demo rows.

**Tech Stack:** Python, FastAPI, Textual TUI, SQLite/SQLAlchemy, OpenAI `openai-agents`, OpenAI Responses API client, pytest, PowerShell launcher checks.

---

## Non-Negotiable Product Contract

No more canned data in product paths.

In production/runtime flows, "result" means data derived from one of these sources:

- the local MarketRadar database populated by real provider ingest, real SEC ingest, Schwab read-only sync, or explicit user-imported market data with file provenance;
- a deterministic scan over those stored rows;
- a real OpenAI Agents SDK run over a redacted snapshot of those stored rows.

The dashboard, CLI, API, and agent layer must not fall back to demo seed rows, fake model output, hard-coded ACME-style examples, synthetic provider responses, or fixture-backed "sample" results. Those remain allowed only in tests and explicit demo commands such as `seed-dashboard-demo`.

When real inputs are incomplete, the correct behavior is:

```text
No real result yet.
Required next step: Run/import real market data, then run `catalyst-radar priced-in-answer --limit 50`.
Provider calls made while viewing: 0.
```

This contract is the acceptance gate for every task below.

## File Map

- Modify `src/catalyst_radar/dashboard/data.py`: classify snapshot/result provenance and expose real-data readiness in dashboard payloads.
- Modify `src/catalyst_radar/dashboard/tui.py`: show real-data state and block fake/canned substitutions in TUI pages and commands.
- Modify `src/catalyst_radar/api/routes/agents.py`: add explicit real-run API contract with `execute=true`, max-call limits, and real-data gates.
- Modify `src/catalyst_radar/agents/sdk_orchestrator.py`: run real Agents SDK only after the snapshot passes real-data and safety gates.
- Modify `src/catalyst_radar/cli.py`: add CLI preview/execute paths for real agent runs and keep preview zero-call.
- Create `src/catalyst_radar/agents/run_audit.py`: persist real agent run audit records behind a focused repository API.
- Modify tests under `tests/unit/` and `tests/integration/`: prove real-data gates, no hidden calls, no fake fallback, and persisted audit behavior.
- Modify `README.md`: document "real results only" and how to intentionally run demos/tests separately.

## Task 1: Add Real-Data Provenance Gate

**Files:**
- Modify: `src/catalyst_radar/dashboard/data.py`
- Test: `tests/integration/test_dashboard_data.py`

- [x] **Step 1: Write tests for real vs demo/canned provenance**

Add focused tests that build payloads with and without real scan provenance. Define this helper in the same test file:

```python
def _dashboard_snapshot_payload_for_test(database_url: str) -> dict[str, object]:
    engine = engine_from_url(database_url)
    create_schema(engine)
    return dashboard_snapshot_payload(
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
        dotenv_loaded=False,
        filters=DashboardFilters(),
    )
```

Then add:

```python
def test_dashboard_payload_marks_missing_real_results_without_demo_fallback(tmp_path, monkeypatch):
    database_url = f"sqlite:///{(tmp_path / 'empty-real-results.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    payload = _dashboard_snapshot_payload_for_test(database_url)

    assert payload["real_results"]["status"] == "missing"
    assert "No real result yet" in payload["real_results"]["headline"]
    assert payload["priced_in_queue"]["rows"] == []
    assert payload["external_calls_made"] == 0
```

```python
def test_dashboard_payload_accepts_provider_backed_scan_rows(tmp_path, monkeypatch):
    database_url = f"sqlite:///{(tmp_path / 'real-results.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _insert_candidate_state(engine, ticker="MSFT", state_id="state-MSFT")
    _insert_signal_features_with_priced_in_context(engine)

    payload = _dashboard_snapshot_payload_for_test(database_url)

    assert payload["real_results"]["status"] == "ready"
    assert payload["real_results"]["source"] == "local_database_provider_backed_scan"
    assert payload["priced_in_queue"]["rows"][0]["ticker"] == "MSFT"
```

- [x] **Step 2: Run tests and confirm failure**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py -k "real_results or demo_fallback" -q
```

Expected: fails because `real_results` is not present.

- [x] **Step 3: Implement minimal provenance payload**

Add a `real_results` block to the dashboard payload:

```python
"real_results": {
    "status": "ready" if real_scan_row_count else "missing",
    "source": "local_database_provider_backed_scan" if real_scan_row_count else "none",
    "headline": (
        f"{real_scan_row_count} real scan result row(s) available."
        if real_scan_row_count
        else "No real result yet."
    ),
    "next_action": (
        "Open Insights or Candidates."
        if real_scan_row_count
        else "Run/import/validate real market data before asking agents for analysis."
    ),
    "canned_data_allowed": False,
}
```

Use existing scan/candidate counts from the payload; do not add a new database query unless the existing payload lacks the needed count.

- [x] **Step 4: Verify tests pass**

Run the same pytest command and confirm pass.

- [x] **Step 5: Commit**

```powershell
git add src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
git commit -m "Add real-results provenance gate"
```

## Task 2: Remove Product-Path Canned Fallbacks

**Files:**
- Modify: `src/catalyst_radar/dashboard/tui.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_dashboard_demo_seed_cli.py`

- [x] **Step 1: Write tests for empty real state**

Add a TUI once-mode test using an empty local database:

```python
def test_dashboard_once_empty_database_shows_no_real_result_not_demo(monkeypatch, tmp_path, capsys):
    database_url = f"sqlite:///{(tmp_path / 'empty-dashboard.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["dashboard-tui", "--once"]) == 0
    output = capsys.readouterr().out

    assert "No real result yet" in output
    assert "ACME" not in output
    assert "Bullish not priced" not in output
    assert "External calls made: 0" in output
```

- [x] **Step 2: Run test and confirm failure**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_once_empty_database_shows_no_real_result_not_demo -q
```

Expected: fails until the TUI reads `real_results` and prints the real empty state.

- [x] **Step 3: Implement TUI empty state**

On Insights, Candidates, Agent, and Review pages, if `payload["real_results"]["status"] == "missing"`, show the empty real-results message and the next action. Do not synthesize rows.

- [x] **Step 4: Keep explicit demo command isolated**

`seed-dashboard-demo` remains available for tests and demos, but the dashboard must never call it automatically and must never show demo rows unless the database actually contains those rows because the user explicitly ran the demo command.

- [x] **Step 5: Verify**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -k "empty_database or defaults_to_latest_scan_results or demo_seed" -q
```

Expected: all selected tests pass.

- [x] **Step 6: Commit**

```powershell
git add src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_demo_seed_cli.py
git commit -m "Block canned dashboard fallbacks"
```

## Task 3: Add Real Agents SDK Execute Contract

**Files:**
- Modify: `src/catalyst_radar/api/routes/agents.py`
- Modify: `src/catalyst_radar/cli.py`
- Modify: `src/catalyst_radar/agents/sdk_orchestrator.py`
- Test: `tests/integration/test_api_routes.py`
- Test: `tests/unit/test_agent_sdk_orchestrator.py`

- [x] **Step 1: Write API tests**

Add tests for preview and execute:

```python
def test_agent_brief_real_preview_makes_zero_openai_calls(client):
    response = client.post("/api/agents/brief/run", json={"mode": "real", "execute": False})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "preview"
    assert payload["external_calls_planned"]["openai"] >= 1
    assert payload["external_calls_made"]["openai"] == 0
```

```python
def test_agent_brief_real_execute_blocks_without_real_results(client):
    response = client.post(
        "/api/agents/brief/run",
        json={"mode": "real", "execute": True, "max_openai_calls": 4},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["error"] == "real_results_required"
```

- [x] **Step 2: Add request model**

Add a request model with these fields:

```python
class AgentBriefRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["dry_run", "real"] = "dry_run"
    execute: bool = False
    goal: str | None = Field(default=None, max_length=500)
    ticker: str | None = Field(default=None, min_length=1, max_length=12)
    scan_limit: int = Field(default=10, ge=1, le=50)
    max_openai_calls: int = Field(default=4, ge=1, le=8)
```

- [x] **Step 3: Implement preview behavior**

If `execute` is false, return a preview object with planned calls and gate status. Preview must call zero OpenAI APIs.

- [x] **Step 4: Implement execute behavior**

If `execute` is true, require:

- `real_results.status == "ready"`;
- `agent_sdk_gate_payload(config).status == "ready"`;
- `mode == "real"`;
- role `ANALYST`;
- `max_openai_calls` within local policy.

Then call `run_market_radar_agents(..., real=True, operator_goal=goal)`.

- [x] **Step 5: Verify**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_api_routes.py -k "agent_brief" -q
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\unit\test_agent_sdk_orchestrator.py -q
```

- [x] **Step 6: Commit**

```powershell
git add src\catalyst_radar\api\routes\agents.py src\catalyst_radar\cli.py src\catalyst_radar\agents\sdk_orchestrator.py tests\integration\test_api_routes.py tests\unit\test_agent_sdk_orchestrator.py
git commit -m "Add real agent execute contract"
```

## Task 4: Add Read-Only Agent Tools Over Real Stored Data

**Files:**
- Modify: `src/catalyst_radar/agents/sdk_orchestrator.py`
- Create: `src/catalyst_radar/agents/tools.py`
- Test: `tests/unit/test_agent_sdk_orchestrator.py`

- [x] **Step 1: Define the allowlist**

The only first-pass tools are:

```text
get_visible_scan_rows
get_candidate_detail
get_source_coverage
get_real_results_status
```

Each tool reads the current redacted snapshot or local database-derived payload. No tool may call Polygon, SEC, Schwab, broker order APIs, shell, filesystem writes, or network fetches.

- [x] **Step 2: Add tests that disallow provider and broker tools**

```python
def test_real_agent_tool_allowlist_has_no_provider_or_broker_calls():
    names = {tool.name for tool in build_market_radar_agent_tools(snapshot={})}

    assert names == {
        "get_visible_scan_rows",
        "get_candidate_detail",
        "get_source_coverage",
        "get_real_results_status",
    }
    assert "polygon" not in " ".join(names).lower()
    assert "schwab" not in " ".join(names).lower()
    assert "order" not in " ".join(names).lower()
```

- [x] **Step 3: Wire tools into the manager agent**

Use the OpenAI Agents SDK function-tool mechanism for the four read-only tools. Keep specialist agents as tools or handoffs only after the manager tool path is stable.

- [x] **Step 4: Verify**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\unit\test_agent_sdk_orchestrator.py -q
```

- [x] **Step 5: Commit**

```powershell
git add src\catalyst_radar\agents\sdk_orchestrator.py src\catalyst_radar\agents\tools.py tests\unit\test_agent_sdk_orchestrator.py
git commit -m "Add read-only real-data agent tools"
```

## Task 5: Persist Real Agent Run Audit

**Files:**
- Create or modify: `src/catalyst_radar/agents/run_audit.py`
- Modify: `src/catalyst_radar/storage/schema.py`
- Test: `tests/integration/test_agent_run_audit.py`

Implementation note: the shipped slice reuses the existing append-only
`audit_events` table through `run_audit.py` instead of adding a second audit
table. This keeps the first real-test path smaller while still persisting run
id, model, snapshot hash, planned/made call counts, token usage, status, summary,
and safety verdict.

- [x] **Step 1: Write audit tests**

```python
def test_real_agent_run_audit_records_model_calls_and_snapshot_hash(tmp_path, monkeypatch):
    database_url = f"sqlite:///{(tmp_path / 'agent-audit.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    run_id = record_agent_run_audit(
        database_url=database_url,
        mode="real",
        model="gpt-5.1",
        snapshot_hash="sha256:test",
        external_calls_made={"openai": 2, "market_data": 0, "broker": 0},
        status="completed",
    )

    row = load_agent_run_audit(database_url, run_id)
    assert row["snapshot_hash"] == "sha256:test"
    assert row["external_calls_made"]["openai"] == 2
```

- [x] **Step 2: Implement schema/repository**

Persist:

- run id;
- created timestamp;
- mode;
- model;
- operator goal;
- snapshot hash;
- redaction version;
- external calls planned/made;
- token usage if SDK returns it;
- status;
- final output summary;
- safety verdict.

- [x] **Step 3: Verify**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_agent_run_audit.py -q
```

- [x] **Step 4: Commit**

```powershell
git add src\catalyst_radar\agents\run_audit.py src\catalyst_radar\storage\schema.py tests\integration\test_agent_run_audit.py
git commit -m "Persist real agent run audits"
```

## Task 6: Add TUI Preview/Execute Commands

**Files:**
- Modify: `src/catalyst_radar/dashboard/tui.py`
- Test: `tests/integration/test_dashboard_demo_seed_cli.py`

- [x] **Step 1: Add command tests**

```python
def test_tui_agent_run_preview_is_zero_call():
    app = build_dashboard_app_for_test(real_results_ready=True)

    result = app.handle_command("agent run AAPL")

    assert result.external_calls_made["openai"] == 0
    assert "OpenAI calls planned" in result.message
    assert "agent run AAPL execute" in result.message
```

```python
def test_tui_agent_run_execute_blocks_without_real_results():
    app = build_dashboard_app_for_test(real_results_ready=False)

    result = app.handle_command("agent run AAPL execute")

    assert result.external_calls_made["openai"] == 0
    assert "No real result yet" in result.message
```

- [x] **Step 2: Implement commands**

Support:

```text
agent run
agent run <ticker>
agent run <ticker> execute
```

Preview is zero-call. Execute requires real-results readiness and Agents SDK gates.

- [x] **Step 3: Verify**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -k "agent_run" -q
```

- [x] **Step 4: Commit**

```powershell
git add src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git commit -m "Add TUI real agent run command"
```

## Task 7: Documentation And Final Gate

**Files:**
- Modify: `README.md`
- Test: `scripts/debug-dashboard-e2e.ps1`

- [x] **Step 1: Document the real-results rule**

Add a short section:

```markdown
### Real Results Only

`radar` does not show canned market analysis. It renders real rows from the local database, explicit user imports, and real provider-backed scans. If those are missing, the dashboard shows "No real result yet" and the exact next action. Demo rows are available only through explicit demo/test commands.
```

- [x] **Step 2: Run checks**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\unit\test_agent_sdk_orchestrator.py tests\integration\test_api_routes.py -k "agent" -q
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -k "agent_run or empty_database or defaults_to_latest_scan_results" -q
powershell -ExecutionPolicy Bypass -File scripts\debug-dashboard-e2e.ps1
git diff --check
```

- [x] **Step 3: Commit**

```powershell
git add README.md scripts\debug-dashboard-e2e.ps1
git commit -m "Document real-results agent workflow"
```

## Final Acceptance Checklist

- [x] `radar` startup makes zero OpenAI, Polygon, SEC, Schwab, broker, or order calls.
- [x] Empty or incomplete databases show "No real result yet" instead of demo rows.
- [x] Production dashboard pages do not display canned analysis.
- [x] Agent preview makes zero OpenAI calls.
- [x] Agent execute makes OpenAI calls only after explicit `execute=true` or `execute` command.
- [x] Agents never mutate deterministic scan `Gap`, `Score`, `priced_in_status`, or source rows.
- [x] Agents can only use read-only allowlisted tools.
- [x] Every real agent run is persisted with snapshot hash, model, call counts, and safety verdict.
- [x] Test/demo fixtures remain available only through explicit test/demo commands.
