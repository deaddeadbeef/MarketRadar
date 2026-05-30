# Ops HTML Reports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make headless MarketRadar ops runs return aggregation-ready JSON and generate polished HTML reports from that JSON.

**Architecture:** Add a dedicated report module that normalizes an ops run into `report.json` and renders `report.html` from that JSON payload. Keep `result.json` as the run envelope, but make `report.json` the stable aggregation contract for agents. Integrate both report artifacts into the existing allowlisted ops run service, API artifact resolver, OneDrive copy path, CLI, and capability catalog.

**Tech Stack:** Python 3.11, existing ops-run service, HTML/CSS string rendering with escaping, pytest, ruff.

---

### Task 1: Report JSON and HTML Renderer

**Files:**
- Create: `src/catalyst_radar/ops/reports.py`
- Test: `tests/unit/test_ops_reports.py`

- [ ] **Step 1: Write tests for normalized report JSON**

Create a test that calls `build_ops_run_report_payload` with a minimal result, dashboard snapshot, and terminal text. Assert:

```python
assert payload["schema_version"] == "ops-run-report-v1"
assert payload["run"]["run_id"] == "20260530T000000Z-12345678"
assert payload["summary"]["external_calls_made"] == 0
assert payload["rows"][0]["ticker"] == "MSFT"
assert payload["artifacts"][0]["name"] == "result.json"
```

- [ ] **Step 2: Write tests for HTML rendering**

Render the payload and assert:

```python
assert "<!doctype html>" in html
assert "MarketRadar Ops Report" in html
assert "MSFT" in html
assert '<script type="application/json" id="ops-report-data">' in html
assert "terminal.png" in html
```

- [ ] **Step 3: Implement report builder**

Implement:

```python
def build_ops_run_report_payload(*, result, snapshot, terminal_text) -> dict[str, object]:
    ...
```

The payload must include `schema_version`, `generated_at`, `run`, `summary`, `artifacts`, `rows`, `next_steps`, `boundary`, `renderer`, and `terminal_preview`.

- [ ] **Step 4: Implement HTML renderer**

Implement:

```python
def render_ops_run_report_html(payload: Mapping[str, object]) -> str:
    ...
```

The report should be a complete standalone HTML document with responsive CSS, metric cards, next-action panel, artifact links, candidate table, terminal preview, and embedded JSON for aggregation.

### Task 2: Ops Run Integration

**Files:**
- Modify: `src/catalyst_radar/ops/remote_runs.py`
- Modify: `src/catalyst_radar/ops/capabilities.py`
- Test: `tests/unit/test_remote_ops_runs.py`
- Test: `tests/unit/test_ops_capabilities.py`
- Test: `tests/integration/test_ops_run_api_routes.py`
- Test: `tests/integration/test_ops_run_cli.py`

- [ ] **Step 1: Add report artifacts**

Add `report.json` and `report.html` to the artifact allowlist, metadata ordering, and copy-to-OneDrive set.

- [ ] **Step 2: Generate reports from JSON**

After writing `snapshot.json` and terminal artifacts, build `result` JSON, then build `report.json` from `result`, `snapshot`, and terminal text. Render `report.html` from `report.json`, then update `result.json` with final artifact metadata.

- [ ] **Step 3: Update catalog**

Update the `radar-dashboard` action artifact list and agent evidence guidance to prefer `report.json` for aggregation and `report.html` for human review.

- [ ] **Step 4: Update CLI headless output**

Make `catalyst-radar ops run` and `ops show` emit JSON by default. Keep `--json` as a compatibility no-op and add `--human` for the previous text summary.

### Task 3: Validation and Delivery

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document report artifacts**

Update the ops API docs to mention `report.json` and `report.html`, and show the default JSON CLI behavior.

- [ ] **Step 2: Run tests and lint**

Run:

```powershell
..\..\.venv\Scripts\python.exe -m pytest tests/unit/test_ops_reports.py tests/unit/test_remote_ops_runs.py tests/unit/test_ops_capabilities.py tests/integration/test_ops_run_api_routes.py tests/integration/test_ops_run_cli.py -q
..\..\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\ops\reports.py src\catalyst_radar\ops\remote_runs.py src\catalyst_radar\ops\capabilities.py src\catalyst_radar\cli.py tests\unit\test_ops_reports.py tests\unit\test_remote_ops_runs.py tests\unit\test_ops_capabilities.py tests\integration\test_ops_run_api_routes.py tests\integration\test_ops_run_cli.py
```

- [ ] **Step 3: Run real artifact smoke**

Run:

```powershell
python -m catalyst_radar.cli ops run radar-dashboard --page overview --renderer auto --copy-to-onedrive
```

Expected: JSON output with `report.json`, `report.html`, and OneDrive paths. Open `report.html` and verify the report is visually coherent.
