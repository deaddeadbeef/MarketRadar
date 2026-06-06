# Tauri Trading Workbench Data Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect the Tauri trading workbench module pages to a local, zero-execution dashboard snapshot contract.

**Architecture:** Add a compact `trading_workbench` snapshot derived from existing MarketRadar, broker, validation, paper-trading, value, and runtime payloads. The Tauri frontend reads that contract for module status, KPIs, source keys, row previews, and the execution boundary while continuing to block live trading and broker order submission.

**Tech Stack:** Python dashboard payload assembly, existing SQLite-backed dashboard fixtures, vanilla Tauri frontend JavaScript/CSS, pytest, ruff, and browser smoke testing.

---

### Task 1: Backend Workbench Snapshot

**Files:**
- Modify: `src/catalyst_radar/dashboard/tui.py`
- Test: `tests/integration/test_dashboard_data.py`

- [x] **Step 1: Add `trading_workbench` to `dashboard_snapshot_payload`**

Call `_trading_workbench_snapshot_payload(...)` after the existing MarketRadar queue and validation data are loaded, then include the result at `payload["trading_workbench"]`.

- [x] **Step 2: Add fixture-backed assertions**

Add a pytest that builds `dashboard_snapshot_payload(...)` with `_insert_dashboard_fixture(engine)` and asserts the workbench schema, zero calls, disabled live trading, disabled broker order submission, Market Radar queue count, paper trade count, and read-only broker module.

- [x] **Step 3: Verify backend test**

Run:

```powershell
$env:PYTHONPATH='C:\Users\fpan1\MarketRadar\.worktrees\trading-workbench-data\src'
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_dashboard_snapshot_payload_exposes_trading_workbench_contract -q
```

Expected: `1 passed`.

### Task 2: Frontend Module Data Rendering

**Files:**
- Modify: `apps/radar-desktop/frontend/app.js`
- Modify: `apps/radar-desktop/frontend/styles.css`
- Test: `tests/integration/test_desktop_dashboard_frontend.py`

- [x] **Step 1: Add workbench snapshot helpers**

Add helpers that read `snapshot.trading_workbench` and return the module object for the active workbench page.

- [x] **Step 2: Render module metrics and source keys**

Use the helper in `renderPlatformModulePage(...)` so each workbench page shows module summary, status, metrics, source keys, rows/focus, and live-trading-disabled boundary.

- [x] **Step 3: Add frontend static assertions**

Extend `test_tauri_trading_workbench_shell_exposes_platform_tools` to assert the new helper names, `trading_workbench`, `data-testid="platform-module-metrics"`, `data-testid="platform-module-sources"`, and `data-testid="platform-module-row"`.

- [x] **Step 4: Verify frontend static test**

Run:

```powershell
$env:PYTHONPATH='C:\Users\fpan1\MarketRadar\.worktrees\trading-workbench-data\src'
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_desktop_dashboard_frontend.py::test_tauri_trading_workbench_shell_exposes_platform_tools -q
```

Expected: `1 passed`.

### Task 3: Full Slice Verification And PR

**Files:**
- Verify: backend, frontend, and browser smoke evidence

- [x] **Step 1: Run lint and focused tests**

Run:

```powershell
$env:PYTHONPATH='C:\Users\fpan1\MarketRadar\.worktrees\trading-workbench-data\src'
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_desktop_dashboard_frontend.py
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_dashboard_snapshot_payload_exposes_trading_workbench_contract tests\integration\test_desktop_dashboard_frontend.py::test_tauri_trading_workbench_shell_exposes_platform_tools -q
```

- [x] **Step 2: Run browser smoke**

Serve `apps/radar-desktop/frontend` with a stubbed Tauri invoke that returns a fixture snapshot containing `trading_workbench`, then verify the overview and Trade Planner page render data rows and `provider_calls=0`.

- [ ] **Step 3: Commit, push, PR, merge**

Commit this slice on `codex/trading-workbench-data`, open a PR, merge it after verification, fast-forward root `main`, and clean up the worktree.
