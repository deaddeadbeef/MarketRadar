# Tauri Trading Workbench Plan Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface the existing zero-execution trading-platform plan inside the Tauri Trade Planner workbench page.

**Architecture:** Reuse `build_trading_platform_plan` against the latest local decision card already stored by MarketRadar, attach a compact `active_plan` payload to `trading_workbench`, and render that plan in the Tauri module page. The plan remains read-only: external calls, DB writes, and broker order submission all stay at zero/disabled.

**Tech Stack:** Python dashboard snapshot assembly, existing decision-card storage, existing trading plan builder, vanilla Tauri frontend JavaScript/CSS, pytest, ruff, and browser smoke testing.

---

### Task 1: Backend Plan Attachment

**Files:**
- Modify: `src/catalyst_radar/dashboard/tui.py`
- Test: `tests/integration/test_dashboard_data.py`

- [x] **Step 1: Select the active decision card**

Use `ValidationRepository(engine).decision_card_payload(...)` for the first workbench focus row with a decision-card id, falling back to no active plan when no card exists.

- [x] **Step 2: Build the zero-execution plan**

Call `build_trading_platform_plan(card, available_at=data_available_at or datetime.now(UTC), config=config, broker_data_stale=...)` and store `plan.to_payload()` under `trading_workbench["active_plan"]`.

- [x] **Step 3: Update module metrics**

Attach plan status, autonomy level, paper/live approvals, and order-submission disabled state to Trade Planner, Risk Desk, Paper Trading, and Broker modules.

- [x] **Step 4: Verify backend contract**

Run:

```powershell
$env:PYTHONPATH='C:\Users\fpan1\MarketRadar\.worktrees\trading-workbench-plan-surface\src'
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_dashboard_snapshot_payload_exposes_trading_workbench_contract -q
```

Expected: `1 passed`.

### Task 2: Tauri Plan Rendering

**Files:**
- Modify: `apps/radar-desktop/frontend/app.js`
- Modify: `apps/radar-desktop/frontend/styles.css`
- Test: `tests/integration/test_desktop_dashboard_frontend.py`

- [x] **Step 1: Render the active plan**

Add a `renderWorkbenchActivePlan(...)` panel for Trade Planner and Paper Trading pages with strategy, risk approval, order intent, and supervision command previews.

- [x] **Step 2: Preserve responsive layout**

Use grid rows that collapse to one column under the existing mobile breakpoint and keep long commands wrapping.

- [x] **Step 3: Add static frontend assertions**

Assert `function renderWorkbenchActivePlan`, `data-testid="workbench-active-plan"`, `data-testid="workbench-plan-controls"`, and active-plan field names in the frontend test.

### Task 3: Verification And PR

**Files:**
- Verify: backend, frontend, and browser smoke evidence

- [x] **Step 1: Run lint and focused tests**

Run ruff plus the backend contract test and full desktop frontend test.

- [x] **Step 2: Run browser smoke**

Verify overview and Trade Planner plan rendering at desktop and mobile widths using the local Tauri static harness.

- [ ] **Step 3: Commit, push, PR, merge**

Commit this slice on `codex/trading-workbench-plan-surface`, open and merge a PR, fast-forward root `main`, and clean up the worktree.
