# Tauri Trading Workbench Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the Tauri app from a MarketRadar dashboard into a comprehensive trading workbench shell with MarketRadar as one tool and live trading visibly disabled.

**Architecture:** Keep the existing zero-call Tauri snapshot contract and extend the desktop configuration with a trading-platform module map. The first UI slice adds routeable platform pages and shell copy while reusing local snapshot data; deeper trading workflows remain backend-owned through explicit preview-first commands.

**Tech Stack:** Rust/Tauri 2, static HTML/CSS/JavaScript frontend, Python FastAPI dashboard config parity, pytest, cargo test.

---

## File Structure

- Modify `crates/radar-tui/src/model.rs` to add platform page keys and aliases.
- Modify `apps/radar-desktop/src/main.rs` to expose the workbench product frame, page manifest, platform module map, and automation safety notes.
- Modify `apps/radar-desktop/frontend/index.html` to reframe the shell as the Trading Workbench and add platform automation landmarks.
- Modify `apps/radar-desktop/frontend/app.js` to render the platform overview, module cards, platform pages, aliases, and command reference.
- Modify `apps/radar-desktop/frontend/styles.css` to support the platform map without nested card clutter or layout overflow.
- Modify `apps/radar-desktop/tauri.conf.json` to use the new native window title/product frame.
- Modify `src/catalyst_radar/api/routes/dashboard.py` so API desktop config exposes the same platform modules.
- Modify `tests/integration/test_desktop_dashboard_frontend.py` and `tests/integration/test_dashboard_api_routes.py` for contract coverage.

## Task 1: Platform Manifest

**Files:**
- Modify: `crates/radar-tui/src/model.rs`
- Modify: `apps/radar-desktop/src/main.rs`
- Modify: `src/catalyst_radar/api/routes/dashboard.py`
- Test: `tests/integration/test_dashboard_api_routes.py`

- [x] **Step 1: Add failing manifest assertions**

```python
def test_desktop_config_exposes_trading_workbench_platform_map() -> None:
    response = client.get("/api/dashboard/desktop-config")
    payload = response.json()

    assert payload["app_name"] == "MarketRadar Trading Workbench"
    assert payload["platform"]["schema_version"] == "trading-workbench-platform-v1"
    assert any(module["key"] == "market-radar" for module in payload["platform"]["modules"])
    assert any(module["key"] == "trade-planner" for module in payload["platform"]["modules"])
    assert payload["platform"]["execution_boundary"]["live_trading_enabled"] is False
```

- [x] **Step 2: Implement manifest structures**

Add a `TradingPlatformManifest` with module rows for Command Center, Portfolio, Market Radar, Trade Planner, Risk Desk, Paper Trading, Broker Desk, Backtest/Replay, Alerts, Journal, and Agent Cockpit. Include an execution boundary that reports `live_trading_enabled=false`.

- [x] **Step 3: Verify**

Run: `C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_api_routes.py -q`

Expected: pass.

## Task 2: Tauri Shell Routes

**Files:**
- Modify: `crates/radar-tui/src/model.rs`
- Modify: `apps/radar-desktop/src/main.rs`
- Modify: `apps/radar-desktop/frontend/app.js`
- Test: `tests/integration/test_desktop_dashboard_frontend.py`

- [x] **Step 1: Add static contract assertions**

```python
assert 'data-testid="trading-workbench-overview"' in source
assert "renderTradingWorkbenchOverview" in app_js
assert "market-radar" in app_js
assert "trade-planner" in app_js
assert "risk-desk" in app_js
assert "paper-trading" in app_js
assert "journal" in app_js
```

- [x] **Step 2: Add route aliases**

Add page enum variants and JS aliases for `command-center`, `portfolio`, `market-radar`, `trade-planner`, `risk-desk`, `paper-trading`, `broker-desk`, `backtest`, `journal`, and `agent-cockpit`. Preserve legacy aliases like `overview`, `broker`, `agent`, and `validation`.

- [x] **Step 3: Add platform renderers**

Render the Command Center overview as a platform map plus the existing queue and safety panels. Render each new page as a focused module page with data sources, current capability, disabled live trading state, and next safe command.

- [x] **Step 4: Verify**

Run: `C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_desktop_dashboard_frontend.py -q`

Expected: pass.

## Task 3: Visual And Native QA

**Files:**
- Modify: `apps/radar-desktop/frontend/styles.css`
- Modify: `apps/radar-desktop/tauri.conf.json`

- [x] **Step 1: Reframe native title and shell copy**

Use `MarketRadar Trading Workbench` in the HTML title, native product frame, topbar, automation text, and Tauri window title.

- [x] **Step 2: Add responsive platform styles**

Add a compact, dense module grid. Cards must not be nested inside other cards, text must wrap cleanly, and the first viewport must show the product frame plus actionable status.

- [x] **Step 3: Verify**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_desktop_dashboard_frontend.py tests\integration\test_dashboard_api_routes.py -q
cargo test -p radar-desktop
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m ruff check src tests
git diff --check
```

Expected: all pass.

## Completion Criteria For This Plan

- The workbench shell is routeable and contract-tested.
- MarketRadar is represented as a scouting tool inside the platform.
- Live trading is explicitly disabled in the UI and manifests.
- The changes are committed, pushed, opened as a PR, and merged by rebase.
