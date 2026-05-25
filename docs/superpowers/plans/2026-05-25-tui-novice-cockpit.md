# TUI Novice Cockpit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `radar` open as an intuitive market-insight cockpit for a first-time, non-professional user.

**Architecture:** Keep the existing `dashboard_snapshot_payload` contract and Textual app. Add a small presentation layer in `src/catalyst_radar/dashboard/tui.py` that turns the current snapshot into plain-English cards, workflow navigation labels, empty-state guidance, and scan legends. Do not add provider/model calls and do not move scan logic.

**Tech Stack:** Python 3.11, Textual, existing snapshot renderer, pytest, ruff.

---

## Requirements

This plan tracks GitHub issue #755 and milestone M4. Completion means:

- First screen says what MarketRadar does, whether the current answer is actionable, why it is blocked or safe, and exactly one recommended next step.
- Navigation reads like a workflow, not internal modules: Start, Scan Results, Evidence Gaps, Candidate Review, Safe Run, Agent, Ops.
- Actions and responses are visually distinct; any execute path shows call/write/OpenAI cost before execution.
- Empty scan state gives a concrete beginner path instead of raw blocker jargon.
- Scan rows include a plain legend for emotion, price reaction, gap, confidence, and decision readiness.
- Mouse and keyboard navigation still work.
- Dashboard browsing remains zero provider, broker, OpenAI, shell, web, and order calls.

## Files

- Modify `src/catalyst_radar/dashboard/tui.py`: novice cockpit copy, workflow nav labels, guide/action/response panels, overview and agent table rows, render text.
- Modify `tests/integration/test_dashboard_demo_seed_cli.py`: snapshot/render tests for beginner first screen, empty state, legend, and command response separation.
- Modify `README.md`: short operator note for the redesigned TUI.

## Task 1: Beginner Cockpit View Model

**Files:**
- Modify `src/catalyst_radar/dashboard/tui.py`
- Test `tests/integration/test_dashboard_demo_seed_cli.py`

- [x] **Step 1: Write failing render tests**

Add tests that call `dashboard_snapshot_payload(...)` and `render_dashboard_tui(..., page="overview")` for an empty database and a seeded demo database. Assert the output includes:

```python
assert "MarketRadar answers one question" in rendered
assert "Can I act?" in rendered
assert "Best next step" in rendered
assert "No scan rows yet" in rendered
assert "Browsing this dashboard made 0 calls" in rendered
```

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -k "novice or dashboard_tui" -q
```

Expected: new tests fail because the strings do not exist yet.

- [x] **Step 2: Add a pure helper for beginner summary cards**

In `src/catalyst_radar/dashboard/tui.py`, add:

```python
def _novice_cockpit_cards(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    answer = _mapping(payload.get("priced_in_answer"))
    readiness = _mapping(payload.get("readiness"))
    real_results = _mapping(payload.get("real_results"))
    next_step = _priced_in_operator_step(payload) or _mapping(payload.get("operator_next_step"))
    queue = _mapping(payload.get("priced_in_queue"))
    row_count = int(_number_or_zero(queue.get("total_count") or queue.get("count")))
    safe = bool(readiness.get("safe_to_make_investment_decision"))
    return [
        {
            "label": "What this is",
            "value": "MarketRadar answers one question",
            "detail": "Has market emotion toward a stock already been priced in?",
        },
        {
            "label": "Can I act?",
            "value": "No - research only" if not safe else "Manual review only",
            "detail": str(answer.get("answer") or readiness.get("headline") or "Evidence is not ready."),
        },
        {
            "label": "Best next step",
            "value": str(next_step.get("action") or readiness.get("next_action") or "Open Scan Results."),
            "detail": str(next_step.get("expected_response") or real_results.get("next_action") or "No provider call while browsing."),
        },
        {
            "label": "Rows",
            "value": f"{row_count} scan row(s)",
            "detail": "No scan rows yet" if row_count == 0 else "Open a row to inspect evidence.",
        },
    ]
```

- [x] **Step 3: Render those cards in `_overview_lines` and `_guide_text`**

Make overview begin with the cards before the existing dense source/audit detail. Keep current lower sections available for power users.

- [x] **Step 4: Run the tests**

Run the command from Step 1. Expected: tests pass.

## Task 2: Workflow Navigation Labels

**Files:**
- Modify `src/catalyst_radar/dashboard/tui.py`
- Test `tests/integration/test_dashboard_demo_seed_cli.py`

- [x] **Step 1: Write failing nav tests**

Assert the rendered page and Textual sidebar labels include beginner workflow names:

```python
assert "0 Start" in rendered
assert "1 Scan Results" in rendered
assert "2 Evidence Gaps" in rendered
assert "3 Safe Run" in rendered
assert "4 Candidate Review" in rendered
assert "10 Agent Coach" in rendered
```

- [x] **Step 2: Rename display labels without changing page IDs**

Update `PAGE_ORDER` labels only:

```python
("overview", "1", "Scan Results")
("readiness", "2", "Evidence Gaps")
("run", "3", "Safe Run")
("candidates", "4", "Candidate Review")
("agent", "10", "Agent Coach")
```

Keep existing aliases and page IDs stable so commands/tests do not break.

- [x] **Step 3: Run focused render tests**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -k "navigation or novice" -q
```

Expected: pass.

## Task 3: Action vs Response Separation

**Files:**
- Modify `src/catalyst_radar/dashboard/tui.py`
- Test `tests/integration/test_dashboard_demo_seed_cli.py`

- [x] **Step 1: Write tests for action/response language**

Assert the bottom panels use beginner-safe labels:

```python
assert "NEXT SAFE ACTION" in rendered
assert "LAST RESPONSE" in rendered
assert "Cost before execute" in rendered
```

- [x] **Step 2: Update `_action_text`, `_response_text`, and execute command messages**

Use labels:

```text
NEXT SAFE ACTION
LAST RESPONSE
Cost before execute: provider calls X, OpenAI calls Y, DB writes Z.
```

Keep the existing command parser and cost counters.

- [x] **Step 3: Verify execute paths still require explicit commands**

Run tests covering `run execute`, `agent execute`, `bars saved capture confirm`, and empty browsing.

## Task 4: Scan Legend and Empty State

**Files:**
- Modify `src/catalyst_radar/dashboard/tui.py`
- Test `tests/integration/test_dashboard_demo_seed_cli.py`

- [x] **Step 1: Write tests for the legend**

Assert overview includes:

```python
assert "Legend:" in rendered
assert "Emotion" in rendered
assert "Price reaction" in rendered
assert "Gap" in rendered
assert "Decision-ready" in rendered
```

- [x] **Step 2: Add `_priced_in_beginner_legend`**

Return a one-line legend for overview and review pages:

```text
Legend: Emotion = market excitement/fear; Price reaction = how much price already moved; Gap = emotion minus reaction; Decision-ready = enough evidence for manual review.
```

- [x] **Step 3: Replace raw empty table text**

When no rows are visible, render:

```text
No scan rows yet.
Start here:
1. Import or fetch a ticker universe.
2. Fill fresh market bars.
3. Run a capped scan.
```

## Task 5: Documentation, Verification, PR

**Files:**
- Modify `README.md`

- [x] **Step 1: Document the redesigned dashboard**

Add a short section explaining:

```powershell
radar
radar --once --page overview
```

and that browsing remains zero-call.

- [x] **Step 2: Run verification**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -k "novice or dashboard_tui or navigation" -q
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
git diff --check
```

- [ ] **Step 3: Commit, push, PR, project update, merge**

Commit to `codex/tui-novice-redesign`, open a PR linked to #755, attach milestone M4, keep the project item In Progress until merged, then mark Done.

## Self-Review

- Spec coverage: all #755 acceptance items map to Tasks 1-5.
- Placeholder scan: no TBD/TODO/later placeholders.
- Type consistency: all helpers use existing local helper conventions: `_mapping`, `_number_or_zero`, `_priced_in_operator_step`, and `Mapping[str, object]`.
