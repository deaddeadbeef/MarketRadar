# Event-First Discovery Implementation Plan

> **For agentic workers:** Implement task-by-task. Steps use checkbox syntax.

**Goal:** Ship an event-first discovery spine: world-event JSON → ticker map → priced-in join → CLI brief + Tauri World Events page.

**Architecture:** File-based world events (written by Grok pilot/tasks) feed a pure Python discovery package. Optional local DB join attaches priced-in gap scores. Dashboard snapshot exposes `event_discovery`; Tauri renders a `world-events` page. No live X API in-process.

**Tech Stack:** Python 3.11+, existing `catalyst_radar` CLI/dashboard, Rust Tauri desktop + `radar-tui` Page enum, pytest.

## Global Constraints

- Decision support only: `investment_advice=false`, never auto-trade  
- Browse/snapshot paths: `external_calls_made=0`  
- Social-only discoveries stay `research_only`  
- Isolated branch `feat/event-first-discovery` (no main edits)  
- Rebase merge for PRs  

---

### Task 1: Discovery package + fixture

**Files:**
- Create: `src/catalyst_radar/discovery/__init__.py`
- Create: `src/catalyst_radar/discovery/models.py`
- Create: `src/catalyst_radar/discovery/mapper.py`
- Create: `src/catalyst_radar/discovery/brief.py`
- Create: `data/sample/world_events.json`
- Create: `tests/unit/test_discovery_brief.py`

- [x] Models + load/validate `world-events-v1`
- [x] Theme/ticker mapping
- [x] Brief builder with optional priced-in join
- [x] Unit tests

### Task 2: CLI + dashboard snapshot

**Files:**
- Modify: `src/catalyst_radar/cli.py`
- Modify: `src/catalyst_radar/dashboard/tui.py`

- [x] `discovery-brief` command
- [x] `event_discovery` on dashboard snapshot

### Task 3: Tauri World Events page

**Files:**
- Modify: `crates/radar-tui/src/model.rs`
- Modify: `crates/radar-tui/src/ui.rs` (match arm)
- Modify: `apps/radar-desktop/src/main.rs` (shortcut/description)
- Modify: `apps/radar-desktop/frontend/app.js`

- [x] `Page::WorldEvents`
- [x] Frontend renderer for events + discoveries

### Task 4: Pilot + task state + docs

**Files:**
- Create: `docs/designs/2026-07-19-event-first-discovery.md`
- Create: `.tasks/2026-07-19-event-first-discovery.md`
- Create: `.state/discovery-pilot-latest.md` (local pilot narrative)

- [x] Design + plan
- [x] Live pilot brief from X
- [x] Grok scheduled daily task

### Task 5: Validate

- [ ] `pytest tests/unit/test_discovery_brief.py -q`
- [ ] `discovery-brief --json` smoke
- [ ] Desktop frontend tests if present for page keys
