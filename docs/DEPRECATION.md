# Deprecation and phased removal plan

**Authority date:** 2026-07-19  
**Product scope:** `docs/PRODUCT_SCOPE.md`  
**Policy:** Mark → hide from default UX → warn on CLI → remove code/tests in phases.

Nothing in this file deletes runtime behavior yet except labeling and navigation
emphasis. Code stays importable until its removal phase.

---

## Status legend

| Status | Meaning |
|--------|---------|
| **active** | Supported product surface |
| **supporting** | Not the product hero, but required by discovery (keep) |
| **deprecated** | Do not build features; scheduled for removal |
| **legacy-test-only** | Kept only until tests migrate off it |

---

## Python packages

| Package | Status | Notes |
|---------|--------|-------|
| `discovery` | **active** | Event-first product core |
| `core`, `storage`, `security` | **active** | Shared infra |
| `scoring.priced_in`, `scoring.score`, `scoring.policy` | **supporting** | Join + fail-closed policy |
| `features.market` | **supporting** | Returns / reaction inputs |
| `pipeline.scan` | **supporting** | Mapped-ticker scan |
| `market`, `connectors.polygon*` | **supporting** | Bars for join quality |
| `validation.value_ledger`, `value_outcomes`, `value_report` | **supporting** | Proof loop |
| `events` (models, fan-out, source quality) | **supporting** | Confirmation / social fan-out |
| `agents.llm_provider`, sparse LLM client | **supporting** | Optional Grok synthesis |
| `agents` (orchestrator, paper_trading, full SDK loop) | **deprecated** | Not primary product UX |
| `alerts` | **deprecated** | Optional later; not discovery core |
| `brokers`, `trading` | **deprecated** | No order product path |
| `decision_cards` | **deprecated** | Not discovery primary UX |
| `ipo` | **deprecated** | Separate product surface |
| `portfolio` | **deprecated** | Workbench-only |
| `textint` | **deprecated** | Not required for world-events spine |
| `universe` (liquid seed hero) | **deprecated** | Discovery uses mapped tickers |
| `ops.remote_runs` | **deprecated** | Infra side-quest |
| `dashboard.tui` full workbench | **deprecated** | Desktop discovery is primary |
| `dashboard.data` (monolith) | **legacy-test-only** | Shrink with page removal |
| `api.routes` non-discovery | **deprecated** | Keep health/db until callers die |
| `jobs` full daily radar | **supporting→trim** | Keep mapped scan path; trim hero residual |

Registry source of truth in code: `src/catalyst_radar/deprecation.py`.

---

## Desktop / TUI pages

| Page key | Status |
|----------|--------|
| `world-events` | **active** (primary) |
| `help` | **active** (docs/keys) |
| `overview` / workbench | **deprecated** |
| `portfolio`, `market-radar`, `trade-planner`, `risk-desk` | **deprecated** |
| `paper-trading`, `backtest`, `broker` | **deprecated** |
| `readiness`, `run`, `candidates`, `review` | **deprecated** as primary path |
| `alerts`, `ipo`, `ops`, `telemetry`, `agent` | **deprecated** |
| `themes`, `validation`, `costs`, `features`, `journal` | **deprecated** |
| `tutorial` | **deprecated** (replace with discovery quickstart later) |

Labels in the UI use a `deprecated` prefix so operators see the boundary.

---

## CLI command families

### Active / supporting (keep)

- `discovery-brief`, `discovery-ingest`, `discovery-case`, `discovery-label`
- `market-bars` / `ingest-polygon` (supporting bar fill)
- `scan` / `run-daily` **when used for mapped tickers** (supporting)
- `value-ledger`, `value-report`, `value-outcome*` (proof)
- `init-db`, config/env helpers

### Deprecated (warn; remove later)

- Full workbench dashboard command surface as product
- Broker interactive / order preview / paper-decision product commands
- IPO S-1 analysis as product
- Agent cockpit execute as product
- Alert digest delivery as product
- Full-universe residual-repair hero scripts as the default onboarding

Exact lists: `DEPRECATED_CLI_COMMANDS` in `deprecation.py`.  
Runtime: `catalyst-radar product-scope --json`.

---

## Removal phases

### Phase D1 — Contract and labels (this change)

- [x] `docs/PRODUCT_SCOPE.md`
- [x] `docs/DEPRECATION.md`
- [x] `catalyst_radar.deprecation` registry + `product-scope` CLI
- [x] README points at scope
- [x] Desktop page labels mark deprecated surfaces
- [x] Package `__init__` module docs for major deprecated packages
- [x] CLI stderr warning on deprecated commands
- [x] Discovery-home nav limited to World Events + Help

### Phase D2 — Default UX lockdown

- [x] Hide deprecated pages unless `CATALYST_ENABLE_LEGACY_WORKBENCH=true`
- [x] Discovery-home nav is World Events + Help only
- [x] Workbench pages labeled Legacy when flag is on
- [x] README / PRODUCT_SCOPE document residual-repair as non-primary
- [x] Goal join-coverage banner on World Events (target ≥50%)

### Phase D3 — CLI warnings

- [x] Emit stderr notice when deprecated commands run
- [x] Unit guard: `discovery/` does not import brokers/trading/ipo/alerts
- [x] `discovery-outcomes` active CLI for forward proof

### Phase D4 — Code quarantine

- Move deprecated packages under `src/catalyst_radar/legacy/` **or** delete
  with test migration
- Drop unused API routes and Tauri pages
- Shrink `dashboard/data.py` toward discovery snapshot only

### Phase D5 — Delete

- Remove legacy packages, tests, scripts, and docs that only serve deprecated UX
- Keep git history; do not force-push

---

## Rules for contributors

1. **New features** only land in **active** or **supporting** areas unless an ADR
   expands `PRODUCT_SCOPE.md`.
2. Do not “fix” deprecated pages except security/correctness blockers.
3. Prefer deleting call sites over extending deprecated APIs.
4. Discovery browse remains zero-call by default.

---

## Exit criteria for “scope limited”

- Operator opens desktop → World Events is the product  
- README start path is only event-first  
- `product-scope --json` lists active vs deprecated  
- Deprecation phases D2–D5 tracked as separate tasks/PRs  
