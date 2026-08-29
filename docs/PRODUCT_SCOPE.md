# MarketRadar product scope (event-first discovery)

**Authority date:** 2026-08-29  
**Status:** Pointer plus ship-gate table. Not the narrative contract.

**Narrative product contract:**
`docs/designs/2026-08-15-marketradar-product-spec.md`  
**Signal types (pending binary vs gap-up):**
`docs/designs/2026-08-19-catalyst-signals.md`

This file keeps the supported-surface map, product laws, and ship-gate table.
Runtime registry: `src/catalyst_radar/deprecation.py`. Removal plan:
`docs/DEPRECATION.md`. Grok `event_id` contract: spec §15.1 (no separate file).

---

## One-sentence product

**MarketRadar is an operator-produced weekday briefing that a market newbie can
read: pending binaries and world events, mapped to companies, with honest
recent-tape context and a trust ladder. It is decision support only.**

The human reads **CLI JSON** (`catalyst-radar brief`). Ops may file that JSON
into Notion outside this repo. CLI discovery is primary. Desktop/TUI are
deprecated.

---

## In scope (supported)

### Operator journey

1. **Hunt** — Grok skill `/market-radar hunt` / `grok -p "/market-radar hunt"` writes `x-posts-v1`
2. **Convert** — `catalyst-radar convert` → `data/local/world_events.json`
3. **Brief** — `catalyst-radar brief` JSON (human receive surface)
4. **Case file** — `catalyst-radar discovery-case` operator analysis, trust ladder, invalidation
5. **Proof** — `discovery_row` value-ledger labels and history
6. **Supporting data path** — local bars/scan for **mapped tickers only**
   (`catalyst-radar bars`) so join/reaction is real
7. **Ready** — `catalyst-radar ready` / `assert-discovery-ready`
8. **Scope** — `catalyst-radar product-scope`

### Code / surfaces (keep)

| Area | Path / surface | Role |
|------|----------------|------|
| Discovery core | `src/catalyst_radar/discovery/` | Primary product logic |
| World-events I/O | `catalyst-radar convert` / `discovery-ingest`, `scripts/radar_grok.py` | Daily loop |
| **CLI home** | `catalyst-radar hunt\|convert\|brief\|bars\|ready\|product-scope` | **Primary product surface** (JSON) |
| Priced-in join | `scoring/priced_in.py`, `features/market.py`, `pipeline/scan.py` | Reaction join for mapped names |
| Market bars (supporting) | `market/`, `connectors/polygon*.py`, `ingest-polygon` / `market-bars` | Fill gaps for discovery |
| Proof ledger | `validation/value_ledger.py`, `discovery/label.py`, `discovery/proof.py` | Attention-value proof |
| Shared infra | `core/`, `storage/`, `security/`, `events/models.py` (+ light fan-out) | Platform primitives |
| Sparse Grok (optional) | `agents/llm_provider.py`, gated agent brief | Optional synthesis only |
| Config / docs | `.env.example`, README event-first path, this file | Operator contract |

### Operator leftover

| Area | Path / surface | Role |
|------|----------------|------|
| Mapped-bar leftover | `scripts/fill-discovery-gaps.*` | Operator leftover — not the keep supporting path |
| Desktop snapshot | `scripts/discovery-snapshot.py` | Leftover for deprecated Tauri client — not the daily reader |
| GUI launchers | `scripts/open-market-radar.sh`, `scripts/open-market-radar.ps1` | Deprecated; refuse to launch |

### Product laws (non-negotiable)

- Decision support only; `investment_advice: false`
- Browse/snapshot default: zero hidden provider/broker/LLM calls
- Social/X-only leads stay `research_only` until primary confirmation
- Discovery never auto-submits broker orders
- Do not block discovery on full-universe SEC residual fill
- Do not tell anyone to open the GUI or press R

Full law text: spec §6.

---

## Out of scope (deprecated product surface)

These may still run for legacy tests/ops, but they are **not** the product and
must not be presented as the primary path:

- Tauri desktop / World Events page (`apps/radar-desktop`)
- Rust TUI (`crates/radar-tui`) and Python `dashboard-tui`
- Streamlit (`apps/dashboard/Home.py`)
- Full trading workbench (portfolio, trade planner, risk desk, paper trading,
  order tickets, broker desk as product)
- Full-market residual-repair hero path as the daily operator loop
- IPO/S-1 product surface
- Alerts digests as primary discovery delivery (optional later)
- Agent cockpit / autonomous agent loops as primary UX
- Decision-card capital workflow as primary discovery UX
- Themes / features inventory pages as primary navigation
- Backtest / replay / shadow-investable gates as the discovery success criterion
- Remote ops runner as product (keep only if needed for infra later)

Cargo desktop crates may remain in the workspace so leftover CI can compile.
They are **not required** to use the product.

Details and removal phases: `docs/DEPRECATION.md`.

---

## Success metrics (product)

| Metric | Target |
|--------|--------|
| Fresh world events | Most weekdays `< 24h` age (`assert-discovery-ready`) |
| Top discovery join rate | ≥50% of top-20 **event-time** joins (bars in the event window, not old candidate rows) |
| Labels on discovery_row | Ongoing; enough for value-report ≠ empty |
| Safety | 0 accidental broker orders; social never buy-review |

Ship gate: `catalyst-radar ready` / `assert-discovery-ready --json`. Do not use
`assert-trial-ready`, `assert-shadow-ready`, or `assert-investable-readiness`
as the discovery success criterion.

---

## Related plans

- `docs/designs/2026-08-15-marketradar-product-spec.md` (narrative contract)
- `docs/superpowers/plans/2026-07-19-marketradar-event-first-product.md` (historical)
- `docs/superpowers/plans/2026-07-19-goal-and-phases.md` (historical)
