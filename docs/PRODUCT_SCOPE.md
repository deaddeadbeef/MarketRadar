# MarketRadar product scope (event-first discovery)

**Authority date:** 2026-08-15  
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

---

## In scope (supported)

### Operator journey

1. **World events** — refresh via Grok daily task / `world-events-v1` JSON  
2. **Discovery queue** — mapped tickers + emotion vs reaction join  
3. **Case file** — operator analysis, trust ladder, invalidation  
4. **Proof** — `discovery_row` value-ledger labels and history  
5. **Supporting data path** — local bars/scan for **mapped tickers only**  
   (so join/reaction is real)

### Code / surfaces (keep)

| Area | Path / surface | Role |
|------|----------------|------|
| Discovery core | `src/catalyst_radar/discovery/` | Primary product logic |
| World-events I/O | `scripts/discovery-snapshot.py`, `scripts/import-world-events.ps1` | Daily loop |
| Desktop home | Tauri **World Events** page (`world-events`) | Primary UI |
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

### Product laws (non-negotiable)

- Decision support only; `investment_advice: false`
- Browse/snapshot default: zero hidden provider/broker/LLM calls
- Social/X-only leads stay `research_only` until primary confirmation
- Discovery never auto-submits broker orders
- Do not block discovery on full-universe SEC residual fill

Full law text: spec §6.

---

## Out of scope (deprecated product surface)

These may still run for legacy tests/ops, but they are **not** the product and
must not be presented as the primary path:

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

Details and removal phases: `docs/DEPRECATION.md`.

---

## Success metrics (product)

| Metric | Target |
|--------|--------|
| Fresh world events | Most weekdays `< 24h` age (`assert-discovery-ready`) |
| Top discovery join rate | ≥50% of top-20 **event-time** joins (bars in the event window, not old candidate rows) |
| Labels on discovery_row | Ongoing; enough for value-report ≠ empty |
| Safety | 0 accidental broker orders; social never buy-review |

Ship gate: `catalyst-radar assert-discovery-ready --json`. Do not use
`assert-trial-ready`, `assert-shadow-ready`, or `assert-investable-readiness`
as the discovery success criterion.

---

## Related plans

- `docs/designs/2026-08-15-marketradar-product-spec.md` (narrative contract)
- `docs/superpowers/plans/2026-07-19-marketradar-event-first-product.md` (historical)
- `docs/superpowers/plans/2026-07-19-goal-and-phases.md` (historical)
