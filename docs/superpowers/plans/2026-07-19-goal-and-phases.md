# Goal reminder and careful phase plan

**Date:** 2026-07-19  
**Branch:** `feat/event-first-discovery`

## What you asked for (original goal)

> Find stocks **worth investing** whose **price hasn't been discovered yet** according to **world events**.

Revamp MarketRadar with:

- Public X / world narrative as a first-class input (via Grok Build automation)
- A usable front end for that discovery loop
- Decision support only (not auto-trading, not investment advice as a product claim)

## How we interpret “goal reached”

“Worth investing” in this product means **worth human research attention**, not “guaranteed alpha” or auto-buy. A capital decision stays human + policy.

| # | Success criterion | Why it maps to your goal |
|---|-------------------|---------------------------|
| G1 | Daily world events in → ranked equity discoveries out | World events drive the list |
| G2 | Each lead shows **emotion/event vs price reaction** (or honest `missing_scan`) | “Price not discovered yet” is measurable |
| G3 | Quiet / under-reacted names rank above already-moved hype when join exists | Filters “already priced” |
| G4 | Case file: *why this ticker, which event, what would invalidate* | Research-grade, not a ticker dump |
| G5 | Trust ladder: social-only stays research-only until primary confirmation | “Worth investing” requires confirmation |
| G6 | Label discoveries (useful / noisy / too-late / false-positive) and track outcomes | Prove the radar is useful over time |
| G7 | Zero hidden provider/broker side effects while browsing | Safe daily tool |
| G8 | Nice front end centered on Events → Discovery → Case → Proof | Operator can actually use it |

**Goal reached** when G1–G7 work end-to-end on the branch (G8 present for Events/Discovery/Case; polish can continue).

Not required for “goal reached”: full-market SEC residual repair, Schwab orders, or LLM-owned scores.

## Phase plan (careful)

```text
World events (X/Grok)
        │
        ▼
  Discovery brief  ──G1──  ranked tickers
        │
        ├── join reaction  ──G2/G3──  “not discovered yet?”
        │
        ▼
  Case file  ──G4/G5──  evidence + confirmation + invalidation
        │
        ▼
  Label + outcomes  ──G6──  is this radar worth it?
        │
        ▼
  Default UX  ──G8──  product feels like discovery, not ops
```

| Phase | Name | Criteria advanced | Status |
|-------|------|-------------------|--------|
| 0 | Spine | G1 partial, G7 | **Done** — JSON, brief, World Events page |
| 1 | Daily loop | G1, G2, G3, G7 | **Done** — freshness, ingest, join status, quiet-tape, fan-out |
| 2 | Case file | G4, G5 | **Done** — `discovery-case`, case panel in World Events |
| 3 | Proof / labels | G6 | **Done** — `discovery_row` ledger type + `discovery-label` |
| 4 | Product default UX | G8 | **Done** — default landing World Events; ops demoted |
| 5 | Enrichment | Better confirmation sources | Optional |

## Dependency rules

1. Do not block discovery on full-universe SEC fill.  
2. Social never elevates past research-only without primary/corroboration.  
3. Case file and labels do not require live broker.  
4. Browse paths remain zero-call / zero-write unless explicit execute.

## Stop line for this workstream

Stop claiming “goal reached” only when:

- `discovery-brief` returns ranked discoveries from world events  
- Case file exists for a discovery ticker with event lineage + invalidation  
- `value-ledger` accepts `discovery_row` labels  
- Desktop shows Events + Discovery + Case path  
- Social-only remains research-only  
- Tests cover brief, case, and ledger type  

Then: run live 5 days with Grok task (operator ops) and fill reaction joins against the local DB for G2 quality.
