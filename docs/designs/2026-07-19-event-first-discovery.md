# Event-First Discovery Radar — Design

**Date:** 2026-07-19  
**Status:** Multi-phase — P0 shipped (PR #1114); Phase 1 daily loop in progress  
**Product goal:** Surface stocks that may still be **under-discovered by price** relative to **world events**, for human research only (not investment advice, not autonomous trading).  
**Full product plan:** `docs/superpowers/plans/2026-07-19-marketradar-event-first-product.md`

## Problem

MarketRadar already has a strong **priced-in gap** model (emotion vs price reaction) and a full-market scan surface. Live operator state shows market bars and a full scan can be complete while the product still blocks on thousands of **SEC catalyst-event** gaps.

That inverted the valuable loop:

- **Desired:** world event → map equities → check whether price reacted → human review  
- **Current default:** fill per-ticker filings until a full-market trust gate passes  

X / world narrative is not a first-class ingest path. Social is only a low-quality `SourceCategory`. The Tauri workbench optimizes for trading ops, not event discovery.

## Design thesis

Keep deterministic scoring and fail-closed action gates. **Re-center the product object** on **world events**, with Grok Build (X search + scheduled tasks) as the event bus.

```text
X / world posts  ──ingest file──►  WorldEvent store
                                      │
                         theme + ticker mapper
                                      │
                         local priced-in join
                                      │
                         Discovery Brief (research-only)
                                      │
                         Tauri World Events + Discovery UI
```

## Non-goals (P0)

- Live X API OAuth inside MarketRadar  
- Autonomous trading or broker order submission  
- Replacing SEC/Polygon pipelines  
- Trusting social-only claims as “manual buy review” eligible  

## Trust ladder

| Evidence | Highest allowed use |
|---|---|
| X / social only | `research_only` |
| + primary/regulatory confirmation | `watch` |
| + quiet tape + liquidity + no hard blocks | existing policy may allow higher states later |

Emotion from social may raise attention; **action still requires independent confirmation**.

## P0 contract

### World event JSON schema (`world-events-v1`)

```json
{
  "schema_version": "world-events-v1",
  "generated_at": "2026-07-19T12:00:00+00:00",
  "source": "grok_x_pilot",
  "events": [
    {
      "id": "evt_hormuz_shipping_2026_07",
      "title": "Strait of Hormuz shipping risk elevates energy and logistics premia",
      "summary": "...",
      "themes": ["energy_security", "shipping", "defense"],
      "tickers": ["XOM", "CVX", "LMT"],
      "secondary_tickers": ["ETN", "VRT"],
      "direction": "bullish",
      "materiality": 0.72,
      "source_quality": 0.35,
      "source_category": "social",
      "sources": [
        {
          "provider": "x",
          "url": "https://x.com/i/status/123",
          "author": "@example",
          "published_at": "2026-07-18T00:00:00+00:00",
          "engagement": {"likes": 100, "views": 10000}
        }
      ],
      "available_at": "2026-07-19T12:00:00+00:00"
    }
  ]
}
```

### Discovery brief (`discovery-brief-v1`)

Zero provider calls when reading local event JSON + optional local DB join:

- `events[]` with mapped tickers  
- `discoveries[]` ranked by `emotion_reaction_gap` when priced-in rows exist, else by materiality × source quality  
- `investment_advice: false`  
- `can_make_investment_decision: false`  
- `external_calls_made: 0`, `db_writes_made: 0` unless a future explicit import path is approved  

### Surfaces

| Surface | Role |
|---|---|
| `catalyst-radar discovery-brief` | Scriptable zero-call brief |
| Dashboard snapshot `event_discovery` | Feed desktop/TUI |
| Tauri page `world-events` | Human event inbox + discovery queue |
| Grok scheduled task | Refresh event JSON daily (pilot) |

## Tradeoffs

1. **File-based X ingest vs live API**  
   Chosen for P0: Grok automation writes JSON; MarketRadar never needs X credentials. Latency = task cadence.

2. **Event-first vs full-universe trust gate**  
   Discovery brief can surface research leads without clearing 5k SEC gaps. Full-market trust remains separate.

3. **Theme mapper quality**  
   Deterministic theme→ticker map + event-declared tickers first. Sparse LLM mapping is P1.

4. **Misinformation**  
   Social source quality capped; discoveries are research-only until primary sources confirm.

5. **UI complexity**  
   Add one World Events page; do not remove workbench pages in P0.

## Success metrics

- Operator can open World Events and see today’s clustered events without provider calls  
- Discovery queue names at least one second-order ticker with gap rationale  
- Shadow/value loop can later label discovery rows (P1/P2)  
- No hidden calls/writes on browse  

## Phases

- **P0 (this PR):** design, pilot fixture, discovery module, CLI, snapshot field, Tauri World Events page  
- **P1:** scheduled ingest path into event store, stronger theme ontology, daily Grok task output path under `data/local/`  
- **P2:** case file joins X + SEC; shadow outcomes for discovery rows  
- **P3:** demote residual-bar ops from default UX  
