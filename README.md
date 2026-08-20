# MarketRadar

**Product scope (authoritative):** event-first discovery only.  
Narrative contract: `docs/designs/2026-08-15-marketradar-product-spec.md`.  
Ship-gate table and laws: `docs/PRODUCT_SCOPE.md`. Everything else is
**deprecated** (`docs/DEPRECATION.md`, `docs/legacy/`).

MarketRadar turns **world events** into a ranked list of equities whose **price
may not have fully discovered** the event yet. It is decision support only — not
investment advice, and it never submits broker orders.

Inspect live scope: `catalyst-radar product-scope --json`  
Ship gate: `catalyst-radar assert-discovery-ready --json`

## Grok Build (`/market-radar`)

This repo is a **skill-triggered** Grok Build. The skill mines X and analyzes;
the desktop is the last screen you look at.

This repo is meant to be opened **as Grok Build**, not as a generic coding agent.

```powershell
cd C:\Users\fpan1\MarketRadar
grok
```

Then one skill with params:

| Command | What it does |
|---------|----------------|
| `/market-radar hunt` | Mine X, install today’s file |
| `/market-radar brief` | Show stories |
| `/market-radar ready` | Ship gate |
| `/market-radar` | Hunt if the file is stale, else brief |

Headless / daily:

```powershell
grok -p "/market-radar hunt" --cwd C:\Users\fpan1\MarketRadar
grok -p "/market-radar brief" --cwd C:\Users\fpan1\MarketRadar
```

Scripts: `scripts/radar-grok.ps1 brief|convert|ready|bars|status`.  
Skill: `.grok/skills/market-radar/SKILL.md` (`/market-radar hunt`). The desktop only **reads**.

## Daily path

1. Produce a fresh `world-events-v1` JSON (Grok daily task or manual file).
   Do **not** install `data/sample/world_events.json` as if it were live.
2. Install and smoke-check:

```powershell
# From a local x-posts-v1 dump (zero provider calls):
catalyst-radar discovery-from-posts --posts path\to\x_posts.json --execute
# Or install an already-built world-events-v1 file:
powershell -ExecutionPolicy Bypass -File scripts/refresh-world-events.ps1 -EventsPath path\to\world_events.json -Execute

# Optional: mapped bars so the join is event-time, not missing_scan
# Discovery bar path (mapped tickers only). Confirm, then add --execute to write.
catalyst-radar discovery-bars --polygon --confirm-external-call
# Local CSV alternative (zero provider calls):
catalyst-radar discovery-bars --csv path\to\mapped_bars.csv --execute

catalyst-radar discovery-brief --json --persist
catalyst-radar discovery-insights
catalyst-radar assert-discovery-ready --json

Real-data path (Polygon mapped tickers only, explicit confirm):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-real-discovery.ps1 -Execute -ConfirmExternalCall
```
```

3. Open the desktop app on **World Events** (`scripts/open-market-radar.ps1` or
   `target/release/radar-desktop.exe`). Default snapshot is
   `scripts/discovery-snapshot.py`.
4. Review the discovery queue. Social/X-only rows stay `research_only`.
5. Open a case, confirm with primary sources, then label:

```powershell
catalyst-radar discovery-case MU --json
catalyst-radar discovery-label --ticker MU --label good-research --preview --json
```

6. After bars advance:

```powershell
catalyst-radar discovery-outcomes --preview --json
```

Join coverage is **event-time**: a ticker is `joined` only when local daily bars
reach the event window and are less than 7 days stale. Old `candidate_states`
rows do not count. Missing/stale bars are `missing_scan`, not quiet tape.

## Product tests

```powershell
$env:PYTHONPATH='src'
.\.venv\Scripts\python.exe -m pytest tests\unit\test_discovery_*.py tests\unit\test_product_scope.py -q
```

## Out of scope

Trading workbench, Streamlit, Python TUI, broker orders, IPO desk, alerts as
product, agent cockpit, and full-market residual-repair are deprecated. They
stay importable only when `CATALYST_ENABLE_LEGACY_WORKBENCH=true`.

Historical notes: `docs/legacy/`, `handoff.md`.
