# MarketRadar

**Product scope (authoritative):** event-first discovery only.  
Narrative contract: `docs/designs/2026-08-15-marketradar-product-spec.md`.  
Ship-gate table and laws: `docs/PRODUCT_SCOPE.md`. Everything else is
**deprecated** (`docs/DEPRECATION.md`, `docs/legacy/`).

MarketRadar turns **world events** into a ranked list of equities whose **price
may not have fully discovered** the event yet. It is decision support only — not
investment advice, and it never submits broker orders.

**CLI discovery is primary.** The human receive surface is `catalyst-radar` JSON
(prefer `--json`, which is the default on hunt/convert/brief/bars/ready/product-scope).
Tauri World Events, radar-tui, and Streamlit are deprecated. Do not open the GUI.
Ops may file JSON into Notion **outside this repo**.

Inspect live scope: `catalyst-radar product-scope`  
Ship gate: `catalyst-radar ready`

## Grok Build (`/market-radar`)

This repo is a **skill-triggered** Grok Build. The skill mines X and analyzes;
the CLI is the last screen you look at.

This repo is meant to be opened **as Grok Build**, not as a generic coding agent.

```bash
cd /path/to/MarketRadar
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

```bash
grok -p "/market-radar hunt" --cwd /path/to/MarketRadar
grok -p "/market-radar brief" --cwd /path/to/MarketRadar
```

CLI (JSON by default):

```bash
catalyst-radar hunt
catalyst-radar convert --posts data/local/inbox/x_posts_YYYY-MM-DD.json --execute
catalyst-radar brief
catalyst-radar bars --public --confirm-external-call --execute
catalyst-radar ready
catalyst-radar product-scope
```

If the console script is not on PATH:

```bash
PYTHONPATH=src .venv/bin/python -m catalyst_radar.cli brief
```

Windows helper: `scripts/radar-grok.ps1 hunt|brief|convert|ready|bars|status`.  
Cross-platform: `python scripts/radar_grok.py …`.  
Skill: `.grok/skills/market-radar/SKILL.md` (`/market-radar hunt`).

`scripts/open-market-radar.sh` and `scripts/open-market-radar.ps1` are
**deprecated** and refuse to launch the GUI.

## Daily path

1. Produce a fresh `world-events-v1` JSON (Grok daily task or manual file).
   Do **not** install `data/sample/world_events.json` as if it were live.
2. Install and smoke-check:

```bash
# From a local x-posts-v1 dump (zero provider calls):
catalyst-radar convert --posts path/to/x_posts.json --execute
# Or install an already-built world-events-v1 file:
catalyst-radar discovery-ingest --events path/to/world_events.json --execute
# Windows leftover:
# powershell -ExecutionPolicy Bypass -File scripts/refresh-world-events.ps1 -EventsPath path\to/world_events.json -Execute

# Optional: mapped bars so the join is event-time, not missing_scan
# No-key public daily bars (Yahoo chart API). Confirm, then --execute to write.
catalyst-radar bars --public --confirm-external-call --execute
# Stooq alternative (same confirm/execute; some networks JS-challenge block it):
catalyst-radar bars --stooq --confirm-external-call --execute
# Paid Polygon path (unchanged; needs CATALYST_POLYGON_API_KEY):
catalyst-radar bars --polygon --confirm-external-call --execute
# Local CSV alternative (zero provider calls):
catalyst-radar discovery-bars --csv path/to/mapped_bars.csv --execute

catalyst-radar brief --persist
catalyst-radar discovery-insights
catalyst-radar ready
```

Real-data path (Polygon mapped tickers only, explicit confirm):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-real-discovery.ps1 -Execute -ConfirmExternalCall
```

3. Read the briefing JSON. Social/X-only rows stay `research_only`.
   Do not launch `scripts/discovery-snapshot.py` as the daily reader
   (leftover for the deprecated desktop).
4. Open a case, confirm with primary sources, then label:

```bash
catalyst-radar discovery-case MU --json
catalyst-radar discovery-label --ticker MU --label good-research --preview --json
```

5. After bars advance:

```bash
catalyst-radar discovery-outcomes --preview --json
```

Join coverage is **event-time**: a ticker is `joined` only when local daily bars
reach the event window and are less than 7 days stale. Old `candidate_states`
rows do not count. Missing/stale bars are `missing_scan`, not quiet tape.

## Product tests

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/test_discovery_*.py tests/unit/test_product_scope.py -q
```

```powershell
$env:PYTHONPATH='src'
.\.venv\Scripts\python.exe -m pytest tests\unit\test_discovery_*.py tests\unit\test_product_scope.py -q
```

## Out of scope

Trading workbench, Streamlit, Python TUI, Tauri World Events, radar-desktop,
radar-tui, broker orders, IPO desk, alerts as product, agent cockpit, and
full-market residual-repair are deprecated. They stay importable only when
`CATALYST_ENABLE_LEGACY_WORKBENCH=true`. Cargo desktop crates may still
compile; they are **not required** to use the product.

Historical notes: `docs/legacy/`, `handoff.md`.
