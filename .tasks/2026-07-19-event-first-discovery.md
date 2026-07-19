# Task: Event-First Discovery P0

## Goal
Find stocks whose price may not yet reflect world events, via X-fed event spine + priced-in gap + discovery UI.

## Acceptance criteria
1. Design doc and implementation plan committed on `feat/event-first-discovery`.
2. `catalyst-radar discovery-brief --events data/sample/world_events.json --json` returns `discovery-brief-v1` with events and discoveries, 0 external calls, investment_advice false.
3. Dashboard snapshot includes `event_discovery` when sample events are present.
4. Tauri exposes `world-events` page rendering events + discovery queue.
5. Live pilot brief produced; Grok daily task created for ongoing briefs.
6. Unit tests for discovery brief pass.

## Validation commands
```powershell
cd C:\Users\fpan1\MarketRadar\.worktrees\event-first-discovery
$env:PYTHONPATH='src'
..\..\.venv\Scripts\python.exe -m pytest tests\unit\test_discovery_brief.py -q
..\..\.venv\Scripts\python.exe -m catalyst_radar.cli discovery-brief --events data\sample\world_events.json --json
```

## Changed files
(filled at handoff)

## Risks
- Social misinformation → research_only only
- Theme mapper coverage is thin at P0
- Sample tickers may not join live priced-in rows without matching universe

## Status
in_progress
