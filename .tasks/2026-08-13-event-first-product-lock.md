# Task: Event-first product lock (phases 0–5)

## Goal
Make MarketRadar operable as event-first discovery: fresh events, event-time
price join, one default UI, hard-blocked legacy CLI, CI, and a measured gate.

## Acceptance criteria
1. `assert-discovery-ready --json` is the ship gate (not trial/shadow/investable).
2. Join uses mapped-ticker bars vs the event window, never `load_candidate_rows`.
3. Missing/stale bars are `missing_scan`, never quiet-tape.
4. Tickers are unique in the top queue; social stays `research_only` without
   SEC/EDGAR/PRIMARY/REGULATORY confirmation.
5. Stale refresh without `-EventsPath` errors (no validate-only dead end).
6. Default desktop snapshot is `scripts/discovery-snapshot.py`.
7. Deprecated CLI is blocked unless `CATALYST_ENABLE_LEGACY_WORKBENCH=true`.
8. CI runs discovery unit tests + ruff on the discovery surface.
9. README is event-first only; workbench docs are under `docs/legacy/`.

## Validation
```powershell
$env:PYTHONPATH='src'
.\.venv\Scripts\python.exe -m pytest tests\unit\test_discovery_*.py tests\unit\test_product_scope.py -q
.\.venv\Scripts\python.exe -m catalyst_radar.cli assert-discovery-ready --json
```

## Status
done — branch feat/event-first-product-lock

## Validation evidence
- pytest discovery + product_scope + local_scripts + desktop frontend: pass
- cargo test -p radar-desktop: 13 passed
- ruff on new discovery modules: pass
- assert-discovery-ready --no-db: exit 1, first_blocker=stale_events
