# Task: Daily discovery operability

## Goal
Close the morning loop so an operator can go from X/Grok posts + mapped bars
to `assert-discovery-ready` without the full-market workbench.

## Acceptance criteria
1. `discovery-from-posts` turns `x-posts-v1` into `world-events-v1` with zero calls.
2. `discovery-bars` upserts mapped-ticker daily bars from a CSV (preview/execute).
3. Fixture path: posts + bars → `assert-discovery-ready` reports `ready=true`.
4. `scripts/run-discovery-daily.ps1` accepts `-PostsPath` and `-BarsCsv`.
5. Unit tests cover transform, bar import, and the ready gate.

## Validation
```powershell
$env:PYTHONPATH='src'
.\.venv\Scripts\python.exe -m pytest tests\unit\test_discovery_from_posts.py tests\unit\test_discovery_bars.py tests\unit\test_discovery_daily_loop.py tests\unit\test_discovery_ready.py -q
```

## Status
done — branch feat/discovery-daily-operability

## Validation evidence
- pytest from_posts, bars, daily_loop, ready, product_scope, import_guard, local_scripts: 30 passed
