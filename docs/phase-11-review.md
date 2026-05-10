# Phase 11 Review: Alerts And Feedback Loop

## Outcome

Phase 11 adds shadow-mode alert artifacts, deterministic alert routing and dedupe, alert suppressions, dry-run delivery, alert feedback, API routes, CLI commands, and dashboard review surfaces.

Alerts remain decision-support prompts only. External delivery is dry-run only, and feedback writes an audit row plus the useful-alert validation label without mutating candidate states, policy output, Decision Cards, paper trades, or portfolio data.

## Verification

Full verification:

```text
python -m pytest
367 passed in 147.63s (0:02:27)

python -m ruff check src tests apps
All checks passed!
```

Focused post-review verification:

```text
python -m pytest tests/integration/test_alerts_cli.py tests/integration/test_alert_api_routes.py tests/integration/test_api_routes.py tests/integration/test_dashboard_data.py
38 passed in 71.50s (0:01:11)

python -m pytest tests/integration/test_dashboard_data.py
10 passed in 17.76s
```

## CLI Smoke

Fresh SQLite smoke database:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
scanned candidates=3
built candidate_packets=2
built decision_cards=2
built_alerts alerts=1 suppressions=2 available_at=2026-05-10T14:00:00+00:00
BBB alert route=daily_digest status=planned dedupe_key=alert-dedupe-v1:BBB:daily_digest:AddToWatchlist:state_transition:state_transition:None->AddToWatchlist
alert_digest groups=1 alerts=1 suppressed=2
send_alerts dry_run=true alerts=1
```

Because `2026-05-10T14:00:00Z` was still future relative to the smoke run clock, a second current-time `build-alerts` command was run against the same database to create a visible API/dashboard alert:

```text
built_alerts alerts=1 suppressions=2 available_at=2026-05-10T00:58:39.386069+00:00
```

## API Smoke

Started Uvicorn on `127.0.0.1:8010`.

Results:

- `GET /api/health` returned `{"status":"ok","service":"catalyst-radar"}`.
- `GET /docs` loaded Swagger UI with title `Catalyst Radar API - Swagger UI`.
- `GET /api/alerts` returned the visible planned BBB alert from the smoke database.

## Dashboard Smoke

Started Streamlit on `127.0.0.1:8509`.

Results:

- `/_stcore/health` returned `ok`.
- `/Alerts` loaded with the Alerts title, alert queue, and alert detail sections.
- The only browser console errors were Streamlit direct-page `_stcore` 404s.
- The earlier Streamlit `use_container_width` deprecation warning was fixed by moving dashboard dataframes to `width="stretch"`.

## Review Findings Fixed

- Replaced raw composite alert IDs with deterministic URL-safe digest IDs.
- Normalized alert payloads to JSON-safe values before persistence.
- Canonicalized evidence fingerprints and removed raw URLs from dedupe keys.
- Prevented dry-run delivery payload defaults from overriding invariant audit fields.
- Added feedback/source linkage fields to digest payloads.
- Filtered validation summary useful labels with the same artifact-matching rules as cost summary.
- Prevented planner limit behavior from dropping high-priority candidates by ticker order.
- Made packet/card planning respect both `available_at` and `created_at` cutoffs.
- Made `send-alerts` process only planned alerts.
- Made CLI digest counts match digest-channel groups while preserving total visible alert count.
- Blocked generic feedback for future artifacts.
- Returned 422 for invalid alert route/status filters.
- Made feedback audit and useful-label writes atomic.
- Rejected unknown JSON fields in feedback request models.
- Applied current-time cutoffs to default dashboard and CLI alert views so future alerts stay hidden.
- Routed the legacy `useful-label` CLI through the validated feedback service so it writes `user_feedback` and rejects missing, mismatched, future, or invalid artifacts.

## Residual Risks

- Real email/webhook/Slack/Telegram delivery remains out of scope.
- There is no scheduler or background worker yet.
- API authentication and authorization are still absent.
- Alert tuning is deterministic but not yet calibrated on live usefulness history.
- The sample fixture smoke validates the path, but broader market coverage still needs real provider data and scheduled replay.
