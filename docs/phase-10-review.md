# Phase 10 Review

## Outcome

Phase 10 adds a FastAPI review API, shared dashboard data helpers, Streamlit review pages, and explicit feedback recording for useful-alert labels.

The API and UI remain decision-support surfaces. Feedback is persisted as an audit label only and does not mutate candidate state, scores, policy output, or simulated-paper rows.

## Verification

Focused checks:

```powershell
python -m pytest tests/integration/test_api_routes.py tests/integration/test_dashboard_data.py
```

Result:

```text
17 passed in 28.29s
```

Lint:

```powershell
python -m ruff check apps src tests
```

Result:

```text
All checks passed!
```

Full suite:

```powershell
python -m pytest
```

Result:

```text
325 passed in 109.70s (0:01:49)
```

Full lint:

```powershell
python -m ruff check src tests apps
```

Result:

```text
All checks passed!
```

## API Smoke

Runtime setup:

```powershell
$env:PYTHONPATH="src"
$env:CATALYST_DATABASE_URL="sqlite:///C:/Users/fpan1/MarketRadar/.worktrees/phase-10-api-dashboard-expansion/tmp/phase10-smoke.db"
python -m catalyst_radar.cli init-db
python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8010
```

Browser checks:

- `http://127.0.0.1:8010/docs` rendered Swagger UI with the Catalyst Radar API title and decision-support description.
- `http://127.0.0.1:8010/api/health` returned `{"status":"ok","service":"catalyst-radar"}`.

Observed browser console note:

- Browser requested `/favicon.ico` and received 404. This is cosmetic and does not affect API behavior.

## Dashboard Smoke

Runtime setup:

```powershell
$env:PYTHONPATH="src"
$env:CATALYST_DATABASE_URL="sqlite:///C:/Users/fpan1/MarketRadar/.worktrees/phase-10-api-dashboard-expansion/tmp/phase10-smoke.db"
streamlit run apps/dashboard/Home.py --server.port 8509 --server.headless true
```

Browser checks:

- `http://127.0.0.1:8509/` rendered the Catalyst Radar home page with navigation for Home, Ticker Detail, Themes, Validation, Costs, and Ops.
- `Ticker_Detail`, `Themes`, `Validation`, `Costs`, and `Ops` pages rendered against the empty smoke database with no-data states instead of Python tracebacks.
- `http://127.0.0.1:8509/_stcore/health` returned `ok`.

Observed browser console note:

- Direct page URLs emitted Streamlit internal `_stcore/health` and `_stcore/host-config` 404 console entries under the page path. The pages still rendered, the root Streamlit health endpoint returned 200, and server logs did not show application tracebacks.

## Review Findings And Fixes

- Fixed the Ops page stale-data warning so it only appears when `stale_data.detected` is true. The first implementation treated any non-empty stale-data object as stale, including `{"detected": false}`.
- Fixed candidate queue semantics so Home and `/api/radar/candidates` show the latest candidate state per ticker instead of historical duplicates.
- Fixed ticker detail helpers so events, snippets, validation rows, paper trades, and history are bounded by a decision-time availability cutoff unless an explicit cutoff is provided.
- Fixed validation summary selection to use only successful validation runs with non-null `finished_at`, avoiding PostgreSQL NULL ordering surprises from unfinished runs.
- Fixed feedback persistence so labels can only be recorded against an existing artifact with a matching ticker.
- Fixed cost summary math so latest-run cost is scoped to useful labels relevant to that validation run instead of all-time labels.
- Confirmed feedback POST records a useful-alert label, uppercases ticker, rejects unknown labels and artifact types, and leaves candidate rows unchanged.
- Confirmed dashboard data helpers return ticker detail, theme rows, validation summary, cost summary, and ops health from persisted rows.

## Residual Risks

- The dashboard has usable Streamlit tables and metrics, but not production chart polish.
- Authentication, role-based access, scheduler controls, alert delivery, and broker/order integrations remain out of scope for this phase.
- Direct Streamlit page URLs show nonblocking internal 404 console entries for path-relative `_stcore` probes; this should be revisited during production hosting hardening if it persists outside local Streamlit smoke tests.
