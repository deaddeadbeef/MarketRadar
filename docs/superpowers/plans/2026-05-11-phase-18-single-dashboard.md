# Phase 18: Single Multi-Layer Dashboard

## Objective

Integrate the current Market Radar surfaces into one Streamlit dashboard entry point so candidates, ticker detail, IPO S-1 analysis, alerts, themes, validation, cost, and ops health can be reviewed without switching pages.

This phase builds on Phase 17 in `feature/phase-17-ipo-s1-analysis`.

## Success Criteria

- `apps/dashboard/Home.py` becomes the primary command-center UI.
- The UI has layered review sections for:
  - overview and candidate queue
  - ticker workbench
  - IPO/S-1 filings and offering analysis
  - alerts and alert detail
  - themes
  - validation and paper outcomes
  - costs
  - ops health
- The dashboard uses the existing Streamlit stack and existing data helpers where possible.
- IPO/S-1 analysis is loaded from persisted canonical events with `payload.ipo_analysis`.
- Existing page modules can remain as direct routes, but the Home dashboard must no longer require page switching for normal review.
- Add dashboard data tests for IPO/S-1 rows and point-in-time filtering.
- Run focused dashboard tests, full pytest, ruff, and whitespace checks.
- Launch the dashboard locally and inspect the integrated UI in the browser.

## Implementation Plan

### Task 1: Add IPO/S-1 Dashboard Data Loader

Edit:

- `src/catalyst_radar/dashboard/data.py`
- `tests/integration/test_dashboard_data.py`

Add `load_ipo_s1_rows(engine, ticker=None, available_at=None, limit=50)`.

Requirements:

- Read persisted `events` rows.
- Keep only events whose payload contains `ipo_analysis`.
- Filter by ticker when provided.
- Respect `available_at` cutoff, defaulting to `datetime.now(UTC)`.
- Return compact rows with event identity, filing metadata, document URL/hash, summary, extracted terms, underwriters, risk flags, and sections found.
- Add a test that proves visible IPO rows are returned and future IPO rows are hidden.

### Task 2: Rebuild Home Dashboard As Single Layered UI

Edit:

- `apps/dashboard/Home.py`

Requirements:

- Keep `require_viewer()`, dotenv loading, and configured database engine.
- Use one global sidebar for ticker, alert filters, and cutoff.
- Use tabs for dashboard layers:
  - `Overview`
  - `Ticker`
  - `IPO/S-1`
  - `Alerts`
  - `Themes`
  - `Validation`
  - `Costs`
  - `Ops`
- Reuse current display behavior from existing pages, but keep it denser and organized for repeated review.
- Add durable JSON/date formatting helpers inside the page module.
- The IPO/S-1 layer must display the latest offering-analysis fields, risk flags, and source URL.

### Task 3: Verification

Run:

```powershell
python -m pytest tests\integration\test_dashboard_data.py::test_load_ipo_s1_rows_returns_visible_analysis_and_filters_future_rows -q
python -m pytest tests\integration\test_dashboard_data.py tests\integration\test_sec_ipo_cli.py -q
python -m ruff check src tests apps
git diff --check
python -m pytest
```

Then launch:

```powershell
python -m streamlit run apps\dashboard\Home.py --server.headless true --server.port 8501
```

Inspect with the browser plugin at `http://localhost:8501`.

