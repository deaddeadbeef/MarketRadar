# Phase 10 API And Dashboard Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add review-oriented API and Streamlit dashboard workflows so candidates, ticker detail, themes, validation, costs, ops health, and feedback can be reviewed from product surfaces.

**Architecture:** Build a thin FastAPI layer on top of read-only dashboard/query helpers, with one write endpoint for explicit feedback labels. Keep database access in `src/catalyst_radar/dashboard/data.py` and small API route modules so Streamlit and API share the same contracts. Dashboard pages should remain decision-support only and never present trade instructions.

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy Core, SQLite/PostgreSQL-compatible queries, Streamlit, pytest, ruff.

---

## Current Baseline

Build from:

```text
main @ f5f3f7c
```

Verified baseline:

```text
python -m pytest
308 passed in 76.04s

python -m ruff check src tests apps
All checks passed!
```

Important current limits:

- There is no `apps/api` FastAPI app.
- `fastapi` is not currently installed in the local environment or declared in `pyproject.toml`.
- Dashboard is a single Streamlit home page.
- Dashboard data exposes candidate rows, but not ticker detail, theme aggregation, validation summaries, cost rows, ops health, or feedback helpers.
- Feedback currently exists as CLI useful-alert labels only.

## Scope

In this phase, implement:

- FastAPI app factory and route modules.
- Read-only radar candidate endpoints.
- Ticker detail endpoint and shared data helper.
- Ops and costs read endpoints.
- Feedback endpoint that records explicit useful-alert labels for decision cards, packets, paper trades, or alerts.
- Streamlit pages for ticker detail, themes, validation, costs, and ops.
- Integration tests for API routes and dashboard data.

Out of scope:

- Authentication and roles.
- Alert delivery.
- Scheduler/job execution.
- Broker integration or order execution.
- Rich chart polish beyond usable Streamlit tables/metrics.

## File Structure

Create:

- `apps/api/__init__.py`
- `apps/api/main.py`
- `src/catalyst_radar/api/__init__.py`
- `src/catalyst_radar/api/routes/__init__.py`
- `src/catalyst_radar/api/routes/radar.py`
- `src/catalyst_radar/api/routes/ops.py`
- `src/catalyst_radar/api/routes/costs.py`
- `src/catalyst_radar/api/routes/feedback.py`
- `apps/dashboard/pages/1_Ticker_Detail.py`
- `apps/dashboard/pages/2_Themes.py`
- `apps/dashboard/pages/3_Validation.py`
- `apps/dashboard/pages/4_Costs.py`
- `apps/dashboard/pages/5_Ops.py`
- `tests/integration/test_api_routes.py`
- `tests/integration/test_dashboard_data.py`
- `docs/phase-10-review.md`

Modify:

- `pyproject.toml`
- `src/catalyst_radar/dashboard/data.py`
- `apps/dashboard/Home.py`

## Data Contracts

Candidate list row:

```text
ticker
as_of
state
final_score
hard_blocks
candidate_packet_id
decision_card_id
setup_type
entry_zone
invalidation_price
top_supporting_evidence
top_disconfirming_evidence
manual_review_disclaimer
```

Ticker detail payload:

```text
ticker
latest_candidate
state_history
features
events
snippets
candidate_packet
decision_card
setup_plan
portfolio_impact
validation_results
paper_trades
```

Theme summary row:

```text
theme
candidate_count
avg_score
top_tickers
states
latest_as_of
```

Validation summary:

```text
latest_run
report
paper_trades
useful_labels
```

Ops health:

```text
providers
jobs
database
stale_data
```

Feedback request:

```json
{
  "artifact_type": "decision_card",
  "artifact_id": "decision-card-v1:MSFT:...",
  "ticker": "MSFT",
  "label": "useful",
  "notes": "raised priority for review"
}
```

Allowed feedback labels:

```text
useful, noisy, too_late, too_early, ignored, acted
```

## Invariants

Decision-support invariant:

```text
API and UI copy must use review, candidate, evidence, setup, and simulated-paper wording. It must not say buy now, sell now, execute, place order, or automatic trade.
```

Point-in-time invariant:

```text
Data helpers that accept available_at must filter persisted rows by available_at <= cutoff.
```

Feedback invariant:

```text
Feedback is a user label and audit input only. It must not alter candidate states, scores, or policy outputs in this phase.
```

## Task 1: Dependencies And API Skeleton

**Files:**

- Modify: `pyproject.toml`
- Create: `apps/api/__init__.py`
- Create: `apps/api/main.py`
- Create: `src/catalyst_radar/api/__init__.py`
- Create: `src/catalyst_radar/api/routes/__init__.py`
- Test: `tests/integration/test_api_routes.py`

- [ ] **Step 1: Add FastAPI dependencies**

Add these dependencies to `pyproject.toml`:

```toml
dependencies = [
  "fastapi>=0.115",
  "numpy>=1.26",
  "pandas>=2.2",
  "psycopg[binary]>=3.2",
  "python-dotenv>=1.0",
  "sqlalchemy>=2.0",
  "streamlit>=1.35",
  "uvicorn>=0.30",
]
```

Run:

```powershell
python -m pip install -e .
```

Expected:

```text
Successfully installed catalyst-radar-0.1.0
```

- [ ] **Step 2: Write API health test**

Create `tests/integration/test_api_routes.py` with:

```python
from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.main import create_app


def test_api_health() -> None:
    client = TestClient(create_app())

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "catalyst-radar"}
```

Run:

```powershell
python -m pytest tests/integration/test_api_routes.py::test_api_health
```

Expected before implementation:

```text
ModuleNotFoundError
```

- [ ] **Step 3: Implement API app skeleton**

Create `apps/api/main.py`:

```python
from __future__ import annotations

from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(
        title="Catalyst Radar API",
        version="0.1.0",
        description="Decision-support API for reviewing market radar candidates.",
    )

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "catalyst-radar"}

    return app


app = create_app()
```

Create empty package markers:

```python
# apps/api/__init__.py
```

```python
# src/catalyst_radar/api/__init__.py
```

```python
# src/catalyst_radar/api/routes/__init__.py
```

Run:

```powershell
python -m pytest tests/integration/test_api_routes.py::test_api_health
python -m ruff check apps src tests
```

Expected:

```text
1 passed
All checks passed!
```

## Task 2: Shared Dashboard Data Helpers

**Files:**

- Modify: `src/catalyst_radar/dashboard/data.py`
- Test: `tests/integration/test_dashboard_data.py`

- [ ] **Step 1: Write dashboard data tests**

Create `tests/integration/test_dashboard_data.py` with fixture data inserted into an isolated SQLite DB. Tests must cover:

```python
def test_load_ticker_detail_returns_candidate_packet_card_events_and_validation(tmp_path): ...
def test_load_theme_rows_groups_candidate_themes(tmp_path): ...
def test_load_validation_summary_returns_latest_run_report_and_paper_trades(tmp_path): ...
def test_load_ops_health_reports_provider_status_and_database(tmp_path): ...
```

Use existing schema tables directly:

- `candidate_states`
- `signal_features`
- `candidate_packets`
- `decision_cards`
- `events`
- `text_snippets`
- `validation_runs`
- `validation_results`
- `paper_trades`
- `provider_health`

- [ ] **Step 2: Implement detail helpers**

Add these functions to `src/catalyst_radar/dashboard/data.py`:

```python
def load_ticker_detail(engine: Engine, ticker: str) -> dict[str, object] | None: ...
def load_theme_rows(engine: Engine) -> list[dict[str, object]]: ...
def load_validation_summary(engine: Engine) -> dict[str, object]: ...
def load_cost_summary(engine: Engine) -> dict[str, object]: ...
def load_ops_health(engine: Engine) -> dict[str, object]: ...
```

Implementation rules:

- Return JSON-safe dictionaries and lists.
- Preserve timezone-aware datetimes as `datetime` objects in Python helpers; API routes can serialize them.
- Use latest candidate/card/packet by availability and `created_at`.
- Include `manual_review_only: True` in detail payloads when a Decision Card exists.
- Use `ValidationRepository` for validation rows and useful labels where practical.

Run:

```powershell
python -m pytest tests/integration/test_dashboard_data.py
python -m ruff check src tests
```

Expected:

```text
4 passed
All checks passed!
```

## Task 3: Radar API Routes

**Files:**

- Create: `src/catalyst_radar/api/routes/radar.py`
- Modify: `apps/api/main.py`
- Test: `tests/integration/test_api_routes.py`

- [ ] **Step 1: Extend API tests for radar endpoints**

Append tests:

```python
def test_get_candidates_returns_rows(tmp_path, monkeypatch): ...
def test_get_candidate_detail_returns_404_for_missing_ticker(tmp_path, monkeypatch): ...
def test_get_candidate_detail_returns_payload(tmp_path, monkeypatch): ...
```

Use `monkeypatch.setenv("CATALYST_DATABASE_URL", f"sqlite:///{db}")`, insert one candidate, and call:

```python
client.get("/api/radar/candidates")
client.get("/api/radar/candidates/MSFT")
```

- [ ] **Step 2: Implement radar router**

Create `src/catalyst_radar/api/routes/radar.py`:

```python
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import load_candidate_rows, load_ticker_detail
from catalyst_radar.storage.db import engine_from_url

router = APIRouter(prefix="/api/radar", tags=["radar"])


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


@router.get("/candidates")
def candidates() -> dict[str, object]:
    return {"items": load_candidate_rows(_engine())}


@router.get("/candidates/{ticker}")
def candidate_detail(ticker: str) -> dict[str, object]:
    detail = load_ticker_detail(_engine(), ticker)
    if detail is None:
        raise HTTPException(status_code=404, detail="candidate not found")
    return detail
```

Modify `apps/api/main.py` to include:

```python
from catalyst_radar.api.routes.radar import router as radar_router

app.include_router(radar_router)
```

Run:

```powershell
python -m pytest tests/integration/test_api_routes.py
python -m ruff check apps src tests
```

Expected:

```text
all API route tests pass
All checks passed!
```

## Task 4: Ops, Costs, And Feedback API Routes

**Files:**

- Create: `src/catalyst_radar/api/routes/ops.py`
- Create: `src/catalyst_radar/api/routes/costs.py`
- Create: `src/catalyst_radar/api/routes/feedback.py`
- Modify: `apps/api/main.py`
- Test: `tests/integration/test_api_routes.py`

- [ ] **Step 1: Add route tests**

Append tests:

```python
def test_get_ops_health(tmp_path, monkeypatch): ...
def test_get_cost_summary(tmp_path, monkeypatch): ...
def test_post_feedback_records_useful_alert_label(tmp_path, monkeypatch): ...
def test_post_feedback_rejects_unknown_label(tmp_path, monkeypatch): ...
```

Feedback POST body:

```python
{
    "artifact_type": "decision_card",
    "artifact_id": "card-MSFT",
    "ticker": "MSFT",
    "label": "useful",
    "notes": "worth review",
}
```

- [ ] **Step 2: Implement ops and costs routers**

`ops.py`:

```python
from catalyst_radar.dashboard.data import load_ops_health
```

Route:

```text
GET /api/ops/health
```

`costs.py`:

```python
from catalyst_radar.dashboard.data import load_cost_summary
```

Route:

```text
GET /api/costs/summary
```

- [ ] **Step 3: Implement feedback router**

Use `pydantic.BaseModel`, `ValidationRepository`, `UsefulAlertLabel`, and `useful_alert_label_id`.

Rules:

- Allowed artifact types: `candidate_packet`, `decision_card`, `paper_trade`, `alert`.
- Allowed labels: `useful`, `noisy`, `too_late`, `too_early`, `ignored`, `acted`.
- Uppercase ticker.
- Return persisted id and label.
- Do not update candidate state, score, or paper trade.

Run:

```powershell
python -m pytest tests/integration/test_api_routes.py
python -m ruff check apps src tests
```

Expected:

```text
all API route tests pass
All checks passed!
```

## Task 5: Streamlit Dashboard Pages

**Files:**

- Modify: `apps/dashboard/Home.py`
- Create: `apps/dashboard/pages/1_Ticker_Detail.py`
- Create: `apps/dashboard/pages/2_Themes.py`
- Create: `apps/dashboard/pages/3_Validation.py`
- Create: `apps/dashboard/pages/4_Costs.py`
- Create: `apps/dashboard/pages/5_Ops.py`

- [ ] **Step 1: Update Home page copy and columns**

Requirements:

- Caption must say deterministic decision-support review.
- Candidate table must include state, score, evidence, packet/card IDs, setup type, and next review.
- No text may say buy now, sell now, execute, place order, or automatic trade.

- [ ] **Step 2: Add Ticker Detail page**

Page behavior:

- Sidebar text input for ticker.
- Show latest candidate metrics.
- Show setup plan, hard blocks, top evidence, event/snippet sections, portfolio impact, validation rows, paper trades.
- If missing, show `Ticker not found in current radar data.`

- [ ] **Step 3: Add Themes page**

Page behavior:

- Show theme summary rows from `load_theme_rows`.
- Include candidate count, average score, top tickers, state mix, latest as-of.

- [ ] **Step 4: Add Validation page**

Page behavior:

- Show latest validation run, precision, useful-alert rate, false positives, missed opportunities, leakage failures.
- Show paper trades table and useful label table if available.

- [ ] **Step 5: Add Costs page**

Page behavior:

- Show placeholder-safe deterministic cost summary: total cost defaults to 0, useful alerts count, cost per useful alert.
- Do not invent paid model spend.

- [ ] **Step 6: Add Ops page**

Page behavior:

- Show database status, provider health rows, stale-data banner when detected.
- No scheduler/job claims unless rows exist.

Run:

```powershell
python -m ruff check apps src tests
```

Expected:

```text
All checks passed!
```

## Task 6: Browser Smoke And Phase Review

**Files:**

- Create: `docs/phase-10-review.md`

- [ ] **Step 1: Run focused tests**

```powershell
python -m pytest tests/integration/test_api_routes.py tests/integration/test_dashboard_data.py
python -m ruff check apps src tests
```

- [ ] **Step 2: Run full verification**

```powershell
python -m pytest
python -m ruff check src tests apps
```

- [ ] **Step 3: Run API smoke**

```powershell
$env:PYTHONPATH="src"
$env:CATALYST_DATABASE_URL="sqlite:///tmp/phase10-smoke.db"
python -m catalyst_radar.cli init-db
python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8010
```

Open:

```text
http://127.0.0.1:8010/docs
http://127.0.0.1:8010/api/health
```

- [ ] **Step 4: Run dashboard smoke**

```powershell
$env:PYTHONPATH="src"
$env:CATALYST_DATABASE_URL="sqlite:///tmp/phase10-smoke.db"
streamlit run apps/dashboard/Home.py --server.port 8509
```

Open:

```text
http://localhost:8509
```

- [ ] **Step 5: Document phase outcome**

`docs/phase-10-review.md` must include:

- Outcome.
- Verification outputs.
- API smoke result.
- Dashboard smoke result.
- Review findings and fixes.
- Residual risks.

## Exit Criteria

- API health, candidate list, candidate detail, ops, costs, and feedback endpoints are implemented and tested.
- Shared dashboard data can return candidate detail, theme rows, validation summary, costs, and ops health.
- Streamlit has usable pages for home, ticker detail, themes, validation, costs, and ops.
- Feedback can be recorded as useful-alert labels.
- UI/API language remains decision-support only.
- Full pytest and ruff pass.
- Phase review document exists with smoke evidence.
