# Phase 4 Portfolio-Aware Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make scan results portfolio-aware by enforcing position sizing, exposure gates, setup-specific trade plans, and persistent portfolio-impact evidence before any candidate can reach `EligibleForManualBuyReview`.

**Architecture:** Preserve the deterministic scanner and Phase 3 provider/universe spine. Add portfolio state normalization, setup-policy modules, portfolio impact computation, portfolio-impact persistence, and policy gates that use explicit exposure details rather than a single generic penalty. Keep all rules config-driven and fixture-testable; no broker, event, text, LLM, or trade execution behavior belongs in this phase.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite-compatible local schema, PostgreSQL-compatible migration SQL, pandas for fixture CSV handling, pytest, ruff.

---

## Starting Point

Current `main` baseline:

```text
3a3ed8e merge: integrate phase 3 real market data
```

Verified Phase 3 evidence on merged `main`:

```text
python -m pytest
124 passed

python -m ruff check src tests apps
All checks passed!
```

Smoke flows:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
provider=csv status=healthy
scanned candidates=3
```

```text
initialized database
ingested provider=polygon raw=4 normalized=4 securities=4 daily_bars=0 rejected=0
ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 rejected=0
ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 rejected=1
provider=polygon status=degraded
built universe=liquid-us members=2 excluded=1
scanned candidates=2
```

## Non-Goals

- Do not add broker connectivity or order placement.
- Do not add SEC/news/event ingestion.
- Do not add options connectors.
- Do not add LLM calls, evidence packets, or Decision Cards.
- Do not replace the dashboard; only extend read payloads so dashboard rows can show setup type and portfolio blocks.
- Do not tune scoring weights from backtests in this phase.

## Phase Exit Criteria

- Holdings snapshots include portfolio value and cash, with backward-compatible CSV defaults.
- Scanner uses latest point-in-time holdings to compute proposed position size and portfolio impact.
- Candidate metadata includes setup type, entry zone, invalidation, reward/risk, position size, and portfolio impact.
- `portfolio_impacts` persistence records exposure before/after, sizing, hard blocks, source/availability timestamps, and setup type.
- Policy blocks excessive single-name, sector, and theme exposure with explicit hard-block names.
- `EligibleForManualBuyReview` requires a complete trade plan and portfolio impact.
- Setup policies deterministically produce entry/invalidation/reward-risk for breakout, pullback, post-earnings, sector-rotation, and filings-catalyst placeholders.
- CSV and Polygon fixture smokes still pass.

## File Structure

Files to create:

- `src/catalyst_radar/portfolio/holdings.py`: latest holdings snapshot extraction, portfolio value/cash resolution, current-position map.
- `src/catalyst_radar/portfolio/correlation.py`: deterministic correlated-basket placeholder using sector/theme clusters.
- `src/catalyst_radar/scoring/setups.py`: setup enum/dataclasses and setup metadata serialization.
- `src/catalyst_radar/scoring/setup_policies.py`: deterministic setup selectors and trade-plan generators.
- `sql/migrations/004_portfolio_policy.sql`: holdings metadata columns and `portfolio_impacts` table.
- `tests/unit/test_holdings_portfolio.py`: holdings defaults and latest snapshot selection.
- `tests/unit/test_setup_policies.py`: deterministic setup plans.
- `tests/integration/test_portfolio_policy_scan.py`: scanner/policy integration.

Files to modify:

- `.env.example`: add portfolio-value/cash and exposure policy examples.
- `src/catalyst_radar/core/config.py`: add `portfolio_value`, `portfolio_cash`, and optional setup policy thresholds.
- `src/catalyst_radar/core/models.py`: extend `HoldingSnapshot` and `PortfolioImpact`; add immutable metadata where needed.
- `src/catalyst_radar/connectors/csv_market.py`: load optional holdings `portfolio_value` and `cash`.
- `src/catalyst_radar/connectors/market_data.py`: normalize optional holdings `portfolio_value` and `cash`.
- `src/catalyst_radar/connectors/provider_ingest.py`: promote optional holdings portfolio fields.
- `src/catalyst_radar/portfolio/risk.py`: align defaults with `AppConfig`; add before/after exposure fields and exact hard-block names.
- `src/catalyst_radar/scoring/score.py`: accept extra candidate metadata.
- `src/catalyst_radar/scoring/policy.py`: enforce explicit portfolio impact and setup/trade-plan requirements.
- `src/catalyst_radar/pipeline/scan.py`: wire holdings, position sizing, setup policies, and portfolio impact into scan.
- `src/catalyst_radar/storage/schema.py`: add holdings columns and `portfolio_impacts` table.
- `src/catalyst_radar/storage/repositories.py`: read/write new holdings fields and persist portfolio impacts during `save_scan_result`.
- `src/catalyst_radar/dashboard/data.py`: expose setup type and portfolio hard blocks from stored candidate payloads.
- `tests/fixtures/holdings.csv` and `data/sample/holdings.csv`: add portfolio value/cash columns.

## Task 1: Portfolio Config and Holdings Snapshot Fields

**Objective:** Add portfolio value/cash fields and make current fixture holdings backward-compatible.

**Steps:**

- [ ] Add failing tests in `tests/unit/test_holdings_portfolio.py` proving a holdings CSV without `portfolio_value` and `cash` still loads, while a CSV with those columns preserves them.
- [ ] Extend `HoldingSnapshot` with `portfolio_value: float = 0.0` and `cash: float = 0.0`.
- [ ] Add `portfolio_value` and `portfolio_cash` to `AppConfig`, parsed from `CATALYST_PORTFOLIO_VALUE` and `CATALYST_PORTFOLIO_CASH`; defaults are `0.0`.
- [ ] Add `portfolio_value` and `cash` nullable/defaulted columns to `holdings_snapshots` in `schema.py` and `sql/migrations/004_portfolio_policy.sql`.
- [ ] Update CSV loaders and provider normalization/promotion to carry optional holdings fields.
- [ ] Update sample holdings fixtures to include `portfolio_value=100000` and `cash=25000` for deterministic tests.
- [ ] Verify:

```powershell
python -m pytest tests/unit/test_holdings_portfolio.py tests/integration/test_csv_ingest.py -q
python -m ruff check src tests
git add .env.example src tests data sql
git commit -m "feat: add portfolio snapshot fields"
```

## Task 2: Portfolio State and Exposure Computation

**Objective:** Convert holdings snapshots into current position maps and compute before/after exposure with exact block reasons.

**Steps:**

- [ ] Create `src/catalyst_radar/portfolio/holdings.py` with `PortfolioState`, `latest_portfolio_state(holdings, as_of, fallback_value, fallback_cash)`, and `positions_by_ticker(state)`.
- [ ] Update `PortfolioPolicy` defaults to match config/spec: `max_position_pct=0.08`, `risk_per_trade_pct=0.005`, `max_sector_pct=0.30`, `max_theme_pct=0.35`.
- [ ] Extend `PortfolioImpact` with before/after fields:
  - `single_name_before_pct`
  - `single_name_after_pct`
  - `sector_before_pct`
  - `sector_after_pct`
  - `theme_before_pct`
  - `theme_after_pct`
  - `correlated_before_pct`
  - `correlated_after_pct`
  - `proposed_notional`
  - `max_loss`
- [ ] Rename portfolio hard blocks to policy-facing names:
  - `single_name_exposure_hard_block`
  - `sector_exposure_hard_block`
  - `theme_exposure_hard_block`
  - `invalid_portfolio_input`
- [ ] Add unit tests for single-name, sector, theme, invalid account equity, and zero proposed-notional cases.
- [ ] Verify:

```powershell
python -m pytest tests/unit/test_holdings_portfolio.py tests/unit/test_portfolio.py -q
python -m ruff check src tests
git add src/catalyst_radar/portfolio src/catalyst_radar/core/models.py tests/unit/test_holdings_portfolio.py tests/unit/test_portfolio.py
git commit -m "feat: compute portfolio exposure state"
```

## Task 3: Setup Policies and Trade Plans

**Objective:** Add deterministic setup-type selection and trade-plan generation.

**Steps:**

- [ ] Create `src/catalyst_radar/scoring/setups.py` with:
  - `SetupType` values: `breakout`, `pullback`, `post_earnings`, `sector_rotation`, `filings_catalyst`, `market_momentum`.
  - `SetupPlan` fields: `setup_type`, `entry_zone`, `invalidation_price`, `target_price`, `reward_risk`, `chase_block`, `reasons`, `metadata`.
- [ ] Create `src/catalyst_radar/scoring/setup_policies.py` with deterministic functions:
  - `breakout_plan(bars, features)`
  - `pullback_plan(bars, features)`
  - `post_earnings_plan(bars, features)`
  - `sector_rotation_plan(bars, features)`
  - `filings_catalyst_plan(bars, features)`
  - `select_setup_plan(bars, features)`
- [ ] Use existing market features only; event-dependent policies must return placeholder reasons without event promotion until later phases.
- [ ] Replace `_basic_trade_plan()` in `scan.py` with `select_setup_plan()`.
- [ ] Add tests proving:
  - breakout near highs generates a breakout setup and invalidation below entry.
  - extended/chasing names get `chase_block=True`.
  - reward/risk below 2.0 remains possible but blocks buy-review through policy.
  - event-dependent setup placeholders do not promote without event inputs.
- [ ] Verify:

```powershell
python -m pytest tests/unit/test_setup_policies.py tests/integration/test_scan_pipeline.py -q
python -m ruff check src tests
git add src/catalyst_radar/scoring src/catalyst_radar/pipeline/scan.py tests/unit/test_setup_policies.py
git commit -m "feat: add deterministic setup policies"
```

## Task 4: Wire Portfolio Impact Into Scan and Policy

**Objective:** Use holdings and setup plans during scan so policy decisions reflect real portfolio constraints.

**Steps:**

- [ ] Modify `score.candidate_from_features()` to accept `metadata: Mapping[str, Any] | None` and merge it with score metadata.
- [ ] In `run_scan()`, read latest holdings through `MarketRepository.list_holdings()`, create `PortfolioState`, compute position size from setup entry/invalidation, and compute portfolio impact.
- [ ] Pass `portfolio_penalty=impact.portfolio_penalty` into scoring.
- [ ] Add candidate metadata:
  - `setup_type`
  - `setup_reasons`
  - `chase_block`
  - `position_size`
  - `portfolio_impact`
- [ ] Modify `evaluate_policy()`:
  - add exact portfolio hard blocks from candidate metadata.
  - add `portfolio_impact_missing` if missing and candidate would otherwise reach buy-review.
  - add `chase_block` as a Warning/buy-review blocker, not a hard block unless combined with overextension risk.
  - preserve existing stale/liquidity/risk behavior.
- [ ] Add integration tests:
  - excessive existing `AAA` holding blocks `AAA`.
  - excessive sector exposure blocks or warns according to configured hard rule.
  - missing invalidation/reward-risk below 2.0 prevents buy-review.
  - no holdings falls back to configured portfolio value and still records impact.
- [ ] Verify:

```powershell
python -m pytest tests/integration/test_portfolio_policy_scan.py tests/unit/test_policy.py tests/unit/test_score.py -q
python -m ruff check src tests
git add src/catalyst_radar/pipeline/scan.py src/catalyst_radar/scoring src/catalyst_radar/portfolio tests/integration/test_portfolio_policy_scan.py tests/unit/test_policy.py tests/unit/test_score.py
git commit -m "feat: enforce portfolio-aware scan policy"
```

## Task 5: Persist Portfolio Impact and Dashboard Read Fields

**Objective:** Store portfolio impact evidence and expose setup/block metadata for review.

**Steps:**

- [ ] Add `portfolio_impacts` table to `schema.py` and `sql/migrations/004_portfolio_policy.sql` with:
  - `id TEXT PRIMARY KEY`
  - `ticker TEXT NOT NULL`
  - `as_of TIMESTAMPTZ NOT NULL`
  - `setup_type TEXT NOT NULL`
  - `proposed_notional DOUBLE PRECISION NOT NULL`
  - `max_loss DOUBLE PRECISION NOT NULL`
  - before/after exposure columns for single-name, sector, theme, correlated basket
  - `portfolio_penalty DOUBLE PRECISION NOT NULL`
  - `hard_blocks JSONB NOT NULL`
  - `source_ts TIMESTAMPTZ NOT NULL`
  - `available_at TIMESTAMPTZ NOT NULL`
  - `payload JSONB NOT NULL`
  - `created_at TIMESTAMPTZ NOT NULL`
- [ ] In `MarketRepository.save_scan_result()`, persist a portfolio-impact row when candidate metadata contains `portfolio_impact`.
- [ ] Add indexes on `(ticker, as_of)` and `(setup_type, as_of)`.
- [ ] Update `dashboard/data.py` to include `setup_type`, `portfolio_hard_blocks`, `entry_zone`, and `invalidation_price` from `signal_features.payload`.
- [ ] Add integration tests proving impact persistence and dashboard row fields.
- [ ] Verify:

```powershell
python -m pytest tests/integration/test_portfolio_policy_scan.py tests/integration/test_scan_pipeline.py -q
python -m ruff check src tests apps
git add src/catalyst_radar/storage src/catalyst_radar/dashboard sql/migrations/004_portfolio_policy.sql tests/integration
git commit -m "feat: persist portfolio impact evidence"
```

## Task 6: Phase Verification and Review

**Objective:** Prove the phase is integrated and regression-safe before merge.

**Steps:**

- [ ] Run:

```powershell
python -m pytest
python -m ruff check src tests apps
```

- [ ] Run unchanged CSV smoke:

```powershell
Remove-Item data\local\catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL='sqlite:///data/local/catalyst_radar.db'
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
python -m catalyst_radar.cli provider-health --provider csv
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

- [ ] Run Polygon fixture smoke:

```powershell
Remove-Item data\local\catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL='sqlite:///data/local/catalyst_radar.db'
$env:CATALYST_POLYGON_API_KEY='fixture-key'
$env:CATALYST_MARKET_PROVIDER='polygon'
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-polygon tickers --date 2026-05-08 --fixture tests/fixtures/polygon/tickers_page_1.json
python -m catalyst_radar.cli ingest-polygon grouped-daily --date 2026-05-07 --fixture tests/fixtures/polygon/grouped_daily_2026-05-07.json
python -m catalyst_radar.cli ingest-polygon grouped-daily --date 2026-05-08 --fixture tests/fixtures/polygon/grouped_daily_2026-05-08.json
python -m catalyst_radar.cli provider-health --provider polygon
python -m catalyst_radar.cli build-universe --name liquid-us --provider polygon --as-of 2026-05-08
python -m catalyst_radar.cli scan --as-of 2026-05-08 --universe liquid-us
```

- [ ] Request two subagent reviews:
  - correctness review: schema, policy gates, scan behavior, persistence.
  - product review: exit criteria, real-world testing readiness, residual risk.
- [ ] Fix all high/medium findings.
- [ ] Create `docs/phase-4-review.md` with exact verification output and known residual risks.
- [ ] Commit final review notes:

```powershell
git add docs/phase-4-review.md
git commit -m "docs: record phase 4 verification"
```

## Implementation Risks

- Existing fixtures have a single holding row. Portfolio value/cash defaults must keep current CSV smoke output stable.
- Portfolio impact must not use future holdings. Use latest holding snapshot with `as_of <= scan timestamp`.
- A missing portfolio should not silently allow buy-review without evidence. Use config fallback and mark `portfolio_source=config_fallback`; if neither holdings nor config value exists, buy-review must be blocked.
- Setup policies are deterministic placeholders for event-dependent setups until event ingestion exists. They must label this in reasons rather than inventing event evidence.
- Avoid schema churn that breaks existing SQLite databases without migration SQL.
