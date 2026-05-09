# Catalyst Radar Full Product Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete Catalyst Radar product described by the v1.1.1 architecture and engineering specs: production-grade market data ingestion, deterministic full-universe scanning, event/text intelligence, validation, sparse LLM evidence review, Decision Cards, alerts, dashboards, and operational controls.

**Architecture:** Continue the deterministic-first spine proven in Phase 1. Add one independently testable subsystem at a time: provider-grade data, universe-scale scanning, event/text intelligence, validation, then sparse LLM synthesis. The system must preserve point-in-time correctness, fail-closed policy gates, source-linked evidence, budget controls, and the human approval boundary.

**Tech Stack:** Python 3.11, pandas initially with polars introduced for large scans, SQLAlchemy Core, Postgres/TimescaleDB, pgvector, FastAPI, Streamlit first-dashboard track, Redis Queue/Celery or Prefect for jobs, OpenAI API only behind a budgeted router, pytest, ruff, Docker Compose.

---

## Current Starting Point

Implementation branch:

```text
feature/phase-1-deterministic-mvp
```

Current verified capabilities:

- Local SQLite database initialization.
- Local CSV securities, daily bars, and holdings ingestion.
- Point-in-time daily-bar filtering through `available_at`.
- Deterministic market features.
- Deterministic scoring and policy gates.
- Liquidity, risk, stale-data, and portfolio-penalty hard blocks.
- Candidate state persistence.
- Minimal Streamlit dashboard.
- Point-in-time validation helpers.
- 46 passing tests.
- No runtime LLM integration.

Current explicit limits:

- Data comes from local CSV.
- No production market data provider.
- No SEC/news/earnings/text pipeline.
- No local embeddings or novelty scoring.
- No options aggregate features.
- No real backtesting engine or paper-trading ledger.
- No alerting.
- No LLM evidence packets or Decision Cards.
- Holdings are persisted but not yet applied to scanner scoring.
- No scheduled jobs, deployment, observability, or production ops.

## Product Completion Definition

The product is complete enough for a limited real-capital pilot only when all of these are true:

- Full liquid U.S. universe scans nightly without premium LLM calls.
- Every feature, event, snippet, state transition, evidence packet, and Decision Card stores `source_ts` and `available_at`.
- Backtests and shadow scans replay candidate states using availability timestamps.
- Portfolio exposure gates are enforced before `EligibleForManualBuyReview`.
- Warning-or-higher candidates have source-linked supporting and disconfirming evidence.
- Decision Cards include entry zone, invalidation, sizing, reward/risk, portfolio impact, evidence, conflicts, hard blocks, and next review time.
- Monthly AI spend can be capped and enforced automatically.
- Dashboard shows candidate state, state history, escalation reason, block reason, evidence links, validation outcomes, cost, and provider health.
- Alerts are deduped and record user feedback.
- Shadow mode and paper trading have run long enough to expose false positives, stale-data incidents, cost per useful alert, and missed opportunities.
- No automated trade placement exists.

## Execution Rule

Do not execute this master plan as one long coding batch. Execute it as a sequence of phase plans. Before each phase begins, create a phase-specific plan under:

```text
docs/superpowers/plans/YYYY-MM-DD-phase-N-<name>.md
```

Each phase plan must include exact file edits, tests, fixtures, commands, and review gates. The phase order below is intentional; do not start sparse LLM work before validation and event/text evidence packets exist.

## Phase 0: Integrate Phase 1 Baseline

**Objective:** Put the verified Phase 1 MVP on the primary branch so future phases build from one source of truth.

**Primary files and commands:**

- Branch/worktree: `feature/phase-1-deterministic-mvp`
- Base branch: `main`
- Verification:

```powershell
python -m pytest
python -m ruff check src tests apps
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
.\.venv\Scripts\catalyst-radar.exe init-db
.\.venv\Scripts\catalyst-radar.exe ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
.\.venv\Scripts\catalyst-radar.exe scan --as-of 2026-05-08
```

**Tasks:**

- [ ] Choose branch handling: local merge, PR, keep worktree, or discard.
- [ ] If merging locally, merge `feature/phase-1-deterministic-mvp` into `main`.
- [ ] Re-run tests and smoke flow on the merged result.
- [ ] Record the merged commit in `docs/phase-1-review.md`.

**Exit criteria:**

- Primary branch contains Phase 1.
- `python -m pytest` passes.
- `python -m ruff check src tests apps` passes.
- Sample smoke flow still reports `ingested securities=6 daily_bars=36 holdings=1` and `scanned candidates=3`.

## Phase 1: Production Data Foundation

**Objective:** Replace sample-only ingestion with provider-ready raw/normalized storage, connector contracts, provider health, job runs, and data quality incidents.

**Primary files:**

- Modify: `src/catalyst_radar/core/models.py`
- Modify: `src/catalyst_radar/storage/schema.py`
- Modify: `src/catalyst_radar/storage/repositories.py`
- Modify: `src/catalyst_radar/cli.py`
- Create: `src/catalyst_radar/connectors/base.py`
- Create: `src/catalyst_radar/connectors/market_data.py`
- Create: `src/catalyst_radar/connectors/provider_registry.py`
- Create: `src/catalyst_radar/storage/provider_repositories.py`
- Create: `sql/migrations/002_provider_foundation.sql`
- Create: `tests/unit/test_connector_contracts.py`
- Create: `tests/integration/test_provider_storage.py`

**Data model additions:**

- `raw_provider_records`
- `provider_health`
- `job_runs`
- `data_quality_incidents`
- `universe_snapshots`
- `universe_members`

**Implementation tasks:**

- [ ] Define `ConnectorRequest`, `RawRecord`, `NormalizedRecord`, `ConnectorHealth`, and `ProviderCostEstimate` dataclasses.
- [ ] Add connector protocol methods: `fetch`, `normalize`, `healthcheck`, `estimate_cost`.
- [ ] Add raw-provider persistence with provider, request hash, payload hash, source timestamp, fetch timestamp, availability timestamp, license tag, and retention policy.
- [ ] Add provider-health persistence with degraded/healthy states and reason strings.
- [ ] Add job-run persistence for ingest/scan/validation jobs.
- [ ] Add data-quality incident persistence with severity, affected tickers, and fail-closed action.
- [ ] Add a dry-run provider adapter that reads the current CSV fixtures through the new connector interface.
- [ ] Keep the current CSV connector working for local use.

**Tests:**

- [ ] Unit tests verify connector protocol dataclasses reject missing timestamps.
- [ ] Integration tests persist raw records and normalized records.
- [ ] Integration tests prove raw payload replay can rebuild normalized records.
- [ ] Integration tests prove provider outage writes `provider_health=degraded`.
- [ ] Regression tests prove missing `available_at` prevents action-state promotion.

**Exit criteria:**

- Local CSV path works through the new connector interface.
- Raw and normalized records are persisted separately.
- Provider health and job-run tables are populated during ingest.
- No scan can promote a candidate using records without availability timestamps.

## Phase 2: Full Universe and Real Market Data

**Objective:** Scan a real liquid U.S. universe nightly using production-grade securities and daily-bar data.

**Provider selection gate:**

Before implementation, choose the first real market data provider. The phase plan must compare provider coverage, cost, adjusted bars, corporate actions, rate limits, and license restrictions. Do not commit to paid provider integration without explicit approval.

**Primary files:**

- Modify: `src/catalyst_radar/connectors/market_data.py`
- Modify: `src/catalyst_radar/features/market.py`
- Modify: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Create: `src/catalyst_radar/universe/builder.py`
- Create: `src/catalyst_radar/universe/filters.py`
- Create: `src/catalyst_radar/features/sector.py`
- Create: `src/catalyst_radar/storage/universe_repositories.py`
- Create: `tests/integration/test_universe_builder.py`
- Create: `tests/integration/test_market_provider_scan.py`
- Create: `tests/golden/test_market_scan_golden.py`

**Implementation tasks:**

- [ ] Add universe filters for active common stocks and ADRs.
- [ ] Add configurable thresholds: price, market cap, average dollar volume, exchange, ADR inclusion, options requirement.
- [ ] Store point-in-time universe membership snapshots.
- [ ] Add corporate action checks before adjusted bars are accepted.
- [ ] Add sector ETF mapping and fallback behavior.
- [ ] Expand market features to include 20d/60d/120d relative strength, volatility compression, accumulation days, and stale-data flags.
- [ ] Add scan batching so 1,000-2,000 tickers complete under the configured SLA.
- [ ] Add golden fixtures for strong, weak, stale, corporate-action-mismatch, and illiquid tickers.

**Tests:**

- [ ] Universe builder excludes inactive, sub-threshold, stale, and low-liquidity tickers.
- [ ] Point-in-time universe snapshots replay correctly.
- [ ] Scanner completes fixture-scale batch deterministically.
- [ ] Corporate-action mismatch blocks scoring.
- [ ] Full scan requires zero LLM configuration.

**Exit criteria:**

- A full liquid universe scan runs without premium LLM calls.
- At least 90 percent of scanned tickers exit before event/text processing.
- Each candidate state can explain its source features and fail-closed blocks.

## Phase 3: Portfolio-Aware Policy and Setup Plugins

**Objective:** Enforce real portfolio exposure, sizing, and setup-specific entry/invalidation logic before buy-review eligibility.

**Primary files:**

- Modify: `src/catalyst_radar/portfolio/risk.py`
- Modify: `src/catalyst_radar/scoring/policy.py`
- Modify: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `src/catalyst_radar/portfolio/holdings.py`
- Create: `src/catalyst_radar/portfolio/correlation.py`
- Create: `src/catalyst_radar/scoring/setups.py`
- Create: `src/catalyst_radar/scoring/setup_policies.py`
- Create: `sql/migrations/003_portfolio_policy.sql`
- Create: `tests/unit/test_setup_policies.py`
- Create: `tests/integration/test_portfolio_policy_scan.py`

**Implementation tasks:**

- [ ] Add portfolio-value and cash fields to holdings snapshots.
- [ ] Compute single-name, sector, theme, and correlated-basket exposure before and after proposed position.
- [ ] Add `portfolio_impact` persistence.
- [ ] Wire holdings into scanner scoring and policy, not only ingestion.
- [ ] Add `BreakoutPolicy`, `PullbackPolicy`, `PostEarningsPolicy`, `SectorRotationPolicy`, and `FilingsCatalystPolicy`.
- [ ] Generate entry zone, invalidation, chase block, reward/risk, and setup type deterministically.
- [ ] Enforce missing trade plan as Warning, not buy-review.
- [ ] Add override/audit fields for any hard-block bypass.

**Tests:**

- [ ] Candidate with excessive single-name exposure is Blocked.
- [ ] Candidate with excessive sector/theme exposure is Blocked or Warning according to configured rule.
- [ ] Missing invalidation prevents `EligibleForManualBuyReview`.
- [ ] Reward/risk below 2.0 prevents buy-review eligibility.
- [ ] Setup plugins generate deterministic entry/invalidation for golden fixtures.

**Exit criteria:**

- `EligibleForManualBuyReview` requires a complete trade plan and portfolio impact.
- Dashboard can show block reasons and setup type for every candidate.
- Portfolio rules are config-driven and covered by tests.

## Phase 4: Event Connectors

**Objective:** Add SEC filings, earnings calendar, and news/event ingestion with source quality and availability timestamps.

**Primary files:**

- Modify: `src/catalyst_radar/storage/schema.py`
- Modify: `src/catalyst_radar/storage/repositories.py`
- Create: `src/catalyst_radar/connectors/sec.py`
- Create: `src/catalyst_radar/connectors/news.py`
- Create: `src/catalyst_radar/connectors/earnings.py`
- Create: `src/catalyst_radar/events/models.py`
- Create: `src/catalyst_radar/events/classifier.py`
- Create: `src/catalyst_radar/events/source_quality.py`
- Create: `src/catalyst_radar/events/dedupe.py`
- Create: `sql/migrations/004_events.sql`
- Create: `tests/unit/test_event_classifier.py`
- Create: `tests/integration/test_event_ingest.py`

**Implementation tasks:**

- [ ] Add canonical `events` table with event type, ticker, source, source URL, title, body hash, materiality, source quality, source timestamp, availability timestamp, and payload.
- [ ] Add SEC submissions and company-facts connector through official SEC endpoints.
- [ ] Add earnings calendar connector through selected provider.
- [ ] Add news connector through selected provider or RSS source.
- [ ] Add canonical URL/hash dedupe.
- [ ] Add source-quality scoring for primary sources, reputable news, transcripts, press releases, social/promotional sources.
- [ ] Add event taxonomy: earnings, guidance, 8-K, 10-Q/10-K, insider, analyst revision, sector read-through, product/customer announcement, legal/regulatory, financing, corporate action.
- [ ] Add event materiality rules.
- [ ] Store unresolved source conflicts and downgrade candidates when conflicts exist.

**Tests:**

- [ ] SEC fixture normalizes into event records with source and availability timestamps.
- [ ] Duplicate news URLs collapse into one canonical event.
- [ ] Promotional low-quality source cannot promote candidate above Research Only without confirmation.
- [ ] Future-available event cannot affect scan state.
- [ ] Provider outage creates degraded health state and does not crash deterministic scan.

**Exit criteria:**

- Event ingestion updates candidate state without LLM calls.
- Warning-or-higher candidates can show at least one event/evidence reason when event-driven.
- Unresolved conflicts downgrade action state.

## Phase 5: Local Text Intelligence

**Objective:** Add local text triage: snippets, ontology matching, sentiment/theme classification, embeddings, and novelty scoring.

**Primary files:**

- Modify: `pyproject.toml`
- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `config/themes.yaml`
- Create: `src/catalyst_radar/textint/snippets.py`
- Create: `src/catalyst_radar/textint/ontology.py`
- Create: `src/catalyst_radar/textint/sentiment.py`
- Create: `src/catalyst_radar/textint/embeddings.py`
- Create: `src/catalyst_radar/textint/novelty.py`
- Create: `src/catalyst_radar/textint/pipeline.py`
- Create: `sql/migrations/005_textint.sql`
- Create: `tests/unit/test_ontology.py`
- Create: `tests/unit/test_snippet_selection.py`
- Create: `tests/integration/test_text_pipeline.py`

**Implementation tasks:**

- [ ] Add `text_snippets` table with snippet text, section, source ID, hash, source quality, ontology hits, embedding vector ID, source timestamp, and availability timestamp.
- [ ] Add `text_features` table with local narrative, novelty, source quality, theme match, sentiment direction, and conflict flags.
- [ ] Add ontology YAML with initial themes from the spec: AI infrastructure storage and datacenter power.
- [ ] Implement deterministic snippet extraction by section and event type.
- [ ] Implement source-quality-aware snippet ranking.
- [ ] Add local embeddings using a configurable local model or provider-free embedding path.
- [ ] Add novelty against prior ticker/theme memory.
- [ ] Ensure snippets sent to any future LLM are capped and source-linked.

**Tests:**

- [ ] Ontology terms match expected themes.
- [ ] Duplicate snippets collapse by hash.
- [ ] Top snippets prefer primary/high-quality sources.
- [ ] Novelty decreases when text repeats prior known claims.
- [ ] Future-available snippet cannot affect scan state.

**Exit criteria:**

- Event/text MVP can improve candidate state using local methods only.
- Candidate packet can include selected snippets with source IDs.
- No premium LLM calls are required.

## Phase 6: Options, Sector, and Theme Features

**Objective:** Add non-LLM options aggregate features, sector rotation, theme velocity, and peer read-through scoring.

**Primary files:**

- Modify: `src/catalyst_radar/features/market.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Create: `src/catalyst_radar/features/options.py`
- Create: `src/catalyst_radar/features/theme.py`
- Create: `src/catalyst_radar/features/peers.py`
- Create: `src/catalyst_radar/connectors/options.py`
- Create: `sql/migrations/006_options_theme.sql`
- Create: `tests/unit/test_options_features.py`
- Create: `tests/unit/test_theme_features.py`
- Create: `tests/integration/test_options_ingest.py`

**Implementation tasks:**

- [ ] Add `option_features` table for aggregate call/put, OI, IV percentile, skew, and abnormal activity.
- [ ] Add options connector for aggregate chain data or provider-provided summary data.
- [ ] Add sector ETF trend and relative acceleration features.
- [ ] Add theme membership and theme velocity scores.
- [ ] Add peer read-through mapping and peer confirmation score.
- [ ] Update scoring to include options flow, sector rotation, local narrative, fundamental event, and novelty.

**Tests:**

- [ ] Options abnormality is finite-safe and point-in-time.
- [ ] Missing options data does not block non-options candidates by default.
- [ ] Sector rotation score is deterministic for fixture ETFs.
- [ ] Peer confirmation raises local narrative or sector score only when source events are available.

**Exit criteria:**

- Scoring covers all specified pillars.
- Missing optional options data degrades gracefully.
- Theme view has enough data for dashboard rendering.

## Phase 7: Candidate Packets and Unified Scoring

**Objective:** Build the complete candidate packet and scoring/policy flow across market, event, text, options, portfolio, and setup data.

**Primary files:**

- Modify: `src/catalyst_radar/core/models.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Modify: `src/catalyst_radar/scoring/policy.py`
- Modify: `src/catalyst_radar/pipeline/scan.py`
- Create: `src/catalyst_radar/pipeline/candidate_packet.py`
- Create: `src/catalyst_radar/pipeline/escalation.py`
- Create: `tests/golden/test_candidate_packets.py`
- Create: `tests/integration/test_full_deterministic_pipeline.py`

**Implementation tasks:**

- [ ] Define `CandidatePacket` containing features, events, snippets, risk blocks, portfolio impact, setup plan, source IDs, and state history.
- [ ] Add deterministic escalation reasons for local NLP, LLM review, and Decision Card eligibility.
- [ ] Persist candidate packet snapshots or reconstructable packet inputs.
- [ ] Add score delta over 5 days.
- [ ] Add `ThesisWeakening` and `ExitInvalidateReview` transitions for held/watch candidates.
- [ ] Ensure every transition stores reasons and source/computed feature IDs.

**Tests:**

- [ ] Candidate packet includes all required inputs for Warning and above.
- [ ] Escalation reasons match configured rules.
- [ ] Blocked candidates explain hard blocks.
- [ ] Score/state replay is deterministic from persisted point-in-time data.

**Exit criteria:**

- System can show exactly why a candidate was escalated or blocked.
- Every Warning candidate has supporting and disconfirming evidence placeholders ready for LLM/text evidence.
- Candidate packet is sufficient input for validation and Decision Cards.

## Phase 8: Backtesting, Shadow Mode, and Paper Trading

**Objective:** Prove the radar beats simple baselines and can measure usefulness before any real-capital workflow.

**Primary files:**

- Modify: `src/catalyst_radar/validation/backtest.py`
- Create: `src/catalyst_radar/validation/replay.py`
- Create: `src/catalyst_radar/validation/baselines.py`
- Create: `src/catalyst_radar/validation/paper.py`
- Create: `src/catalyst_radar/validation/outcomes.py`
- Create: `src/catalyst_radar/validation/reports.py`
- Create: `sql/migrations/007_validation.sql`
- Create: `tests/unit/test_backtest_replay.py`
- Create: `tests/integration/test_paper_trading.py`
- Create: `tests/golden/test_no_leakage_replay.py`

**Implementation tasks:**

- [ ] Add `paper_trades`, `validation_runs`, `validation_results`, and `useful_alert_labels` tables.
- [ ] Implement point-in-time replay over historical dates.
- [ ] Implement baselines: SPY momentum, sector momentum, event-only watchlist, random eligible universe, user watchlist if available.
- [ ] Compute labels: 10d/15, 20d/25, 60d/40, sector outperformance, max adverse excursion, max favorable excursion.
- [ ] Implement paper-trade workflow: approve/reject/defer, simulated entry, invalidation monitoring, outcome capture.
- [ ] Add validation reports for precision, false positives, cost per useful alert, and missed opportunities.

**Tests:**

- [ ] Future-available bars/events/snippets are excluded from replay.
- [ ] Baseline comparisons use same universe and availability rules.
- [ ] Paper trade exits when invalidation is triggered.
- [ ] Useful-alert labels are stored and aggregated.

**Exit criteria:**

- Backtests can replay candidate states using availability timestamps.
- Shadow mode can run live without real-capital actions.
- Paper trading can compute outcome and cost metrics.
- No real-capital pilot is permitted until this phase produces reviewed results.

## Phase 9: API and Dashboard Expansion

**Objective:** Provide usable review workflows: radar home, ticker detail, theme view, cost view, validation view, ops view, and feedback capture.

**Primary files:**

- Create: `apps/api/main.py`
- Create: `src/catalyst_radar/api/routes/radar.py`
- Create: `src/catalyst_radar/api/routes/ops.py`
- Create: `src/catalyst_radar/api/routes/costs.py`
- Create: `src/catalyst_radar/api/routes/feedback.py`
- Modify: `apps/dashboard/Home.py`
- Create: `apps/dashboard/pages/1_Ticker_Detail.py`
- Create: `apps/dashboard/pages/2_Themes.py`
- Create: `apps/dashboard/pages/3_Validation.py`
- Create: `apps/dashboard/pages/4_Costs.py`
- Create: `apps/dashboard/pages/5_Ops.py`
- Create: `tests/integration/test_api_routes.py`
- Create: `tests/integration/test_dashboard_data.py`

**Implementation tasks:**

- [ ] Add FastAPI app with read-only radar endpoints.
- [ ] Add `GET /api/radar/candidates`.
- [ ] Add `GET /api/radar/candidates/{ticker}`.
- [ ] Add `GET /api/radar/decision-cards/{id}` after Decision Cards exist.
- [ ] Add `POST /api/radar/decision-cards/{id}/feedback` after feedback table exists.
- [ ] Add ops health endpoint with provider/job status.
- [ ] Expand Streamlit dashboard pages.
- [ ] Add user feedback capture: useful, noisy, too late, too early, ignored, acted.

**Tests:**

- [ ] API route tests return expected schema.
- [ ] Dashboard data functions handle empty and populated states.
- [ ] Feedback is persisted and visible in validation summaries.

**Exit criteria:**

- User can review candidates, evidence, state history, blocks, validation, provider health, and costs from the UI.
- Feedback can be recorded for every alert and Decision Card.

## Phase 10: Alerts and Feedback Loop

**Objective:** Send deduped actionable notifications and measure whether they are useful.

**Primary files:**

- Create: `src/catalyst_radar/alerts/models.py`
- Create: `src/catalyst_radar/alerts/router.py`
- Create: `src/catalyst_radar/alerts/dedupe.py`
- Create: `src/catalyst_radar/alerts/channels/email.py`
- Create: `src/catalyst_radar/alerts/channels/webhook.py`
- Create: `src/catalyst_radar/alerts/digest.py`
- Create: `sql/migrations/008_alerts.sql`
- Create: `tests/unit/test_alert_dedupe.py`
- Create: `tests/integration/test_alert_routing.py`

**Implementation tasks:**

- [ ] Add `alerts`, `alert_suppressions`, and `user_feedback` tables.
- [ ] Route `EligibleForManualBuyReview` to immediate alert.
- [ ] Route high-delta Warning candidates to digest or optional push.
- [ ] Route ResearchOnly/AddToWatchlist to daily digest.
- [ ] Route ThesisWeakening/ExitInvalidateReview to position-watch alert.
- [ ] Suppress duplicate articles and unchanged states.
- [ ] Add feedback links or dashboard actions for each alert.

**Tests:**

- [ ] Alert dedupe suppresses repeated state/no-new-event alerts.
- [ ] New high-quality event reopens alert route.
- [ ] Invalidation state sends position-watch alert.
- [ ] Weekly summary includes suppressed count.

**Exit criteria:**

- Alerts are actionable, deduped, and measurable.
- Useful-alert rate is part of validation output.

## Phase 11: Budget Ledger and Sparse LLM Router

**Objective:** Add controlled, auditable LLM review without allowing LLMs into deterministic scanning or scoring.

**Primary files:**

- Modify: `pyproject.toml`
- Modify: `src/catalyst_radar/core/config.py`
- Create: `src/catalyst_radar/agents/models.py`
- Create: `src/catalyst_radar/agents/budget.py`
- Create: `src/catalyst_radar/agents/router.py`
- Create: `src/catalyst_radar/agents/tasks.py`
- Create: `src/catalyst_radar/agents/schemas.py`
- Create: `src/catalyst_radar/agents/prompts/evidence_review_v1.md`
- Create: `sql/migrations/009_budget_llm.sql`
- Create: `tests/unit/test_budget_controller.py`
- Create: `tests/unit/test_llm_router.py`

**Implementation tasks:**

- [ ] Add `budget_ledger` table.
- [ ] Add model-pricing config with input, cached input, and output token rates.
- [ ] Add `BudgetController` daily/monthly/task caps.
- [ ] Add `LLMRouter` that returns skip decisions when budget or state gates fail.
- [ ] Add task definitions: mini extraction, mid review, skeptic review, decision card, transcript deep dive.
- [ ] Add dry-run/fake LLM client for tests.
- [ ] Log estimated and actual cost, prompt version, schema version, model, token counts, ticker, candidate state, and outcome.
- [ ] Enforce default local/dev behavior: premium LLM disabled unless explicitly configured.

**Tests:**

- [ ] Full universe scan still runs with no LLM configuration.
- [ ] LLM route skips when budget exceeded.
- [ ] LLM route skips when candidate state is below configured threshold.
- [ ] Budget ledger records calls and skips.
- [ ] Pricing config missing prevents premium calls.

**Exit criteria:**

- LLM calls are sparse, gated, budgeted, and auditable.
- Monthly spend can be capped and enforced automatically.

## Phase 12: Evidence Packets, Skeptic Review, and Decision Cards

**Objective:** Produce source-linked evidence packets, human-readable bear cases, and complete Decision Cards for manual buy review.

**Primary files:**

- Modify: `src/catalyst_radar/agents/router.py`
- Create: `src/catalyst_radar/agents/evidence.py`
- Create: `src/catalyst_radar/agents/skeptic.py`
- Create: `src/catalyst_radar/decision_cards/models.py`
- Create: `src/catalyst_radar/decision_cards/builder.py`
- Create: `src/catalyst_radar/decision_cards/schemas.py`
- Create: `src/catalyst_radar/decision_cards/repository.py`
- Create: `src/catalyst_radar/agents/prompts/skeptic_v1.md`
- Create: `src/catalyst_radar/agents/prompts/decision_card_v1.md`
- Create: `sql/migrations/010_evidence_decision_cards.sql`
- Create: `tests/unit/test_evidence_packet_schema.py`
- Create: `tests/unit/test_decision_card_schema.py`
- Create: `tests/evals/test_llm_source_faithfulness.py`

**Implementation tasks:**

- [ ] Add `evidence_packets` table.
- [ ] Add `decision_cards` table.
- [ ] Build evidence packet from selected snippets, computed features, disconfirming evidence, conflicts, and policy context.
- [ ] Validate every claim has `source_id` or `computed_feature_id`.
- [ ] Add Skeptic Agent for Warning and buy-review candidates only.
- [ ] Add Decision Card builder for candidates passing all deterministic gates.
- [ ] Downgrade if JSON schema validation fails twice.
- [ ] Reject unsupported claims.
- [ ] Ensure Decision Card never says the system is making a buy decision.

**Tests and evals:**

- [ ] Evidence packet schema rejects claims without source IDs.
- [ ] Decision Card schema rejects missing entry, invalidation, sizing, reward/risk, portfolio impact, or next review time.
- [ ] Fake LLM unsupported claim is rejected.
- [ ] Conflicting evidence causes downgrade recommendation.
- [ ] GPT-5.5 route only occurs after configured gates pass.

**Exit criteria:**

- Every buy-review candidate has a complete Decision Card.
- Every Warning-or-higher candidate has supporting and disconfirming evidence.
- LLM outputs are schema-validated and source-linked.

## Phase 13: Operations, Scheduling, and Observability

**Objective:** Make the system run reliably as a daily research assistant.

**Primary files:**

- Create: `apps/worker/main.py`
- Create: `src/catalyst_radar/jobs/scheduler.py`
- Create: `src/catalyst_radar/jobs/tasks.py`
- Create: `src/catalyst_radar/ops/health.py`
- Create: `src/catalyst_radar/ops/metrics.py`
- Create: `src/catalyst_radar/ops/runbooks.py`
- Modify: `docker-compose.yml`
- Create: `infra/docker/Dockerfile`
- Create: `infra/docker/docker-compose.prod.yml`
- Create: `docs/runbooks/provider-failure.md`
- Create: `docs/runbooks/llm-failure.md`
- Create: `docs/runbooks/score-drift.md`
- Create: `tests/integration/test_jobs.py`
- Create: `tests/integration/test_ops_health.py`

**Implementation tasks:**

- [ ] Add scheduled jobs: daily bar ingest, feature scan, event ingest, text triage, scoring policy, LLM review, digest, validation update.
- [ ] Add job locks to avoid overlapping runs.
- [ ] Add provider-health banners to dashboard data.
- [ ] Add degraded mode: disable states above AddToWatchlist when core data is stale.
- [ ] Add score-distribution drift detection.
- [ ] Add metrics for stage counts, cost, useful alerts, stale incidents, unsupported-claim rate, and false-positive rate.
- [ ] Add local Docker Compose for Postgres, worker, API, dashboard, and Redis if chosen.

**Tests:**

- [ ] Job runner records success/failure and duration.
- [ ] Provider failure degrades health and disables action states above AddToWatchlist.
- [ ] LLM failure keeps deterministic scanner running.
- [ ] Score drift freezes new buy-review states.

**Exit criteria:**

- System can run scheduled locally or on a VM.
- Ops dashboard shows provider health, job status, stale data, and schema failures.
- Runbooks exist for major failure modes.

## Phase 14: Security, Secrets, and Compliance Controls

**Objective:** Protect credentials, account data, provider licenses, audit logs, and human approval boundaries.

**Primary files:**

- Modify: `src/catalyst_radar/core/config.py`
- Create: `src/catalyst_radar/security/secrets.py`
- Create: `src/catalyst_radar/security/redaction.py`
- Create: `src/catalyst_radar/security/audit.py`
- Create: `src/catalyst_radar/security/access.py`
- Create: `sql/migrations/011_security_audit.sql`
- Create: `docs/runbooks/secrets.md`
- Create: `tests/unit/test_redaction.py`
- Create: `tests/integration/test_audit_logs.py`

**Implementation tasks:**

- [ ] Load secrets from `.env.local` in dev and an encrypted/managed secret source in production.
- [ ] Redact API keys and account notes from logs and prompts.
- [ ] Add audit logs for user decisions, overrides, hard-block bypasses, and model calls.
- [ ] Add dashboard roles: admin, analyst, viewer.
- [ ] Enforce no broker order placement.
- [ ] Add provider license tags and retention policies.

**Tests:**

- [ ] Secrets are not logged.
- [ ] Prompt payload redaction removes configured sensitive fields.
- [ ] Hard-block override writes an audit record.
- [ ] Viewer role cannot perform feedback/override actions if roles are enabled.

**Exit criteria:**

- No secrets are committed or logged.
- User decisions and overrides are auditable.
- System language remains decision-support only.

## Phase 15: Shadow Mode, Paper Trading, and Pilot Readiness

**Objective:** Validate the complete system in live conditions before any real-capital use.

**Primary files:**

- Modify: `src/catalyst_radar/validation/reports.py`
- Modify: `apps/dashboard/pages/3_Validation.py`
- Create: `docs/release-gates/pilot-readiness.md`
- Create: `docs/release-gates/monthly-review-template.md`
- Create: `tests/integration/test_release_gates.py`

**Implementation tasks:**

- [ ] Run shadow production live for at least one earnings/event cycle.
- [ ] Record all alerts, paper decisions, invalidations, missed opportunities, false positives, and useful-alert labels.
- [ ] Compare results against baselines.
- [ ] Review cost per useful alert and cost per buy-review candidate.
- [ ] Review LLM unsupported-claim rate and schema failure rate.
- [ ] Produce pilot-readiness report.

**Tests and checks:**

- [ ] Release gate fails if no shadow-mode run exists.
- [ ] Release gate fails if point-in-time leakage test fails.
- [ ] Release gate fails if Decision Card schema failures exceed threshold.
- [ ] Release gate fails if cost budget is exceeded.
- [ ] Release gate fails if no user feedback labels exist.

**Exit criteria:**

- System has completed shadow mode.
- Paper-trading outcomes are visible.
- Pilot report documents performance, failures, costs, and known limits.
- User explicitly chooses whether to use outputs for real-capital manual review.

## Cross-Phase Non-Negotiables

- Never add autonomous order execution in this product version.
- Never let an LLM compute or override market math, risk limits, or portfolio exposure.
- Never promote a candidate using data without `available_at`.
- Never allow unsupported LLM claims into evidence or Decision Cards.
- Never continue premium LLM calls after budget cap is exceeded.
- Never hide hard-block reasons from the user.
- Every new connector must have replay fixtures and provider-health behavior.
- Every new score component must be finite-safe and point-in-time testable.
- Every new user-facing recommendation surface must use decision-support language.

## Master Verification Suite

Run this suite at the end of every phase:

```powershell
python -m pytest
python -m ruff check src tests apps
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
.\.venv\Scripts\catalyst-radar.exe init-db
.\.venv\Scripts\catalyst-radar.exe ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
.\.venv\Scripts\catalyst-radar.exe scan --as-of 2026-05-08
```

As phases add jobs, API, dashboard pages, and validation, extend this suite with:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/golden tests/evals
.\.venv\Scripts\uvicorn.exe apps.api.main:app --host 127.0.0.1 --port 8010
.\.venv\Scripts\streamlit.exe run apps/dashboard/Home.py --server.port 8509
```

Use browser verification for dashboard and API docs after visible UI changes.

## Recommended Next Phase

Build Phase 2 next only after Phase 1 is integrated. The next coding plan should be:

```text
docs/superpowers/plans/2026-05-09-phase-2-production-data-foundation.md
```

That phase should implement provider abstraction, raw/normalized provider storage, provider health, job runs, and data-quality incidents before choosing or paying for a real data provider.

