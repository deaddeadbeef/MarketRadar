# Catalyst Radar Full Product Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement each phase task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete Catalyst Radar product described by the v1.1.1 architecture and engineering specs: production-grade market data ingestion, deterministic full-universe scanning, event/text intelligence, validation, sparse LLM evidence review, Decision Cards, alerts, dashboards, and operational controls.

**Architecture:** Continue the deterministic-first spine now merged on `main`. Add one independently testable subsystem at a time: local text intelligence, options/theme features, candidate packets, validation, user workflows, alerts, then sparse LLM synthesis. The system must preserve point-in-time correctness, fail-closed policy gates, source-linked evidence, budget controls, and the human approval boundary.

**Tech Stack:** Python 3.11, pandas/numpy, SQLAlchemy Core, SQLite-compatible local development, Postgres/TimescaleDB production target, pgvector later, FastAPI later, Streamlit dashboard, Redis Queue/Celery or equivalent scheduler later, OpenAI API only behind a budgeted router, pytest, ruff, Docker Compose.

---

## Current Baseline

Build from:

```text
main @ d8af7f6 docs: mark phase 11 complete
```

Verified capabilities now on `main`:

- Local SQLite database initialization.
- CSV market and holdings ingest through provider-ready ingest orchestration.
- Provider-neutral connector contracts.
- Raw and normalized provider storage.
- Provider health, job runs, data-quality incidents, universe snapshots, and universe members.
- Polygon fixture connector for ticker/reference and grouped daily bars.
- Point-in-time liquid-universe construction and named-universe scans.
- Deterministic market feature computation.
- Deterministic setup policies, entry zone, invalidation, reward/risk, and proposed sizing.
- Portfolio exposure, cash, and concentration hard gates before buy-review eligibility.
- Candidate state persistence and idempotent scan-result writes.
- Canonical event storage.
- SEC submissions, news fixture, and earnings fixture connectors.
- Deterministic event taxonomy, materiality, source quality, dedupe, and guidance conflict detection.
- Event-aware scan metadata, bounded event score support, event-driven setup selection, and dashboard event fields.
- Local text snippets, ontology/theme matching, sentiment, hashing-vector embeddings, novelty, and text-feature metadata.
- Options aggregate features, sector/theme/peer features, and optional-evidence-neutral scoring integration.
- Candidate packets and Decision Cards with supporting and disconfirming evidence.
- Point-in-time validation replay, baselines, useful-alert labels, shadow/paper trade workflow, and validation reports.
- FastAPI review API, expanded Streamlit dashboard pages, cost/ops views, and feedback capture.
- Shadow-mode alert artifacts, deterministic alert routing, dedupe/suppression ledger, dry-run delivery, alert feedback, alert API routes, alert CLI commands, and dashboard alert review page.
- Budget ledger, fail-closed LLM config, sparse router, fake-client review path, LLM budget CLI commands, and ledger-backed cost surfaces.
- No real LLM provider integration, no real external alert delivery, no scheduler/worker automation, and no broker/order execution.

Most recent verification:

```text
python -m pytest
415 passed in 241.16s (0:04:01)

python -m ruff check src tests apps
All checks passed!
```

Important current limits:

- Polygon live mode still needs a configured API key and live contract drift testing.
- SEC live mode is gated behind `CATALYST_SEC_ENABLE_LIVE=1` and a compliant `CATALYST_SEC_USER_AGENT`.
- News and earnings connectors are fixture/provider skeletons until a licensed provider is selected.
- Alerting is still shadow/dry-run only; real external delivery remains disabled.
- Sparse LLM infrastructure is fake-client only; real provider integration remains disabled.
- There is no scheduler, worker, production Docker deployment, auth/roles, observability runbooks, or pilot release gate.

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

Do not execute this master plan as one long coding batch. Execute it as a sequence of phase plans. Before each phase begins, create or update a phase-specific implementation plan under:

```text
docs/superpowers/plans/YYYY-MM-DD-phase-N-<name>.md
```

Each phase plan must include exact file edits, tests, fixtures, commands, and review gates. Each phase must finish with:

- `python -m pytest`
- `python -m ruff check src tests apps`
- fixture smoke flow for all affected CLI commands
- a phase review file under `docs/phase-N-review.md`
- a review pass focused on point-in-time correctness, fail-closed behavior, score bounds, and regressions

## Completed Phases

### Phase 1: Deterministic MVP

Status: complete and merged.

Review file:

```text
docs/phase-1-review.md
```

Delivered:

- CSV securities, daily bars, and holdings ingest.
- SQLite schema creation.
- Deterministic market features.
- Score and policy state assignment.
- Liquidity hard block.
- Minimal dashboard.
- Point-in-time validation helpers.

### Phase 2: Production Data Foundation

Status: complete and merged.

Review file:

```text
docs/phase-2-review.md
```

Delivered:

- Provider-neutral connector contracts.
- Raw provider records and normalized provider records.
- Provider health.
- Job runs.
- Data-quality incidents.
- Universe snapshots and members.
- CSV dry-run provider path.
- Fail-closed missing availability behavior.

### Phase 3: Full Universe and Real Market Data

Status: complete and merged.

Review file:

```text
docs/phase-3-review.md
```

Delivered:

- Provider comparison and Polygon first-adapter decision.
- Polygon ticker/reference and grouped-daily fixture connector.
- Provider ingest orchestration shared by CSV and Polygon.
- Universe filters and named universe construction.
- Provider-specific scan filtering.
- Golden no-network fixture scan.
- Provider health degradation on rejected records.

### Phase 4: Portfolio-Aware Policy and Setup Plugins

Status: complete and merged.

Review file:

```text
docs/phase-4-review.md
```

Delivered:

- Holdings normalization and latest-per-ticker resolution.
- Portfolio impact computation and persistence.
- Setup policies for breakout, pullback, sector rotation, post-earnings, and filings catalyst.
- Entry zone, invalidation, reward/risk, chase block, and proposed sizing.
- Cash, concentration, sector/theme, and portfolio hard gates.
- Idempotent scan result persistence.

### Phase 5: Event Connectors

Status: complete and merged.

Review file:

```text
docs/phase-5-review.md
```

Delivered:

- Canonical event models and `events` storage.
- SEC, news, and earnings fixture connectors.
- Provider promotion from normalized event records into canonical events.
- Event source quality, materiality, URL/body dedupe, and guidance conflict detection.
- Event CLI commands.
- Event-aware scan metadata.
- Bounded event score support.
- Event-driven setup selection.
- Conflict downgrade to `ResearchOnly`.

## Phase 6: Local Text Intelligence

Status: complete and merged.

Review file:

```text
docs/phase-6-review.md
```

Detailed executable plan:

```text
docs/superpowers/plans/2026-05-10-phase-6-local-text-intelligence.md
```

Objective:

- Add deterministic, provider-free local text intelligence on top of Phase 5 events: source-linked snippets, ontology/theme matching, sentiment direction, hashing-vector embeddings, novelty, and text-feature metadata.

Primary files:

- Create: `config/themes.yaml`
- Create: `src/catalyst_radar/textint/__init__.py`
- Create: `src/catalyst_radar/textint/models.py`
- Create: `src/catalyst_radar/textint/ontology.py`
- Create: `src/catalyst_radar/textint/snippets.py`
- Create: `src/catalyst_radar/textint/sentiment.py`
- Create: `src/catalyst_radar/textint/embeddings.py`
- Create: `src/catalyst_radar/textint/novelty.py`
- Create: `src/catalyst_radar/textint/pipeline.py`
- Create: `src/catalyst_radar/storage/text_repositories.py`
- Create: `sql/migrations/006_textint.sql`
- Modify: `src/catalyst_radar/storage/schema.py`
- Modify: `src/catalyst_radar/cli.py`
- Modify: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Modify: `src/catalyst_radar/dashboard/data.py`

Implementation tasks:

- [ ] Add `text_snippets` and `text_features` tables with `source_ts` and `available_at`.
- [ ] Add `TextRepository` with upsert, point-in-time snippet reads, and latest feature reads by ticker.
- [ ] Add initial ontology for `ai_infrastructure_storage` and `datacenter_power`.
- [ ] Extract snippets from event titles, bodies, summaries, and payload text.
- [ ] Rank snippets by source quality, materiality, ontology hits, and event type.
- [ ] Score deterministic sentiment with conservative finance phrase lists.
- [ ] Compute deterministic 64-dimension hashing-vector embeddings.
- [ ] Compute novelty against prior ticker/theme snippets only when prior snippets were available in time.
- [ ] Add `run-textint` and `text-features` CLI commands.
- [ ] Attach text feature metadata to scans.
- [ ] Add bounded local narrative score support that cannot bypass hard policy gates.
- [ ] Add dashboard fields for narrative, novelty, sentiment, theme hits, and selected snippets.

Exit criteria:

- Text snippets and features are persisted separately from events.
- Every snippet and text feature is point-in-time safe.
- Scan integration honors `available_at`.
- Local narrative support is bounded and cannot overpower stale-data, liquidity, risk, cash, portfolio, or unresolved-conflict gates.
- Existing event, CSV, and Polygon smokes still pass.

## Phase 7: Options, Sector, Theme, and Peer Features

Status: complete and merged.

Review file:

```text
docs/phase-7-review.md
```

Objective:

- Add non-LLM options aggregate features, sector rotation, theme velocity, and peer read-through scoring.

Primary files:

- Create: `src/catalyst_radar/features/options.py`
- Create: `src/catalyst_radar/features/theme.py`
- Create: `src/catalyst_radar/features/peers.py`
- Create: `src/catalyst_radar/connectors/options.py`
- Create: `src/catalyst_radar/storage/feature_repositories.py`
- Create: `sql/migrations/007_options_theme.sql`
- Modify: `src/catalyst_radar/features/market.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Modify: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/dashboard/data.py`
- Test: `tests/unit/test_options_features.py`
- Test: `tests/unit/test_theme_features.py`
- Test: `tests/unit/test_peer_readthrough.py`
- Test: `tests/integration/test_options_ingest.py`

Implementation tasks:

- [ ] Add `option_features` table for aggregate call/put volume, open interest, IV percentile, skew, abnormality, provider, `source_ts`, and `available_at`.
- [ ] Add fixture-first options connector using aggregate chain summary data, not per-contract trading logic.
- [ ] Add sector ETF trend and relative acceleration features.
- [ ] Add theme membership config and theme velocity score.
- [ ] Add peer read-through mapping from event/text themes to related tickers.
- [ ] Update scoring with finite-safe options, sector, theme, and peer support components.
- [ ] Keep missing optional options data neutral unless a setup explicitly requires options confirmation.

Exit criteria:

- Optional options data improves evidence but does not block ordinary equity candidates.
- Sector/theme/peer features are deterministic and point-in-time testable.
- Scoring covers the major spec pillars without LLM calls.

## Phase 8: Candidate Packets and Unified Scoring

Status: complete and merged.

Review file:

```text
docs/phase-8-review.md
```

Objective:

- Build the complete candidate packet and final deterministic scoring/policy flow across market, event, text, options, portfolio, and setup data.

Primary files:

- Create: `src/catalyst_radar/pipeline/candidate_packet.py`
- Create: `src/catalyst_radar/pipeline/escalation.py`
- Create: `src/catalyst_radar/storage/candidate_packet_repositories.py`
- Create: `sql/migrations/008_candidate_packets.sql`
- Modify: `src/catalyst_radar/core/models.py`
- Modify: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Modify: `src/catalyst_radar/scoring/policy.py`
- Test: `tests/golden/test_candidate_packets.py`
- Test: `tests/integration/test_full_deterministic_pipeline.py`

Implementation tasks:

- [ ] Define `CandidatePacket` containing features, events, snippets, risk blocks, setup plan, portfolio impact, source IDs, and state history.
- [ ] Persist packet snapshots or enough packet inputs to reconstruct each state.
- [ ] Add deterministic escalation reasons for local review, future LLM review, and Decision Card eligibility.
- [ ] Add score delta and state delta over configurable lookback windows.
- [ ] Add `ThesisWeakening` and `ExitInvalidateReview` transitions for held/watch candidates.
- [ ] Require supporting evidence and explicit disconfirming-evidence sections for Warning and above.
- [ ] Ensure every transition stores reasons and source/computed feature IDs.

Exit criteria:

- Every candidate state can explain why it exists.
- Every Warning-or-higher candidate has a candidate packet ready for validation and optional LLM review.
- Replay from persisted point-in-time inputs yields the same packet and state.

## Phase 9: Backtesting, Shadow Mode, and Paper Trading

Status: complete and merged.

Review file:

```text
docs/phase-9-review.md
```

Objective:

- Prove the radar beats simple baselines and can measure usefulness before any real-capital workflow.

Primary files:

- Create: `src/catalyst_radar/validation/replay.py`
- Create: `src/catalyst_radar/validation/baselines.py`
- Create: `src/catalyst_radar/validation/paper.py`
- Create: `src/catalyst_radar/validation/outcomes.py`
- Create: `src/catalyst_radar/validation/reports.py`
- Create: `sql/migrations/009_validation.sql`
- Modify: `src/catalyst_radar/validation/backtest.py`
- Test: `tests/unit/test_backtest_replay.py`
- Test: `tests/integration/test_paper_trading.py`
- Test: `tests/golden/test_no_leakage_replay.py`

Implementation tasks:

- [ ] Add `paper_trades`, `validation_runs`, `validation_results`, and `useful_alert_labels` tables.
- [ ] Implement point-in-time replay over historical dates.
- [ ] Implement baselines: SPY momentum, sector momentum, event-only watchlist, random eligible universe, and user watchlist when available.
- [ ] Compute outcome labels: 10d/15, 20d/25, 60d/40, sector outperformance, max adverse excursion, and max favorable excursion.
- [ ] Implement paper workflow: approve, reject, defer, simulated entry, invalidation monitoring, and outcome capture.
- [ ] Add validation reports for precision, false positives, cost per useful alert, missed opportunities, and no-leakage failures.

Exit criteria:

- Backtests replay candidate states using availability timestamps.
- Shadow mode can run live without real-capital actions.
- Paper trading computes outcome and cost metrics.
- No pilot can proceed until validation output is reviewed.

## Phase 10: API and Dashboard Expansion

Status: complete and merged.

Review file:

```text
docs/phase-10-review.md
```

Objective:

- Provide usable review workflows: radar home, ticker detail, theme view, validation view, cost view, ops view, and feedback capture.

Primary files:

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
- Test: `tests/integration/test_api_routes.py`
- Test: `tests/integration/test_dashboard_data.py`

Implementation tasks:

- [ ] Add FastAPI app with read-only radar endpoints.
- [ ] Add `GET /api/radar/candidates`.
- [ ] Add `GET /api/radar/candidates/{ticker}`.
- [ ] Add ticker detail data with features, events, snippets, candidate packet, state history, setup plan, and portfolio impact.
- [ ] Add ops health endpoint with provider and job status.
- [ ] Expand Streamlit dashboard pages.
- [ ] Add feedback capture: useful, noisy, too late, too early, ignored, acted.
- [ ] Keep all displayed recommendation language as decision support, not trade instruction.

Exit criteria:

- User can review candidates, evidence, state history, blocks, validation, provider health, and costs from the UI.
- Feedback can be recorded for every alert and future Decision Card.

## Phase 11: Alerts and Feedback Loop

Status: complete and merged.

Review file:

```text
docs/phase-11-review.md
```

Detailed executable plan:

```text
docs/superpowers/plans/2026-05-10-phase-11-alerts-feedback-loop.md
```

Objective:

- Send deduped review notifications and measure whether they are useful.

Primary files:

- Create: `src/catalyst_radar/alerts/models.py`
- Create: `src/catalyst_radar/alerts/routing.py`
- Create: `src/catalyst_radar/alerts/dedupe.py`
- Create: `src/catalyst_radar/alerts/planner.py`
- Create: `src/catalyst_radar/alerts/channels/email.py`
- Create: `src/catalyst_radar/alerts/channels/webhook.py`
- Create: `src/catalyst_radar/alerts/digest.py`
- Create: `src/catalyst_radar/storage/alert_repositories.py`
- Create: `src/catalyst_radar/feedback/service.py`
- Create: `src/catalyst_radar/api/routes/alerts.py`
- Create: `apps/dashboard/pages/6_Alerts.py`
- Create: `sql/migrations/010_alerts.sql`
- Test: `tests/unit/test_alert_dedupe.py`
- Test: `tests/unit/test_alert_routing.py`
- Test: `tests/integration/test_alert_repository.py`
- Test: `tests/integration/test_alerts_cli.py`
- Test: `tests/integration/test_alert_api_routes.py`

Delivered:

- `alerts`, `alert_suppressions`, and `user_feedback` tables plus PostgreSQL migration.
- Deterministic alert models, routes, trigger fingerprints, and URL-safe stable IDs.
- Point-in-time alert planner over latest visible candidate states, packets, and Decision Cards.
- Dedupe/suppression records for unchanged repeat triggers and non-alertable states.
- Digest grouping and dry-run channel adapters with no network I/O.
- CLI commands: `build-alerts`, `alerts-list`, `alert-digest`, and `send-alerts`.
- Shared feedback service that validates artifacts, enforces ticker match, writes `user_feedback`, and projects to `useful_alert_labels`.
- FastAPI alert list/detail/feedback routes.
- Streamlit Alerts page plus Home/Costs/Validation summary integration.
- Review fixes for future-alert leakage, feedback audit bypasses, payload serialization, URL-safe IDs, and default cutoff behavior.

Exit criteria:

- Alerts are actionable, deduped, and measurable.
- Useful-alert rate is part of validation output.

## Phase 12: Budget Ledger and Sparse LLM Router

Status: complete in `feature/phase-12-budget-ledger-sparse-llm-router`.

Review file:

```text
docs/phase-12-review.md
```

Detailed executable plan:

```text
docs/superpowers/plans/2026-05-10-phase-12-budget-ledger-sparse-llm-router.md
```

Objective:

- Add controlled, auditable LLM review without allowing LLMs into deterministic scanning or scoring.

Primary files:

- Create: `src/catalyst_radar/agents/models.py`
- Create: `src/catalyst_radar/agents/budget.py`
- Create: `src/catalyst_radar/agents/router.py`
- Create: `src/catalyst_radar/agents/tasks.py`
- Create: `src/catalyst_radar/agents/schemas.py`
- Create: `src/catalyst_radar/agents/prompts/evidence_review_v1.md`
- Create: `sql/migrations/011_budget_llm.sql`
- Modify: `src/catalyst_radar/core/config.py`
- Test: `tests/unit/test_budget_controller.py`
- Test: `tests/unit/test_llm_router.py`

Implementation tasks:

- [x] Add `budget_ledger` table.
- [x] Add model-pricing config with input, cached input, and output token rates.
- [x] Add `BudgetController` with daily, monthly, and per-task caps.
- [x] Add `LLMRouter` that returns skip decisions when budget, config, or state gates fail.
- [x] Add fake LLM client for deterministic tests.
- [x] Log estimated and actual cost, prompt version, schema version, model, token counts, ticker, candidate state, and outcome.
- [x] Enforce default local/dev behavior: premium LLM disabled unless explicitly configured.

Delivered:

- Budget ledger schema, migration, SQLAlchemy table, repository, and summaries.
- Strict LLM task/status/skip-reason models and append-only attempt ledger IDs.
- Config-driven pricing and caps with fail-closed validation.
- Budget controller gates for disabled premium, manual-only tasks, ineligible states, missing model/pricing, stale/future pricing, per-task caps, daily cap, monthly cap, and GPT-5.5 soft cap.
- Router/fake-client foundation with budget skips, dry runs, completed fake calls, schema rejection, and client-failure ledgering.
- Source-linked evidence review schema validation and versioned prompt.
- `llm-budget-status` and `run-llm-review` CLI commands.
- Ledger-backed `/api/costs/summary` and Streamlit Costs page.
- Review fixes for future ledger leakage, append-only repeated attempts, missing-packet audit rows, paid failed/rejected attempt caps, unsupported schema rejection, attempt-time budget windows, cost aggregate truncation, point-in-time validation cost leakage, cached-token math, non-fake provider exit codes, schema hardening, and test env isolation.

Exit criteria:

- LLM calls are sparse, gated, budgeted, and auditable.
- Full universe scan still runs with no LLM configuration.
- Monthly spend can be capped and enforced automatically.

## Phase 13: Evidence Packets, Skeptic Review, and Decision Cards

Status: complete in `feature/phase-13-evidence-skeptic-decision-cards`.

Review file:

```text
docs/phase-13-review.md
```

Detailed executable plan:

```text
docs/superpowers/plans/2026-05-10-phase-13-evidence-skeptic-decision-cards.md
```

Objective:

- Produce source-linked evidence packets, human-readable bear cases, and complete Decision Cards for manual buy review.

Primary files:

- Create: `src/catalyst_radar/agents/evidence.py`
- Create: `src/catalyst_radar/agents/skeptic.py`
- Create: `src/catalyst_radar/agents/openai_client.py`
- Create: `src/catalyst_radar/agents/prompts/skeptic_v1.md`
- Create: `src/catalyst_radar/agents/prompts/decision_card_v1.md`
- Modify: `src/catalyst_radar/agents/schemas.py`
- Modify: `src/catalyst_radar/agents/router.py`
- Modify: `src/catalyst_radar/agents/tasks.py`
- Modify: `src/catalyst_radar/cli.py`
- Modify: `src/catalyst_radar/decision_cards/builder.py`
- Test: `tests/unit/test_evidence_packet_schema.py`
- Test: `tests/unit/test_agent_schemas.py`
- Test: `tests/unit/test_decision_card_builder.py`
- Test: `tests/unit/test_llm_router.py`
- Test: `tests/unit/test_skeptic_review.py`
- Test: `tests/integration/test_llm_cli.py`
- Test: `tests/evals/test_llm_source_faithfulness.py`

Implementation tasks:

- [x] Reuse existing `candidate_packets` and `decision_cards` storage instead of adding duplicate evidence tables.
- [x] Build agent-facing evidence packet views from selected snippets, computed features, disconfirming evidence, conflicts, and policy context.
- [x] Validate every accepted LLM claim has a known `source_id`, `source_url`, or `computed_feature_id`.
- [x] Add Skeptic review for Warning, ThesisWeakening, and buy-review candidates only.
- [x] Add Decision Card draft review for deterministic `EligibleForManualBuyReview` candidates only.
- [x] Reject schema-invalid or unsupported LLM output and ledger it.
- [x] Reject unsupported claims.
- [x] Ensure Decision Cards never say the system is making a buy decision.
- [x] Add an optional OpenAI Responses API client with `store=False`, strict JSON schema output, and no-key fail-closed behavior.
- [x] Verify fake skeptic and Decision Card review paths; real OpenAI provider smoke remains pending until an API key is supplied.

Verification:

- `python -m pytest` -> `490 passed in 138.32s (0:02:18)`.
- `python -m ruff check src tests apps` -> `All checks passed!`.
- Fake LLM smoke completed `skeptic_review` and seeded eligible `gpt55_decision_card` paths.
- No-key OpenAI provider smoke failed closed and ledgered `failed/client_error`.

Exit criteria:

- Every buy-review candidate has a complete Decision Card.
- Every Warning-or-higher candidate has supporting and disconfirming evidence.
- LLM outputs are schema-validated and source-linked.

## Phase 14: Operations, Scheduling, and Observability

Objective:

- Make the system run reliably as a daily research assistant.

Primary files:

- Create: `apps/worker/main.py`
- Create: `src/catalyst_radar/jobs/scheduler.py`
- Create: `src/catalyst_radar/jobs/tasks.py`
- Create: `src/catalyst_radar/ops/health.py`
- Create: `src/catalyst_radar/ops/metrics.py`
- Create: `src/catalyst_radar/ops/runbooks.py`
- Create: `infra/docker/Dockerfile`
- Create: `infra/docker/docker-compose.prod.yml`
- Create: `docs/runbooks/provider-failure.md`
- Create: `docs/runbooks/llm-failure.md`
- Create: `docs/runbooks/score-drift.md`
- Modify: `docker-compose.yml`
- Test: `tests/integration/test_jobs.py`
- Test: `tests/integration/test_ops_health.py`

Implementation tasks:

- [x] Add scheduled jobs: daily bar ingest, feature scan, event ingest, text triage, scoring policy, LLM review, digest, and validation update.
- [x] Add job locks to avoid overlapping runs.
- [x] Add provider-health banners to dashboard data.
- [x] Add degraded mode that disables states above AddToWatchlist when core data is stale.
- [x] Add score-distribution drift detection.
- [x] Add metrics for stage counts, cost, useful alerts, stale incidents, unsupported-claim rate, and false-positive rate.
- [x] Add local Docker Compose for Postgres, worker, API, dashboard, and Redis if chosen.

Exit criteria:

- System can run scheduled locally or on a VM.
- Ops dashboard shows provider health, job status, stale data, and schema failures.
- Runbooks exist for major failure modes.

## Phase 15: Security, Secrets, and Compliance Controls

Objective:

- Protect credentials, account data, provider licenses, audit logs, and human approval boundaries.

Primary files:

- Create: `src/catalyst_radar/security/secrets.py`
- Create: `src/catalyst_radar/security/redaction.py`
- Create: `src/catalyst_radar/security/audit.py`
- Create: `src/catalyst_radar/security/access.py`
- Create: `sql/migrations/013_security_audit.sql`
- Create: `docs/runbooks/secrets.md`
- Modify: `src/catalyst_radar/core/config.py`
- Test: `tests/unit/test_redaction.py`
- Test: `tests/integration/test_audit_logs.py`

Implementation tasks:

- [x] Load secrets from `.env.local` in dev and a managed/encrypted secret source in production.
- [x] Redact API keys, account notes, and personal account data from logs and prompts.
- [x] Add audit logs for user decisions, overrides, hard-block bypasses, and model calls.
- [x] Add dashboard roles: admin, analyst, viewer.
- [x] Enforce no broker order placement.
- [x] Add provider license tags and retention policies.

Exit criteria:

- No secrets are committed or logged.
- User decisions and overrides are auditable.
- System language remains decision-support only.

## Phase 16: Shadow Mode, Paper Trading, and Pilot Readiness

Objective:

- Validate the complete system in live conditions before any real-capital use.

Primary files:

- Create: `docs/release-gates/pilot-readiness.md`
- Create: `docs/release-gates/monthly-review-template.md`
- Create: `tests/integration/test_release_gates.py`
- Modify: `src/catalyst_radar/validation/reports.py`
- Modify: `apps/dashboard/pages/3_Validation.py`

Implementation tasks:

- [ ] Run shadow production live for at least one earnings/event cycle.
- [ ] Record all alerts, paper decisions, invalidations, missed opportunities, false positives, and useful-alert labels.
- [ ] Compare results against baselines.
- [ ] Review cost per useful alert and cost per buy-review candidate.
- [ ] Review LLM unsupported-claim rate and schema failure rate.
- [ ] Produce pilot-readiness report.

Exit criteria:

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
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities tests/fixtures/securities.csv --daily-bars tests/fixtures/daily_bars.csv --holdings tests/fixtures/holdings.csv
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

Event regression smoke:

```powershell
python -m catalyst_radar.cli ingest-sec submissions --ticker MSFT --cik 0000789019 --fixture tests/fixtures/sec/submissions_msft.json
python -m catalyst_radar.cli ingest-news --fixture tests/fixtures/news/ticker_news_msft.json
python -m catalyst_radar.cli ingest-earnings --fixture tests/fixtures/earnings/calendar_msft.json
python -m catalyst_radar.cli events --ticker MSFT --as-of 2026-05-10 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

Polygon fixture smoke:

```powershell
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-polygon tickers --fixture tests/fixtures/polygon/tickers_page_1.json --date 2026-05-08
python -m catalyst_radar.cli ingest-polygon grouped-daily --fixture tests/fixtures/polygon/grouped_daily_2026-05-07.json --date 2026-05-07
python -m catalyst_radar.cli ingest-polygon grouped-daily --fixture tests/fixtures/polygon/grouped_daily_2026-05-08.json --date 2026-05-08
python -m catalyst_radar.cli provider-health --provider polygon
python -m catalyst_radar.cli build-universe --name liquid-us --provider polygon --as-of 2026-05-08
python -m catalyst_radar.cli scan --as-of 2026-05-08 --universe liquid-us
```

As phases add API, dashboard pages, jobs, evals, and validation, extend the suite with:

```powershell
python -m pytest tests/golden tests/evals
python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8010
streamlit run apps/dashboard/Home.py --server.port 8509
```

Use browser verification for dashboard and API docs after visible UI changes.

## Recommended Next Phase

Build Phase 6 next:

```text
docs/superpowers/plans/2026-05-10-phase-6-local-text-intelligence.md
```

This phase is the right next step because event connectors now exist, and local text intelligence is needed before candidate packets, sparse LLM review, or Decision Cards can be useful.
