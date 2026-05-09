# Phase 9 Validation, Shadow Mode, And Paper Trading Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add point-in-time validation, simple baselines, outcome labels, and paper-trading workflows so Catalyst Radar can measure whether candidate packets and decision cards are useful before any real-capital workflow.

**Architecture:** Validation is read-only over persisted market data, candidate states, packets, and cards. Replay uses `available_at` gates and never recomputes future-aware state. Paper trading records simulated decisions and outcomes only; it does not send orders or imply investment advice.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite-compatible local storage with PostgreSQL migration SQL, existing market/candidate packet repositories, pytest, ruff.

---

## Current Baseline

Build from:

```text
main @ 3e8a7b7
```

Current verified baseline:

- `python -m pytest` passes with 280 tests.
- `python -m ruff check src tests apps` passes.
- Candidate packets and decision cards persist point-in-time versions.
- CLI can build and inspect packets/cards.
- Dashboard data exposes packet/card availability and top evidence.
- `src/catalyst_radar/validation/backtest.py` has basic leakage assertion and forward-return labels.

Important current limit:

- There is no validation schema for runs, results, paper trades, or useful-alert labels.
- Replay is not yet a first-class object and does not rebuild a validation run from stored candidate state.
- Baselines are not implemented.
- Paper trading does not record approve/reject/defer, simulated entry, invalidation, or outcomes.
- No report summarizes precision, false positives, missed opportunities, or cost per useful alert.

## Scope

In this phase, implement:

- `validation_runs`, `validation_results`, `paper_trades`, and `useful_alert_labels` tables.
- Validation dataclasses with timezone and point-in-time validation.
- Point-in-time replay snapshots from persisted candidate states, packets, and cards.
- Baseline selectors for SPY momentum, sector momentum, event-only watchlist, random eligible universe, and user watchlist.
- Outcome label computation for 10d/15, 20d/25, 60d/40, sector outperformance, max adverse excursion, and max favorable excursion.
- Paper decision workflow: approve, reject, defer, simulated entry, invalidation monitoring, and outcome update.
- Validation reports for precision, false positives, missed opportunities, useful-alert rate, cost per useful alert, and leakage failures.
- CLI commands to run replay, record paper decisions, update outcomes, and print validation summary.

Out of scope:

- Automated order placement.
- Brokerage integration.
- Live alert delivery.
- LLM evaluations or model-cost ledger enforcement.
- Statistical optimization or score retuning.

## File Structure

Create:

- `src/catalyst_radar/validation/models.py`  
  Validation run/result, replay row, baseline candidate, paper trade, useful-alert label dataclasses.
- `src/catalyst_radar/validation/replay.py`  
  Point-in-time replay from persisted candidate states, packets, cards, and scan payloads.
- `src/catalyst_radar/validation/baselines.py`  
  Simple deterministic baselines.
- `src/catalyst_radar/validation/outcomes.py`  
  Forward-return, MAE/MFE, invalidation, and sector-outperformance labels.
- `src/catalyst_radar/validation/paper.py`  
  Paper decision and simulated trade state transitions.
- `src/catalyst_radar/validation/reports.py`  
  Summary metrics and report payloads.
- `src/catalyst_radar/storage/validation_repositories.py`  
  Persistence and point-in-time reads.
- `sql/migrations/009_validation.sql`
- `tests/unit/test_backtest_replay.py`
- `tests/unit/test_validation_baselines.py`
- `tests/unit/test_validation_outcomes.py`
- `tests/unit/test_validation_reports.py`
- `tests/integration/test_paper_trading.py`
- `tests/integration/test_validation_cli.py`
- `tests/golden/test_no_leakage_replay.py`
- `docs/phase-9-review.md`

Modify:

- `src/catalyst_radar/validation/backtest.py`  
  Keep existing compatibility helpers and delegate richer labels to `outcomes.py`.
- `src/catalyst_radar/storage/schema.py`  
  Add validation tables and indexes.
- `src/catalyst_radar/cli.py`  
  Add validation and paper workflow commands.
- Existing tests that assert table inventory or dashboard row shape if needed.

## Data Contracts

`validation_runs` rows:

```text
id                    deterministic run id or generated UUID
run_type              replay, shadow, paper_update, report
as_of_start           inclusive decision start
as_of_end             inclusive decision end
decision_available_at replay decision availability cutoff
status                running, success, failed
config                JSON run config
metrics               JSON summary metrics
started_at            runtime timestamp
finished_at           nullable runtime timestamp
created_at            persistence timestamp
```

`validation_results` rows:

```text
id                    deterministic run/ticker/as_of/state id
run_id                validation_runs.id
ticker                uppercase ticker
as_of                 candidate timestamp
available_at          replay cutoff used for this row
state                 candidate state
final_score           persisted score
candidate_state_id    candidate_states.id
candidate_packet_id   nullable candidate_packets.id
decision_card_id      nullable decision_cards.id
baseline              nullable baseline name
labels                JSON outcome labels
leakage_flags         JSON leakage or missing availability flags
payload               JSON audit payload
created_at            persistence timestamp
```

`paper_trades` rows:

```text
id                    deterministic decision-card/action id
decision_card_id      decision_cards.id
ticker                uppercase ticker
as_of                 card as_of
decision              approved, rejected, deferred
state                 pending_entry, open, invalidated, closed, rejected, deferred
entry_price           nullable simulated entry
entry_at              nullable simulated entry timestamp
invalidation_price    nullable deterministic invalidation
shares                simulated shares copied from card sizing
notional              simulated notional copied from card sizing
max_loss              deterministic max loss
outcome_labels        JSON labels
source_ts             card source timestamp
available_at          decision availability timestamp
created_at            persistence timestamp
updated_at            persistence timestamp
```

`useful_alert_labels` rows:

```text
id                    deterministic artifact/label id
artifact_type         candidate_packet, decision_card, paper_trade, alert
artifact_id           referenced artifact id
ticker                uppercase ticker
label                 useful, noisy, too_late, too_early, ignored, acted
notes                 optional text
created_at            persistence timestamp
```

## Invariants

Point-in-time invariant:

```text
Replay may include only records whose available_at <= decision_available_at.
```

No-leakage invariant:

```text
Any record with missing availability or future availability is excluded and counted in leakage_flags.
```

No-execution invariant:

```text
Paper trading records simulated decisions only. It must never call a broker, produce order tickets, or claim that a trade was actually placed.
```

Baseline invariant:

```text
Baselines are deterministic reference comparisons. They do not alter candidate states or policy results.
```

## Task 1: Validation Schema, Models, And Repository

**Files:**

- Create: `src/catalyst_radar/validation/models.py`
- Create: `src/catalyst_radar/storage/validation_repositories.py`
- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `sql/migrations/009_validation.sql`
- Test: `tests/integration/test_paper_trading.py`

- [x] **Step 1: Write repository tests**

Cover:

- Validation run insert/update lifecycle.
- Validation results upsert by deterministic ID.
- Paper trade approve/reject/defer round trip.
- Useful-alert label insert and latest lookup.
- Future-available paper/validation rows are excluded when queried point-in-time.

- [x] **Step 2: Add validation dataclasses**

Include:

```python
class ValidationRunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

class PaperDecision(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    DEFERRED = "deferred"

class PaperTradeState(StrEnum):
    PENDING_ENTRY = "pending_entry"
    OPEN = "open"
    INVALIDATED = "invalidated"
    CLOSED = "closed"
    REJECTED = "rejected"
    DEFERRED = "deferred"

@dataclass(frozen=True)
class ValidationRun:
    id: str
    run_type: str
    as_of_start: datetime
    as_of_end: datetime
    decision_available_at: datetime
    status: ValidationRunStatus
    config: Mapping[str, Any]
    metrics: Mapping[str, Any]
    started_at: datetime
    finished_at: datetime | None

@dataclass(frozen=True)
class ValidationResult:
    id: str
    run_id: str
    ticker: str
    as_of: datetime
    available_at: datetime
    state: ActionState
    final_score: float
    labels: Mapping[str, Any]
    leakage_flags: Sequence[str]

@dataclass(frozen=True)
class PaperTrade:
    id: str
    decision_card_id: str
    ticker: str
    decision: PaperDecision
    state: PaperTradeState
    available_at: datetime
    payload: Mapping[str, Any]

@dataclass(frozen=True)
class UsefulAlertLabel:
    id: str
    artifact_type: str
    artifact_id: str
    ticker: str
    label: str
    created_at: datetime
```

Validation:

- Reject naive timestamps.
- Uppercase tickers.
- Validate enum values.
- Freeze JSON payloads.

- [x] **Step 3: Add schema and migration**

Tables:

- `validation_runs`
- `validation_results`
- `paper_trades`
- `useful_alert_labels`

Indexes:

- `ix_validation_results_run_ticker_as_of`
- `ix_validation_results_available_at`
- `ix_paper_trades_ticker_state`
- `ix_paper_trades_decision_card`
- `ix_useful_alert_labels_artifact`

- [x] **Step 4: Add repository methods**

Methods:

```python
upsert_validation_run(run: ValidationRun) -> None
finish_validation_run(run_id: str, status: ValidationRunStatus, metrics: Mapping[str, Any]) -> None
upsert_validation_results(rows: Iterable[ValidationResult]) -> int
upsert_paper_trade(trade: PaperTrade) -> None
latest_paper_trade_for_card(decision_card_id: str, available_at: datetime) -> PaperTrade | None
insert_useful_alert_label(label: UsefulAlertLabel) -> None
list_validation_results(run_id: str) -> list[ValidationResult]
```

## Task 2: Point-In-Time Replay

**Files:**

- Create: `src/catalyst_radar/validation/replay.py`
- Modify: `src/catalyst_radar/validation/backtest.py`
- Test: `tests/unit/test_backtest_replay.py`
- Test: `tests/golden/test_no_leakage_replay.py`

- [x] **Step 1: Write replay tests**

Cover:

- Replayed candidate states use `candidate_states.created_at <= decision_available_at`.
- Candidate packets and decision cards use their own `available_at` gates.
- Future packets/cards are excluded from replay row.
- Missing availability records are counted as leakage/missing flags.
- Replaying the same persisted inputs is deterministic.

- [x] **Step 2: Implement replay row model and builder**

Replay row payload:

```text
ticker
as_of
decision_available_at
state
final_score
candidate_state_id
candidate_packet_id
decision_card_id
hard_blocks
transition_reasons
score_delta_5d
leakage_flags
payload
```

- [x] **Step 3: Implement validation run builder**

Function:

```python
build_replay_results(packet_repo, validation_repo, *, as_of_start, as_of_end, decision_available_at, states) -> list[ValidationResult]
```

Use existing persisted candidate inputs and latest packet/card lookups.

## Task 3: Baselines

**Files:**

- Create: `src/catalyst_radar/validation/baselines.py`
- Test: `tests/unit/test_validation_baselines.py`

- [x] **Step 1: Write baseline tests**

Cover:

- SPY-relative momentum ranks eligible tickers by 20d/60d stored return fields.
- Sector-relative momentum ranks by sector-relative score.
- Event-only watchlist includes candidates with material event support.
- Random eligible universe sample is deterministic with a seed.
- User watchlist returns configured tickers when present and empty otherwise.

- [x] **Step 2: Implement baseline selectors**

Return `BaselineCandidate` rows with:

- `baseline`
- `ticker`
- `as_of`
- `rank`
- `score`
- `reason`
- `payload`

Baselines should consume replay rows or scan payloads, not mutate product state.

## Task 4: Outcomes And Paper Trading

**Files:**

- Create: `src/catalyst_radar/validation/outcomes.py`
- Create: `src/catalyst_radar/validation/paper.py`
- Test: `tests/unit/test_validation_outcomes.py`
- Test: `tests/integration/test_paper_trading.py`

- [x] **Step 1: Expand outcome labels**

Keep compatibility with existing `label_forward_return()`, then add:

```python
compute_forward_outcomes(entry_price, future_prices, sector_future_prices, invalidation_price) -> OutcomeLabels
```

Labels:

- `target_10d_15`
- `target_20d_25`
- `target_60d_40`
- `sector_outperformance`
- `max_adverse_excursion`
- `max_favorable_excursion`
- `invalidated`

- [x] **Step 2: Implement paper decision workflow**

Functions:

```python
create_paper_trade_from_card(card, decision, available_at) -> PaperTrade
mark_simulated_entry(trade, entry_price, entry_at) -> PaperTrade
update_trade_outcome(trade, outcome_labels, updated_at) -> PaperTrade
```

Rules:

- Approved creates `pending_entry` unless entry price is supplied.
- Rejected creates `rejected` with no entry.
- Deferred creates `deferred` with next review retained.
- Open trade can become `invalidated` when price breaches invalidation.
- No function may call external broker/order APIs.

## Task 5: Reports And CLI

**Files:**

- Create: `src/catalyst_radar/validation/reports.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/unit/test_validation_reports.py`
- Test: `tests/integration/test_validation_cli.py`

- [x] **Step 1: Add report tests**

Cover:

- Precision by label.
- False positive count.
- Useful-alert rate.
- Cost per useful alert with zero-cost safe behavior.
- Missed opportunities from baseline winners absent from candidate results.
- Leakage failures count.

- [x] **Step 2: Implement report builder**

Output:

```text
run_id
candidate_count
useful_alert_rate
precision
false_positive_count
missed_opportunity_count
cost_per_useful_alert
leakage_failure_count
state_mix
baseline_comparison
```

- [x] **Step 3: Add CLI commands**

Commands:

```text
validation-replay --as-of-start YYYY-MM-DD --as-of-end YYYY-MM-DD --available-at ISO [--outcome-available-at ISO]
validation-report --run-id RUN_ID
paper-decision --decision-card-id ID --decision approved|rejected|deferred --available-at ISO
paper-update-outcomes --decision-card-id ID --available-at ISO
useful-label --artifact-type decision_card --artifact-id ID --ticker TICKER --label useful
```

Human output examples:

```text
validation_run=validation-replay-v1:2026-05-10:2026-05-10:2026-05-10T13:00:00+00:00 results=3 leakage_failures=0
paper_trade=paper-trade-v1:card-123 state=pending_entry decision=approved
validation_report run_id=validation-replay-v1:2026-05-10:2026-05-10:2026-05-10T13:00:00+00:00 precision_target_20d_25=0.33 useful_alert_rate=0.50
```

## Task 6: Verification, Smoke, Review, And Documentation

**Files:**

- Create: `docs/phase-9-review.md`
- Modify this phase plan checklist while executing.

- [x] **Step 1: Run focused tests**

Commands:

```text
python -m pytest tests/unit/test_backtest.py tests/unit/test_backtest_replay.py tests/unit/test_validation_baselines.py tests/unit/test_validation_outcomes.py tests/unit/test_validation_reports.py
python -m pytest tests/integration/test_paper_trading.py tests/integration/test_validation_cli.py
python -m pytest tests/golden/test_no_leakage_replay.py
```

- [x] **Step 2: Run full verification**

Commands:

```text
python -m pytest
python -m ruff check src tests apps
```

- [x] **Step 3: Run fixture smoke**

Use an isolated SQLite database:

```text
$env:PYTHONPATH="src"
$env:CATALYST_DATABASE_URL="sqlite:///tmp/phase9-smoke.db"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities tests/fixtures/securities.csv --daily-bars tests/fixtures/daily_bars.csv --holdings tests/fixtures/holdings.csv
python -m catalyst_radar.cli ingest-news --fixture tests/fixtures/news/ticker_news_msft.json
python -m catalyst_radar.cli ingest-sec submissions --ticker MSFT --cik 0000789019 --fixture tests/fixtures/sec/submissions_msft.json
python -m catalyst_radar.cli ingest-earnings --fixture tests/fixtures/earnings/calendar_msft.json
python -m catalyst_radar.cli ingest-options --fixture tests/fixtures/options/options_summary_2026-05-08.json
python -m catalyst_radar.cli run-textint --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli scan --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli build-packets --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli build-decision-cards --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli validation-replay --as-of-start 2026-05-10 --as-of-end 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli validation-report --run-id <printed-run-id>
```

- [x] **Step 4: Review pass**

Ask subagents to review:

```text
Review Phase 9 validation and paper trading implementation. Focus on point-in-time replay, no future leakage, baseline correctness, paper-trading no-execution boundary, report metrics, CLI workflow, and schema/repository persistence. Do not edit files.
```

- [x] **Step 5: Document phase outcome**

`docs/phase-9-review.md` must include:

- Outcome.
- Verification commands and exact pass/fail outputs.
- Fixture smoke output.
- Review findings and fixes.
- Residual risks.

## Exit Criteria

- Validation runs can replay persisted candidate states using availability timestamps.
- Future packets/cards are excluded from historical replay.
- Simple baselines can be generated deterministically.
- Outcome labels include 10d/20d/60d targets, sector outperformance, MAE, MFE, and invalidation.
- Paper decisions can be recorded without any execution path.
- Validation reports include precision, false positives, useful-alert rate, cost per useful alert, missed opportunities, and leakage failures.
- CLI can run replay and print reports.
- Full pytest and ruff pass.
- Phase review document exists with smoke evidence.
