# Phase 9 Review

## Outcome

Phase 9 adds validation, replay, deterministic baselines, outcome labeling, manual paper decision tracking, useful-alert labels, and validation reports.

The implementation remains a decision-support and validation workflow only. Paper trades store simulated decisions and outcomes with `manual_review_only` and `no_execution` payload fields, and no broker/order path was added.

## Verification

Focused Phase 9 verification:

```text
python -m pytest tests/unit/test_backtest.py tests/unit/test_backtest_replay.py tests/unit/test_validation_baselines.py tests/unit/test_validation_outcomes.py tests/unit/test_validation_reports.py tests/integration/test_paper_trading.py tests/integration/test_validation_cli.py tests/golden/test_no_leakage_replay.py
32 passed in 15.89s
```

Full verification:

```text
python -m pytest
308 passed in 70.15s (0:01:10)

python -m ruff check src tests apps
All checks passed!
```

Fixture smoke:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
ingested provider=news_fixture raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
ingested provider=sec raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
ingested provider=earnings_fixture raw=1 normalized=1 securities=0 daily_bars=0 holdings=0 events=1 rejected=0
ingested provider=options_fixture raw=1 normalized=1 option_features=1 rejected=0
processed text_features=1 snippets=5
scanned candidates=3
built candidate_packets=3
built decision_cards=3
validation_replay run_id=validation-replay-v1:2026-05-10T21:00:00+00:00:2026-05-10T21:00:00+00:00:f84baaa4db2e1f25 candidate_results=3 baseline_results=15 results=18 decision_available_at=2026-05-10T13:00:00+00:00 outcome_available_at=2026-05-10T13:00:00+00:00 leakage_failures=0 precision_target_20d_25=0.00
validation_report run_id=validation-replay-v1:2026-05-10T21:00:00+00:00:2026-05-10T21:00:00+00:00:f84baaa4db2e1f25 candidates=3 useful_alert_rate=0.00 precision_target_20d_25=0.00 false_positives=3 missed_opportunities=2 leakage_failures=0
```

## Review Findings And Fixes

Subagent spec review found gaps in outcome labels, baseline wiring, future artifact leakage counting, point-in-time reads, deferred next-review retention, paper trade IDs, and missing review documentation.

Fixes made:

- Replay labels are computed from stored daily bars whose `available_at` is within the replay/report cutoff.
- Replay now separates decision availability (`--available-at`) from optional outcome availability (`--outcome-available-at`) so future outcome labels do not expand the historical decision boundary.
- Baselines are persisted as validation result rows, compare candidates against the broader historical daily-bar universe, use the decision cutoff for input data, and compare by ticker plus replay date.
- Replay can flag newer future packet/card artifacts while retaining the latest visible artifact for the row.
- Validation results and useful-alert labels support point-in-time reads.
- Deterministic validation reruns clear stale prior rows for the same run id.
- Deferred paper decisions retain card `next_review_at` in payload.
- Paper trade IDs include the decision action, and outcome updates create later-visible paper-trade versions rather than rewriting the original decision-time row.
- Paper decisions cannot be backdated to before a decision card was available.
- Useful-alert relabeling is handled by using the latest label per artifact as of the report cutoff.
- `docs/phase-9-review.md` records verification, smoke output, review findings, and residual risks.

## Residual Risks

- Outcome labels are only as complete as available daily-bar history; partial future windows intentionally produce conservative labels.
- Baselines are simple deterministic references, not optimized strategies.
- Paper trading is still local simulation; no broker reconciliation, fill modeling, slippage model, or alert delivery exists yet.
- Validation reports are CLI-first; dashboard/API exposure remains for later phases.
