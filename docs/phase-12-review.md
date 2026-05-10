# Phase 12 Review

## What Shipped

- `budget_ledger` schema, PostgreSQL migration, SQLAlchemy table, repository, and ledger summaries.
- Config-driven LLM provider/model/pricing/budget fields with fail-closed defaults.
- `BudgetController` with disabled-premium, state, model, pricing, stale-pricing, per-task, daily, monthly, and monthly soft-cap gates.
- Sparse `LLMRouter` foundation with fake deterministic client only.
- Evidence review prompt and schema validation requiring source-linked claims.
- CLI commands: `llm-budget-status` and `run-llm-review`.
- Cost API/dashboard summary backed by budget ledger rows.
- Streamlit Costs page metrics and ledger/task/model tables.

## Verification

- `python -m pytest` -> `434 passed in 140.85s (0:02:20)`.
- `python -m ruff check src tests apps` -> `All checks passed!`.
- Deterministic no-LLM CLI smoke:
  - `init-db` -> initialized database.
  - `ingest-csv` -> `securities=6 daily_bars=36 holdings=1`.
  - `scan` -> `candidates=3`.
  - `build-packets` -> `candidate_packets=2`.
  - `build-decision-cards` -> `decision_cards=2`.
  - `build-alerts` -> `alerts=1 suppressions=2`.
  - `llm-budget-status` -> zero actual/estimated cost and zero attempts.
- Fake LLM review smoke:
  - `run-llm-review --ticker AAA --task mid_review --fake` -> `status=completed model=fake-evidence-review-v1`.
  - `llm-budget-status --json` -> one completed `mid_review` ledger row with prompt/schema version, token counts, ticker, state, and model.

## Safety Boundaries

- No real LLM provider dependency was added.
- Premium LLM remains disabled by default.
- Non-fake provider CLI paths are blocked by `_SafeDisabledLLMClient`, ledgered as `failed/client_error`, and return nonzero.
- Router does not mutate candidate scores, policy states, packets, cards, alerts, or portfolio data.
- Budget skip, dry-run, completed fake call, schema rejection, and client failure paths each write auditable ledger rows.
- Missing candidate packets are ledgered as `skipped/candidate_packet_missing`.
- Repeated review attempts append distinct ledger rows instead of replacing prior attempts.
- Ledger `ts` and budget windows use the attempt time; `available_at` remains the point-in-time data cutoff.
- Ledger and cost-summary reads hide future rows by default.
- Validation-derived cost is surfaced separately from ledger spend to avoid double counting.

## Review Fixes

- Hid future budget ledger rows by default.
- Hardened model config gates for blank values.
- Fixed cached-token cost math so cached tokens are not double-counted.
- Failed closed for future-dated pricing config.
- Bounded monthly soft-cap percentage to `0.0..1.0`.
- Added client-failure ledger tests.
- Made enabled non-fake CLI provider attempts return nonzero.
- Fixed cost aggregates so totals are not limited to display rows.
- Made cost summary validation metrics and useful labels point-in-time safe.
- Isolated LLM environment variables in dashboard/API tests.
- Made budget ledger writes append-only for repeated attempts.
- Counted paid-looking `schema_rejected` and `failed` attempts against spend and task caps.
- Rejected unsupported LLM output schemas instead of validating them as evidence reviews.
- Separated LLM attempt time from packet `available_at` for budget windows and ledger `ts`.
- Hardened evidence-review validation so claim text/source references must be strings and confidence/source-quality/sentiment stay in range.

## Known Limits

- Real LLM provider integration remains out of scope.
- Evidence review output is fake-client only.
- Phase 13 must add real evidence packet generation, skeptic review, source-faithfulness evals, and LLM-assisted Decision Card writing.
- Pricing values are operator configured and must be refreshed before any real paid model path is enabled.
