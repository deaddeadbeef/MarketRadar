# Phase 4 Review: Portfolio-Aware Policy

Date: 2026-05-10

## Outcome

Phase 4 is ready for real-world testing with fixture and local CSV data. The scanner now creates deterministic setup plans, sizes proposed positions from the portfolio policy, computes portfolio impact before policy evaluation, persists portfolio-impact evidence, and blocks candidates on explicit portfolio hard gates before manual buy review.

## Verification

```text
python -m pytest
159 passed in 14.13s
```

```text
python -m ruff check src tests apps
All checks passed!
```

CSV smoke:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
provider=csv status=healthy
scanned candidates=3
```

Polygon fixture smoke:

```text
initialized database
ingested provider=polygon raw=4 normalized=4 securities=4 daily_bars=0 rejected=0
ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 rejected=0
ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 rejected=1
provider=polygon status=degraded
built universe=liquid-us members=2 excluded=1
scanned candidates=2
```

Subagent re-review:

```text
No high/medium issues remain in the reviewed Phase 4 portfolio scan/persistence/policy paths.
python -m pytest tests/integration/test_portfolio_policy_scan.py tests/unit/test_portfolio.py tests/unit/test_score.py
29 passed in 1.09s
```

## Issues Found And Fixed

- Existing SQLite databases created before portfolio fields were added failed on holdings read/write. Fixed by idempotently upgrading SQLite holdings columns from `create_schema()`.
- Partial holdings refreshes could understate exposure by keeping only one global latest timestamp. Fixed by selecting latest rows per ticker and resolving account-level values deterministically.
- Setup-policy wiring left older tests expecting the removed basic trade plan. Updated expectations to the deterministic setup-policy output.
- Candidate state persistence was not idempotent, causing duplicate dashboard rows on retry. Fixed by replacing existing `(ticker, as_of, feature_version)` state rows.
- Portfolio impact evidence timestamps fell back to scan timestamp. Fixed by persisting evidence timestamps from market bars and portfolio snapshot data.
- Cash was captured but not enforced. Fixed by emitting `insufficient_cash_hard_block` when available cash cannot fund proposed notional.

## Residual Risks

- Setup policies are deterministic market-data placeholders. Post-earnings and filings-catalyst setups intentionally do not promote without future event ingestion.
- Correlated exposure is a sector/theme placeholder, not a statistical correlation model.
- Holdings quality matters: missing or stale position rows can still affect exposure, though partial refreshes are now handled conservatively.
- No broker connectivity or order placement exists by design; all buy states remain manual-review only.
- Polygon provider remains fixture-tested here. Live API reliability, rate limits, and data-contract drift still need real-world testing with a production key.
