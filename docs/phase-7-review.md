# Phase 7 Review: Options, Theme, And Peer Features

Date: 2026-05-10

## Outcome

Phase 7 is ready for fixture-backed real-world testing. Catalyst Radar now has aggregate option feature storage and ingest, deterministic option flow/risk scoring, static theme and peer mappings, sector rotation scoring, cross-ticker peer read-through, bounded optional score support, and dashboard fields for the new evidence.

The system still does not generate options trades. Options, sector, theme, and peer data are evidence signals only; they cannot bypass stale data, liquidity, portfolio, chase, unresolved event-conflict, or other hard policy gates.

## Verification

```text
python -m pytest
250 passed in 37.59s
```

```text
python -m ruff check src tests apps
All checks passed!
```

Options/theme smoke:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
ingested provider=options_fixture raw=1 normalized=1 option_features=1 rejected=0
scanned candidates=3
```

Text regression smoke:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
ingested provider=news_fixture raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
processed text_features=1 snippets=2
MSFT local_narrative=48.51 novelty=100.00 snippets=2
scanned candidates=3
```

Event regression smoke:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
ingested provider=sec raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
ingested provider=news_fixture raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
ingested provider=earnings_fixture raw=1 normalized=1 securities=0 daily_bars=0 holdings=0 events=1 rejected=0
MSFT 2026-05-10T12:31:00+00:00 news materiality=0.23 quality=0.10 source=Sponsored Stocks Daily title=MSFT could double soon
MSFT 2026-05-10T12:35:00+00:00 earnings materiality=0.76 quality=0.85 source=Reuters title=Microsoft raises cloud guidance after earnings
MSFT 2026-05-10T12:01:00+00:00 guidance materiality=0.85 quality=1.00 source=SEC EDGAR title=MSFT 8-K
MSFT 2026-05-10T12:00:00+00:00 earnings materiality=0.55 quality=0.55 source=earnings_fixture title=Microsoft earnings date
MSFT 2026-05-09T12:01:00+00:00 sec_filing materiality=0.65 quality=1.00 source=SEC EDGAR title=MSFT 10-Q
scanned candidates=3
```

Polygon regression smoke:

```text
initialized database
ingested provider=polygon raw=4 normalized=4 securities=4 daily_bars=0 holdings=0 events=0 rejected=0
ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 holdings=0 events=0 rejected=0
ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 holdings=0 events=0 rejected=1
provider=polygon status=degraded
built universe=liquid-us members=2 excluded=1
scanned candidates=2
```

## Issues Found And Fixed

- Task 1 review found no spec compliance issues in option models, scoring, storage, or point-in-time reads.
- Task 2 review found no spec compliance issues in fixture options ingest, provider ingest fail-closed behavior, or CLI output.
- Final code review found neutral sector rotation was adding positive support because neutral sector score is `50.0`. Fixed scoring so sector support counts only positive distance above neutral, preserving optional-data neutrality.
- Final spec review found peer read-through used only the candidate's own text evidence. Fixed scan integration to aggregate point-in-time theme hits from other tickers and score only configured peers.
- Final spec review required this review note with verification and smoke evidence. Added this document.
- Polygon smoke initially produced zero universe members when run with the default CSV market provider. Reran with `CATALYST_MARKET_PROVIDER=polygon`, matching the Polygon fixture provider and restoring the expected universe result.

## Review

Task review checkpoints:

```text
Task 1 spec review: APPROVED
Task 2 spec review: APPROVED
Final review before fixes: 3 medium findings
Post-fix code re-review: APPROVED
Post-fix spec re-review: APPROVED
```

Review findings fixed:

```text
neutral/missing sector rotation support
cross-ticker peer read-through evidence
missing phase review note
```

Post-fix verification:

```text
python -m pytest
250 passed in 37.59s
```

```text
python -m ruff check src tests apps
All checks passed!
```

## Residual Risks

- Options connector is fixture-only aggregate data.
- Options scores are evidence signals, not options trade recommendations.
- Theme and peer mappings are static config, not learned relationships.
- Sector rotation is deterministic fixture-scale math, not a full cross-sectional model.
- Cross-ticker peer read-through depends on available local text/theme evidence.
- Candidate packets and validation remain future phases.
