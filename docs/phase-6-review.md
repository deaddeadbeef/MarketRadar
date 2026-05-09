# Phase 6 Review: Local Text Intelligence

Date: 2026-05-10

## Outcome

Phase 6 is ready for fixture-backed real-world testing. Catalyst Radar now has deterministic local text intelligence on top of canonical events: text snippet storage, text feature storage, ontology/theme matching, conservative phrase sentiment, deterministic hashing-vector embeddings, point-in-time novelty scoring, text pipeline CLI commands, scan metadata wiring, bounded local narrative score support, and dashboard text fields.

The system remains deterministic-first. Local narrative evidence can support a candidate, but it cannot bypass stale-data, liquidity, risk, chase, portfolio, cash, or unresolved event-conflict policy gates.

## Verification

```text
python -m pytest
222 passed in 27.28s
```

```text
python -m ruff check src tests apps
All checks passed!
```

Text intelligence smoke:

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

- Extracted snippets initially carried `OntologyHit` dataclasses, which were not directly JSON-serializable for repository persistence. Fixed by exposing JSON-ready ontology hit payloads and covering extraction-to-storage persistence.
- Snippet text initially flattened non-text event payload metadata such as dates and provider fields into snippet text and hashes. Fixed by restricting snippet text to title plus text-bearing payload keys: `body`, `summary`, `description`, and `items`.
- Cosine similarity initially accepted non-finite vector components. Fixed by rejecting NaN and infinity values before similarity math.
- Text pipeline novelty initially used snippets from the same current run after reruns, changing novelty and narrative scores. Fixed by excluding current selected snippet hashes and event IDs from prior novelty history.
- Text pipeline snippet-hash semantics changed during development from event/ticker/text to text-only, which could duplicate same-event rows written by earlier branch commits. Fixed by replacing existing same-event, same-section snippets during upsert.

## Review

Task review checkpoints passed:

```text
Task 1+2 spec review: APPROVED
Task 3 re-review after fixes: APPROVED
Task 4 spec review: APPROVED
Task 4 code quality review: APPROVED
```

Task 4 reviewer verification included:

```text
python -m pytest tests/integration/test_text_scan_integration.py tests/unit/test_score.py tests/integration/test_event_scan_integration.py -q
18 passed
```

## Residual Risks

- Hashing-vector embeddings are deterministic fallback embeddings, not semantic transformer embeddings.
- The ontology parser supports only the repo's simple YAML subset.
- Sentiment is phrase-based and intentionally conservative.
- Text features currently derive from event title/body-style fields only; deeper filings sections, transcripts, and article paragraph selection remain future work.
- No LLM evidence packets or Decision Cards exist yet.
- No paid transcripts/news provider is integrated.
