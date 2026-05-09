# Phase 5 Review: Event Connectors

Date: 2026-05-10

## Outcome

Phase 5 is ready for fixture-backed real-world testing. Catalyst Radar now has canonical event storage, deterministic event classification, source-quality scoring, URL/body dedupe, SEC/news/earnings fixture connectors, event CLI commands, provider promotion into the event store, event-aware scan metadata, bounded event score support, event-driven setup selection, source-conflict downgrades, and dashboard event fields.

The system still remains deterministic-first. Event evidence can support a candidate, but it cannot bypass stale-data, liquidity, risk, chase, portfolio, or cash policy gates.

## Verification

```text
python -m pytest
200 passed in 21.26s
```

```text
python -m ruff check src tests apps
All checks passed!
```

Event connector smoke:

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

- News connector source-quality scoring initially bypassed the shared source-quality rules. Fixed by using the event source-quality/classifier helpers so promotional source names and domains stay low quality even if provider metadata claims a better category.
- Event URL dedupe initially duplicated helper logic in the SEC connector. Fixed by using shared event dedupe helpers across SEC, news, and earnings connectors.
- SEC fixture replay initially used runtime fetch time as availability, which made historical fixture reads disappear after the fixture date. Fixed so fixture SEC records use deterministic source-time availability.
- News canonical payloads initially omitted article body, preventing conflict detection when raise/cut language only appeared in body text. Fixed by preserving body text in the canonical payload used by conflict detection.
- Polygon provider test expectations still used the pre-event provider output contract. Updated the test to include `holdings=0 events=0`.

## Residual Risks

- SEC live mode is gated behind `CATALYST_SEC_ENABLE_LIVE=1` and a required `CATALYST_SEC_USER_AGENT`, but it still needs live endpoint testing with a compliant user agent before production use.
- News and earnings connectors are fixture/provider skeletons. A licensed real provider still needs to be selected and contract-tested.
- Source quality and materiality are conservative deterministic rules, not local NLP. Promotional and conflicting evidence is handled, but deeper sentiment, novelty, and ontology work remains for the local text intelligence phase.
- Event support is intentionally capped at an 8-point score bonus. This is safer for policy, but it means event-driven candidates may remain under-ranked until text intelligence and candidate packets are added.
- Conflict detection currently covers guidance direction conflicts. Other conflict classes, such as legal/regulatory contradictions or analyst revision clusters, remain future work.
