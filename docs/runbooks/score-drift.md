# Score Drift Runbook

## Trigger

Score distribution, rank order, or escalation volume shifts outside the expected operating band compared with recent runs.

## Immediate Controls

- Freeze new buy-review states until drift is diagnosed.
- Keep watchlist and deterministic evidence views available for manual review.
- Preserve the affected run inputs, outputs, and score summaries for replay.

## Diagnosis

- Inspect the latest versus previous score distribution, including percentiles, state counts, and top-ranked tickers.
- Inspect provider freshness and schema failures for the affected run window.
- Check recent scoring configuration, feature availability, and hard-block changes.
- Compare replay inputs against the prior stable run before attributing the drift to market movement.

## Recovery

- Fix data freshness, schema, or scoring defects before re-enabling escalation.
- Run replay validation before re-enabling escalation or new buy-review states.
- Reprocess the affected run only after replay output matches the expected distribution band or the drift is accepted.

## Closeout

- Record whether the drift was data, scoring, or regime-driven.
- Confirm buy-review escalation is re-enabled only after replay validation passes.
- Record any false positives, false negatives, and follow-up calibration work.
