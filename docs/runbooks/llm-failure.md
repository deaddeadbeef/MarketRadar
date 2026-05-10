# LLM Failure Runbook

## Trigger

LLM review jobs fail, skip unexpectedly, exceed budget, or produce degraded status in the Ops dashboard, API, or `budget_ledger`.

## Immediate Controls

- Keep the deterministic scanner running; deterministic candidate generation and evidence review continue without LLM enrichment.
- Confirm premium model calls fail closed when an API key is absent, the provider is disabled, or the budget is exhausted.
- Keep eligible candidates reviewable without synthetic model claims or placeholder recommendations.

## Diagnosis

- Inspect `/api/ops/health` and the dashboard cost/LLM status panels.
- Inspect `budget_ledger` rows for the affected task, ticker, status, and skip reasons.
- Confirm the provider, model, and budget configuration came from the intended environment.
- Check whether each affected candidate has a valid source-linked evidence packet.

## Recovery

- Restore the provider key or budget only after confirming the requested model and spending cap are approved.
- Rerun LLM review only for candidates with valid source-linked evidence packets.
- Keep dry-run mode enabled for smoke checks until ledger rows show expected costs and statuses.

## Closeout

- Confirm new `budget_ledger` rows show expected status, cost, model, and skip-reason behavior.
- Confirm reviewed candidates retain source links and do not contain unsupported synthetic model claims.
- Record whether the incident was caused by missing credentials, budget exhaustion, provider outage, or evidence-packet defects.
