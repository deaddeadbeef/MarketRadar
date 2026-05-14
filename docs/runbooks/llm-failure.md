# LLM Failure Runbook

## Trigger

LLM review jobs fail, skip unexpectedly, exceed budget, or produce degraded status in the Ops dashboard, API, or `budget_ledger`.

## Immediate Controls

- Keep the deterministic scanner running; deterministic candidate generation and evidence review continue without LLM enrichment.
- Confirm premium model calls fail closed when an API key is absent, the provider is disabled, or the budget is exhausted.
- Keep eligible candidates reviewable without synthetic model claims or placeholder recommendations.
- Do not use `run-daily --real-llm`; daily real LLM review is intentionally rejected. Use `run-llm-review` for explicit per-candidate review.

## Diagnosis

Inspect ops health and budget status:

```powershell
curl.exe --insecure --fail --silent --show-error --request GET https://127.0.0.1:8443/api/ops/health
python -m catalyst_radar.cli llm-budget-status --json
```

Inspect ledger rows:

```sql
select task, provider, model, status, skip_reason, estimated_cost, actual_cost, available_at
from budget_ledger
order by available_at desc, id desc
limit 50;
```

Confirm runtime configuration:

```powershell
$env:CATALYST_ENABLE_PREMIUM_LLM
$env:CATALYST_LLM_PROVIDER
$env:CATALYST_LLM_EVIDENCE_MODEL
$env:CATALYST_LLM_SKEPTIC_MODEL
$env:CATALYST_LLM_DECISION_CARD_MODEL
```

Check whether the candidate has source-linked evidence:

```powershell
python -m catalyst_radar.cli candidate-packet --ticker MSFT --as-of 2026-05-09 --available-at 2026-05-10T01:00:00+00:00 --json
```

## Recovery

- Restore the provider key or budget only after confirming the requested model and spending cap are approved.
- Rerun LLM review only for candidates with valid source-linked evidence packets.
- Keep dry-run mode enabled for smoke checks until ledger rows show expected costs and statuses.

Dry-run review:

```powershell
python -m catalyst_radar.cli run-llm-review --ticker MSFT --as-of 2026-05-09 --available-at 2026-05-10T01:00:00+00:00 --task skeptic_review --dry-run --json
```

Dashboard/API dry-run review:

```powershell
curl.exe --insecure --fail --silent --show-error --request POST https://127.0.0.1:8443/api/agents/review --header "Content-Type: application/json" --data '{"ticker":"MSFT","as_of":"2026-05-09","available_at":"2026-05-10T01:00:00+00:00","task":"skeptic_review","mode":"dry_run"}'
```

Fake-provider smoke:

```powershell
python -m catalyst_radar.cli run-llm-review --ticker MSFT --as-of 2026-05-09 --available-at 2026-05-10T01:00:00+00:00 --task skeptic_review --fake --json
```

## Closeout

- Confirm new `budget_ledger` rows show expected status, cost, model, and skip-reason behavior.
- Confirm reviewed candidates retain source links and do not contain unsupported synthetic model claims.
- Record whether the incident was caused by missing credentials, budget exhaustion, provider outage, or evidence-packet defects.
