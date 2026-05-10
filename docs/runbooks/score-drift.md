# Score Drift Runbook

## Trigger

Score distribution, rank order, or escalation volume shifts outside the expected operating band compared with recent runs.

## Immediate Controls

- Freeze new buy-review states until drift is diagnosed.
- Keep watchlist and deterministic evidence views available for manual review.
- Preserve the affected run inputs, outputs, and score summaries for replay.
- Keep daily LLM and external alert delivery in dry-run mode while drift is active.

## Diagnosis

Inspect ops drift:

```powershell
Invoke-RestMethod http://localhost:8000/api/ops/health | ConvertTo-Json -Depth 8
```

Compare score distributions:

```sql
select as_of, state, count(*) as candidates, avg(final_score) as avg_score,
       min(final_score) as min_score, max(final_score) as max_score
from candidate_states
group by as_of, state
order by as_of desc, state
limit 50;

select ticker, as_of, state, final_score, score_delta_5d, created_at
from candidate_states
order by as_of desc, final_score desc, ticker
limit 50;
```

Inspect freshness and schema failures:

```sql
select provider, status, checked_at, reason
from provider_health
order by checked_at desc, id desc
limit 20;

select kind, severity, detected_at, reason, fail_closed_action, payload
from data_quality_incidents
where lower(kind) like '%schema%' or lower(reason) like '%schema%'
order by detected_at desc, id desc
limit 20;
```

## Recovery

- Fix data freshness, schema, or scoring defects before re-enabling escalation.
- Run replay validation before re-enabling escalation or new buy-review states.
- Reprocess the affected run only after replay output matches the expected distribution band or the drift is accepted.

Replay validation:

```powershell
python -m catalyst_radar.cli validation-replay --as-of-start 2026-05-09 --as-of-end 2026-05-09 --available-at 2026-05-10T01:00:00+00:00 --outcome-available-at 2026-06-10T01:00:00+00:00
python -m catalyst_radar.cli validation-report --run-id <run-id> --json
```

Reprocess the run:

```powershell
python -m catalyst_radar.cli run-daily --as-of 2026-05-09 --available-at 2026-05-10T01:00:00+00:00 --outcome-available-at 2026-06-10T01:00:00+00:00 --json
```

## Closeout

- Record whether the drift was data, scoring, or regime-driven.
- Confirm buy-review escalation is re-enabled only after replay validation passes.
- Record any false positives, false negatives, and follow-up calibration work.
