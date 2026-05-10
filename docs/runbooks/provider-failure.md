# Provider Failure Runbook

## Trigger

Provider health is `degraded`, `down`, `failed`, `error`, or stale beyond the freshness window.

## Immediate Controls

- Confirm the Ops dashboard degraded-mode banner is present.
- Confirm states above `AddToWatchlist` are disabled for the affected run.
- Keep deterministic scans available only when source freshness is acceptable.
- Do not run `run-daily --deliver-alerts`; daily alert delivery is intentionally disabled.

## Diagnosis

Inspect the API:

```powershell
Invoke-RestMethod http://localhost:8000/api/ops/health | ConvertTo-Json -Depth 8
```

Check provider health and incidents:

```sql
select provider, status, checked_at, reason
from provider_health
order by checked_at desc, id desc
limit 20;

select kind, severity, detected_at, provider, ticker, reason, fail_closed_action
from data_quality_incidents
order by detected_at desc, id desc
limit 20;
```

Check recent job status:

```sql
select job_type, provider, status, started_at, finished_at, error_summary
from job_runs
where job_type in ('daily_bar_ingest', 'event_ingest', 'feature_scan', 'scoring_policy')
order by started_at desc, id desc
limit 30;
```

## Recovery

- Retry the affected provider job after rate-limit or outage clears.
- Use backup provider inputs only when licensing and freshness are known.
- Re-run `run-daily` with explicit `--as-of` and `--available-at`.

Local rerun:

```powershell
python -m catalyst_radar.cli run-daily --as-of 2026-05-09 --available-at 2026-05-10T01:00:00+00:00 --json
```

Docker rerun:

```powershell
docker compose run --rm worker python -m catalyst_radar.cli run-daily --as-of 2026-05-09 --available-at 2026-05-10T01:00:00+00:00 --json
```

If a worker crashed, inspect the lock before forcing another run:

```sql
select lock_name, owner, acquired_at, heartbeat_at, expires_at, metadata
from job_locks
where lock_name = 'daily-run';
```

## Closeout

- Confirm a healthy provider row after recovery.
- Confirm stale-data banner clears.
- Record whether the incident created false alerts or missed opportunities.
