# Provider Failure Runbook

## Trigger

Provider health is `degraded`, `down`, `failed`, `error`, or stale beyond the freshness window.

## Immediate Controls

- Confirm the Ops dashboard degraded-mode banner is present.
- Confirm states above `AddToWatchlist` are disabled for the affected run.
- Keep deterministic scans available only when source freshness is acceptable.

## Diagnosis

- Inspect `/api/ops/health`.
- Check latest `provider_health` rows.
- Check latest `data_quality_incidents` rows for affected providers and tickers.
- Check recent `job_runs` for `daily_bar_ingest`, `event_ingest`, and `feature_scan`.

## Recovery

- Retry the affected provider job after rate-limit or outage clears.
- Use backup provider inputs only when licensing and freshness are known.
- Re-run `run-daily` with explicit `--as-of` and `--available-at`.

## Closeout

- Confirm a healthy provider row after recovery.
- Confirm stale-data banner clears.
- Record whether the incident created false alerts or missed opportunities.
