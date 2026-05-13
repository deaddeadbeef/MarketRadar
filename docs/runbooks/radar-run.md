# Radar Run

Use the dashboard **Run Radar** control or call the API to execute one guarded daily radar pass:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri https://127.0.0.1:8443/api/radar/runs `
  -SkipCertificateCheck `
  -ContentType 'application/json' `
  -Body '{}'
```

The run uses the existing `daily-run` job lock, so another dashboard/API/worker run will return `409` instead of overlapping.

The radar run does not call Schwab. It can read the latest synced broker context already in the database when building decision cards, but Schwab portfolio and market refreshes stay behind the separate broker controls and their server-side rate guards.

## Market Data Provider

By default, local runs use fixture CSV market data:

```text
CATALYST_DAILY_MARKET_PROVIDER=csv
```

To let a daily radar run ingest Polygon grouped daily bars, set:

```text
CATALYST_DAILY_MARKET_PROVIDER=polygon
CATALYST_DAILY_PROVIDER=polygon
CATALYST_POLYGON_API_KEY=<your key>
```

The daily Polygon path fails closed before making a request when
`CATALYST_POLYGON_API_KEY` is missing. A guarded run makes one grouped-daily
request for the selected `as_of` date. Seed/refresh Polygon ticker reference data
separately with `ingest-polygon tickers` when the securities master is empty or
stale.

To inspect the last persisted daily radar pass without starting a new one:

```powershell
Invoke-RestMethod `
  -Uri https://127.0.0.1:8443/api/radar/runs/latest `
  -SkipCertificateCheck
```

Optional JSON fields:

```json
{
  "as_of": "2026-05-09",
  "decision_available_at": "2026-05-10T01:00:00Z",
  "outcome_available_at": "2026-06-10T01:00:00Z",
  "provider": "csv",
  "universe": "liquid-us",
  "tickers": ["MSFT", "NVDA"],
  "run_llm": false,
  "llm_dry_run": true,
  "dry_run_alerts": true
}
```

Real daily LLM execution and real alert delivery intentionally fail closed. Use the explicit per-candidate LLM review and alert dry-run workflows until those release gates are opened.
