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
Manual dashboard/API runs also use `CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS`
as a durable cooldown. Repeated run requests inside that window return `429`
with `Retry-After`, even if the previous run has already finished.
The dashboard reads the same cooldown lock before submitting a request, so the
**Run Radar** control shows when the next manual run is allowed and disables the
button while the cooldown is active.

The radar run does not call Schwab. It can read the latest synced broker context already in the database when building decision cards, but Schwab portfolio and market refreshes stay behind the separate broker controls and their server-side rate guards.

Before enabling live sources, check the dashboard **Provider Preflight** table.
It shows the configured provider, live-call budget, safety guardrail, and next
action for market data, SEC/news events, Schwab context, and LLM review. This is
a configuration preflight; it does not call external APIs.

The dashboard **Live Activation Plan** is also DB/config-only. It separates:

- the required scan path, such as market ingest, event ingest, feature scan,
  scoring, and candidate packet creation;
- expected optional gates, such as Decision Cards, LLM review, digest delivery,
  and outcome validation when their trigger is absent;
- blocked inputs that need operator action before relying on the run.

Raw telemetry can still show `status=skipped` for expected optional gates. Treat
the `category`, required-path count, and action-needed count as the operator
truth: `expected_gate` means no failure occurred unless you intentionally wanted
that gate to run.

The dashboard **Recent Radar Telemetry** tape is a compact view of append-only
telemetry audit events. Use it to confirm whether a dashboard/API action was
requested, completed, rejected, blocked by a lock, or rate limited before
rerunning a live operation. Run-step telemetry keeps the raw stored status, but
also surfaces the operator outcome category, so expected optional gates are
shown as `expected_gate` with `raw_status=skipped` instead of looking like scan
failures.

The dashboard **Actionability Breakdown** explains why the current queue is or is
not ready for investment work. It buckets candidates into buy-review, research,
watchlist, blocked/risk-review, and monitor groups, then lists the dominant
risks or gaps plus the next action for each top candidate.

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
CATALYST_POLYGON_TICKERS_MAX_PAGES=1
CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS=300
```

The daily Polygon path fails closed before making a request when
`CATALYST_POLYGON_API_KEY` is missing. A guarded run makes one grouped-daily
request for the selected `as_of` date. Seed/refresh Polygon ticker reference data
separately with `ingest-polygon tickers` when the securities master is empty or
stale. Ticker-reference pagination is capped by `CATALYST_POLYGON_TICKERS_MAX_PAGES`
or `ingest-polygon tickers --max-pages`; start with `1` and raise it deliberately.

The dashboard **Seed Universe** control runs the same capped Polygon ticker-reference
ingest path from the API. It is disabled until `CATALYST_POLYGON_API_KEY` is set.
The API route is rate limited by
`CATALYST_POLYGON_TICKER_SEED_MIN_INTERVAL_SECONDS`:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri https://127.0.0.1:8443/api/radar/universe/seed `
  -SkipCertificateCheck `
  -ContentType 'application/json' `
  -Body '{"provider":"polygon","max_pages":1}'
```

Missing Polygon credentials, provider contract failures, and critical rejected
payloads fail closed and are recorded in provider jobs, data-quality incidents,
and telemetry audit events.

## Catalyst Provider

By default, local runs use fixture news/events:

```text
CATALYST_DAILY_EVENT_PROVIDER=news_fixture
```

To let a daily radar run ingest SEC submissions for active securities with CIK
metadata, set:

```text
CATALYST_DAILY_EVENT_PROVIDER=sec
CATALYST_SEC_ENABLE_LIVE=1
CATALYST_SEC_USER_AGENT="CatalystRadar/0.1 your-email@example.com"
CATALYST_SEC_DAILY_MAX_TICKERS=5
```

The scheduled SEC path fails closed before making a request when live mode or a
User-Agent is missing. The ticker cap bounds SEC submissions calls per radar run.
When using Polygon as the market provider, run `ingest-polygon tickers` first so
active securities include CIK metadata.

To inspect the last persisted daily radar pass without starting a new one:

```powershell
Invoke-RestMethod `
  -Uri https://127.0.0.1:8443/api/radar/runs/latest `
  -SkipCertificateCheck
```

Both `POST /api/radar/runs` and `GET /api/radar/runs/latest` include a
`discovery_snapshot` block. That snapshot is DB-only: it reads the latest stored
run telemetry, candidate queue, provider mode, universe coverage, and freshness
evidence, and it does not call Polygon, SEC, Schwab, or OpenAI. Use it as the
first operator answer to: "what did the latest run discover, and is it fresh
enough to trust?"

Key fields:

```json
{
  "discovery_snapshot": {
    "status": "fixture",
    "yield": {
      "requested_securities": 500,
      "scanned_securities": 42,
      "candidate_states": 8,
      "candidate_packets": 3,
      "decision_cards": 1
    },
    "freshness": {
      "latest_daily_bar_date": "2026-05-10",
      "latest_bars_older_than_as_of": false
    },
    "blockers": [
      {
        "code": "fixture_market_data",
        "finding": "Market data is still fixture-backed."
      }
    ],
    "top_discoveries": []
  }
}
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
