# Radar Run

Use the dashboard **Run Fixture Smoke** or **Run Capped Live Radar** control, or
call the API to execute one guarded daily radar pass:

```powershell
curl.exe --insecure --fail --silent --show-error --request POST https://127.0.0.1:8443/api/radar/runs --header "Content-Type: application/json" --data '{}'
```

The run uses the existing `daily-run` job lock, so another dashboard/API/worker run will return `409` instead of overlapping.
Manual dashboard/API runs also use `CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS`
as a durable cooldown. Repeated run requests inside that window return `429`
with `Retry-After`, even if the previous run has already finished.
The dashboard reads the same cooldown lock before submitting a request, so the
run control shows whether the next click is a fixture smoke test or capped live
run, when the next manual run is allowed, and disables the button while the
cooldown or call-plan setup block is active.

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

The dashboard **Live Data Activation** contract turns that plan into exact
operator steps. It is read-only and makes zero external calls. It shows a
redacted `.env.local` template, safe caps, first-run commands, and the maximum
external calls that would happen only if you manually seed the universe or run a
capped live radar cycle. Placeholder template values, such as
`CATALYST_POLYGON_API_KEY=<your Polygon API key>`, are treated as missing and
will not unlock live calls.

The dashboard **Latest Run Path** is the operator view of the last run. Read
`Required Path`, `Action Needed`, `Optional Gates Not Triggered`, and
`Audit-only Rows` before opening raw audit details. The old raw
`Radar Run Steps` table is not the primary control surface: optional Decision
Cards, LLM review, digest delivery, and outcome validation can remain untriggered
without making the scan unhealthy. Raw stored statuses and reason codes are kept
for auditability, but the dashboard translates them into required-path
completion, expected optional gates, and root-cause rows.

A required-stage row that is waiting for input, such as missing active
securities or missing text snippets, is different from an expected optional
gate. Those rows stay in the required path and make the run incomplete until the
upstream input is fixed. Expected optional gates do not block the run by
themselves; they become action items only when you intentionally want that gate
to execute.

The manual **LLM dry run** option verifies the agent-review path without calling
OpenAI. It reviews Warning, ThesisWeakening, and manual-review candidate packets;
it no longer waits for Decision Cards, because those are later buy-review
artifacts.

For worker automation, use the same plan-first flow:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-worker-once.ps1
```

This command reads local API state and the run call plan only. It starts no
worker and makes zero external calls unless you rerun it with `-Execute`. The
script forces daily worker LLM review off and keeps alerts in dry-run mode.

The dashboard **Telemetry Status Summary** is the first telemetry view to read.
It rolls the append-only event tape into `Needs attention`, `Safety guard`, and
`Healthy` categories so rate-limit protection is visible without looking like a
scan failure. Use **Recent Radar Telemetry** only when you need the detailed
audit trail for a dashboard/API action that was requested, completed, rejected,
blocked by a lock, or rate limited before rerunning a live operation. Each radar
step still writes both `radar_run.step_started` and `radar_run.step_finished`
audit rows, and raw stored status/reason values remain available in collapsed
audit detail.

Rate-limit events stay on the tape, but they are classified as `Safety guard`
rather than `Needs attention`: the guard is proving the no-DDOS safety path.
Failed, rejected, blocked-input, or needs-review events still surface as
`Needs attention`.

When the summary is not enough, export the redacted raw telemetry evidence:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-telemetry.ps1
```

The dashboard also exposes **Download Raw Telemetry Evidence** in the telemetry
section. Both paths use redacted JSON and make zero Polygon, SEC, Schwab, or
OpenAI calls. The script calls `GET /api/ops/telemetry/raw`, writes a JSON
snapshot under `data\ops\telemetry\`, and makes zero provider calls. The raw
endpoint is for local audit/debugging: it preserves append-only event metadata,
before/after payloads, artifacts, timestamps, and reasons while redacting
secret-looking fields and URL query tokens.

To capture the full local operator state in one file, use:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-operator-evidence.ps1
```

This writes health, readiness, latest run, live activation, run call plan,
telemetry summary, raw telemetry, and Schwab status under `data\ops\bundles\`.
It is the fastest zero-call snapshot to attach to an investigation before and
after the first capped live smoke.

The dashboard **Actionability Breakdown** explains why the current queue is or is
not ready for investment work. It buckets candidates into buy-review, research,
watchlist, blocked/risk-review, and monitor groups, then lists the dominant
risks or gaps plus the next action for each top candidate.

The dashboard **Investment Decision Readiness** gate is the operator truth for
whether the visible queue can support a real manual buy review. Fixture data,
missing live sources, thin universes, stale bars, blocked run steps, missing
candidate packets, or missing Decision Cards force `research_only` even when
demo candidates have high scores. The top **System Status** strip also surfaces
`Bars Stale` so freshness cannot be missed when the run itself completed. Treat
the queue as decision-ready only when that gate says `manual_buy_review`.

The **Candidate Queue** repeats that decision mode per row. Its `Decision`
column can show `manual_buy_review`, `research_only`, `missing_card`, `blocked`,
`monitor`, or `not_ready`; use that label before interpreting a high score as
actionable. The queue also shows `Risk / Blocker` from stored hard blocks,
portfolio blocks, transition reasons, or disconfirming evidence. Selecting a
row opens **Blocker Diagnostics**, which is the fastest way to see whether a
candidate is blocked by stale data, portfolio/risk policy, missing evidence, or
a real negative signal.

The dashboard **Research Shortlist** condenses the current queue into the rows
most worth manual attention. It keeps the raw audit trail server-side, but the
visible table focuses on priority, ticker, decision label, why-now evidence,
risk/gap, next step, and Decision Card availability.

The dashboard **Candidate Delta** is DB-only and makes no provider calls. It
compares the latest run's candidate states against the previous state for the
same ticker, then highlights new candidates, state transitions, material score
moves, and hard-block changes. Use it after each radar run to answer "what
changed?" before reading the full queue.

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
curl.exe --insecure --fail --silent --show-error --request POST https://127.0.0.1:8443/api/radar/universe/seed --header "Content-Type: application/json" --data '{"provider":"polygon","max_pages":1}'
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
curl.exe --insecure --fail --silent --show-error --request GET https://127.0.0.1:8443/api/radar/runs/latest
```

To get the single operator contract for whether the radar is actionable without
starting a new run:

```powershell
curl.exe --insecure --fail --silent --show-error --request GET https://127.0.0.1:8443/api/radar/readiness
```

`GET /api/radar/readiness` is DB/config-only. It composes the latest run path,
live activation plan, readiness checklist, discovery snapshot, actionability
breakdown, investment readiness gate, candidate decision labels, and telemetry
tape. Use `safe_to_make_investment_decision=false` as a hard stop; high scores
remain research-only until this endpoint says the queue is ready for
`manual_buy_review`.

For a single local pass/fail gate that matches the dashboard readiness contract,
run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/assert-investable-readiness.ps1
```

The script reads the local API only and makes zero Polygon, SEC, Schwab, or
OpenAI calls. It exits non-zero until `safe_to_make_investment_decision=true`,
live activation is ready, the run call plan is unblocked, and telemetry has no
`Needs attention` events. In the current setup flow this failure is intentional
until the missing live credentials are filled and one capped live smoke has
succeeded.

To inspect the live-data activation contract without starting a run or calling
providers:

```powershell
curl.exe --insecure --fail --silent --show-error --request GET https://127.0.0.1:8443/api/radar/live-activation
```

`GET /api/radar/live-activation` is DB/config-only. It returns missing env vars,
a redacted environment template, safe caps, exact operator commands, and the
call budget if you later choose to activate live data manually. Reading this
endpoint does not call Polygon, SEC, Schwab, or OpenAI.

To get the prioritized research queue without opening the dashboard:

```powershell
curl.exe --insecure --fail --silent --show-error --request GET https://127.0.0.1:8443/api/radar/research-shortlist
```

`GET /api/radar/research-shortlist` is also DB/config-only. It ranks persisted
candidates into manual-review, research-now, missing-card, watchlist, blocked,
or monitor priorities. When a row is backed by restricted provider data, the
API keeps operational fields such as ticker, priority, decision label, score,
card, and next step, while withholding provider-derived prose such as why-now
evidence and risk/gap text.

To inspect the external call budget for a proposed radar run without starting
the run:

```powershell
curl.exe --insecure --fail --silent --show-error --request POST https://127.0.0.1:8443/api/radar/runs/call-plan --header "Content-Type: application/json" --data '{"tickers":["MSFT","NVDA"],"run_llm":true,"llm_dry_run":true}'
```

`POST /api/radar/runs/call-plan` is also DB/config-only. It returns per-layer
`external_call_count_max` rows for market data, SEC/news events, LLM review,
alert delivery, and Schwab. It does not acquire the daily run lock, record
telemetry, call Polygon, call SEC, sync Schwab, or call OpenAI. Use it before
clicking **Run Capped Live Radar** when live providers are configured.

Both `POST /api/radar/runs` and `GET /api/radar/runs/latest` include a
`discovery_snapshot` block. That snapshot is DB-only: it reads the latest stored
run telemetry, candidate queue, provider mode, universe coverage, and freshness
evidence, and it does not call Polygon, SEC, Schwab, or OpenAI. Use it as the
first operator answer to: "what did the latest run discover, and is it fresh
enough to trust?"

If the latest run is newer than the latest available bars or candidate states,
`yield.candidate_states` only counts candidates aligned with the run `as_of`.
The separate `latest_candidate_context` block shows the latest persisted
candidate rows available at the run cutoff, marks whether they are stale
relative to the run, and should be treated as context rather than fresh
discoveries.

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
      "latest_bars_older_than_as_of": false,
      "latest_candidate_as_of": "2026-05-10T00:00:00+00:00",
      "latest_candidate_age_days": 0
    },
    "latest_candidate_context": {
      "candidate_states": 8,
      "latest_candidate_as_of": "2026-05-10T00:00:00+00:00",
      "latest_candidate_age_days": 0,
      "stale_relative_to_run": false,
      "top_candidates": []
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
