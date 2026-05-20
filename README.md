# Catalyst Radar

Catalyst Radar is a deterministic-first market radar for public-equity opportunity review.

Phase 1 builds the scanner, feature engine, policy gates, portfolio risk checks, validation skeleton, and dashboard without LLM calls.

## Local setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item .env.example .env.local
pytest
```

## Local database

SQLite is the default local database:

```powershell
catalyst-radar init-db
catalyst-radar ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
catalyst-radar scan --as-of 2026-05-08
```

To populate the integrated dashboard with a deterministic review fixture:

```powershell
catalyst-radar seed-dashboard-demo
catalyst-radar ipo-s1-analysis --ticker ACME --as-of 2026-05-10 --available-at 2026-05-10T21:05:00Z --json
streamlit run apps/dashboard/Home.py
```

The seed command creates one candidate, one alert, validation/cost rows, ops
health, and one SEC S-1 analysis row from public-style EDGAR fixture data. For
live SEC ingestion, set `CATALYST_SEC_ENABLE_LIVE=1` and a compliant
`CATALYST_SEC_USER_AGENT`, then run `catalyst-radar ingest-sec ipo-s1 --ticker
<SYMBOL> --cik <CIK>`.

Postgres integration is available through Docker Compose:

```powershell
docker compose up -d postgres
$env:CATALYST_DATABASE_URL="postgresql+psycopg://catalyst:catalyst@localhost:54321/catalyst_radar"
catalyst-radar init-db
```

## Provider configuration

Real daily-radar provider settings are configured through environment variables.
For the first useful live smoke without a market-data key, keep market data on
local CSV with `CATALYST_DAILY_MARKET_PROVIDER=csv` and
`CATALYST_DAILY_PROVIDER=csv`, then enable live SEC catalyst discovery with
`CATALYST_DAILY_EVENT_PROVIDER=sec`, `CATALYST_SEC_ENABLE_LIVE=1`, and a
compliant `CATALYST_SEC_USER_AGENT`. `CATALYST_POLYGON_API_KEY` is optional and
only needed if you intentionally switch the market provider to Polygon for fresh
broad-market bars.
`CATALYST_DAILY_MARKET_PROVIDER` controls scheduled daily bar ingest; the
`CATALYST_DAILY_PROVIDER` override keeps manual/default radar runs aligned with
that scheduled provider.

A full-market scan means all active securities currently stored locally, not the
few demo tickers and not the smaller `liquid-us` liquidity filter. Polygon/Massive
grouped-daily bars alone do not create securities. To expand beyond fixtures,
ingest the active ticker reference set, ingest fresh bars, then run the radar
without `--universe`:

```powershell
catalyst-radar priced-in-preflight
catalyst-radar priced-in-preflight --stocks-only
catalyst-radar ingest-polygon tickers --confirm-external-call
catalyst-radar ingest-polygon grouped-daily --date <LATEST_TRADING_DATE> --confirm-external-call
catalyst-radar run-daily --as-of <LATEST_TRADING_DATE> --available-at <UTC-now> --provider polygon --json
```

`priced-in-preflight` is zero-call. It explains why the current queue may only
show a few tickers and returns the exact commands/API routes needed before the
next scan can count as broad-market. Use `--stocks-only` when you want the
preflight blockers for the stock-like priced-in answer instead of funds,
wrappers, rights, warrants, and other instruments. In Polygon/Massive mode it
also exposes the current `CATALYST_POLYGON_TICKERS_MAX_PAGES` cap, because one
ticker page is not the whole market. When broad grouped-daily bars are already
present, it estimates the ticker-reference page count from the latest daily-bar
ticker count. If your Polygon/Massive plan is rate-limited, set
`CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS` before running a multi-page ticker
seed so pagination is paced deliberately.

For the same path as a plan-first PowerShell workflow:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-full-market-scan.ps1
powershell -ExecutionPolicy Bypass -File scripts/run-full-market-scan.ps1 -TickerPages 13 -TickerPageDelaySeconds 12 -Execute
```

Without `-Execute`, the script only reads local preflight state and prints the
provider calls it would make. With `-Execute`, it sets the Polygon/Massive page
cap and page delay only in the current PowerShell process, then runs ticker
seed, grouped-daily ingest, an all-active daily radar run, and priced-in queue
review. Add `-UseUniverse` only when you intentionally want the smaller
`liquid-us`-style selected-universe scan.

The dashboard shows active security count, requested/scanned securities, fresh
bar coverage, and candidate count so a selected universe is not mistaken for a
full-market pass. In the TUI, the Insights page opens in `Full Scan` mode by
default: it shows the first ranked page from the latest scan. When the latest
run used `--universe`, the queue is scoped to that same selected universe and
the readiness status tells you to run without `--universe` for the all-active
full-market pass.
`Mismatches` is the narrower filter for bullish/bearish not-priced-in rows.
Press `M`, click the `SCAN` controls in the sidebar, or type `full` /
`mismatches` in the command box to switch. For non-interactive checks, use
`catalyst-radar dashboard-tui --once --page overview`; pass
`--scan-mode mismatches` only when you intentionally want the smaller queue.
Source-fill actions are deliberately chunked: `batch <source>` shows the
full-scan source plan plus the next safe provider chunk, and
`batch <source> execute` runs only that one guarded chunk. The few tickers in a
chunk are not the scan universe; they are the next rate-limited fill batch for
the broader ranked universe.
`priced-in-source-batches --source all` reports two separate recommendations:
`coverage_first` for filling broad full-scan evidence and `decision_shortcut`
for the smaller set of currently decision-ready/actionable rows. Use the
coverage-first source when your goal is improving the whole-market scan; use the
decision shortcut only when you intentionally want to deepen the current top
answer subset. The JSON payload also includes `mission_brief`, a zero-call
operator summary with the current trusted-answer state, scan progress, next
source, next command, call boundary, next unblock options, and roadmap of
remaining evidence gaps. When market bars block the answer, those unblock
options separate the zero-call manual CSV path from the saved Polygon/Massive
capture approval path, including the expected call count before capture.
The default is still the full scan. Use `ready`, press `D`, or run
`catalyst-radar priced-in-queue --decision-ready` only when you intentionally
want the small decision-useful subset from that full scan.
The scriptable equivalent is:

```powershell
catalyst-radar priced-in-source-batches --source all
catalyst-radar priced-in-source-batches --source catalyst_events
catalyst-radar priced-in-source-batches --source catalyst_events --execute-next
```

`--source all` gives a plan-only overview across market bars, catalyst events,
local text, options, theme/peer context, and broker context. It makes 0
provider calls and cannot be combined with `--execute-next`; choose one source
before executing a chunk. The per-source planning command also makes 0 provider
calls. The `--execute-next` command executes only the next planned chunk. The
API equivalents are
`GET /api/radar/priced-in/source-batches?source=all` and
`POST /api/radar/priced-in/source-batches/execute-next`.

Before any live provider call, run the activation checker and inspect the
call plan:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/prepare-live-env.ps1
powershell -ExecutionPolicy Bypass -File scripts/open-live-env.ps1
powershell -ExecutionPolicy Bypass -File scripts/check-live-activation.ps1
powershell -ExecutionPolicy Bypass -File scripts/run-first-live-smoke.ps1
powershell -ExecutionPolicy Bypass -File scripts/run-worker-once.ps1
curl.exe --insecure --fail --silent --show-error --request POST https://127.0.0.1:8443/api/radar/runs/call-plan --header "Content-Type: application/json" --data '{}'
```

`scripts/prepare-live-env.ps1` only writes safe non-secret local defaults such
as `CATALYST_DAILY_MARKET_PROVIDER=csv`, live SEC with low ticker caps,
disabled order submission, and dry-run LLM/alert settings. It makes 0 external
calls, does not print secrets, and only requires you to fill
`CATALYST_SEC_USER_AGENT` manually for SEC access.
`scripts/open-live-env.ps1` runs that same safe preparation, opens `.env.local`
in VS Code when available, falls back to Notepad, and makes 0 external calls.
`scripts/run-first-live-smoke.ps1` defaults to plan-only mode: it reads local
API readiness and call-plan state, makes 0 external calls, and requires
`-Execute` before running one capped radar cycle. It skips Polygon universe
seeding unless Polygon is actually configured as the market provider.
`scripts/run-worker-once.ps1` does the same for worker automation: plan-only by
default, `-Execute` required, no Schwab calls, and OpenAI disabled for the daily
worker path.

Schwab broker integration is read-only. Configure the Schwab app credentials,
callback URL, and `BROKER_TOKEN_ENCRYPTION_KEY` in `.env.local`, then use the
API routes under `/api/brokers/schwab/*` and `/api/portfolio/*`. See
`docs/runbooks/schwab.md`.
Fixture-backed tests and local CSV flows do not require provider credentials.

Universe defaults are documented in `.env.example`; adjust the threshold values
there before building point-in-time universe snapshots.

## Dashboard

```powershell
powershell -ExecutionPolicy Bypass -File scripts/restart-local.ps1
```

The local restart script starts the API at `https://127.0.0.1:8443` and the
dashboard at `http://127.0.0.1:8514`, loading `.env.local` through the app
startup path. Docker Compose runs the same command-center entry point at
`http://localhost:8501`.

The primary operator dashboard can also run entirely in the terminal:

```powershell
catalyst-radar dashboard-tui
catalyst-radar dashboard-tui --once --page features
```

For a one-command PowerShell launcher, install the profile alias:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/install-dashboard-profile.ps1 -ProfilePath $PROFILE
```

Then open a new PowerShell session and run:

```powershell
radar
radar --once --page tutorial
```

The alias calls `scripts/run-dashboard-tui.ps1`. That script keeps setup local
to this repo: it creates `.venv` if needed, installs the editable
`catalyst-radar` command when `pyproject.toml` changes, fast-forwards clean
`main` to `origin/main`, and then starts the TUI. It does not set `PYTHONPATH`
or mutate the caller's shell environment. Use `radar --no-update` to skip the
Git update step and `radar --force-install` to refresh the editable install.

The TUI is the operational replacement surface for the web dashboard. It loads
the same command-center data helpers and provides pages for tutorial, insights,
readiness, run/call-plan, candidates, alerts, IPO/S-1, broker, ops, telemetry,
themes, validation, costs, and current feature inventory. The Run page now
starts with a Mission Brief: the priced-in question, current trusted-answer
state, scan progress, first trust blocker, useful next action, and the zero-call
boundary before the call-plan details. `radar` opens on
`0 Tutorial` by default; the tutorial now leads with that same mission/current
answer/next-blocker summary before the control walkthrough, so the first screen
answers why the tool exists before it teaches shortcuts. Press `1` or run
`radar --page overview` for Insights.
The insights page is the full-market priced-in queue by default: the first row reports scan
coverage, then candidate rows show emotion score, price-reaction score,
emotion-minus-reaction gap, priced-in status, why the mismatch matters, and the
next action. Candidate rows open the candidate evidence detail, alert rows open
alert detail, blocker rows open readiness or ops, and refresh rows open the
guarded run plan. Click or press `Enter` on an insight row to open the right
operational view. Inside the
TUI, the left sidebar is the primary navigation: click a row, press a page
number, use `Ctrl+N` / `Ctrl+P`, or focus the sidebar and use `Up` / `Down`
plus `Enter`. The compact `KEYS` / `MOUSE` guide keeps shortcuts visible
without adding a second navigation system. Candidate and alert rows are
mouse-selectable in Windows Terminal. The `NEXT ACTION` card shows the useful
operator move for the current page; the `LAST RESPONSE` card shows what the
dashboard just did. Use `ticker <SYMBOL|all>` and `available-at <ISO|latest>`
to filter, `json` to print the redacted machine-readable snapshot, `refresh`
to reload the local database, and `q` to quit. It makes 0 Polygon, SEC, Schwab,
or OpenAI calls while rendering, clicking, filtering, or navigating. From the
run page, `run` explains the guarded execution path and `run execute` starts
one capped scheduler cycle after the call plan is visible.
From the ops/source-gap view, `batch <source>` remains plan-only and zero-call;
`batch <source> execute` is the explicit live/local action for one source-fill
chunk. Use it repeatedly with refreshes when you intentionally want to fill the
full scan under provider caps. Use `batch all` to see a zero-call overview of
all source gaps and the first executable source before choosing one. The Ops
page also shows a `Source Fill Workflow` section from the zero-call preflight
plan so the next source to inspect is visible without remembering commands.
CLI/API automation can use
`priced-in-source-batches --source all`,
`priced-in-source-batches --source <source> --execute-next` or
`POST /api/radar/priced-in/source-batches/execute-next` for the same one-chunk
operation.
The broker page also supports local operator writes that do not submit real
orders: `action <ticker> <watch|ready|simulate_entry|dismiss> [notes]`,
`trigger <ticker> <type> <op> <threshold> [notes]`, `eval-triggers [ticker]`,
`ticket <ticker> <buy|sell> <entry> <stop> [risk_pct] [notes]`, and
`feedback <alert-id|#> <label> [notes]`.

For a zero-call local sitrep:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/market-radar-status.ps1
```

For the faster "what blocks the stock scan right now?" view:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/market-radar-status.ps1 -Quick
```

This reads local API health, readiness, latest run, live activation, recent
ops health, telemetry, and telemetry coverage. The readiness payload includes
`operator_next_step`, the canonical zero-call next action for the dashboard and
scripts. The sitrep also prints active market-bar coverage so a manual CSV
refresh can be verified after import, including how many active tickers have a
bar on the latest daily-bar date and which active tickers are still missing
latest bars. It also reports coverage for the latest run's `as_of` date, which
is the date the manual template must satisfy before the run can become
decision-useful. It makes 0 Polygon, SEC, Schwab, or OpenAI calls.
Quick mode limits the output to health, readiness, the stock-like market-bar
repair plan, saved-capture approval packet, and any local manual-bar template
preview. The saved-capture approval lines show whether approval is required,
how many bars are targeted, the exact external-call count if approved, and the
zero-call question to review before running `bars saved capture confirm`.

For functional end-to-end tests of the same command-center data the dashboard
renders:

```powershell
catalyst-radar dashboard-snapshot --json
catalyst-radar dashboard-snapshot --ticker ACME --available-at 2026-05-10T21:06:00Z
catalyst-radar priced-in-preflight --json
catalyst-radar priced-in-preflight --stocks-only --json
catalyst-radar priced-in-queue --json
catalyst-radar agent-brief --json
```

The snapshot uses the dashboard data helpers for readiness, latest run,
candidate rows, alerts, IPO/S-1 rows, themes, validation, costs, broker context,
ops health, telemetry, telemetry coverage, live activation, and call planning.
It is read-only, redacts restricted provider payloads, and makes 0 Polygon, SEC,
Schwab, or OpenAI calls.

`priced-in-queue` is the scriptable replacement for the TUI insight table. It
returns the same full-scan boundary and ranked emotion-vs-reaction rows used by
the dashboard, with optional `--status`, `--usefulness`, `--source-gap`,
`--decision-gap`, `--min-gap`, `--limit`, and `--json`. Use
`--full-scan --all --json` when you want the complete latest scan export.
`priced-in-answer` summarizes the current answer and points back to
`priced-in-queue --full-scan`; any small ticker list it prints is only the
current display page, not the scan universe. Use
`priced-in-queue --full-scan` to page through the universe and
`priced-in-queue --full-scan --all --json` to export every scanned row.
`--usefulness decision_useful` for names where the core priced-in answer is
ready for human review, `--usefulness research_useful` for names that still
need local review artifacts, `--usefulness blocked` for mismatches blocked by
policy/portfolio checks, or `--usefulness useful` for research- or
decision-useful rows. Use `--source-gap options` or
`--source-gap broker_context` to find rows where optional context is still
missing or stale. Use
`--usefulness research_useful --decision-gap candidate_packet` to list otherwise
useful rows that still need a Candidate Packet before Decision Card review.
Then use `--decision-gap decision_card` to find rows whose local review artifact
is still incomplete. Missing options and broker context remain visible as source
gaps, but they do not block the ordinary equity priced-in answer.
The payload includes queue-level `source_coverage`, so the operator can see
whether market bars, catalyst events, local text, options, theme/peer/sector
context, and broker context are contributing across the visible queue.
Each row also reports which source classes are available, stale, or missing:
market bars, catalyst events, local text, options, theme/peer/sector context,
and broker context. The API equivalent is `GET /api/radar/priced-in`.
Use `GET /api/radar/priced-in?decision_ready=true` for the API equivalent of
the CLI/TUI decision-ready shortcut.
Use `priced-in-preflight --json` first when the queue says `universe_too_small`
or `partial_scan`; its API equivalent is `GET /api/radar/priced-in/preflight`.
`priced-in-answer` and `GET /api/radar/priced-in/answer` answer the narrower
question "Has price fully matched market expectations?" Their
`decision_ready=true` / `priced_in_answer_ready=true` fields mean the
emotion-vs-reaction answer is ready for human review. They deliberately keep
`can_make_investment_decision=false`; trade safety still comes only from the
separate readiness/manual-buy-review gate.

`agent-brief` is the CLI surface for the OpenAI Agents SDK operator layer. By
default it runs a deterministic dry-run brief from the same redacted dashboard
snapshot, with four roles: Data Sentinel, Catalyst Analyst, Risk Officer, and
Operator. It makes 0 Polygon, SEC, Schwab, or OpenAI calls in default mode.
The JSON payload and the TUI Agent page include a `runtime` block that names
`openai_agents_sdk`, marks `copilot_dependency=absent`, and shows that market,
broker, shell, filesystem, and web tools are unavailable to the agent layer.
When market bars block the priced-in answer, the dry-run brief also summarizes
the same redacted unblock options visible in the dashboard: manual CSV, saved
provider capture approval, and saved-file validate/import follow-ups. Those
items are instructions for a human operator; the agent still cannot call
Polygon/Massive or mutate the database.
Real Agents SDK mode is opt-in:

```powershell
catalyst-radar agent-brief --real --json
```

Real mode fails closed unless all explicit gates are set:
`CATALYST_ENABLE_AGENT_SDK=true`, `CATALYST_ENABLE_PREMIUM_LLM=true`,
`CATALYST_LLM_PROVIDER=openai`, `CATALYST_AGENT_SDK_MODEL=<model>`, and
`OPENAI_API_KEY=<secret>`. Even in real mode, the agent receives only an
allowlisted redacted snapshot and has no Polygon/Massive, SEC, Schwab, shell,
filesystem, web, or order-submission tools.

See `docs/dashboard-feature-inventory.md` for the current dashboard feature
inventory and TUI coverage.

When the sitrep or `priced-in-preflight --json` says the broad scan
is blocked on `market_bars`, inspect the repair plan first:

```powershell
catalyst-radar market-bars repair-plan --expected-as-of 2026-05-15 --json
```

That plan is the operator contract for both CLI and dashboard. It tells you the
manual CSV path, the saved Polygon/Massive response path, whether that response
file already exists, and the next zero-call command. It also exposes API-ready
request bodies so a UI does not need to guess parameters:
`provider_saved_file_capture_approval_packet` is the compact approval packet
for the one saved grouped-daily provider call; it includes the target date,
coverage scope, current missing-bar count, safe/confirm request bodies,
external-call count, DB-write boundary, and exact TUI follow-up commands.
`provider_saved_file_capture_request_body` is the safe approval boundary with
`confirm_external_call=false`; `provider_saved_file_capture_confirm_request_body`
is the explicit one-provider-call capture body with `confirm_external_call=true`;
`dashboard_manual_template_command`,
`dashboard_manual_import_preview_command`, and
`dashboard_manual_import_execute_command` are the dashboard-native zero-provider-call
commands to show in the TUI before falling back to long CLI commands;
`provider_saved_file_validate_request_body`,
`provider_saved_file_import_preview_request_body`, and
`provider_saved_file_import_request_body` are the zero-provider-call saved-file
preview/import bodies. The TUI exposes the same workflow from the Run page.
For manual zero-call repair, `bars manual template` generates the full
active-universe missing-bar CSV by default, `bars manual import` previews
complete rows only with 0 provider calls and 0 DB writes, and
`bars manual import execute` writes only completed rows into the local database.
Use `bars manual stocks template` when you intentionally want the narrower
stock-like scope instead of the full active universe. For saved-provider repair,
`bars saved capture` shows the approval boundary, current missing-bar count,
the saved-capture approval packet, and the zero-call validate/import request
bodies without making provider calls;
`bars saved capture confirm` is the explicit one-call Polygon/Massive capture
and immediately prints a zero-call post-capture preview of whether the saved
file covers current missing bars; `bars saved validate` checks the saved
grouped-daily JSON from disk when you want to re-check it later;
`bars saved import` previews the local import; and `bars saved import execute`
writes that saved file into the local database with 0 provider calls.

Options evidence now has the same dashboard-native operator path.
`options template` creates the point-in-time options JSON scaffold for the
current scan-date options gaps, `options validate` checks the local fixture with
0 provider calls and db_writes=0, `options import` previews the same validation,
and `options import execute` explicitly persists the validated fixture to local
option features with 0 provider calls. Add `stocks` or `full` to the command
when you intentionally want a stock-like-only or full active-universe template.
The matching API route is `POST /api/radar/options/fixture-import`: omit
`execute` to preview validation with 0 provider calls and db_writes=0, then set
`execute=true` only to persist a validated local fixture.

SEC CIK repair is also available from the terminal dashboard. `cik template` creates `data\local\cik-overrides-template.csv` for catalyst-event rows that are blocked only because a company-like ticker lacks CIK metadata. `cik validate` and `cik import` are zero-provider-call previews with db_writes=0. `cik import execute` is the explicit local metadata update path; after it updates CIKs, rerun `batch catalyst_events` or `priced-in-source-batches --source catalyst_events` before approving SEC source-fill calls. API parity already exists through `GET /api/radar/sec/cik-overrides-template`, `POST /api/radar/sec/cik-overrides/validate`, and `POST /api/radar/sec/cik-overrides`. Do not guess CIKs; use exact SEC CIKs or an explicitly approved SEC company-tickers refresh.

For a manual repair, generate or reuse the local ignored CSV scaffold from the
current database universe, fill only complete OHLCV rows, preview them, then
execute the import only after the preview is clean:

```powershell
catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data/local/manual-bars-2026-05-15.csv --missing-only
catalyst-radar market-bars import --daily-bars data/local/manual-bars-2026-05-15.csv --expected-as-of 2026-05-15 --complete-rows-only
catalyst-radar market-bars import --daily-bars data/local/manual-bars-2026-05-15.csv --expected-as-of 2026-05-15 --complete-rows-only --execute
```

For the saved-provider repair path, one explicit capture can write the raw
grouped-daily response to disk and immediately show a zero-call post-capture
coverage preview. Validate remains available for later re-checks, and import
from that saved file makes 0 provider calls:

```powershell
catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --save-response data/local/polygon-grouped-daily-2026-05-15.json --confirm-external-call
catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --fixture data/local/polygon-grouped-daily-2026-05-15.json --validate-only
catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --fixture data/local/polygon-grouped-daily-2026-05-15.json
```

The template command writes a local ignored CSV scaffold for missing active
tickers in the live database. Missing-only rows are sorted with stock-like
instruments first, then unknown types, then fund/wrapper rows. Fill `open`,
`high`, `low`, `close`, `volume`, and `vwap`; preview reports missing or
invalid bar fields and validates active-ticker coverage before any import.
Saved-file validation/import and manual CSV import make 0 Polygon, SEC, Schwab,
or OpenAI calls. After importing, rerun `scripts/market-radar-status.ps1`, then
use the plan-only smoke before any capped live radar cycle.

For a redacted raw telemetry evidence snapshot:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-telemetry.ps1
```

This reads `GET /api/ops/telemetry/raw`, writes a JSON export under
`data\ops\telemetry\`, and makes 0 Polygon, SEC, Schwab, or OpenAI calls.

For a complete zero-call operator evidence bundle:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-operator-evidence.ps1
```

This writes health, readiness, operator next step, latest run, live activation,
call plan, telemetry, telemetry coverage, raw telemetry, Schwab status, and
checked-in PR change ledger evidence under `data\ops\bundles\`. It makes 0
Polygon, SEC, Schwab, or OpenAI calls.

To refresh the checked-in PR/change ledger after a merge:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-pr-ledger.ps1
```

This writes `docs\changes\pr-ledger.json` from GitHub PR metadata. It makes no
Polygon, SEC, Schwab, or OpenAI calls; it does make one GitHub metadata request
through `gh`.

For an up-to-the-minute ignored ledger snapshot after the latest merge, write:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-pr-ledger.ps1 -OutputPath data\ops\bundles\pr-ledger-current.json
```

`scripts/export-operator-evidence.ps1` prefers that ignored current snapshot
when it exists, then falls back to the checked-in ledger.

For a zero-call deployment/readiness gate that exits non-zero until Market Radar
is safe to use for investment decisions:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/assert-investable-readiness.ps1
```

This checks local readiness, live activation, call-plan, and telemetry state. It
makes 0 Polygon, SEC, Schwab, or OpenAI calls and is expected to fail closed
while required live credentials or data-quality gates are missing.

After editing `.env.local`, run the activation checker before making live
provider calls:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/prepare-live-env.ps1
powershell -ExecutionPolicy Bypass -File scripts/open-live-env.ps1
powershell -ExecutionPolicy Bypass -File scripts/check-live-activation.ps1
powershell -ExecutionPolicy Bypass -File scripts/run-first-live-smoke.ps1
powershell -ExecutionPolicy Bypass -File scripts/run-worker-once.ps1
```

The preparation script writes local defaults only. The activation checker reads
the local API activation contract only. The first-live-smoke script also makes
0 external calls unless `-Execute` is supplied. Together they print the missing
values plus the safe next commands before any provider request can happen. The
worker script uses the same plan-first contract before starting a one-shot
worker cycle.

## Phase 1 rule

No premium LLM calls are used or required in Phase 1.

## Agent Review And Agents SDK Boundary

The agent-review loop is not connected to GitHub Copilot. Runtime dependencies
do not include a Copilot SDK, and the source tree has a regression test that
fails if Copilot references are added to application code. Real review mode is
gated behind `CATALYST_ENABLE_PREMIUM_LLM=true`,
`CATALYST_LLM_PROVIDER=openai`, and `OPENAI_API_KEY`, then uses the official
`openai` Python SDK `responses.create(...)` path through
`OpenAIResponsesClient`. Dry-run and fake modes do not call OpenAI.

The separate `agent-brief` command uses the `openai-agents` package for the
new manager-style operator layer. It is disabled by default, requires
`CATALYST_ENABLE_AGENT_SDK=true` plus the same OpenAI premium gates for real
mode, and exposes only specialist agents as tools. It does not grant model
access to market-data providers, Schwab, local files, shell, web browsing, or
order submission. The CLI/API payload and Agent TUI page expose this as
`runtime.orchestrator=openai_agents_sdk`,
`runtime.copilot_dependency=absent`, and zero market-data/broker/shell/web
tool flags so the operator can verify the boundary without reading source.
The brief may recommend `bars saved capture confirm` only as an explicit human
approval step; it does not run the capture or import provider data.

## Verification commands

```powershell
python -m pytest
python -m ruff check src tests apps
```

## Phase 1 acceptance

Phase 1 is accepted when:

- sample ingest works from CSV
- scan produces candidate states
- CCC is blocked for liquidity
- dashboard renders current candidates
- all tests pass
- no LLM configuration is required
