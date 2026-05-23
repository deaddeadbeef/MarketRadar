# Catalyst Radar

Catalyst Radar is a deterministic-first market radar for public-equity opportunity review.

The product goal is to scan the broad stock market and highlight stocks where
market emotion or expectations may not be fully matched by price reaction. The
downstream value goal is to prove at least $40/month of attributable
decision-support value, enough to offset 20% of a $200/month ChatGPT Pro
subscription. That value must be measured with evidence such as useful surfaced
opportunities, avoided bad decisions, time saved, and paper/live outcomes; it
is not a profit guarantee or investment advice.

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
For one-off local fixture checks, `run-daily --provider csv` is stricter: it
pins the run to CSV market bars and the bundled news fixture even if your
scheduled environment is configured for Polygon/Massive or SEC.
For live scheduled providers, `run-daily` fails closed unless
`--confirm-external-call` is present. The unconfirmed JSON output reports
`external_calls_planned`, `db_writes_planned`, `external_calls_made=0`, and
`db_writes_made=0` before any provider call or database write.

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
catalyst-radar run-daily --as-of <LATEST_TRADING_DATE> --available-at <UTC-now> --provider polygon --confirm-external-call --json
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
capture approval path, including the expected call count before capture. The
same brief also exposes `recommended_unblock_action`, and CLI / TUI `batch all`
print it before the option list so the source-map view agrees with the trust
gate about the single next safe unblock step. That action keeps the legacy
`command` field as the dashboard/TUI alias, while `cli_command` carries the
copy-pasteable `catalyst-radar ...` command and `tui_command` carries the
short command-box alias. CLI text output prefers `cli_command` and appends
`tui=...` when the terminal-dashboard alias differs; this applies to both the
source-map mission brief and the first `priced-in-answer` trust-gate action /
unblock rows.
Roadmap rows distinguish total gaps from immediately useful work:
`gap_rows` is the broad evidence gap, `plannable_gap_rows` is the part a safe
plan can currently address, `routed_gap_rows` is the already assigned subset
of unplannable rows, and remaining unplannable rows are blocked by missing
prerequisites. The CLI mirrors this as `gaps=`, `plan=`, `routed=`, and
`blocked=` so a large market-wide gap does not look like a single direct action.
JSON source rows also expose `blocked_gap_rows`, the already-derived remaining
blocked count after routed rows are removed, so API/dashboard clients do not
need to recompute it or risk treating routed funds and benchmarks as blockers.
The all-source API payload also includes `source_execution_gate`: when
`market_bars` is still incomplete, source chunks may be planned but
`execute_next_allowed=false`, and the terminal dashboard suppresses "First
executable" wording until the market-bar gate clears.
The terminal dashboard `batch all` response mirrors the same summary on demand,
so it stays fast while still separating total gaps from rows that can be planned
now.
`priced-in-answer` and `GET /api/radar/priced-in/answer` also include
`full_market_trust_gate`, a zero-call yes/no contract for whether the current
full-market priced-in answer is trustworthy yet. The TUI mission rows surface
the same gate so the dashboard starts with the answer, the blocker, and safety.
The gate is a readiness contract only: `blocked` means keep collecting
evidence, not that a trade should be made or skipped. When market bars are
the first blocker, `blocker_detail` includes manual CSV fill counts and saved
provider-file status so the CLI/TUI can show whether the local unblock file
is actually filled yet. Its `unblock_options` list names the zero-call manual
CSV path, the explicit saved-provider capture approval path, and zero-call
saved-file validate/import follow-ups with command, API, request body, call,
and write boundaries. The trust gate also promotes one `recommended_action`
from those options so CLI/API/dashboard clients can show the next safest unblock
step on first view. Dashboard clients should use these option payloads rather
than reconstructing endpoint parameters from CLI text.
If active-universe bars are incomplete, `market_bars` remains the first
full-market blocker even when the latest visible scan was a selected-universe
run; the separate `scan_scope` payload still says that the latest run was
selected and is not the all-active answer. The answer payload includes
`external_calls_made=0` and `db_writes_made=0`.
It also carries a `missing_universe` summary plus
`missing_security_type_counts`, zero-volume counts, and zero-market-cap counts
so the operator can see what kind of active rows are blocking coverage without
using that context to exclude rows or reduce the full scan. `blocker_ladder`
lists the ordered evidence blockers that must be cleared after the first
blocker. When another blocker is already known, `after_current_blocker`
previews the next source, why it matters, and the zero-call plan/API plus
guarded execute-next affordance to use only after the current blocker is
cleared. Its `next_source_plan` is a compact zero-call
source-batch summary for dashboard and CLI clients: total gap rows, plannable
rows, routed non-company rows, blocked rows, missing-CIK samples, next chunk
call count, and CIK repair commands when they apply. The text CLI also prints a
`trust_gate_next_source_unblock` line for the next source, including the CIK
refresh, template, validate, and import commands; the terminal dashboard shows
those same CIK repair commands in the post-bar next-source preview. It shows the
same split on the overview and run pages as a compact next-source plan, so a
human can see what will be scanned after the current blocker. The snapshot keeps
planning rows internal and does not return `planning_rows` in
the display `priced_in_queue` packet.
`blocker_detail.manual_csv` gives dashboard
clients the fillable local CSV context: path, required fields, current complete
/ partial / empty counts, sample missing tickers, and a full manual
`market-bars import` preview command. Saved-file `saved-import` commands stay
under the saved-provider options, not the manual CSV context.
`blocker_detail.saved_provider_capture`
shows the guarded saved Polygon/Massive response path as its own zero-call
contract: current saved-file status, whether a key and explicit approval are
required, active/existing/missing bar counts for the capture target, call/write
counts, missing ticker sample, missing security-type counts, zero-call
missing-universe diagnostics, capture API request bodies, and the post-capture
validate/import steps. Confirmed saved capture also carries an approval guard:
the reviewed active, existing-bar, and missing-bar counts must still match the
local database before the one provider call can run. For stock-like scopes, the
capture request body and generated confirm command preserve `stocks_only=true`
and `--stocks-only`, so the guard checks the stock-like gap instead of falling
back to the full active universe.
`catalyst-radar market-bars status` and
`GET /api/radar/market-bars/status` also return `unblock_checklist`, a
zero-call checklist for the exact market-bar path: review counts, explicitly
approve a saved provider-file capture when desired, validate the saved file,
preview the import, execute the import, then rerun the priced-in answer. The
text CLI prints the next checklist step with its call/write count so the
operator can distinguish action from response before spending a provider call.
The same answer payload includes `reviewable_subset`, a zero-call count and
sample of scanned-subset leads that can be inspected as research-only while the
full-market trust gate remains blocked.
The full-scan accounting also distinguishes raw unscanned rows from rows that
actually block the answer. `full_scan.unscanned_rows` is the active universe
minus scored rows. `full_scan.unscanned_blocker_rows` subtracts intentional
benchmark-reference exclusions, currently `SPY`, `XLK`, and `XLI`, so those
relative-strength helper ETFs do not keep the trust gate blocked after all real
evidence gaps clear. The CLI mirrors this as `unscanned=`,
`unscanned_blockers=`, and `excluded=`.
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
calls. When market bars are the current trusted-answer blocker, per-source
plans include `current_blocker_gate=status=blocked`, suppress source execution
commands, and the TUI says the source is review-only until the price-reaction
gate clears. The `--execute-next` command executes only the next planned chunk
when that gate is clear. The API equivalents are
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
dashboard just did. Type `now` or `what-now` for the single priced-in action,
expected response, approval requirement, provider-call count, and database-write
count. Use `ticker <SYMBOL|all>` and `available-at <ISO|latest>`
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
plan, including full-scan gap counts and an `Inspect` command per source, so
the next source to inspect is visible without remembering long CLI/API commands.
CLI/API automation can use
`priced-in-source-batches --source all`,
`priced-in-source-batches --source <source> --execute-next` or
`POST /api/radar/priced-in/source-batches/execute-next` for the same one-chunk
operation. Source-batch JSON rows expose `command` and `plan_command` as the
safe default planning/review action for that row; provider or DB-writing work
stays behind explicit `execute_next_command`, `execute_batches_command`, or
manual import execute commands. Dashboard coverage-first and decision-shortcut
recommendations use the same safe plan alias, not the execute command, as
their primary displayed command. If a source execution is attempted while the
current price-reaction gate is still blocked, the run payload exposes
`execution_blocker`, keeps `external_calls_made=0`, and points `next_command`
back to the blocker repair command instead of suggesting another source
execution retry.
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
the active/existing/missing bar counts for the target date, missing ticker
sample, security-type counts, missing-universe diagnostic, the exact external
call count if approved, and the zero-call question to review before running
`bars saved capture confirm`. The generated confirm command and API body include
the reviewed counts and scope, including `--stocks-only` / `stocks_only=true`
when the plan is stock-like; stale or missing guard values block capture with
`external_calls_made=0` and `db_writes_made=0`.

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
When the active universe is missing, `priced-in-answer`, `agent-brief`, and
`market-bars status` now point at the universe setup step first instead of
showing market-bar template work that cannot know which securities to repair.
That setup step is provider-aware: CSV mode prints the exact `ingest-csv`
command with configured file paths, while Polygon/Massive mode prints the
guarded `ingest-polygon tickers --max-pages <cap> --confirm-external-call`
command plus the `POST /api/radar/universe/seed` request body. The API seed
body must include `confirm_external_call=true`; the zero-call preflight/answer
payloads show the planned Polygon/Massive page cap and the one local DB import
operation before any seed can run.
`priced-in-answer` and `GET /api/radar/priced-in/answer` answer the narrower
question "Has price fully matched market expectations?" Their
`decision_ready=true` / `priced_in_answer_ready=true` fields mean the
emotion-vs-reaction answer is ready for human review. They deliberately keep
`can_make_investment_decision=false`; trade safety still comes only from the
separate readiness/manual-buy-review gate. The same payload now includes
`operator_next_step`, a single zero-call action card with trust status, first
blocker, next command, TUI alias, approval flag, provider-call count,
database-write count, and expected response after the action.

`agent-brief` is the CLI surface for the OpenAI Agents SDK operator layer. By
default it runs a deterministic dry-run brief from the same redacted dashboard
snapshot, with four roles: Data Sentinel, Catalyst Analyst, Risk Officer, and
Operator. It makes 0 Polygon, SEC, Schwab, or OpenAI calls in default mode.
The JSON payload and the TUI Agent page include a `runtime` block that names
`openai_agents_sdk`, marks `copilot_dependency=absent`, and shows that market,
broker, shell, filesystem, and web tools are unavailable to the agent layer.
When market bars block the priced-in answer, the dry-run brief also summarizes
the same redacted unblock options visible in the dashboard: manual CSV, saved
provider capture approval, and saved-file validate/import follow-ups. It also
lifts the trust-gate `recommended_action` into
`priced_in.recommended_unblock_action` and leads `insights` / `next_actions`
with that single next step before listing alternatives. While that current
blocker action exists, lower-priority readiness and work-queue suggestions are
suppressed so the Agent page does not present competing next actions. Those
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
For a quick operator checkpoint, type `bars` or `bars status`; it prints
the current missing-bar count, stock-like sub-scope gap, manual CSV progress,
saved-capture boundary, and the single recommended next unblock action with
provider-call and DB-write boundaries. If no active universe exists yet, it
returns `status=setup_required` / `first_blocker=universe` and sends the
operator to the exact CSV or Polygon/Massive universe seed command instead of
offering unusable bar repair. The readiness checklist also surfaces a separate
`Scan universe` row, so a configured live provider cannot hide the missing
local universe.
The same zero-call checkpoint is available outside the TUI as
`catalyst-radar market-bars status` and
`GET /api/radar/market-bars/status`; when `expected_as_of` is omitted, these
read-only status paths use the latest stored daily-bar date and report
`expected_as_of_source=latest_daily_bar`. If the active universe exists but no
daily bars are stored yet, they keep `first_blocker=market_bars` and ask for
`--expected-as-of YYYY-MM-DD` / `?expected_as_of=YYYY-MM-DD` instead of sending
the operator back to universe setup. Pass `--expected-as-of YYYY-MM-DD`
or `?expected_as_of=YYYY-MM-DD` to pin the check. Those CLI/API payloads include
the missing ticker sample, missing security-type counts, zero-call missing
universe diagnostics, `stock_scope` for the stock-like gap inside the same
all-instrument status, `configured_universe_scope` for the latest configured
universe snapshot, `recommended_action`, and `after_market_bars_clear`.
Configured-universe coverage is separate from the all-active market-bar gate:
it can show that a built investable universe such as `liquid-us` has complete
bars while the full active universe remains blocked.
`recommended_action` is the UI-friendly contract for choosing the next safe
button or command; `after_market_bars_clear` previews the next priced-in source
blocker to inspect after the bar gap is repaired. Both fields report zero calls
while planning so a dashboard can separate the current action from the next
response. The TUI `bars status` response mirrors the same missing-ticker sample,
zero-call unblock checklist,
stock-like gap, non-stock remainder, and `after_market_bars_clear` preview, so
the dashboard explains both the current blocker and the next source to inspect
after the blocker clears.
If a saved grouped-daily file exists but covers none of the remaining missing
rows, and the residual set looks like zero-liquidity universe-quality data,
`recommended_action.kind` becomes `residual_universe_review`. Use the zero-call
review command before filling bars or changing scan scope:

```powershell
catalyst-radar market-bars residual-review --expected-as-of 2026-05-15 --json
catalyst-radar market-bars residual-review --expected-as-of 2026-05-15 --stocks-only --json
```

The matching API route is `GET /api/radar/market-bars/residual-review`.
Residual review does not clear the market-bar gate. It reports stock-like versus
non-stock residual counts, security-type counts, saved-file projection,
zero-volume/zero-market-cap evidence, manual repair commands, and explicit
decision options while keeping `external_calls_made=0` and `db_writes_made=0`.
When `priced-in-answer` reaches this residual state, its top-level
`next_action` / `next_command`, `full_market_trust_gate`, and
`operator_next_step` all point at the same residual-review action. The same
residual-review command is also propagated into the first market-bar entries in
`decision_readiness`, `evidence_completeness`, `trust_blockers`, and
`blocker_ladder`, so clients do not mix it with the older manual-bar template
path. The terminal dashboard mission brief uses the same operator step for its
`Useful next` row instead of falling back to stale audit-blocker text.
If the review shows strict zero-liquidity/no-history rows that should not remain
active, use `market-bars residual-repair` as a guarded local-universe repair.
It previews by default with 0 provider calls and 0 DB writes. Execution requires
`--execute` plus the exact missing and eligible counts from the reviewed preview;
if those counts changed, it writes nothing and returns `status=stale_approval`.

```powershell
catalyst-radar market-bars residual-repair --expected-as-of 2026-05-15 --json
catalyst-radar market-bars residual-repair --expected-as-of 2026-05-15 --expect-missing-count <MISSING> --expect-eligible-count <ELIGIBLE> --execute --json
```

The matching API route is `POST /api/radar/market-bars/residual-repair`.
Preview request bodies use `{"expected_as_of":"YYYY-MM-DD","execute":false}`.
Execute request bodies must include `execute=true`, `expected_missing_count`, and
`expected_eligible_count`. This path only sets qualifying local
`securities.is_active=false` rows with a repair marker; it never fills bars,
calls Polygon/Massive, SEC, Schwab, OpenAI, web providers, or submits orders.
For manual zero-call repair, `bars manual template` generates the full
active-universe missing-bar CSV by default, `bars manual import` previews
complete rows only with 0 provider calls and 0 DB writes, and
`bars manual import execute` writes only completed rows into the local database.
Use `bars manual stocks template` when you intentionally want the narrower
stock-like scope instead of the full active universe. For saved-provider repair, the dashboard prefers the saved-file capture path over
direct live grouped-daily ingest because capture makes the one provider call and 0
DB writes before validation/import review.
`bars saved capture` shows the approval boundary plus target scope, active,
existing, and missing scan-date bar counts, missing ticker sample,
security-type counts, missing-universe diagnostic,
the saved-capture approval packet, and the zero-call validate, preview-import,
and explicit execute-import commands/request bodies without making provider calls;
`bars saved capture confirm` is the explicit one-call Polygon/Massive capture
but first rechecks the approval guard against the local DB. If active,
existing, or missing counts changed since review, it stops before the provider
call and asks you to review `bars saved capture` again. Stock-like saved
capture, saved validation, and saved import keep `--stocks-only` through
CLI/API/TUI commands and request bodies, so the reviewed guard and post-import
verification do not silently switch back to full active-universe counts. When
the guard matches, it immediately prints a zero-call post-capture preview of
whether the saved file covers current missing bars; `bars saved validate` checks
the saved grouped-daily JSON from disk when you want to re-check it later;
`bars saved import` previews the local import with 0 DB writes; and
`bars saved import execute` writes that saved file into the local database with
0 provider calls after review.

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

For the saved-provider repair path, use the high-level `market-bars saved-*`
commands. The first command is plan-only and makes 0 provider calls; it reports
coverage scope plus active, existing, and missing scan-date bar counts so the
approved capture target is visible before any provider call. Use the generated
confirm command from the plan output; it includes the reviewed count guards and
preserves `--stocks-only` when the plan is scoped to stock-like bars.
Add `--confirm-external-call` only when you intentionally approve the one
Polygon/Massive grouped-daily capture. Validation and import read the saved
JSON from disk and make 0 provider calls:

```powershell
catalyst-radar market-bars saved-capture --expected-as-of 2026-05-15 --json
catalyst-radar market-bars saved-capture --expected-as-of 2026-05-15 --stocks-only --json
catalyst-radar market-bars saved-capture --expected-as-of 2026-05-15 --out data/local/polygon-grouped-daily-2026-05-15.json --expect-active-count <ACTIVE> --expect-existing-count <EXISTING> --expect-missing-count <MISSING> --confirm-external-call
catalyst-radar market-bars saved-validate --expected-as-of 2026-05-15 --fixture data/local/polygon-grouped-daily-2026-05-15.json
catalyst-radar market-bars saved-import --expected-as-of 2026-05-15 --fixture data/local/polygon-grouped-daily-2026-05-15.json
catalyst-radar market-bars saved-import --expected-as-of 2026-05-15 --fixture data/local/polygon-grouped-daily-2026-05-15.json --execute
catalyst-radar market-bars saved-validate --expected-as-of 2026-05-15 --fixture data/local/polygon-grouped-daily-2026-05-15.json --stocks-only
catalyst-radar market-bars saved-import --expected-as-of 2026-05-15 --fixture data/local/polygon-grouped-daily-2026-05-15.json --stocks-only
```

The lower-level `ingest-polygon grouped-daily --save-response` and fixture
commands still exist for diagnostics, but dashboard, API, TUI, and CLI
operator flows should prefer the `market-bars saved-*` wrappers because they
expose the same approval, validate, and import contract everywhere.

```powershell
catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --save-response data/local/polygon-grouped-daily-2026-05-15.json --confirm-external-call
```

The stock-like saved-file path is a narrower staged unblock, not the trusted
full-market answer. Use `--stocks-only` only when the reviewed plan is scoped to
stock-like bars; the verifier then uses stock-like missing counts. The default
full-market path omits the flag and remains the gate for trusted broad-scan
answers.

Saved-capture responses include `post_capture_verification`, using the same
zero-call projection as import previews. After a capture writes the provider
response to disk, CLI/API/TUI surfaces immediately report whether importing that
saved file would clear the current market-bar blocker or still leave missing
rows. The capture itself still makes 0 database changes; it only projects the
later saved import.

Every manual CSV import and saved-file import response includes
`post_import_verification`. That payload is zero-call and reports whether the
operation was preview-only, whether `market_bars` is still blocking the trusted
priced-in answer, or whether the market-bar blocker cleared. Preview responses
also include `projected_missing_after_import_count`, `preview_projection_status`,
and booleans such as `preview_would_clear_market_bars`, so the operator can see
whether executing the reviewed import would clear the blocker before making a
database change. The verifier also reports the current scan-date bar gap, the
next blocker when known, a rerun command, `external_calls_made=0`, and
`db_changes_made`. Treat an import as operationally complete only after this
verifier says `market_bars_cleared`; if it says `market_bars_still_blocked` or
`would_still_block_market_bars`, fill the remaining rows before source chunks.

The template command writes a local ignored CSV scaffold for missing active
tickers in the live database. Missing-only rows are sorted with stock-like
instruments first, then unknown types, then fund/wrapper rows. Fill `open`,
`high`, `low`, `close`, `volume`, and `vwap`; preview reports missing or
invalid bar fields and validates active-ticker coverage before any import. Invalid
or empty manual rows do not count toward `coverage_after_import_count`; use
`--complete-rows-only` when you intentionally want to preview/import only the
completed rows while leaving blank rows for later.
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

For the zero-call battle-test release gate that exits non-zero until Market
Radar has enough evidence for a future limited-capital manual review:

```powershell
catalyst-radar assert-investable-readiness --json
```

The same payload is available at `GET /api/radar/investable/readiness`; the
PowerShell wrapper below calls that endpoint:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/assert-investable-readiness.ps1
```

This gate is stricter than shadow readiness. It requires 30 valid full-scan
shadow days, complete market-bar coverage, forward outcomes, baseline
comparisons, precision@5/10, false-positive feedback, cost per useful alert,
monthly value evidence, balanced Decision Cards, paper-trading logs, disabled
broker order submission, and disabled real LLM modes. It makes 0 Polygon, SEC,
Schwab, OpenAI, broker, order, or web calls and 0 database writes. Passing this
gate still means decision support only; the highest allowed product state is
`EligibleForManualBuyReview`, not autonomous trading or investment advice.

For the daily shadow-mode gate, use:

```powershell
catalyst-radar assert-shadow-ready --json
```

The same payload is available at `GET /api/radar/shadow/readiness` and in the
terminal dashboard Readiness page. It checks active universe, latest market-bar
coverage, full-scan trust gate, candidate-state pipeline, candidate packets,
Decision Cards, provider-call boundaries, alert dry-run state, broker-order kill
switch, value-ledger/outcome schema availability, disabled real LLM mode, and
validation readiness. The
command itself makes 0 provider calls and 0 database writes; it prints those
counters plus the planned run's maximum external-call count. When the first
blocker exposes a zero-call review command, the readiness next action uses that
command instead of only the prose explanation. A selected-universe or partial
scan is reported as `partial_only` only after the active universe and market-bar
setup checks are otherwise clear; empty universe or missing market bars still
return `setup_required`. Config-only safety blockers such as enabled broker
order submission return `not_configured`.

To persist a daily shadow-mode audit row from local data only:

```powershell
catalyst-radar shadow-mode status --json
catalyst-radar shadow-mode run --available-at <UTC-cutoff> --preview --json
catalyst-radar shadow-mode run --available-at <UTC-cutoff> --execute --json
catalyst-radar shadow-mode latest --json
```

`GET /api/radar/shadow/status`, `POST /api/radar/shadow/runs`, and
`GET /api/radar/shadow/runs/latest` expose the same contract. Preview makes
0 provider calls and 0 database writes. Execute makes 0 provider calls and writes
one `shadow_mode_runs` audit row that classifies the current local scan as
`valid_full_scan`, `valid_selected_universe_scan`, `partial_scan`,
`blocked_scan`, or `setup_required`. This is a shadow evidence record, not an
order, alert-delivery, or live-provider command.

To track whether Market Radar is paying for itself, record local value evidence
with the value ledger. The default `add` command is a preview and writes nothing;
add `--execute` only after the displayed entry is correct:

```powershell
catalyst-radar value-ledger record --artifact-type candidate_state --artifact-id <ID> --label good-research --supported-action research --user-decision accepted --estimated-value-usd 10 --confidence 0.5 --notes "screened faster" --json
catalyst-radar value-ledger record --artifact-type candidate_state --artifact-id <ID> --label good-research --supported-action research --user-decision accepted --estimated-value-usd 10 --confidence 0.5 --execute --json
catalyst-radar value-ledger label --artifact-type candidate_state --artifact-id <ID> --label useful --supported-action research --user-decision accepted --estimated-value-usd 10 --confidence 0.5 --execute --json
catalyst-radar value-ledger show <VALUE_LEDGER_ID> --json
catalyst-radar value-ledger summary --json
```

`GET /api/value-ledger/summary`, `GET /api/value-ledger/entries`,
`GET /api/value-ledger/entries/{id}`, and `POST /api/value-ledger/entries`
expose the same local ledger contract. These endpoints make 0 provider calls.
The POST endpoint defaults to preview mode and requires `execute=true` before it
writes one ledger row. Known artifact types such as `candidate_state`,
`candidate_packet`, `decision_card`, `paper_trade`, and `alert` must reference
an existing local artifact. Ledger feedback labels are validated against the
first supported set: `useful`, `noisy`, `acted`, `ignored`, `avoided-loss`,
`missed`, `false-positive`, `false-negative`, `too-late`, `too-early`,
`good-research`, `duplicate`,
`not-understandable`, and `blocked-correctly`. The terminal dashboard Costs
page shows weighted value, the $40/month target, and the percent offset against
the $200/month ChatGPT Pro cost.

Forward outcomes are computed from already stored point-in-time daily bars:

```powershell
catalyst-radar value-outcome update --ledger-id <VALUE_LEDGER_ID> --outcome-available-at <UTC-outcome-cutoff> --json
catalyst-radar value-outcome update --ledger-id <VALUE_LEDGER_ID> --outcome-available-at <UTC-outcome-cutoff> --execute --json
catalyst-radar value-outcome list --ledger-id <VALUE_LEDGER_ID> --json
catalyst-radar value-outcome show <VALUE_OUTCOME_ID> --json
```

`POST /api/value-outcomes/update`, `GET /api/value-outcomes`, and
`GET /api/value-outcomes/{id}` expose the same contract. Outcome preview makes 0
provider calls and 0 database writes; execute makes 0 provider calls and writes
only a `value_outcomes` row. The update never mutates the source value-ledger
row, candidate state, candidate packet, decision card, score, or policy output.
When fewer than 60 future trading bars are visible at the supplied cutoff, the
outcome row is marked `insufficient_data` and only available horizons are
populated. The outcome payload includes `expected_review_horizon_days` and
`expected_review_horizon_expired` so clients can distinguish a fully elapsed
review window from a still-pending one without inferring that from return
fields.

To answer the monthly value-proof question, generate a read-only value report:

```powershell
catalyst-radar value-report --month YYYY-MM
catalyst-radar value-report --month YYYY-MM --json
```

`GET /api/value-report/monthly?month=YYYY-MM` exposes the same report. It reads
local value-ledger and outcome rows only, makes 0 provider/broker/model calls,
and writes 0 database rows. The report returns `pass`, `fail`, or
`insufficient_evidence`, states whether the $40/month decision-support threshold
was plausibly met, includes uncertainty and false positives, and separates
decision-support value from realized profit or investment advice. The terminal
dashboard Costs page shows the current month's verdict and net
decision-support value. If any ledger row involved LLM review, the report also
shows LLM-reviewed entry count, useful LLM-reviewed entry count, LLM-linked
cost, and cost per useful LLM-reviewed candidate without making model calls.

To compare MarketRadar against simple deterministic baselines before tuning
scores or adding more intelligence, run a point-in-time validation replay and
then inspect the report:

```powershell
catalyst-radar validation-replay --as-of-start YYYY-MM-DD --as-of-end YYYY-MM-DD --available-at <UTC-decision-cutoff> --outcome-available-at <UTC-outcome-cutoff>
catalyst-radar validation-report --run-id <printed-run-id> --json
```

The report compares MarketRadar candidates with relative strength, volume
breakout, sector ETF rotation, news/event-only, and random sector-matched
baselines. Baseline rows are selected using only data available at the decision
cutoff and are labeled only from bars visible at the outcome cutoff. The report
shows precision@5/10, false-positive rate, excursion averages, overlap/missed
tickers, and whether MarketRadar won, lost, tied, or lacks enough evidence.
The same report includes score-calibration buckets for `50_59`, `60_69`,
`70_79`, `80_89`, and `90_plus`, plus score distribution groups for sector,
market regime, setup type, priced-in status, action state, source coverage, and
feedback label. These buckets and groups are evidence only and do not change
scoring weights, policy thresholds, trade plans, or action gates.
It also reports local text measurement for the existing narrative, novelty,
source quality, sentiment, theme match, theme velocity, and theme-hit signals.
Those measurements are evidence only; they do not replace the local text model
or change scoring until validation evidence justifies a separate policy change.

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
