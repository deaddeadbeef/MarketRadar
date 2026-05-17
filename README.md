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
radar --once --page overview
```

The alias calls `scripts/run-dashboard-tui.ps1`. That script keeps setup local
to this repo: it creates `.venv` if needed, installs the editable
`catalyst-radar` command when `pyproject.toml` changes, fast-forwards clean
`main` to `origin/main`, and then starts the TUI. It does not set `PYTHONPATH`
or mutate the caller's shell environment. Use `radar --no-update` to skip the
Git update step and `radar --force-install` to refresh the editable install.

The TUI is the operational replacement surface for the web dashboard. It loads
the same command-center data helpers and provides pages for overview,
readiness, run/call-plan, candidates, alerts, IPO/S-1, broker, ops, telemetry,
themes, validation, costs, and current feature inventory. Inside the TUI, use
the top tabs, the left sidebar, page numbers, or `Ctrl+N` / `Ctrl+P` to
navigate. Use `Tab` / `Shift+Tab` to move focus and `Enter` to activate a
focused nav/action row. Candidate and alert rows are mouse-selectable in
Windows Terminal. The legend explains active-page markers, row counts, colors,
and the zero-provider-call navigation contract. The `ACTION` strip shows what
the operator can do next; the `RESPONSE` strip shows what the dashboard just
did. Use `ticker <SYMBOL|all>` and `available-at <ISO|latest>` to filter,
`json` to print the redacted machine-readable snapshot, `refresh` to reload the
local database, and `q` to quit. It makes 0 Polygon, SEC, Schwab, or OpenAI
calls while rendering, clicking, filtering, or navigating. From the run page,
`run` explains the guarded execution path and `run execute` starts one capped
scheduler cycle after the call plan is visible.
The broker page also supports local operator writes that do not submit real
orders: `action <ticker> <watch|ready|simulate_entry|dismiss> [notes]`,
`trigger <ticker> <type> <op> <threshold> [notes]`, `eval-triggers [ticker]`,
`ticket <ticker> <buy|sell> <entry> <stop> [risk_pct] [notes]`, and
`feedback <alert-id|#> <label> [notes]`.

For a zero-call local sitrep:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/market-radar-status.ps1
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

For functional end-to-end tests of the same command-center data the dashboard
renders:

```powershell
catalyst-radar dashboard-snapshot --json
catalyst-radar dashboard-snapshot --ticker ACME --available-at 2026-05-10T21:06:00Z
```

The snapshot uses the dashboard data helpers for readiness, latest run,
candidate rows, alerts, IPO/S-1 rows, themes, validation, costs, broker context,
ops health, telemetry, telemetry coverage, live activation, and call planning.
It is read-only, redacts restricted provider payloads, and makes 0 Polygon, SEC,
Schwab, or OpenAI calls.

See `docs/dashboard-feature-inventory.md` for the current dashboard feature
inventory and TUI coverage.

If the sitrep reports stale CSV market bars, import a manually prepared daily
bar CSV with the same schema as `data/sample/daily_bars.csv`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/refresh-csv-market-data.ps1 -TemplateOut data/local/manual-bars-2026-05-16.csv -ExpectedAsOf 2026-05-16
powershell -ExecutionPolicy Bypass -File scripts/refresh-csv-market-data.ps1 -DailyBars <fresh-bars.csv> -ExpectedAsOf 2026-05-16
powershell -ExecutionPolicy Bypass -File scripts/refresh-csv-market-data.ps1 -DailyBars <fresh-bars.csv> -ExpectedAsOf 2026-05-16 -Execute
```

The template command writes a local ignored CSV scaffold for active tickers.
Fill `open`, `high`, `low`, `close`, `volume`, and `vwap`, then preview with
the second command. Preview reports all missing or invalid bar fields it finds
and validates active-ticker coverage before any import. The `-Execute` command
wraps the existing local `ingest-csv` path, records provider health, and makes
0 Polygon, SEC, Schwab, or OpenAI calls. After importing, rerun
`scripts/market-radar-status.ps1`, then use the plan-only smoke before any
capped live radar cycle.

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

## Agent Review Provider Boundary

The agent-review loop is not connected to GitHub Copilot. Runtime dependencies
do not include a Copilot SDK, and the source tree has a regression test that
fails if Copilot references are added to application code. Real review mode is
gated behind `CATALYST_ENABLE_PREMIUM_LLM=true`,
`CATALYST_LLM_PROVIDER=openai`, and `OPENAI_API_KEY`, then uses the official
`openai` Python SDK `responses.create(...)` path through
`OpenAIResponsesClient`. Dry-run and fake modes do not call OpenAI.

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
