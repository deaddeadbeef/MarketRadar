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
For the first capped live market run, use the daily provider contract:
`CATALYST_DAILY_MARKET_PROVIDER=polygon`, `CATALYST_DAILY_PROVIDER=polygon`,
and `CATALYST_POLYGON_API_KEY`. For live SEC catalyst discovery, use
`CATALYST_DAILY_EVENT_PROVIDER=sec`, `CATALYST_SEC_ENABLE_LIVE=1`, and a
compliant `CATALYST_SEC_USER_AGENT`. Keep provider keys unset or blank unless
you are intentionally running a capped live provider call.
`CATALYST_DAILY_MARKET_PROVIDER` controls scheduled daily bar ingest; the
`CATALYST_DAILY_PROVIDER` override keeps manual/default radar runs aligned with
that scheduled provider.

Before any live provider call, run the activation checker and inspect the
call plan:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/prepare-live-env.ps1
powershell -ExecutionPolicy Bypass -File scripts/check-live-activation.ps1
powershell -ExecutionPolicy Bypass -File scripts/run-first-live-smoke.ps1
powershell -ExecutionPolicy Bypass -File scripts/run-worker-once.ps1
curl.exe --insecure --fail --silent --show-error --request POST https://127.0.0.1:8443/api/radar/runs/call-plan --header "Content-Type: application/json" --data '{}'
```

`scripts/prepare-live-env.ps1` only writes safe non-secret local defaults such
as `CATALYST_DAILY_MARKET_PROVIDER=polygon`, low provider caps, disabled order
submission, and dry-run LLM/alert settings. It makes 0 external calls, does not
print secrets, and still requires you to fill `CATALYST_POLYGON_API_KEY` and
`CATALYST_SEC_USER_AGENT` manually.
`scripts/run-first-live-smoke.ps1` defaults to plan-only mode: it reads local
API readiness and call-plan state, makes 0 external calls, and requires
`-Execute` before seeding one Polygon universe page and running one capped radar
cycle.
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

For a zero-call local sitrep:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/market-radar-status.ps1
```

This reads local API health, readiness, latest run, live activation, and recent
telemetry. It makes 0 Polygon, SEC, Schwab, or OpenAI calls.

For a redacted raw telemetry evidence snapshot:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-telemetry.ps1
```

This reads `GET /api/ops/telemetry/raw`, writes a JSON export under
`data\ops\telemetry\`, and makes 0 Polygon, SEC, Schwab, or OpenAI calls.

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
