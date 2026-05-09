# Phase 1 Review

Date: 2026-05-09

## What works

- Local CSV securities ingest.
- Local CSV daily bar ingest.
- SQLite local database initialization.
- Deterministic market feature computation.
- Score and policy state assignment.
- Liquidity hard block.
- Candidate dashboard.
- Point-in-time validation helpers.

## Verification

Verified on Windows with the local virtualenv console script:

```powershell
python -m pytest
python -m ruff check src tests apps
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
.\.venv\Scripts\catalyst-radar.exe init-db
.\.venv\Scripts\catalyst-radar.exe ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
.\.venv\Scripts\catalyst-radar.exe scan --as-of 2026-05-08
```

Observed smoke output:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
scanned candidates=3
```

## Current limits

- Data comes from local CSV.
- No SEC/news/text pipeline.
- No local NLP.
- No LLM Decision Cards.
- No broker integration.
- Portfolio holdings ingestion is wired for local CSV snapshots, but portfolio-aware scoring remains a later phase.

## Recommended next phase

Phase 2 should add event and local text intelligence after the deterministic scanner has been reviewed on real daily-bar data.
