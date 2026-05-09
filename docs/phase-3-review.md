# Phase 3 Review

## Provider Decision

Polygon is the first live-data adapter for this phase because its grouped-daily endpoint can fetch all U.S. daily OHLCV bars for one date in one request. EODHD remains the preferred low-cost fallback to evaluate next, especially for historical EOD and bulk exchange downloads.

Provider sources checked on 2026-05-10:

- Polygon pricing: `https://polygon.io/pricing`
- Polygon Grouped Daily endpoint: `https://polygon.io/docs/stocks/get_v2_aggs_grouped_locale_us_market_stocks__date`
- Polygon stocks overview: `https://polygon.io/docs/stocks/getting-started`
- EODHD historical EOD docs: `https://eodhd.com/financial-apis/api-for-historical-data-and-volumes`
- EODHD bulk docs: `https://eodhd.com/knowledgebase/bulk-download-api/`
- EODHD pricing: `https://eodhd.com/pricing`
- Tiingo EOD ingestion KB: `https://www.tiingo.com/kb/article/the-fastest-method-to-ingest-tiingo-end-of-day-stock-api-data/`
- Alpha Vantage docs: `https://www.alphavantage.co/documentation/`
- Alpha Vantage premium page: `https://www.alphavantage.co/premium/`

## Implemented Capabilities

- Provider config and credential guardrails.
- Fixture-testable HTTP transport seam.
- Polygon grouped daily and ticker/reference connector.
- Shared provider ingest orchestration for CSV and Polygon.
- Point-in-time universe filters and snapshot builder.
- `ingest-polygon tickers --date`, `build-universe`, `scan --universe`, and `--available-at` replay controls.
- Deterministic no-network Polygon fixture golden scan.
- Provider-specific scan filtering so CSV and Polygon bars cannot mix inside a named-universe scan.
- Full-universe read-path indexes.

## Known Provider Limitations

- Polygon grouped bars are split-adjusted by default, not total-return bars.
- Polygon ticker/reference payloads may not include sector, industry, market cap, or options coverage, so missing metadata must remain visible to universe filters.
- Provider raw payloads are retained under the stored `retention_policy` value and are intended for local audit/replay only. Before any commercial use or redistribution, provider license terms must be reviewed and retention policy enforcement must be implemented as a scheduled cleanup.

## Verification

```text
python -m pytest
124 passed in 12.35s
```

```text
python -m ruff check src tests apps
All checks passed!
```

CSV smoke:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
provider=csv status=healthy
scanned candidates=3
```

Polygon fixture smoke:

```text
initialized database
ingested provider=polygon raw=4 normalized=4 securities=4 daily_bars=0 rejected=0
ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 rejected=0
ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 rejected=1
provider=polygon status=degraded
built universe=liquid-us members=2 excluded=1
scanned candidates=2
```

Live Polygon smoke skipped: `CATALYST_POLYGON_API_KEY` is not set.

## Real-World Testing Preconditions

- Set `CATALYST_POLYGON_API_KEY` in local environment.
- Confirm provider terms allow local raw-payload retention for audit and replay.
- Run fixture smoke before any live request.
