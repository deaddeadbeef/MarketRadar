# Phase 3 Review

## Provider Decision

Polygon is the first live-data adapter for this phase because its grouped-daily endpoint can fetch all U.S. daily OHLCV bars for one date in one request. EODHD remains the preferred low-cost fallback to evaluate next, especially for historical EOD and bulk exchange downloads.

## Implemented Capabilities

- Provider config and credential guardrails.
- Fixture-testable HTTP transport seam.
- Polygon grouped daily and ticker/reference connector.
- Point-in-time universe filters and snapshot builder.
- Full-universe read-path indexes.

## Known Provider Limitations

- Polygon grouped bars are split-adjusted by default, not total-return bars.
- Polygon ticker/reference payloads may not include sector, industry, market cap, or options coverage, so missing metadata must remain visible to universe filters.
- Provider raw payloads are retained under the stored `retention_policy` value and are intended for local audit/replay only. Before any commercial use or redistribution, provider license terms must be reviewed and retention policy enforcement must be implemented as a scheduled cleanup.

## Verification

In progress.

## Real-World Testing Preconditions

- Set `CATALYST_POLYGON_API_KEY` in local environment.
- Confirm provider terms allow local raw-payload retention for audit and replay.
- Run fixture smoke before any live request.
