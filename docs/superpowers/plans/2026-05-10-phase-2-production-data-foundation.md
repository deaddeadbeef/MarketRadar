# Phase 2 Production Data Foundation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Keep this phase on a feature branch or worktree, not directly on `main`.

**Goal:** Turn the Phase 1 CSV-only deterministic MVP into a provider-ready data foundation. This phase must add connector contracts, raw provider storage, normalized record storage/replay, provider health, job runs, data-quality incidents, and universe snapshots without changing the product boundary: no paid provider commitment, no LLM, no alerts, no trade execution.

**Architecture:** Preserve the Phase 1 deterministic scanner. Add a new data-ingestion layer beside the existing CSV loaders, then route CSV through the same provider-style interface as a dry-run adapter. Store raw payload evidence separately from normalized domain rows. Every provider-derived row must keep `source_ts` and `available_at`; missing availability fails closed.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite-compatible schema for local development with PostgreSQL-compatible migration SQL, pandas for current CSV fixture parsing, pytest, ruff.

---

## Starting Point

Current branch baseline:

```text
main @ 8b37402 docs: record phase 1 integration
```

Verified Phase 1 evidence:

```powershell
python -m pytest
python -m ruff check src tests apps
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

Expected smoke output:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
scanned candidates=3
```

## Non-Goals

- Do not integrate a paid market data provider yet.
- Do not make outbound network calls.
- Do not add OpenAI or other LLM usage.
- Do not implement SEC/news/event ingestion.
- Do not implement alerting.
- Do not implement broker/trading features.
- Do not replace the Streamlit dashboard except for optional provider-health read models if needed for tests.

## Phase Exit Criteria

- Local CSV ingest works through a provider-style dry-run adapter.
- Existing `ingest-csv` command remains backward-compatible.
- Raw provider payloads are persisted separately from normalized rows.
- Raw payload replay can rebuild normalized securities and daily bars.
- Provider health records are written for successful and degraded runs.
- Job runs record command type, provider, status, timestamps, counters, and error summary.
- Data-quality incidents record severity, affected tickers, reason, and fail-closed action.
- Universe snapshots and members can be persisted and read back.
- Missing `source_ts` or `available_at` is rejected by connector/domain validation before scan promotion.
- Existing Phase 1 tests still pass.

## Task 1: Connector Contract Dataclasses and Protocol

**Objective:** Add provider-neutral request, record, health, and cost primitives.

**Files:**

- Modify: `src/catalyst_radar/core/models.py`
- Create: `src/catalyst_radar/connectors/base.py`
- Modify: `src/catalyst_radar/connectors/__init__.py`
- Create: `tests/unit/test_connector_contracts.py`

**Implementation details:**

- Define `ConnectorRecordKind` as a `StrEnum` with at least:
  - `SECURITY`
  - `DAILY_BAR`
  - `HOLDING`
  - `UNIVERSE_MEMBER`
- Define `ConnectorHealthStatus` as a `StrEnum` with:
  - `HEALTHY`
  - `DEGRADED`
  - `DOWN`
- Define core enums in `src/catalyst_radar/core/models.py`:
  - `DataQualitySeverity`: `INFO`, `WARNING`, `ERROR`, `CRITICAL`
  - `JobStatus`: `RUNNING`, `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`
- Define frozen dataclasses:
  - `ConnectorRequest`
  - `RawRecord`
  - `NormalizedRecord`
  - `ConnectorHealth`
  - `ProviderCostEstimate`
- `ConnectorRequest` fields:
  - `provider: str`
  - `endpoint: str`
  - `params: Mapping[str, Any]`
  - `requested_at: datetime`
  - `idempotency_key: str | None = None`
- `RawRecord` fields:
  - `provider: str`
  - `kind: ConnectorRecordKind`
  - `request_hash: str`
  - `payload_hash: str`
  - `payload: Mapping[str, Any]`
  - `source_ts: datetime`
  - `fetched_at: datetime`
  - `available_at: datetime`
  - `license_tag: str`
  - `retention_policy: str`
- `NormalizedRecord` fields:
  - `provider: str`
  - `kind: ConnectorRecordKind`
  - `identity: str`
  - `payload: Mapping[str, Any]`
  - `source_ts: datetime`
  - `available_at: datetime`
  - `raw_payload_hash: str`
- `ConnectorHealth` fields:
  - `provider: str`
  - `status: ConnectorHealthStatus`
  - `checked_at: datetime`
  - `reason: str`
  - `latency_ms: float | None = None`
- `ProviderCostEstimate` fields:
  - `provider: str`
  - `request_count: int`
  - `estimated_cost_usd: float`
  - `currency: str = "USD"`
- Define a `MarketDataConnector` protocol with:
  - `fetch(request: ConnectorRequest) -> list[RawRecord]`
  - `normalize(records: Sequence[RawRecord]) -> list[NormalizedRecord]`
  - `healthcheck() -> ConnectorHealth`
  - `estimate_cost(request: ConnectorRequest) -> ProviderCostEstimate`
- Validation invariants:
  - Provider names must be non-empty.
  - `source_ts`, `fetched_at`, `available_at`, `requested_at`, and `checked_at` must be timezone-aware.
  - `available_at` must not be earlier than `source_ts`.
  - `fetched_at` must not be earlier than `source_ts`.
  - Request params and record payloads must be copied into immutable mapping proxies.
  - Hash fields must be non-empty.

**Tests:**

- Dataclasses reject missing/naive timestamps.
- Dataclasses reject blank provider and blank hashes.
- Payload and params mappings are immutable after construction.
- `available_at < source_ts` raises `ValueError`.
- `estimate_cost` allows zero cost for dry-run CSV.

## Task 2: Provider Schema and Migration

**Objective:** Add SQLite-local and PostgreSQL-compatible schema objects for provider foundation tables.

**Files:**

- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `sql/migrations/002_provider_foundation.sql`
- Create: `tests/integration/test_provider_schema.py`

**Tables to add:**

- `raw_provider_records`
- `normalized_provider_records`
- `provider_health`
- `job_runs`
- `data_quality_incidents`
- `universe_snapshots`
- `universe_members`

**`raw_provider_records` columns:**

- `id TEXT PRIMARY KEY`
- `provider TEXT NOT NULL`
- `kind TEXT NOT NULL`
- `request_hash TEXT NOT NULL`
- `payload_hash TEXT NOT NULL`
- `payload JSONB NOT NULL`
- `source_ts TIMESTAMPTZ NOT NULL`
- `fetched_at TIMESTAMPTZ NOT NULL`
- `available_at TIMESTAMPTZ NOT NULL`
- `license_tag TEXT NOT NULL`
- `retention_policy TEXT NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL`

**`normalized_provider_records` columns:**

- `id TEXT PRIMARY KEY`
- `provider TEXT NOT NULL`
- `kind TEXT NOT NULL`
- `identity TEXT NOT NULL`
- `payload JSONB NOT NULL`
- `source_ts TIMESTAMPTZ NOT NULL`
- `available_at TIMESTAMPTZ NOT NULL`
- `raw_payload_hash TEXT NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL`

**`provider_health` columns:**

- `id TEXT PRIMARY KEY`
- `provider TEXT NOT NULL`
- `status TEXT NOT NULL`
- `checked_at TIMESTAMPTZ NOT NULL`
- `reason TEXT NOT NULL`
- `latency_ms DOUBLE PRECISION`

**`job_runs` columns:**

- `id TEXT PRIMARY KEY`
- `job_type TEXT NOT NULL`
- `provider TEXT`
- `status TEXT NOT NULL`
- `started_at TIMESTAMPTZ NOT NULL`
- `finished_at TIMESTAMPTZ`
- `requested_count INTEGER NOT NULL DEFAULT 0`
- `raw_count INTEGER NOT NULL DEFAULT 0`
- `normalized_count INTEGER NOT NULL DEFAULT 0`
- `error_summary TEXT`
- `metadata JSONB NOT NULL`

**`data_quality_incidents` columns:**

- `id TEXT PRIMARY KEY`
- `provider TEXT NOT NULL`
- `severity TEXT NOT NULL`
- `kind TEXT NOT NULL`
- `affected_tickers JSONB NOT NULL`
- `reason TEXT NOT NULL`
- `fail_closed_action TEXT NOT NULL`
- `payload JSONB NOT NULL`
- `detected_at TIMESTAMPTZ NOT NULL`
- `source_ts TIMESTAMPTZ`
- `available_at TIMESTAMPTZ`

**`universe_snapshots` columns:**

- `id TEXT PRIMARY KEY`
- `name TEXT NOT NULL`
- `as_of TIMESTAMPTZ NOT NULL`
- `provider TEXT NOT NULL`
- `source_ts TIMESTAMPTZ NOT NULL`
- `available_at TIMESTAMPTZ NOT NULL`
- `member_count INTEGER NOT NULL`
- `metadata JSONB NOT NULL`

**`universe_members` columns:**

- `snapshot_id TEXT NOT NULL`
- `ticker TEXT NOT NULL`
- `reason TEXT NOT NULL`
- `rank INTEGER`
- `metadata JSONB NOT NULL`
- Primary key: `(snapshot_id, ticker)`

**Tests:**

- `create_schema(engine)` creates all new tables in SQLite.
- Migration SQL contains all new table names and required timestamp columns.
- `available_at` columns are non-null in raw, normalized, and universe snapshots.
- Incident `source_ts` and `available_at` columns are nullable so missing timestamp payloads can still be audited.
- JSON columns use the existing SQLAlchemy `json_type`.

## Task 3: Provider Repositories and Replay

**Objective:** Persist and read provider records, health, jobs, incidents, and universe snapshots.

**Files:**

- Create: `src/catalyst_radar/storage/provider_repositories.py`
- Modify: `src/catalyst_radar/storage/__init__.py`
- Create: `tests/integration/test_provider_storage.py`

**Implementation details:**

- Create `ProviderRepository`.
- Methods:
  - `save_raw_records(records: Iterable[RawRecord]) -> int`
  - `save_normalized_records(records: Iterable[NormalizedRecord]) -> int`
  - `list_raw_records(provider: str | None = None, kind: ConnectorRecordKind | None = None) -> list[RawRecord]`
  - `list_normalized_records(provider: str | None = None, kind: ConnectorRecordKind | None = None) -> list[NormalizedRecord]`
  - `save_health(health: ConnectorHealth) -> str`
  - `latest_health(provider: str) -> ConnectorHealth | None`
  - `start_job(job_type: str, provider: str | None, metadata: Mapping[str, Any] | None = None) -> str`
  - `finish_job(job_id: str, status: str, requested_count: int, raw_count: int, normalized_count: int, error_summary: str | None = None) -> None`
  - `record_incident(...) -> str`
  - `record_rejected_payload(provider: str, kind: str, payload: Mapping[str, Any], reason: str, severity: DataQualitySeverity, fail_closed_action: str) -> str`
  - `save_universe_snapshot(...) -> str`
  - `list_universe_members(snapshot_id: str) -> list[str]`
- Store IDs as UUID strings.
- Convert all database datetimes through the existing UTC-normalization pattern.
- Preserve raw payload hashes exactly; replay relies on hash equality.

**Replay behavior:**

- Add `replay_normalized_records(raw_records, connector)`.
- It calls `connector.normalize(raw_records)`.
- It verifies each normalized record references a raw payload hash that exists in the raw batch.
- It fails if normalization emits records without `available_at`.

**Tests:**

- Saving raw then listing raw round-trips provider, kind, hash, payload, source timestamp, and availability timestamp.
- Saving normalized then listing normalized round-trips identity and raw payload hash.
- Latest provider health returns most recent `checked_at`.
- Job run transitions from running to success/failure.
- Incident persistence round-trips severity, affected tickers, and fail-closed action.
- Universe snapshot persistence round-trips member list.
- Replay rejects normalized records that reference an unknown raw payload hash.
- Rejected invalid payloads are stored in `data_quality_incidents.payload` even when they cannot be represented as valid `RawRecord` instances.

## Task 4: Dry-Run CSV Provider Adapter

**Objective:** Let the current local CSV fixtures run through the new connector interface.

**Files:**

- Create: `src/catalyst_radar/connectors/market_data.py`
- Create: `src/catalyst_radar/connectors/provider_registry.py`
- Modify: `src/catalyst_radar/connectors/csv_market.py`
- Create: `tests/integration/test_dry_run_csv_provider.py`

**Implementation details:**

- Create `CsvMarketDataConnector`.
- It should accept paths for securities, daily bars, and optional holdings.
- `fetch` should produce raw records for each input row.
- Raw payloads should include enough original fields to replay normalization.
- `normalize` should produce normalized records with payloads compatible with existing `Security`, `DailyBar`, and `HoldingSnapshot` loaders.
- If a row is missing mandatory timestamp fields, the adapter should expose it through the rejected-payload incident path instead of silently dropping it.
- `healthcheck` should return healthy when all configured required paths exist, degraded when optional holdings is missing, and down when required files are missing.
- `estimate_cost` returns zero cost and request count based on configured file count.
- `provider_registry.py` exposes a small registry:
  - `register_connector(name, connector)`
  - `get_connector(name)`
  - `default_csv_connector(...)`
- Keep existing `load_*_csv` functions as stable local helpers.
- Avoid global mutable connector state in tests; registry instances should be explicit or resettable.

**Tests:**

- Dry-run connector fetches raw securities and daily bars from sample files.
- Dry-run connector normalizes raw records into payloads that can construct current domain models.
- Missing daily-bars path produces `DOWN` health.
- Missing optional holdings path produces `DEGRADED` health, not a crash.
- Cost estimate is zero.

## Task 5: CLI Provider Ingest Path

**Objective:** Wire provider foundation into CLI while preserving existing user-facing commands.

**Files:**

- Modify: `src/catalyst_radar/cli.py`
- Modify: `src/catalyst_radar/storage/repositories.py` if conversion helpers are needed.
- Create: `tests/integration/test_provider_ingest_cli.py`

**Implementation details:**

- Keep `ingest-csv --securities --daily-bars --holdings` behavior and output compatible:

```text
ingested securities=6 daily_bars=36 holdings=1
```

- Internally, `ingest-csv` should:
  - create a `CsvMarketDataConnector`
  - run `healthcheck`
  - start a `csv_ingest` job
  - fetch raw records
  - save raw records
  - normalize records
  - save normalized records
  - convert normalized payloads into current `Security`, `DailyBar`, and `HoldingSnapshot` domain objects
  - persist current normalized domain tables via `MarketRepository`
  - finish the job with counts
  - write degraded provider health and data-quality incidents when required fields are absent or invalid
- On connector failure:
  - save provider health as `DOWN`
  - finish the job as `failed`
  - record a high-severity data-quality incident with a fail-closed action
  - return non-zero exit status
- Add optional CLI command:

```powershell
python -m catalyst_radar.cli provider-health --provider csv
```

It should print latest provider health in a stable text format for smoke checks.

**Tests:**

- Existing CLI ingest smoke output remains unchanged.
- Ingest writes raw, normalized, health, and job records.
- Failed ingest returns non-zero and records a failed job.
- Provider health command prints latest state.

## Task 6: Fail-Closed Availability Regression

**Objective:** Prove provider foundation cannot allow action-state promotion from unavailable or timestamp-missing records.

**Files:**

- Modify: `src/catalyst_radar/pipeline/scan.py` only if current gates are insufficient.
- Create or extend: `tests/integration/test_scan_pipeline.py`
- Create: `tests/integration/test_provider_availability_gates.py`

**Implementation details:**

- Keep the current repository query filter:
  - scan as-of date should only see daily bars with `available_at <= as_of`.
- Add provider-foundation regression cases:
  - raw payload missing `available_at` is recorded as a data-quality incident before normalization.
  - raw record missing `available_at` is rejected before normalization.
  - normalized daily bar missing `available_at` is rejected before persistence.
  - future-available daily bar is persisted but invisible to scan for earlier as-of.
  - candidate state is not promoted when all potentially bullish bars are future-available.

**Tests:**

- A future-available bullish bar does not change `scanned candidates=3` fixture expectations.
- A missing-availability row causes ingest failure and records a data-quality incident.
- A scan after the availability timestamp can see the record.

## Task 7: Phase Verification and Documentation

**Objective:** Capture evidence and keep the next phase unblocked.

**Files:**

- Modify: `docs/phase-1-review.md` only if new evidence changes Phase 1 summary.
- Create: `docs/phase-2-review.md`
- Modify: `README.md` if CLI commands changed.

**Verification commands:**

```powershell
python -m pytest
python -m ruff check src tests apps
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
python -m catalyst_radar.cli provider-health --provider csv
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

**Expected smoke output:**

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
provider=csv status=healthy
scanned candidates=3
```

**Review gates:**

- Spec compliance review verifies every task above is implemented or explicitly deferred in `docs/phase-2-review.md`.
- Code quality review verifies no provider code bypasses timestamp validation.
- Final controller audit maps each exit criterion to tests, files, or command output.
