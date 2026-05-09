CREATE TABLE IF NOT EXISTS raw_provider_records (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  kind TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  payload JSONB NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  license_tag TEXT NOT NULL,
  retention_policy TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS normalized_provider_records (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  kind TEXT NOT NULL,
  identity TEXT NOT NULL,
  payload JSONB NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  raw_payload_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_health (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  checked_at TIMESTAMPTZ NOT NULL,
  reason TEXT NOT NULL,
  latency_ms DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS job_runs (
  id TEXT PRIMARY KEY,
  job_type TEXT NOT NULL,
  provider TEXT,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  requested_count INTEGER NOT NULL DEFAULT 0,
  raw_count INTEGER NOT NULL DEFAULT 0,
  normalized_count INTEGER NOT NULL DEFAULT 0,
  error_summary TEXT,
  metadata JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS data_quality_incidents (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  severity TEXT NOT NULL,
  kind TEXT NOT NULL,
  affected_tickers JSONB NOT NULL,
  reason TEXT NOT NULL,
  fail_closed_action TEXT NOT NULL,
  payload JSONB NOT NULL,
  detected_at TIMESTAMPTZ NOT NULL,
  source_ts TIMESTAMPTZ,
  available_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS universe_snapshots (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  provider TEXT NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  member_count INTEGER NOT NULL,
  metadata JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS universe_members (
  snapshot_id TEXT NOT NULL,
  ticker TEXT NOT NULL,
  reason TEXT NOT NULL,
  rank INTEGER,
  metadata JSONB NOT NULL,
  PRIMARY KEY (snapshot_id, ticker)
);
