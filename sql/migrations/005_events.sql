-- PostgreSQL-compatible migration for Phase 5 canonical event storage.

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  event_type TEXT NOT NULL,
  provider TEXT NOT NULL,
  source TEXT NOT NULL,
  source_category TEXT NOT NULL,
  source_url TEXT,
  title TEXT NOT NULL,
  body_hash TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  source_quality DOUBLE PRECISION NOT NULL,
  materiality DOUBLE PRECISION NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_events_dedupe_key
  ON events (dedupe_key);

CREATE INDEX IF NOT EXISTS ix_events_ticker_available_at
  ON events (ticker, available_at);

CREATE INDEX IF NOT EXISTS ix_events_type_materiality
  ON events (event_type, materiality);
