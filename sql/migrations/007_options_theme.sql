-- PostgreSQL-compatible migration for Phase 7 aggregate options feature storage.

CREATE TABLE IF NOT EXISTS option_features (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  provider TEXT NOT NULL,
  call_volume DOUBLE PRECISION NOT NULL,
  put_volume DOUBLE PRECISION NOT NULL,
  call_open_interest DOUBLE PRECISION NOT NULL,
  put_open_interest DOUBLE PRECISION NOT NULL,
  iv_percentile DOUBLE PRECISION NOT NULL,
  skew DOUBLE PRECISION NOT NULL,
  abnormality_score DOUBLE PRECISION NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_option_features_ticker_as_of_provider
  ON option_features (ticker, as_of, provider);

CREATE INDEX IF NOT EXISTS ix_option_features_ticker_available_at
  ON option_features (ticker, available_at);
