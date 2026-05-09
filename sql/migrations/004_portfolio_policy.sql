-- PostgreSQL-compatible migration for Phase 4 portfolio policy.
-- SQLite local database upgrades for these additive holdings columns are handled
-- idempotently by catalyst_radar.storage.db.create_schema().

ALTER TABLE holdings_snapshots
  ADD COLUMN IF NOT EXISTS portfolio_value DOUBLE PRECISION DEFAULT 0;

ALTER TABLE holdings_snapshots
  ADD COLUMN IF NOT EXISTS cash DOUBLE PRECISION DEFAULT 0;

CREATE TABLE IF NOT EXISTS portfolio_impacts (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  setup_type TEXT NOT NULL,
  proposed_notional DOUBLE PRECISION NOT NULL,
  max_loss DOUBLE PRECISION NOT NULL,
  single_name_before_pct DOUBLE PRECISION NOT NULL,
  single_name_after_pct DOUBLE PRECISION NOT NULL,
  sector_before_pct DOUBLE PRECISION NOT NULL,
  sector_after_pct DOUBLE PRECISION NOT NULL,
  theme_before_pct DOUBLE PRECISION NOT NULL,
  theme_after_pct DOUBLE PRECISION NOT NULL,
  correlated_before_pct DOUBLE PRECISION NOT NULL,
  correlated_after_pct DOUBLE PRECISION NOT NULL,
  portfolio_penalty DOUBLE PRECISION NOT NULL,
  hard_blocks JSONB NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_portfolio_impacts_ticker_as_of
  ON portfolio_impacts (ticker, as_of);

CREATE INDEX IF NOT EXISTS ix_portfolio_impacts_setup_type_as_of
  ON portfolio_impacts (setup_type, as_of);
