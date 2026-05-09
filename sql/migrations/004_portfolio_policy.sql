-- PostgreSQL-compatible migration for Phase 4 portfolio policy.
-- SQLite local database upgrades for these additive holdings columns are handled
-- idempotently by catalyst_radar.storage.db.create_schema().

ALTER TABLE holdings_snapshots
  ADD COLUMN IF NOT EXISTS portfolio_value DOUBLE PRECISION DEFAULT 0;

ALTER TABLE holdings_snapshots
  ADD COLUMN IF NOT EXISTS cash DOUBLE PRECISION DEFAULT 0;
