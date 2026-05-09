-- PostgreSQL-compatible migration for Phase 9 validation and paper trading.

CREATE TABLE IF NOT EXISTS validation_runs (
  id TEXT PRIMARY KEY,
  run_type TEXT NOT NULL,
  as_of_start TIMESTAMPTZ NOT NULL,
  as_of_end TIMESTAMPTZ NOT NULL,
  decision_available_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  config JSONB NOT NULL,
  metrics JSONB NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS validation_results (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  state TEXT NOT NULL,
  final_score DOUBLE PRECISION NOT NULL,
  candidate_state_id TEXT,
  candidate_packet_id TEXT,
  decision_card_id TEXT,
  baseline TEXT,
  labels JSONB NOT NULL,
  leakage_flags JSONB NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_validation_results_run_ticker_as_of
  ON validation_results (run_id, ticker, as_of);

CREATE INDEX IF NOT EXISTS ix_validation_results_available_at
  ON validation_results (available_at);

CREATE TABLE IF NOT EXISTS paper_trades (
  id TEXT PRIMARY KEY,
  decision_card_id TEXT NOT NULL,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  decision TEXT NOT NULL,
  state TEXT NOT NULL,
  entry_price DOUBLE PRECISION,
  entry_at TIMESTAMPTZ,
  invalidation_price DOUBLE PRECISION,
  shares DOUBLE PRECISION NOT NULL,
  notional DOUBLE PRECISION NOT NULL,
  max_loss DOUBLE PRECISION NOT NULL,
  outcome_labels JSONB NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_paper_trades_ticker_state
  ON paper_trades (ticker, state);

CREATE INDEX IF NOT EXISTS ix_paper_trades_decision_card
  ON paper_trades (decision_card_id);

CREATE TABLE IF NOT EXISTS useful_alert_labels (
  id TEXT PRIMARY KEY,
  artifact_type TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  ticker TEXT NOT NULL,
  label TEXT NOT NULL,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_useful_alert_labels_artifact
  ON useful_alert_labels (artifact_type, artifact_id);

