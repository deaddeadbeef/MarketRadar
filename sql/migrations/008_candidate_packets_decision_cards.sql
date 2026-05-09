-- PostgreSQL-compatible migration for Phase 8 candidate packets and decision cards.

CREATE TABLE IF NOT EXISTS candidate_packets (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  candidate_state_id TEXT,
  state TEXT NOT NULL,
  final_score DOUBLE PRECISION NOT NULL,
  schema_version TEXT NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_candidate_packets_ticker_as_of_available_at
  ON candidate_packets (ticker, as_of, available_at);

CREATE INDEX IF NOT EXISTS ix_candidate_packets_state_available_at
  ON candidate_packets (state, available_at);

CREATE TABLE IF NOT EXISTS decision_cards (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  candidate_packet_id TEXT NOT NULL,
  action_state TEXT NOT NULL,
  setup_type TEXT,
  final_score DOUBLE PRECISION NOT NULL,
  schema_version TEXT NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  next_review_at TIMESTAMPTZ NOT NULL,
  user_decision TEXT,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_decision_cards_ticker_as_of_available_at
  ON decision_cards (ticker, as_of, available_at);

CREATE INDEX IF NOT EXISTS ix_decision_cards_action_state_available_at
  ON decision_cards (action_state, available_at);

