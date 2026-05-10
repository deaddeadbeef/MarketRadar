-- PostgreSQL-compatible migration for Phase 11 alert artifacts and feedback.

CREATE TABLE IF NOT EXISTS alerts (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  candidate_state_id TEXT,
  candidate_packet_id TEXT,
  decision_card_id TEXT,
  action_state TEXT NOT NULL,
  route TEXT NOT NULL,
  channel TEXT NOT NULL,
  priority TEXT NOT NULL,
  status TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  trigger_kind TEXT NOT NULL,
  trigger_fingerprint TEXT NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  feedback_url TEXT,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  sent_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_alerts_ticker_available_at
  ON alerts (ticker, available_at);

CREATE INDEX IF NOT EXISTS ix_alerts_route_status
  ON alerts (route, status);

CREATE INDEX IF NOT EXISTS ix_alerts_dedupe_key
  ON alerts (dedupe_key);

CREATE TABLE IF NOT EXISTS alert_suppressions (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  candidate_state_id TEXT,
  decision_card_id TEXT,
  route TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  trigger_kind TEXT NOT NULL,
  trigger_fingerprint TEXT NOT NULL,
  reason TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_alert_suppressions_dedupe_key
  ON alert_suppressions (dedupe_key);

CREATE TABLE IF NOT EXISTS user_feedback (
  id TEXT PRIMARY KEY,
  artifact_type TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  ticker TEXT NOT NULL,
  label TEXT NOT NULL,
  notes TEXT,
  source TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_user_feedback_artifact
  ON user_feedback (artifact_type, artifact_id);
