CREATE TABLE IF NOT EXISTS audit_events (
  id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  actor_source TEXT NOT NULL,
  actor_id TEXT,
  actor_role TEXT,
  artifact_type TEXT,
  artifact_id TEXT,
  ticker TEXT,
  candidate_state_id TEXT,
  candidate_packet_id TEXT,
  decision_card_id TEXT,
  budget_ledger_id TEXT,
  status TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  before_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  after_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_audit_events_event_type_occurred
  ON audit_events (event_type, occurred_at);

CREATE INDEX IF NOT EXISTS ix_audit_events_artifact_occurred
  ON audit_events (artifact_type, artifact_id, occurred_at);

CREATE INDEX IF NOT EXISTS ix_audit_events_ticker_occurred
  ON audit_events (ticker, occurred_at);

CREATE INDEX IF NOT EXISTS ix_audit_events_candidate_packet
  ON audit_events (candidate_packet_id, occurred_at);
