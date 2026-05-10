-- SQLite-compatible audit event migration.
-- PostgreSQL trigger/function variant: 013_security_audit.postgres.sql.

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
  paper_trade_id TEXT,
  alert_id TEXT,
  decision TEXT,
  reason TEXT,
  hard_blocks JSON NOT NULL DEFAULT '[]',
  status TEXT NOT NULL,
  metadata JSON NOT NULL DEFAULT '{}',
  before_payload JSON NOT NULL DEFAULT '{}',
  after_payload JSON NOT NULL DEFAULT '{}',
  occurred_at TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_audit_events_event_type_occurred
  ON audit_events (event_type, occurred_at);

CREATE INDEX IF NOT EXISTS ix_audit_events_artifact_occurred
  ON audit_events (artifact_type, artifact_id, occurred_at);

CREATE INDEX IF NOT EXISTS ix_audit_events_artifact
  ON audit_events (artifact_type, artifact_id);

CREATE INDEX IF NOT EXISTS ix_audit_events_ticker_occurred
  ON audit_events (ticker, occurred_at);

CREATE INDEX IF NOT EXISTS ix_audit_events_candidate_packet
  ON audit_events (candidate_packet_id, occurred_at);

CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_update
BEFORE UPDATE ON audit_events
BEGIN
  SELECT RAISE(ABORT, 'audit_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_delete
BEFORE DELETE ON audit_events
BEGIN
  SELECT RAISE(ABORT, 'audit_events is append-only');
END;
