CREATE TABLE IF NOT EXISTS job_locks (
  lock_name TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_job_locks_expires_at
  ON job_locks (expires_at);

CREATE INDEX IF NOT EXISTS ix_job_locks_owner
  ON job_locks (owner);
