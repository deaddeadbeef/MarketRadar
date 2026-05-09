CREATE INDEX IF NOT EXISTS ix_daily_bars_ticker_date_available_at
  ON daily_bars (ticker, date, available_at);

CREATE INDEX IF NOT EXISTS ix_securities_active_ticker
  ON securities (is_active, ticker);

CREATE INDEX IF NOT EXISTS ix_raw_provider_provider_kind_source
  ON raw_provider_records (provider, kind, source_ts);

CREATE INDEX IF NOT EXISTS ix_normalized_provider_identity_available
  ON normalized_provider_records (provider, kind, identity, available_at);

CREATE INDEX IF NOT EXISTS ix_provider_health_provider_checked
  ON provider_health (provider, checked_at);

CREATE INDEX IF NOT EXISTS ix_job_runs_provider_started
  ON job_runs (provider, started_at);

CREATE INDEX IF NOT EXISTS ix_incidents_provider_detected
  ON data_quality_incidents (provider, detected_at);

CREATE INDEX IF NOT EXISTS ix_universe_snapshots_name_asof_available_at
  ON universe_snapshots (name, as_of, available_at);

CREATE INDEX IF NOT EXISTS ix_universe_members_snapshot_rank_ticker
  ON universe_members (snapshot_id, rank, ticker);
