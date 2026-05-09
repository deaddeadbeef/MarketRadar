-- PostgreSQL-compatible migration for Phase 6 local text intelligence storage.

CREATE TABLE IF NOT EXISTS text_snippets (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  event_id TEXT NOT NULL,
  snippet_hash TEXT NOT NULL,
  section TEXT NOT NULL,
  text TEXT NOT NULL,
  source TEXT NOT NULL,
  source_url TEXT,
  source_quality DOUBLE PRECISION NOT NULL,
  event_type TEXT NOT NULL,
  materiality DOUBLE PRECISION NOT NULL,
  ontology_hits JSONB NOT NULL,
  sentiment DOUBLE PRECISION NOT NULL,
  embedding JSONB NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_text_snippets_event_hash
  ON text_snippets (event_id, snippet_hash);

CREATE INDEX IF NOT EXISTS ix_text_snippets_ticker_available_at
  ON text_snippets (ticker, available_at);

CREATE INDEX IF NOT EXISTS ix_text_snippets_snippet_hash
  ON text_snippets (snippet_hash);

CREATE TABLE IF NOT EXISTS text_features (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  feature_version TEXT NOT NULL,
  local_narrative_score DOUBLE PRECISION NOT NULL,
  novelty_score DOUBLE PRECISION NOT NULL,
  sentiment_score DOUBLE PRECISION NOT NULL,
  source_quality_score DOUBLE PRECISION NOT NULL,
  theme_match_score DOUBLE PRECISION NOT NULL,
  conflict_penalty DOUBLE PRECISION NOT NULL,
  selected_snippet_ids JSONB NOT NULL,
  theme_hits JSONB NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_text_features_ticker_as_of_version
  ON text_features (ticker, as_of, feature_version);

CREATE INDEX IF NOT EXISTS ix_text_features_ticker_available_at
  ON text_features (ticker, available_at);
