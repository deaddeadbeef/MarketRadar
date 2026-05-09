CREATE TABLE IF NOT EXISTS securities (
  ticker TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  exchange TEXT NOT NULL,
  sector TEXT NOT NULL,
  industry TEXT NOT NULL,
  market_cap DOUBLE PRECISION NOT NULL,
  avg_dollar_volume_20d DOUBLE PRECISION NOT NULL,
  has_options BOOLEAN NOT NULL DEFAULT FALSE,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_bars (
  ticker TEXT NOT NULL,
  date DATE NOT NULL,
  provider TEXT NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  vwap DOUBLE PRECISION NOT NULL,
  adjusted BOOLEAN NOT NULL DEFAULT TRUE,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (ticker, date, provider)
);

CREATE TABLE IF NOT EXISTS signal_features (
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  feature_version TEXT NOT NULL,
  price_strength DOUBLE PRECISION NOT NULL,
  volume_score DOUBLE PRECISION NOT NULL,
  liquidity_score DOUBLE PRECISION NOT NULL,
  risk_penalty DOUBLE PRECISION NOT NULL,
  portfolio_penalty DOUBLE PRECISION NOT NULL,
  final_score DOUBLE PRECISION NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (ticker, as_of, feature_version)
);

CREATE TABLE IF NOT EXISTS candidate_states (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  state TEXT NOT NULL,
  previous_state TEXT,
  final_score DOUBLE PRECISION NOT NULL,
  score_delta_5d DOUBLE PRECISION NOT NULL DEFAULT 0,
  hard_blocks JSONB NOT NULL,
  transition_reasons JSONB NOT NULL,
  feature_version TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS holdings_snapshots (
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  shares DOUBLE PRECISION NOT NULL,
  market_value DOUBLE PRECISION NOT NULL,
  sector TEXT NOT NULL,
  theme TEXT NOT NULL,
  PRIMARY KEY (ticker, as_of)
);
