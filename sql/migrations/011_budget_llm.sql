CREATE TABLE IF NOT EXISTS budget_ledger (
  id TEXT PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  ticker TEXT,
  candidate_state_id TEXT,
  candidate_packet_id TEXT,
  decision_card_id TEXT,
  task TEXT NOT NULL,
  model TEXT,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  skip_reason TEXT,
  input_tokens BIGINT NOT NULL,
  cached_input_tokens BIGINT NOT NULL,
  output_tokens BIGINT NOT NULL,
  tool_calls JSONB NOT NULL,
  estimated_cost NUMERIC NOT NULL,
  actual_cost NUMERIC NOT NULL,
  currency TEXT NOT NULL,
  candidate_state TEXT,
  prompt_version TEXT,
  schema_version TEXT,
  outcome_label TEXT,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_budget_ledger_available_at
  ON budget_ledger (available_at);

CREATE INDEX IF NOT EXISTS ix_budget_ledger_task_status_ts
  ON budget_ledger (task, status, ts);

CREATE INDEX IF NOT EXISTS ix_budget_ledger_ticker_ts
  ON budget_ledger (ticker, ts);
