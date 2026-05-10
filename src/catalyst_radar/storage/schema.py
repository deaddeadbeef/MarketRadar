from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()
json_type = JSON().with_variant(JSONB, "postgresql")

securities = Table(
    "securities",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("name", Text, nullable=False),
    Column("exchange", String, nullable=False),
    Column("sector", String, nullable=False),
    Column("industry", String, nullable=False),
    Column("market_cap", Float, nullable=False),
    Column("avg_dollar_volume_20d", Float, nullable=False),
    Column("has_options", Boolean, nullable=False, default=False),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("metadata", json_type, nullable=False),
)

daily_bars = Table(
    "daily_bars",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("provider", String, primary_key=True),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("vwap", Float, nullable=False),
    Column("adjusted", Boolean, nullable=False, default=True),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
)

signal_features = Table(
    "signal_features",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("as_of", DateTime(timezone=True), primary_key=True),
    Column("feature_version", String, primary_key=True),
    Column("price_strength", Float, nullable=False),
    Column("volume_score", Float, nullable=False),
    Column("liquidity_score", Float, nullable=False),
    Column("risk_penalty", Float, nullable=False),
    Column("portfolio_penalty", Float, nullable=False),
    Column("final_score", Float, nullable=False),
    Column("payload", json_type, nullable=False),
)

portfolio_impacts = Table(
    "portfolio_impacts",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("setup_type", String, nullable=False),
    Column("proposed_notional", Float, nullable=False),
    Column("max_loss", Float, nullable=False),
    Column("single_name_before_pct", Float, nullable=False),
    Column("single_name_after_pct", Float, nullable=False),
    Column("sector_before_pct", Float, nullable=False),
    Column("sector_after_pct", Float, nullable=False),
    Column("theme_before_pct", Float, nullable=False),
    Column("theme_after_pct", Float, nullable=False),
    Column("correlated_before_pct", Float, nullable=False),
    Column("correlated_after_pct", Float, nullable=False),
    Column("portfolio_penalty", Float, nullable=False),
    Column("hard_blocks", json_type, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

candidate_states = Table(
    "candidate_states",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("state", String, nullable=False),
    Column("previous_state", String),
    Column("final_score", Float, nullable=False),
    Column("score_delta_5d", Float, nullable=False, default=0),
    Column("hard_blocks", json_type, nullable=False),
    Column("transition_reasons", json_type, nullable=False),
    Column("feature_version", String, nullable=False),
    Column("policy_version", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

candidate_packets = Table(
    "candidate_packets",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("candidate_state_id", String),
    Column("state", String, nullable=False),
    Column("final_score", Float, nullable=False),
    Column("schema_version", String, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

decision_cards = Table(
    "decision_cards",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("candidate_packet_id", String, nullable=False),
    Column("action_state", String, nullable=False),
    Column("setup_type", String),
    Column("final_score", Float, nullable=False),
    Column("schema_version", String, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("next_review_at", DateTime(timezone=True), nullable=False),
    Column("user_decision", String),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

alerts = Table(
    "alerts",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("candidate_state_id", String),
    Column("candidate_packet_id", String),
    Column("decision_card_id", String),
    Column("action_state", String, nullable=False),
    Column("route", String, nullable=False),
    Column("channel", String, nullable=False),
    Column("priority", String, nullable=False),
    Column("status", String, nullable=False),
    Column("dedupe_key", String, nullable=False),
    Column("trigger_kind", String, nullable=False),
    Column("trigger_fingerprint", String, nullable=False),
    Column("title", Text, nullable=False),
    Column("summary", Text, nullable=False),
    Column("feedback_url", Text),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("sent_at", DateTime(timezone=True)),
)

budget_ledger = Table(
    "budget_ledger",
    metadata,
    Column("id", String, primary_key=True),
    Column("ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("ticker", String),
    Column("candidate_state_id", String),
    Column("candidate_packet_id", String),
    Column("decision_card_id", String),
    Column("task", String, nullable=False),
    Column("model", String),
    Column("provider", String, nullable=False),
    Column("status", String, nullable=False),
    Column("skip_reason", String),
    Column("input_tokens", BigInteger, nullable=False),
    Column("cached_input_tokens", BigInteger, nullable=False),
    Column("output_tokens", BigInteger, nullable=False),
    Column("tool_calls", json_type, nullable=False),
    Column("estimated_cost", Numeric, nullable=False),
    Column("actual_cost", Numeric, nullable=False),
    Column("currency", String, nullable=False),
    Column("candidate_state", String),
    Column("prompt_version", String),
    Column("schema_version", String),
    Column("outcome_label", String),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

alert_suppressions = Table(
    "alert_suppressions",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("candidate_state_id", String),
    Column("decision_card_id", String),
    Column("route", String, nullable=False),
    Column("dedupe_key", String, nullable=False),
    Column("trigger_kind", String, nullable=False),
    Column("trigger_fingerprint", String, nullable=False),
    Column("reason", String, nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

user_feedback = Table(
    "user_feedback",
    metadata,
    Column("id", String, primary_key=True),
    Column("artifact_type", String, nullable=False),
    Column("artifact_id", String, nullable=False),
    Column("ticker", String, nullable=False),
    Column("label", String, nullable=False),
    Column("notes", Text),
    Column("source", String, nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

validation_runs = Table(
    "validation_runs",
    metadata,
    Column("id", String, primary_key=True),
    Column("run_type", String, nullable=False),
    Column("as_of_start", DateTime(timezone=True), nullable=False),
    Column("as_of_end", DateTime(timezone=True), nullable=False),
    Column("decision_available_at", DateTime(timezone=True), nullable=False),
    Column("status", String, nullable=False),
    Column("config", json_type, nullable=False),
    Column("metrics", json_type, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

validation_results = Table(
    "validation_results",
    metadata,
    Column("id", String, primary_key=True),
    Column("run_id", String, nullable=False),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("state", String, nullable=False),
    Column("final_score", Float, nullable=False),
    Column("candidate_state_id", String),
    Column("candidate_packet_id", String),
    Column("decision_card_id", String),
    Column("baseline", String),
    Column("labels", json_type, nullable=False),
    Column("leakage_flags", json_type, nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

paper_trades = Table(
    "paper_trades",
    metadata,
    Column("id", String, primary_key=True),
    Column("decision_card_id", String, nullable=False),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("decision", String, nullable=False),
    Column("state", String, nullable=False),
    Column("entry_price", Float),
    Column("entry_at", DateTime(timezone=True)),
    Column("invalidation_price", Float),
    Column("shares", Float, nullable=False),
    Column("notional", Float, nullable=False),
    Column("max_loss", Float, nullable=False),
    Column("outcome_labels", json_type, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

useful_alert_labels = Table(
    "useful_alert_labels",
    metadata,
    Column("id", String, primary_key=True),
    Column("artifact_type", String, nullable=False),
    Column("artifact_id", String, nullable=False),
    Column("ticker", String, nullable=False),
    Column("label", String, nullable=False),
    Column("notes", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

holdings_snapshots = Table(
    "holdings_snapshots",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("as_of", DateTime(timezone=True), primary_key=True),
    Column("shares", Float, nullable=False),
    Column("market_value", Float, nullable=False),
    Column("sector", String, nullable=False),
    Column("theme", String, nullable=False),
    Column("portfolio_value", Float, server_default=text("0")),
    Column("cash", Float, server_default=text("0")),
)

raw_provider_records = Table(
    "raw_provider_records",
    metadata,
    Column("id", String, primary_key=True),
    Column("provider", String, nullable=False),
    Column("kind", String, nullable=False),
    Column("request_hash", String, nullable=False),
    Column("payload_hash", String, nullable=False),
    Column("payload", json_type, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("license_tag", String, nullable=False),
    Column("retention_policy", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

normalized_provider_records = Table(
    "normalized_provider_records",
    metadata,
    Column("id", String, primary_key=True),
    Column("provider", String, nullable=False),
    Column("kind", String, nullable=False),
    Column("identity", String, nullable=False),
    Column("payload", json_type, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("raw_payload_hash", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

provider_health = Table(
    "provider_health",
    metadata,
    Column("id", String, primary_key=True),
    Column("provider", String, nullable=False),
    Column("status", String, nullable=False),
    Column("checked_at", DateTime(timezone=True), nullable=False),
    Column("reason", Text, nullable=False),
    Column("latency_ms", Float),
)

job_runs = Table(
    "job_runs",
    metadata,
    Column("id", String, primary_key=True),
    Column("job_type", String, nullable=False),
    Column("provider", String),
    Column("status", String, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("requested_count", Integer, nullable=False, server_default=text("0")),
    Column("raw_count", Integer, nullable=False, server_default=text("0")),
    Column("normalized_count", Integer, nullable=False, server_default=text("0")),
    Column("error_summary", Text),
    Column("metadata", json_type, nullable=False),
)

job_locks = Table(
    "job_locks",
    metadata,
    Column("lock_name", String, primary_key=True),
    Column("owner", String, nullable=False),
    Column("acquired_at", DateTime(timezone=True), nullable=False),
    Column("heartbeat_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("metadata", json_type, nullable=False),
)

data_quality_incidents = Table(
    "data_quality_incidents",
    metadata,
    Column("id", String, primary_key=True),
    Column("provider", String, nullable=False),
    Column("severity", String, nullable=False),
    Column("kind", String, nullable=False),
    Column("affected_tickers", json_type, nullable=False),
    Column("reason", Text, nullable=False),
    Column("fail_closed_action", Text, nullable=False),
    Column("payload", json_type, nullable=False),
    Column("detected_at", DateTime(timezone=True), nullable=False),
    Column("source_ts", DateTime(timezone=True)),
    Column("available_at", DateTime(timezone=True)),
)

universe_snapshots = Table(
    "universe_snapshots",
    metadata,
    Column("id", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("provider", String, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("member_count", Integer, nullable=False),
    Column("metadata", json_type, nullable=False),
)

universe_members = Table(
    "universe_members",
    metadata,
    Column("snapshot_id", String, primary_key=True),
    Column("ticker", String, primary_key=True),
    Column("reason", Text, nullable=False),
    Column("rank", Integer),
    Column("metadata", json_type, nullable=False),
)

events = Table(
    "events",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("event_type", String, nullable=False),
    Column("provider", String, nullable=False),
    Column("source", Text, nullable=False),
    Column("source_category", String, nullable=False),
    Column("source_url", Text),
    Column("title", Text, nullable=False),
    Column("body_hash", String, nullable=False),
    Column("dedupe_key", String, nullable=False),
    Column("source_quality", Float, nullable=False),
    Column("materiality", Float, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

text_snippets = Table(
    "text_snippets",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("event_id", String, nullable=False),
    Column("snippet_hash", String, nullable=False),
    Column("section", String, nullable=False),
    Column("text", Text, nullable=False),
    Column("source", Text, nullable=False),
    Column("source_url", Text),
    Column("source_quality", Float, nullable=False),
    Column("event_type", String, nullable=False),
    Column("materiality", Float, nullable=False),
    Column("ontology_hits", json_type, nullable=False),
    Column("sentiment", Float, nullable=False),
    Column("embedding", json_type, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

text_features = Table(
    "text_features",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("feature_version", String, nullable=False),
    Column("local_narrative_score", Float, nullable=False),
    Column("novelty_score", Float, nullable=False),
    Column("sentiment_score", Float, nullable=False),
    Column("source_quality_score", Float, nullable=False),
    Column("theme_match_score", Float, nullable=False),
    Column("conflict_penalty", Float, nullable=False),
    Column("selected_snippet_ids", json_type, nullable=False),
    Column("theme_hits", json_type, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

option_features = Table(
    "option_features",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("provider", String, nullable=False),
    Column("call_volume", Float, nullable=False),
    Column("put_volume", Float, nullable=False),
    Column("call_open_interest", Float, nullable=False),
    Column("put_open_interest", Float, nullable=False),
    Column("iv_percentile", Float, nullable=False),
    Column("skew", Float, nullable=False),
    Column("abnormality_score", Float, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

Index(
    "ix_daily_bars_ticker_date_available_at",
    daily_bars.c.ticker,
    daily_bars.c.date,
    daily_bars.c.available_at,
)
Index("ix_securities_active_ticker", securities.c.is_active, securities.c.ticker)
Index("ix_portfolio_impacts_ticker_as_of", portfolio_impacts.c.ticker, portfolio_impacts.c.as_of)
Index(
    "ix_portfolio_impacts_setup_type_as_of",
    portfolio_impacts.c.setup_type,
    portfolio_impacts.c.as_of,
)
Index(
    "ix_raw_provider_provider_kind_source",
    raw_provider_records.c.provider,
    raw_provider_records.c.kind,
    raw_provider_records.c.source_ts,
)
Index(
    "ix_normalized_provider_identity_available",
    normalized_provider_records.c.provider,
    normalized_provider_records.c.kind,
    normalized_provider_records.c.identity,
    normalized_provider_records.c.available_at,
)
Index(
    "ix_provider_health_provider_checked",
    provider_health.c.provider,
    provider_health.c.checked_at,
)
Index("ix_job_runs_provider_started", job_runs.c.provider, job_runs.c.started_at)
Index("ix_job_locks_expires_at", job_locks.c.expires_at)
Index("ix_job_locks_owner", job_locks.c.owner)
Index(
    "ix_incidents_provider_detected",
    data_quality_incidents.c.provider,
    data_quality_incidents.c.detected_at,
)
Index(
    "ix_universe_snapshots_name_asof_available_at",
    universe_snapshots.c.name,
    universe_snapshots.c.as_of,
    universe_snapshots.c.available_at,
)
Index(
    "ix_universe_members_snapshot_rank_ticker",
    universe_members.c.snapshot_id,
    universe_members.c.rank,
    universe_members.c.ticker,
)
Index("ux_events_dedupe_key", events.c.dedupe_key, unique=True)
Index("ix_events_ticker_available_at", events.c.ticker, events.c.available_at)
Index("ix_events_type_materiality", events.c.event_type, events.c.materiality)
Index(
    "ux_text_snippets_event_hash",
    text_snippets.c.event_id,
    text_snippets.c.snippet_hash,
    unique=True,
)
Index(
    "ix_text_snippets_ticker_available_at",
    text_snippets.c.ticker,
    text_snippets.c.available_at,
)
Index("ix_text_snippets_snippet_hash", text_snippets.c.snippet_hash)
Index(
    "ux_text_features_ticker_as_of_version",
    text_features.c.ticker,
    text_features.c.as_of,
    text_features.c.feature_version,
    unique=True,
)
Index(
    "ix_text_features_ticker_available_at",
    text_features.c.ticker,
    text_features.c.available_at,
)
Index(
    "ix_option_features_ticker_as_of_provider",
    option_features.c.ticker,
    option_features.c.as_of,
    option_features.c.provider,
)
Index(
    "ix_option_features_ticker_available_at",
    option_features.c.ticker,
    option_features.c.available_at,
)
Index(
    "ix_candidate_packets_ticker_as_of_available_at",
    candidate_packets.c.ticker,
    candidate_packets.c.as_of,
    candidate_packets.c.available_at,
)
Index(
    "ix_candidate_packets_state_available_at",
    candidate_packets.c.state,
    candidate_packets.c.available_at,
)
Index(
    "ix_decision_cards_ticker_as_of_available_at",
    decision_cards.c.ticker,
    decision_cards.c.as_of,
    decision_cards.c.available_at,
)
Index(
    "ix_decision_cards_action_state_available_at",
    decision_cards.c.action_state,
    decision_cards.c.available_at,
)
Index("ix_alerts_ticker_available_at", alerts.c.ticker, alerts.c.available_at)
Index("ix_alerts_route_status", alerts.c.route, alerts.c.status)
Index("ix_alerts_dedupe_key", alerts.c.dedupe_key)
Index("ix_budget_ledger_available_at", budget_ledger.c.available_at)
Index(
    "ix_budget_ledger_task_status_ts",
    budget_ledger.c.task,
    budget_ledger.c.status,
    budget_ledger.c.ts,
)
Index("ix_budget_ledger_ticker_ts", budget_ledger.c.ticker, budget_ledger.c.ts)
Index("ix_alert_suppressions_dedupe_key", alert_suppressions.c.dedupe_key)
Index(
    "ix_user_feedback_artifact",
    user_feedback.c.artifact_type,
    user_feedback.c.artifact_id,
)
Index(
    "ix_validation_results_run_ticker_as_of",
    validation_results.c.run_id,
    validation_results.c.ticker,
    validation_results.c.as_of,
)
Index(
    "ix_validation_results_available_at",
    validation_results.c.available_at,
)
Index("ix_paper_trades_ticker_state", paper_trades.c.ticker, paper_trades.c.state)
Index("ix_paper_trades_decision_card", paper_trades.c.decision_card_id)
Index(
    "ix_useful_alert_labels_artifact",
    useful_alert_labels.c.artifact_type,
    useful_alert_labels.c.artifact_id,
)
