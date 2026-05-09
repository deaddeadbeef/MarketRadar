from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
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

holdings_snapshots = Table(
    "holdings_snapshots",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("as_of", DateTime(timezone=True), primary_key=True),
    Column("shares", Float, nullable=False),
    Column("market_value", Float, nullable=False),
    Column("sector", String, nullable=False),
    Column("theme", String, nullable=False),
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
