from __future__ import annotations

from sqlalchemy import (
    JSON,
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
)

metadata = MetaData()

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
    Column("volume", Integer, nullable=False),
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
    Column("payload", JSON, nullable=False),
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
    Column("hard_blocks", JSON, nullable=False),
    Column("transition_reasons", JSON, nullable=False),
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
