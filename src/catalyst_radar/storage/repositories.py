from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import fields
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import Connection, Engine, delete, insert, select

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import (
    CandidateSnapshot,
    DailyBar,
    HoldingSnapshot,
    PolicyResult,
    Security,
)
from catalyst_radar.scoring.policy import POLICY_VERSION
from catalyst_radar.storage.schema import (
    candidate_states,
    daily_bars,
    holdings_snapshots,
    portfolio_impacts,
    securities,
    signal_features,
)


class MarketRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_securities(self, rows: Iterable[Security]) -> None:
        with self.engine.begin() as conn:
            _upsert_securities(conn, rows)

    def upsert_daily_bars(self, rows: Iterable[DailyBar]) -> None:
        with self.engine.begin() as conn:
            _upsert_daily_bars(conn, rows)

    def list_active_securities(self) -> list[Security]:
        stmt = (
            select(securities)
            .where(securities.c.is_active.is_(True))
            .order_by(securities.c.ticker)
        )
        with self.engine.connect() as conn:
            return [
                Security(
                    ticker=row.ticker,
                    name=row.name,
                    exchange=row.exchange,
                    sector=row.sector,
                    industry=row.industry,
                    market_cap=row.market_cap,
                    avg_dollar_volume_20d=row.avg_dollar_volume_20d,
                    has_options=row.has_options,
                    is_active=row.is_active,
                    updated_at=_as_datetime(row.updated_at),
                    metadata=row._mapping["metadata"],
                )
                for row in conn.execute(stmt)
            ]

    def list_active_securities_by_tickers(self, tickers: Collection[str]) -> list[Security]:
        normalized = {ticker.upper() for ticker in tickers}
        if not normalized:
            return []
        stmt = (
            select(securities)
            .where(securities.c.is_active.is_(True), securities.c.ticker.in_(normalized))
            .order_by(securities.c.ticker)
        )
        with self.engine.connect() as conn:
            return [
                Security(
                    ticker=row.ticker,
                    name=row.name,
                    exchange=row.exchange,
                    sector=row.sector,
                    industry=row.industry,
                    market_cap=row.market_cap,
                    avg_dollar_volume_20d=row.avg_dollar_volume_20d,
                    has_options=row.has_options,
                    is_active=row.is_active,
                    updated_at=_as_datetime(row.updated_at),
                    metadata=row._mapping["metadata"],
                )
                for row in conn.execute(stmt)
            ]

    def upsert_holdings(self, rows: Iterable[HoldingSnapshot]) -> None:
        with self.engine.begin() as conn:
            _upsert_holdings(conn, rows)

    def replace_holdings_snapshot(
        self,
        *,
        as_of: datetime,
        rows: Iterable[HoldingSnapshot],
        previous_tickers: Iterable[str] = (),
        portfolio_value: float = 0.0,
        cash: float = 0.0,
    ) -> None:
        normalized_as_of = _as_datetime(as_of)
        snapshot_rows = list(rows)
        current_tickers = {row.ticker.upper() for row in snapshot_rows}
        tombstones = [
            HoldingSnapshot(
                ticker=ticker,
                shares=0.0,
                market_value=0.0,
                sector="unclassified",
                theme="broker_synced",
                as_of=normalized_as_of,
                portfolio_value=portfolio_value,
                cash=cash,
            )
            for ticker in sorted({ticker.upper() for ticker in previous_tickers} - current_tickers)
        ]
        with self.engine.begin() as conn:
            _upsert_holdings(conn, [*snapshot_rows, *tombstones])

    def upsert_market_snapshot(
        self,
        *,
        securities_rows: Iterable[Security],
        daily_bar_rows: Iterable[DailyBar],
        holding_rows: Iterable[HoldingSnapshot] = (),
    ) -> None:
        with self.engine.begin() as conn:
            _upsert_securities(conn, securities_rows)
            _upsert_daily_bars(conn, daily_bar_rows)
            _upsert_holdings(conn, holding_rows)

    def list_holdings(self) -> list[HoldingSnapshot]:
        stmt = select(holdings_snapshots).order_by(
            holdings_snapshots.c.as_of, holdings_snapshots.c.ticker
        )
        with self.engine.connect() as conn:
            return [
                HoldingSnapshot(
                    ticker=row.ticker,
                    shares=row.shares,
                    market_value=row.market_value,
                    sector=row.sector,
                    theme=row.theme,
                    as_of=_as_datetime(row.as_of),
                    portfolio_value=row.portfolio_value or 0.0,
                    cash=row.cash or 0.0,
                )
                for row in conn.execute(stmt)
            ]

    def daily_bars(
        self,
        ticker: str,
        end: date,
        lookback: int,
        *,
        available_at: datetime | None = None,
        provider: str | None = None,
    ) -> list[DailyBar]:
        filters = [daily_bars.c.ticker == ticker, daily_bars.c.date <= end]
        if available_at is not None:
            filters.append(daily_bars.c.available_at <= _as_datetime(available_at))
        if provider is not None:
            filters.append(daily_bars.c.provider == provider)
        stmt = (
            select(daily_bars)
            .where(*filters)
            .order_by(daily_bars.c.date.desc())
            .limit(lookback)
        )
        with self.engine.connect() as conn:
            rows = list(conn.execute(stmt))
        return [
            DailyBar(
                ticker=row.ticker,
                date=row.date,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                vwap=row.vwap,
                adjusted=row.adjusted,
                provider=row.provider,
                source_ts=_as_datetime(row.source_ts),
                available_at=_as_datetime(row.available_at),
            )
            for row in reversed(rows)
        ]

    def save_scan_result(self, candidate: CandidateSnapshot, policy: PolicyResult) -> None:
        pillar_scores = dict(candidate.metadata.get("pillar_scores", {}))
        with self.engine.begin() as conn:
            conn.execute(
                delete(signal_features).where(
                    signal_features.c.ticker == candidate.ticker,
                    signal_features.c.as_of == candidate.as_of,
                    signal_features.c.feature_version == candidate.features.feature_version,
                )
            )
            conn.execute(
                insert(signal_features).values(
                    ticker=candidate.ticker,
                    as_of=candidate.as_of,
                    feature_version=candidate.features.feature_version,
                    price_strength=pillar_scores.get("price_strength", 0.0),
                    volume_score=pillar_scores.get("volume_liquidity", 0.0),
                    liquidity_score=candidate.features.liquidity_score,
                    risk_penalty=candidate.risk_penalty,
                    portfolio_penalty=candidate.portfolio_penalty,
                    final_score=candidate.final_score,
                    payload=_candidate_payload(candidate, policy),
                )
            )
            _save_portfolio_impact(conn, candidate)
            conn.execute(
                delete(candidate_states).where(
                    candidate_states.c.ticker == candidate.ticker,
                    candidate_states.c.as_of == candidate.as_of,
                    candidate_states.c.feature_version == candidate.features.feature_version,
                )
            )
            conn.execute(
                insert(candidate_states).values(
                    id=str(uuid4()),
                    ticker=candidate.ticker,
                    as_of=candidate.as_of,
                    state=policy.state.value,
                    previous_state=None,
                    final_score=candidate.final_score,
                    score_delta_5d=0.0,
                    hard_blocks=list(policy.hard_blocks),
                    transition_reasons=list(policy.reasons),
                    feature_version=candidate.features.feature_version,
                    policy_version=POLICY_VERSION,
                    created_at=datetime.now(UTC),
                )
            )


def _as_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _coerce_datetime(value: object, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return _as_datetime(value)
    if isinstance(value, str):
        try:
            return _as_datetime(datetime.fromisoformat(value))
        except ValueError:
            return _as_datetime(fallback)
    return _as_datetime(fallback)


def _float_from_mapping(payload: dict[str, Any], key: str) -> float:
    value = payload.get(key, 0.0)
    if value is None:
        return 0.0
    return float(value)


def _save_portfolio_impact(conn: Connection, candidate: CandidateSnapshot) -> None:
    raw_impact = candidate.metadata.get("portfolio_impact")
    if raw_impact is None:
        return

    impact = thaw_json_value(raw_impact)
    if not isinstance(impact, dict):
        return

    setup_type = str(candidate.metadata.get("setup_type") or "unknown")
    source_ts = _coerce_datetime(candidate.metadata.get("source_ts"), candidate.as_of)
    available_at = _coerce_datetime(candidate.metadata.get("available_at"), candidate.as_of)

    conn.execute(
        delete(portfolio_impacts).where(
            portfolio_impacts.c.ticker == candidate.ticker,
            portfolio_impacts.c.as_of == candidate.as_of,
            portfolio_impacts.c.setup_type == setup_type,
        )
    )
    conn.execute(
        insert(portfolio_impacts).values(
            id=str(uuid4()),
            ticker=candidate.ticker,
            as_of=candidate.as_of,
            setup_type=setup_type,
            proposed_notional=_float_from_mapping(impact, "proposed_notional"),
            max_loss=_float_from_mapping(impact, "max_loss"),
            single_name_before_pct=_float_from_mapping(impact, "single_name_before_pct"),
            single_name_after_pct=_float_from_mapping(impact, "single_name_after_pct"),
            sector_before_pct=_float_from_mapping(impact, "sector_before_pct"),
            sector_after_pct=_float_from_mapping(impact, "sector_after_pct"),
            theme_before_pct=_float_from_mapping(impact, "theme_before_pct"),
            theme_after_pct=_float_from_mapping(impact, "theme_after_pct"),
            correlated_before_pct=_float_from_mapping(impact, "correlated_before_pct"),
            correlated_after_pct=_float_from_mapping(impact, "correlated_after_pct"),
            portfolio_penalty=_float_from_mapping(impact, "portfolio_penalty"),
            hard_blocks=thaw_json_value(impact.get("hard_blocks", [])),
            source_ts=source_ts,
            available_at=available_at,
            payload={
                "portfolio_impact": impact,
                "candidate": {
                    "ticker": candidate.ticker,
                    "as_of": candidate.as_of.isoformat(),
                    "feature_version": candidate.features.feature_version,
                    "setup_type": setup_type,
                    "final_score": candidate.final_score,
                    "entry_zone": list(candidate.entry_zone) if candidate.entry_zone else None,
                    "invalidation_price": candidate.invalidation_price,
                    "reward_risk": candidate.reward_risk,
                },
            },
            created_at=datetime.now(UTC),
        )
    )


def _upsert_securities(conn: Connection, rows: Iterable[Security]) -> None:
    for row in rows:
        conn.execute(delete(securities).where(securities.c.ticker == row.ticker))
        conn.execute(
            insert(securities).values(
                ticker=row.ticker,
                name=row.name,
                exchange=row.exchange,
                sector=row.sector,
                industry=row.industry,
                market_cap=row.market_cap,
                avg_dollar_volume_20d=row.avg_dollar_volume_20d,
                has_options=row.has_options,
                is_active=row.is_active,
                updated_at=row.updated_at,
                metadata=thaw_json_value(row.metadata),
            )
        )


def _upsert_daily_bars(conn: Connection, rows: Iterable[DailyBar]) -> None:
    for row in rows:
        conn.execute(
            delete(daily_bars).where(
                daily_bars.c.ticker == row.ticker,
                daily_bars.c.date == row.date,
                daily_bars.c.provider == row.provider,
            )
        )
        conn.execute(
            insert(daily_bars).values(
                ticker=row.ticker,
                date=row.date,
                provider=row.provider,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                vwap=row.vwap,
                adjusted=row.adjusted,
                source_ts=row.source_ts,
                available_at=row.available_at,
            )
        )


def _upsert_holdings(conn: Connection, rows: Iterable[HoldingSnapshot]) -> None:
    for row in rows:
        conn.execute(
            delete(holdings_snapshots).where(
                holdings_snapshots.c.ticker == row.ticker,
                holdings_snapshots.c.as_of == row.as_of,
            )
        )
        conn.execute(
            insert(holdings_snapshots).values(
                ticker=row.ticker,
                as_of=row.as_of,
                shares=row.shares,
                market_value=row.market_value,
                sector=row.sector,
                theme=row.theme,
                portfolio_value=row.portfolio_value,
                cash=row.cash,
            )
        )


def _candidate_payload(candidate: CandidateSnapshot, policy: PolicyResult) -> dict[str, Any]:
    return {
        "candidate": {
            "ticker": candidate.ticker,
            "as_of": candidate.as_of.isoformat(),
            "features": _features_payload(candidate),
            "final_score": candidate.final_score,
            "strong_pillars": candidate.strong_pillars,
            "risk_penalty": candidate.risk_penalty,
            "portfolio_penalty": candidate.portfolio_penalty,
            "data_stale": candidate.data_stale,
            "entry_zone": list(candidate.entry_zone) if candidate.entry_zone else None,
            "invalidation_price": candidate.invalidation_price,
            "reward_risk": candidate.reward_risk,
            "metadata": thaw_json_value(candidate.metadata),
        },
        "policy": {
            "state": policy.state.value,
            "hard_blocks": list(policy.hard_blocks),
            "reasons": list(policy.reasons),
            "missing_trade_plan": list(policy.missing_trade_plan),
            "policy_version": POLICY_VERSION,
        },
    }


def _features_payload(candidate: CandidateSnapshot) -> dict[str, Any]:
    payload = {}
    for field in fields(candidate.features):
        value = getattr(candidate.features, field.name)
        if isinstance(value, (date, datetime)):
            payload[field.name] = value.isoformat()
        else:
            payload[field.name] = value
    return payload
