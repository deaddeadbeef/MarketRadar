from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime

from sqlalchemy import Engine, delete, insert, select

from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.storage.schema import daily_bars, securities


class MarketRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_securities(self, rows: Iterable[Security]) -> None:
        with self.engine.begin() as conn:
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
                    )
                )

    def upsert_daily_bars(self, rows: Iterable[DailyBar]) -> None:
        with self.engine.begin() as conn:
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
                )
                for row in conn.execute(stmt)
            ]

    def daily_bars(self, ticker: str, end: date, lookback: int) -> list[DailyBar]:
        stmt = (
            select(daily_bars)
            .where(daily_bars.c.ticker == ticker, daily_bars.c.date <= end)
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


def _as_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
