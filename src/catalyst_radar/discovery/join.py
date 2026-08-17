"""Event-time price join for discovery.

A row is joined only when mapped-ticker bars reach the event window and are
fresh enough to observe a reaction. Latest candidate_states rows are ignored.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine

from catalyst_radar.core.models import DailyBar
from catalyst_radar.features.market import compute_market_features
from catalyst_radar.storage.schema import daily_bars

JOIN_SCHEMA = "discovery-event-join-v1"
JOIN_LOOKBACK_BARS = 80
EVENT_WINDOW_GRACE_DAYS = 2
CURRENT_STALE_DAYS = 7
MIN_BARS = 2
SPY_TICKER = "SPY"


@dataclass(frozen=True)
class EventJoin:
    join_status: str
    priced_in_status: str
    reaction_score: float
    emotion_reaction_gap: float
    ret_5d_pct: float | None
    ret_20d_pct: float | None
    last_bar_date: str | None
    bar_count: int
    reason: str
    ret_since_event_pct: float | None = None
    post_event_bar_count: int = 0

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": JOIN_SCHEMA,
            "join_status": self.join_status,
            "priced_in_status": self.priced_in_status,
            "reaction_score": self.reaction_score,
            "emotion_reaction_gap": self.emotion_reaction_gap,
            "ret_5d_pct": self.ret_5d_pct,
            "ret_20d_pct": self.ret_20d_pct,
            "ret_since_event_pct": self.ret_since_event_pct,
            "post_event_bar_count": self.post_event_bar_count,
            "last_bar_date": self.last_bar_date,
            "bar_count": self.bar_count,
            "reason": self.reason,
        }


def missing_join(*, reason: str, emotion_score: float) -> EventJoin:
    return EventJoin(
        join_status="missing_scan",
        priced_in_status="unknown",
        reaction_score=0.0,
        emotion_reaction_gap=round(float(emotion_score), 2),
        ret_5d_pct=None,
        ret_20d_pct=None,
        last_bar_date=None,
        bar_count=0,
        reason=reason,
    )


def no_db_join(*, emotion_score: float) -> EventJoin:
    return EventJoin(
        join_status="no_db",
        priced_in_status="unknown",
        reaction_score=0.0,
        emotion_reaction_gap=round(float(emotion_score), 2),
        ret_5d_pct=None,
        ret_20d_pct=None,
        last_bar_date=None,
        bar_count=0,
        reason="No local database attached.",
    )


def load_bars_by_ticker(
    engine: Engine,
    tickers: Sequence[str],
    *,
    end: date,
    lookback: int = JOIN_LOOKBACK_BARS,
) -> dict[str, list[DailyBar]]:
    symbols = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
    if SPY_TICKER not in symbols:
        symbols.append(SPY_TICKER)
    if not symbols:
        return {}
    stmt = (
        select(daily_bars)
        .where(daily_bars.c.ticker.in_(symbols), daily_bars.c.date <= end)
        .order_by(daily_bars.c.ticker, daily_bars.c.date.desc())
    )
    grouped: dict[str, list[DailyBar]] = {}
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            ticker = str(row.ticker).upper()
            bucket = grouped.setdefault(ticker, [])
            if len(bucket) >= lookback:
                continue
            bucket.append(
                DailyBar(
                    ticker=ticker,
                    date=_as_date(row.date),
                    open=float(row.open),
                    high=float(row.high),
                    low=float(row.low),
                    close=float(row.close),
                    volume=int(row.volume),
                    vwap=float(row.vwap or 0.0),
                    adjusted=bool(row.adjusted),
                    provider=str(row.provider or "unknown"),
                    source_ts=row.source_ts,
                    available_at=row.available_at,
                )
            )
    for ticker, rows in grouped.items():
        grouped[ticker] = list(reversed(rows))
    return grouped


def join_event_ticker(
    *,
    ticker: str,
    event_available_at: datetime,
    event_direction: str,
    emotion_score: float,
    bars_by_ticker: Mapping[str, Sequence[DailyBar]],
    now: datetime | None = None,
) -> EventJoin:
    clock = now if now is not None else datetime.now(tz=UTC)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=UTC)
    else:
        clock = clock.astimezone(UTC)
    event_at = event_available_at
    if event_at.tzinfo is None:
        event_at = event_at.replace(tzinfo=UTC)
    else:
        event_at = event_at.astimezone(UTC)

    symbol = str(ticker or "").strip().upper()
    rows = list(bars_by_ticker.get(symbol) or ())
    if len(rows) < MIN_BARS:
        return missing_join(
            reason="Fewer than two local daily bars for this ticker.",
            emotion_score=emotion_score,
        )

    last_bar_date = rows[-1].date
    event_date = event_at.date()
    if last_bar_date < event_date - timedelta(days=EVENT_WINDOW_GRACE_DAYS):
        return EventJoin(
            join_status="missing_scan",
            priced_in_status="unknown",
            reaction_score=0.0,
            emotion_reaction_gap=round(float(emotion_score), 2),
            ret_5d_pct=None,
            ret_20d_pct=None,
            last_bar_date=last_bar_date.isoformat(),
            bar_count=len(rows),
            reason=(
                f"Last bar {last_bar_date.isoformat()} is before event "
                f"{event_date.isoformat()}; cannot observe post-event reaction."
            ),
        )
    if last_bar_date < clock.date() - timedelta(days=CURRENT_STALE_DAYS):
        return EventJoin(
            join_status="missing_scan",
            priced_in_status="stale",
            reaction_score=0.0,
            emotion_reaction_gap=round(float(emotion_score), 2),
            ret_5d_pct=None,
            ret_20d_pct=None,
            last_bar_date=last_bar_date.isoformat(),
            bar_count=len(rows),
            reason=(
                f"Last bar {last_bar_date.isoformat()} is older than "
                f"{CURRENT_STALE_DAYS} days; treating as missing_scan."
            ),
        )

    as_of = datetime(last_bar_date.year, last_bar_date.month, last_bar_date.day, 21, tzinfo=UTC)
    ticker_frame = _bars_frame(rows)
    spy_rows = list(bars_by_ticker.get(SPY_TICKER) or rows)
    spy_frame = _bars_frame(spy_rows)
    features = compute_market_features(
        symbol,
        as_of,
        ticker_frame,
        spy_frame,
        ticker_frame,
    )
    direction = _direction_sign(event_direction)
    reaction = _reaction_from_returns(
        ret_5d=features.ret_5d,
        ret_20d=features.ret_20d,
        direction=direction,
    )
    gap = float(emotion_score) - reaction
    status = _priced_status(
        emotion_score=float(emotion_score),
        reaction_score=reaction,
        gap=gap,
        direction=direction,
    )
    since_event, post_count = _return_since_event(rows, event_date)
    return EventJoin(
        join_status="joined",
        priced_in_status=status,
        reaction_score=round(reaction, 2),
        emotion_reaction_gap=round(gap, 2),
        ret_5d_pct=round(features.ret_5d * 100.0, 2),
        ret_20d_pct=round(features.ret_20d * 100.0, 2),
        last_bar_date=last_bar_date.isoformat(),
        bar_count=len(rows),
        reason="Event-window bars used for reaction; candidate_states ignored.",
        ret_since_event_pct=since_event,
        post_event_bar_count=post_count,
    )


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _bars_frame(rows: Sequence[DailyBar]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [row.date for row in rows],
            "open": [row.open for row in rows],
            "high": [row.high for row in rows],
            "low": [row.low for row in rows],
            "close": [row.close for row in rows],
            "volume": [row.volume for row in rows],
            "vwap": [row.vwap for row in rows],
        }
    )


def _return_since_event(
    rows: Sequence[DailyBar],
    event_date: date,
) -> tuple[float | None, int]:
    post = [row for row in rows if row.date >= event_date]
    if len(post) < 2:
        return None, len(post)
    first = float(post[0].close)
    last = float(post[-1].close)
    if first == 0.0:
        return None, len(post)
    return round((last / first - 1.0) * 100.0, 2), len(post)


def _direction_sign(direction: str) -> int:
    text = str(direction or "mixed").casefold()
    if text == "bearish":
        return -1
    return 1


def _reaction_from_returns(*, ret_5d: float, ret_20d: float, direction: int) -> float:
    directional_5d = direction * float(ret_5d)
    directional_20d = direction * float(ret_20d)
    score = max(0.0, min(100.0, (directional_5d * 280.0) + (directional_20d * 170.0)))
    return score


def _priced_status(
    *,
    emotion_score: float,
    reaction_score: float,
    gap: float,
    direction: int,
) -> str:
    label = "bearish_not_priced_in" if direction < 0 else "bullish_not_priced_in"
    if emotion_score < 25.0 and reaction_score < 30.0:
        return "neutral"
    if reaction_score >= 55.0 and reaction_score >= emotion_score + 20.0:
        return "overextended_hype"
    if emotion_score >= 55.0 and gap >= 20.0:
        return label
    if emotion_score >= 45.0 and gap >= 10.0:
        return label
    if emotion_score >= 45.0 and reaction_score >= max(40.0, emotion_score - 10.0):
        return "fully_priced"
    if reaction_score >= 60.0 and emotion_score < 45.0:
        return "overextended_hype"
    return "neutral"
