from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from sqlalchemy import Engine, delete, insert, select

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.features.options import (
    OptionFeatureInput,
    compute_option_feature_score,
)
from catalyst_radar.storage.schema import option_features


class FeatureRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_option_features(self, rows: Iterable[OptionFeatureInput]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                row_id = _option_feature_id(row)
                conn.execute(delete(option_features).where(option_features.c.id == row_id))
                conn.execute(insert(option_features).values(**_option_feature_row(row, row_id)))
                count += 1
        return count

    def latest_option_features_by_ticker(
        self,
        tickers: Iterable[str],
        as_of: datetime,
        available_at: datetime,
    ) -> dict[str, OptionFeatureInput]:
        normalized = sorted({ticker.upper() for ticker in tickers if ticker.strip()})
        if not normalized:
            return {}
        stmt = (
            select(option_features)
            .where(
                option_features.c.ticker.in_(normalized),
                option_features.c.as_of <= _to_utc_datetime(as_of, "as_of"),
                option_features.c.available_at <= _to_utc_datetime(
                    available_at,
                    "available_at",
                ),
            )
            .order_by(
                option_features.c.ticker,
                option_features.c.as_of.desc(),
                option_features.c.available_at.desc(),
                option_features.c.source_ts.desc(),
                option_features.c.provider,
                option_features.c.id.desc(),
            )
        )
        result: dict[str, OptionFeatureInput] = {}
        with self.engine.connect() as conn:
            for row in conn.execute(stmt):
                feature = _option_feature_from_row(row._mapping)
                result.setdefault(feature.ticker, feature)
        return result


def _option_feature_row(row: OptionFeatureInput, row_id: str) -> dict[str, Any]:
    score = compute_option_feature_score(row)
    return {
        "id": row_id,
        "ticker": row.ticker,
        "as_of": row.as_of,
        "provider": row.provider,
        "call_volume": row.call_volume,
        "put_volume": row.put_volume,
        "call_open_interest": row.call_open_interest,
        "put_open_interest": row.put_open_interest,
        "iv_percentile": row.iv_percentile,
        "skew": row.skew,
        "abnormality_score": score.abnormality_score,
        "source_ts": row.source_ts,
        "available_at": row.available_at,
        "payload": thaw_json_value(row.payload),
        "created_at": datetime.now(UTC),
    }


def _option_feature_from_row(row: Any) -> OptionFeatureInput:
    return OptionFeatureInput(
        ticker=row["ticker"],
        as_of=_as_datetime(row["as_of"]),
        provider=row["provider"],
        call_volume=row["call_volume"],
        put_volume=row["put_volume"],
        call_open_interest=row["call_open_interest"],
        put_open_interest=row["put_open_interest"],
        iv_percentile=row["iv_percentile"],
        skew=row["skew"],
        source_ts=_as_datetime(row["source_ts"]),
        available_at=_as_datetime(row["available_at"]),
        payload=row["payload"],
    )


def _option_feature_id(row: OptionFeatureInput) -> str:
    key = "|".join((row.provider, row.ticker, row.as_of.isoformat()))
    return sha256(key.encode("utf-8")).hexdigest()


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _as_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["FeatureRepository"]
