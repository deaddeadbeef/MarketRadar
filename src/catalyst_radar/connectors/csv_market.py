from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from catalyst_radar.core.models import DailyBar, HoldingSnapshot, Security


def load_securities_csv(path: str | Path) -> list[Security]:
    frame = pd.read_csv(path)
    rows: list[Security] = []
    for record in frame.to_dict(orient="records"):
        rows.append(
            Security(
                ticker=str(record["ticker"]).upper(),
                name=str(record["name"]),
                exchange=str(record["exchange"]),
                sector=str(record["sector"]),
                industry=str(record["industry"]),
                market_cap=float(record["market_cap"]),
                avg_dollar_volume_20d=float(record["avg_dollar_volume_20d"]),
                has_options=_to_bool(record["has_options"], "has_options"),
                is_active=_to_bool(record["is_active"], "is_active"),
                updated_at=_to_utc_datetime(record["updated_at"]),
            )
        )
    return rows


def load_daily_bars_csv(path: str | Path) -> list[DailyBar]:
    frame = pd.read_csv(path)
    rows: list[DailyBar] = []
    for record in frame.to_dict(orient="records"):
        rows.append(
            DailyBar(
                ticker=str(record["ticker"]).upper(),
                date=pd.Timestamp(record["date"]).date(),
                open=float(record["open"]),
                high=float(record["high"]),
                low=float(record["low"]),
                close=float(record["close"]),
                volume=int(record["volume"]),
                vwap=float(record["vwap"]),
                adjusted=_to_bool(record["adjusted"], "adjusted"),
                provider=str(record["provider"]),
                source_ts=_to_utc_datetime(record["source_ts"]),
                available_at=_to_utc_datetime(record["available_at"]),
            )
        )
    return rows


def load_holdings_csv(path: str | Path) -> list[HoldingSnapshot]:
    frame = pd.read_csv(path)
    rows: list[HoldingSnapshot] = []
    for record in frame.to_dict(orient="records"):
        rows.append(
            HoldingSnapshot(
                ticker=str(record["ticker"]).upper(),
                shares=float(record["shares"]),
                market_value=float(record["market_value"]),
                sector=str(record["sector"]),
                theme=str(record["theme"]),
                as_of=_to_utc_datetime(record["as_of"]),
                portfolio_value=_optional_float(record, "portfolio_value"),
                cash=_optional_float(record, "cash"),
            )
        )
    return rows


def _to_bool(value: object, field: str | None = None) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    field_context = f" for {field}" if field is not None else ""
    raise ValueError(f"Invalid boolean value{field_context}: {value!r}")


def _to_utc_datetime(value: object) -> datetime:
    parsed = pd.Timestamp(value).to_pydatetime()
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_float(record: dict[str, object], field: str) -> float:
    value = record.get(field)
    if value is None:
        return 0.0
    try:
        if bool(pd.isna(value)):
            return 0.0
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and not value.strip():
        return 0.0
    return float(value)
