from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, is_dataclass
from typing import Any


@dataclass(frozen=True)
class OutcomeLabels:
    """Forward outcome labels for one simulated candidate or paper trade."""

    target_10d_15: bool
    target_20d_25: bool
    target_60d_40: bool
    sector_outperformance: bool
    max_adverse_excursion: float
    max_favorable_excursion: float
    invalidated: bool

    def as_dict(self) -> dict[str, bool | float]:
        return {
            "target_10d_15": self.target_10d_15,
            "target_20d_25": self.target_20d_25,
            "target_60d_40": self.target_60d_40,
            "sector_outperformance": self.sector_outperformance,
            "max_adverse_excursion": self.max_adverse_excursion,
            "max_favorable_excursion": self.max_favorable_excursion,
            "invalidated": self.invalidated,
        }


@dataclass(frozen=True)
class _PricePoint:
    close: float
    high: float
    low: float


def label_forward_return(
    entry_price: float,
    max_10d_price: float,
    max_20d_price: float,
    max_60d_price: float,
    sector_return: float,
) -> dict[str, bool]:
    """Compatibility helper matching the original backtest label payload."""

    entry = _positive_price(entry_price, "entry_price")
    forward_10d_return = (float(max_10d_price) / entry) - 1
    forward_20d_return = (float(max_20d_price) / entry) - 1
    forward_60d_return = (float(max_60d_price) / entry) - 1
    return {
        "target_10d_15": forward_10d_return >= 0.15,
        "target_20d_25": forward_20d_return >= 0.25,
        "target_60d_40": forward_60d_return >= 0.40,
        "sector_outperformance": (forward_60d_return - float(sector_return)) >= 0.20,
    }


def compute_forward_outcomes(
    entry_price: float,
    future_prices: Sequence[Any] | Iterable[Any],
    sector_future_prices: Sequence[Any] | Iterable[Any] | None = None,
    invalidation_price: float | None = None,
) -> OutcomeLabels:
    """Compute target, excursion, invalidation, and sector labels from future prices.

    Price inputs may be floats or dict/dataclass-like rows with close, high, and low fields.
    The function is point-in-time pure: callers supply the already selected future window.
    """

    entry = _positive_price(entry_price, "entry_price")
    points = _price_points(future_prices)
    sector_points = _price_points(sector_future_prices or ())

    return_10d = _max_return(entry, points[:10])
    return_20d = _max_return(entry, points[:20])
    return_60d = _max_return(entry, points[:60])
    sector_return_60d = _sector_return(sector_points[:60])
    sector_outperformance = (
        sector_return_60d is not None and (return_60d - sector_return_60d) >= 0.20
    )

    mae = _min_return(entry, points)
    mfe = _max_return(entry, points)
    invalidated = _invalidated(points, invalidation_price)

    return OutcomeLabels(
        target_10d_15=return_10d >= 0.15,
        target_20d_25=return_20d >= 0.25,
        target_60d_40=return_60d >= 0.40,
        sector_outperformance=sector_outperformance,
        max_adverse_excursion=mae,
        max_favorable_excursion=mfe,
        invalidated=invalidated,
    )


def outcome_labels_as_dict(labels: OutcomeLabels | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(labels, OutcomeLabels):
        return labels.as_dict()
    return {str(key): value for key, value in labels.items()}


def _price_points(values: Sequence[Any] | Iterable[Any]) -> tuple[_PricePoint, ...]:
    points = []
    for value in values:
        point = _price_point(value)
        if point is not None:
            points.append(point)
    return tuple(points)


def _price_point(value: Any) -> _PricePoint | None:
    if isinstance(value, (int, float)):
        close = _finite_float(value)
        if close is None:
            return None
        return _PricePoint(close=close, high=close, low=close)

    close = _first_float(value, "close", "adj_close", "price", "last", "value")
    if close is None:
        return None
    high = _first_float(value, "high", "close", "price", "last", "value") or close
    low = _first_float(value, "low", "close", "price", "last", "value") or close
    return _PricePoint(close=close, high=high, low=low)


def _max_return(entry_price: float, points: Sequence[_PricePoint]) -> float:
    if not points:
        return 0.0
    return (max(point.high for point in points) / entry_price) - 1


def _min_return(entry_price: float, points: Sequence[_PricePoint]) -> float:
    if not points:
        return 0.0
    return (min(point.low for point in points) / entry_price) - 1


def _sector_return(points: Sequence[_PricePoint]) -> float | None:
    if len(points) < 2:
        return None
    start = points[0].close
    if start <= 0:
        return None
    return (points[-1].close / start) - 1


def _invalidated(points: Sequence[_PricePoint], invalidation_price: float | None) -> bool:
    invalidation = _finite_float(invalidation_price)
    if invalidation is None or invalidation <= 0:
        return False
    return any(point.low <= invalidation for point in points)


def _positive_price(value: Any, field_name: str) -> float:
    number = _finite_float(value)
    if number is None or number <= 0:
        msg = f"{field_name} must be a positive finite price"
        raise ValueError(msg)
    return number


def _first_float(row: Any, *names: str) -> float | None:
    for name in names:
        value = _read(row, name)
        number = _finite_float(value)
        if number is not None:
            return number
    return None


def _read(source: Any, key: str) -> Any:
    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(key)
    if is_dataclass(source) and not isinstance(source, type):
        return getattr(source, key, None)
    keys = getattr(source, "keys", None)
    if callable(keys):
        try:
            return source[key]
        except (KeyError, TypeError):
            return None
    return getattr(source, key, None)


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


__all__ = [
    "OutcomeLabels",
    "compute_forward_outcomes",
    "label_forward_return",
    "outcome_labels_as_dict",
]
