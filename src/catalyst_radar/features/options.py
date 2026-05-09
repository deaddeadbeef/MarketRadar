from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping

OPTION_FEATURE_VERSION = "options-v1"


@dataclass(frozen=True)
class OptionFeatureInput:
    ticker: str
    as_of: datetime
    provider: str
    call_volume: float
    put_volume: float
    call_open_interest: float
    put_open_interest: float
    iv_percentile: float
    skew: float
    source_ts: datetime
    available_at: datetime
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _require_aware_utc(self.as_of, "as_of"))
        object.__setattr__(self, "provider", _required_text(self.provider, "provider"))
        object.__setattr__(
            self,
            "call_volume",
            _finite_nonnegative(self.call_volume),
        )
        object.__setattr__(self, "put_volume", _finite_nonnegative(self.put_volume))
        object.__setattr__(
            self,
            "call_open_interest",
            _finite_nonnegative(self.call_open_interest),
        )
        object.__setattr__(
            self,
            "put_open_interest",
            _finite_nonnegative(self.put_open_interest),
        )
        object.__setattr__(self, "iv_percentile", _clamp(_finite_float(self.iv_percentile), 0, 1))
        object.__setattr__(self, "skew", _finite_float(self.skew))
        object.__setattr__(self, "source_ts", _require_aware_utc(self.source_ts, "source_ts"))
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        _reject_available_before_source(self.source_ts, self.available_at)
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


@dataclass(frozen=True)
class OptionFeatureScore:
    ticker: str
    as_of: datetime
    provider: str
    call_put_ratio: float
    call_oi_ratio: float
    iv_percentile: float
    skew: float
    abnormality_score: float
    options_flow_score: float
    options_risk_score: float
    source_ts: datetime
    available_at: datetime
    payload: Mapping[str, Any]


def compute_option_feature_score(input: OptionFeatureInput) -> OptionFeatureScore:
    call_put_ratio = _ratio(input.call_volume, max(input.put_volume, 1.0))
    call_oi_ratio = _ratio(input.call_open_interest, max(input.put_open_interest, 1.0))
    abnormality_score = _abnormality_score(
        call_put_ratio=call_put_ratio,
        call_oi_ratio=call_oi_ratio,
        iv_percentile=input.iv_percentile,
    )
    options_flow_score = _options_flow_score(
        call_volume=input.call_volume,
        put_volume=input.put_volume,
        call_put_ratio=call_put_ratio,
        call_oi_ratio=call_oi_ratio,
        iv_percentile=input.iv_percentile,
    )
    options_risk_score = _options_risk_score(input.iv_percentile, input.skew)

    return OptionFeatureScore(
        ticker=input.ticker,
        as_of=input.as_of,
        provider=input.provider,
        call_put_ratio=call_put_ratio,
        call_oi_ratio=call_oi_ratio,
        iv_percentile=input.iv_percentile,
        skew=input.skew,
        abnormality_score=abnormality_score,
        options_flow_score=options_flow_score,
        options_risk_score=options_risk_score,
        source_ts=input.source_ts,
        available_at=input.available_at,
        payload=input.payload,
    )


def _abnormality_score(
    *,
    call_put_ratio: float,
    call_oi_ratio: float,
    iv_percentile: float,
) -> float:
    ratio_component = _clamp(call_put_ratio, 0, 4) / 4 * 45
    oi_component = _clamp(call_oi_ratio, 0, 4) / 4 * 35
    iv_component = _clamp(iv_percentile, 0, 1) * 20
    return _clamp(ratio_component + oi_component + iv_component, 0, 100)


def _options_flow_score(
    *,
    call_volume: float,
    put_volume: float,
    call_put_ratio: float,
    call_oi_ratio: float,
    iv_percentile: float,
) -> float:
    if call_volume <= 0 and put_volume <= 0:
        return 0.0
    ratio_component = _clamp(call_put_ratio - 1.0, 0, 3) / 3 * 70
    oi_component = _clamp(call_oi_ratio, 0, 4) / 4 * 20
    iv_component = _clamp(iv_percentile, 0, 1) * 10
    return _clamp(ratio_component + oi_component + iv_component, 0, 100)


def _options_risk_score(iv_percentile: float, skew: float) -> float:
    iv_component = _clamp(iv_percentile - 0.8, 0, 0.2) / 0.2 * 50
    skew_component = _clamp(skew - 1.0, 0, 1.0) * 60
    return _clamp(iv_component + skew_component, 0, 100)


def _ratio(numerator: float, denominator: float) -> float:
    return _finite_float(numerator / denominator)


def _required_text(value: str, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _require_aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _reject_available_before_source(source_ts: datetime, available_at: datetime) -> None:
    if available_at < source_ts:
        msg = "available_at must be greater than or equal to source_ts"
        raise ValueError(msg)


def _finite_nonnegative(value: float) -> float:
    return max(0.0, _finite_float(value))


def _finite_float(value: float, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(result):
        return default
    return result


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(upper, max(lower, _finite_float(value, default=lower)))


__all__ = [
    "OPTION_FEATURE_VERSION",
    "OptionFeatureInput",
    "OptionFeatureScore",
    "compute_option_feature_score",
]
