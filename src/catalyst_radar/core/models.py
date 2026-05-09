from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping


class ActionState(StrEnum):
    NO_ACTION = "NoAction"
    RESEARCH_ONLY = "ResearchOnly"
    ADD_TO_WATCHLIST = "AddToWatchlist"
    WARNING = "Warning"
    ELIGIBLE_FOR_MANUAL_BUY_REVIEW = "EligibleForManualBuyReview"
    BLOCKED = "Blocked"
    THESIS_WEAKENING = "ThesisWeakening"
    EXIT_INVALIDATE_REVIEW = "ExitInvalidateReview"


class DataQualitySeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class JobStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"


@dataclass(frozen=True)
class Security:
    ticker: str
    name: str
    exchange: str
    sector: str
    industry: str
    market_cap: float
    avg_dollar_volume_20d: float
    has_options: bool
    is_active: bool
    updated_at: datetime
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata, "metadata"))


@dataclass(frozen=True)
class DailyBar:
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float
    adjusted: bool
    provider: str
    source_ts: datetime
    available_at: datetime


@dataclass(frozen=True)
class HoldingSnapshot:
    ticker: str
    shares: float
    market_value: float
    sector: str
    theme: str
    as_of: datetime
    portfolio_value: float = 0.0
    cash: float = 0.0


@dataclass(frozen=True)
class MarketFeatures:
    ticker: str
    as_of: datetime
    ret_5d: float
    ret_20d: float
    rs_20_sector: float
    rs_60_spy: float
    near_52w_high: float
    ma_regime: float
    rel_volume_5d: float
    dollar_volume_z: float
    atr_pct: float
    extension_20d: float
    liquidity_score: float
    feature_version: str


@dataclass(frozen=True)
class PortfolioImpact:
    ticker: str
    single_name_after_pct: float
    sector_after_pct: float
    theme_after_pct: float
    portfolio_penalty: float
    hard_blocks: tuple[str, ...] = ()


@dataclass(frozen=True)
class CandidateSnapshot:
    ticker: str
    as_of: datetime
    features: MarketFeatures
    final_score: float
    strong_pillars: int
    risk_penalty: float
    portfolio_penalty: float
    data_stale: bool
    entry_zone: tuple[float, float] | None = None
    invalidation_price: float | None = None
    reward_risk: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata, "metadata"))


@dataclass(frozen=True)
class PolicyResult:
    state: ActionState
    hard_blocks: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    missing_trade_plan: tuple[str, ...] = ()

    @property
    def is_blocked(self) -> bool:
        return self.state == ActionState.BLOCKED
