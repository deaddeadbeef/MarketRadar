from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping


class SetupType(StrEnum):
    BREAKOUT = "breakout"
    PULLBACK = "pullback"
    POST_EARNINGS = "post_earnings"
    SECTOR_ROTATION = "sector_rotation"
    FILINGS_CATALYST = "filings_catalyst"
    MARKET_MOMENTUM = "market_momentum"


@dataclass(frozen=True)
class SetupPlan:
    setup_type: SetupType
    entry_zone: tuple[float, float] | None
    invalidation_price: float | None
    target_price: float | None
    reward_risk: float
    chase_block: bool
    reasons: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata, "metadata"))
