"""DEPRECATED as product hero: full-universe seed / liquid universe builder.

Discovery runs on mapped tickers from world events. See docs/PRODUCT_SCOPE.md.
"""

from catalyst_radar.universe.builder import UniverseBuilder, UniverseSnapshotResult
from catalyst_radar.universe.filters import (
    UniverseDecision,
    UniverseFilterConfig,
    evaluate_universe_member,
)
from catalyst_radar.universe.seed import UniverseSeedResult, seed_polygon_tickers

__all__ = [
    "UniverseBuilder",
    "UniverseDecision",
    "UniverseFilterConfig",
    "UniverseSeedResult",
    "UniverseSnapshotResult",
    "evaluate_universe_member",
    "seed_polygon_tickers",
]
