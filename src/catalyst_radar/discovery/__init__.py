"""ACTIVE product core: event-first discovery.

World events → equity map → priced-in/reaction join → case → proof labels.
Canonical scope: docs/PRODUCT_SCOPE.md.
"""

from catalyst_radar.discovery.brief import build_discovery_brief, load_world_events
from catalyst_radar.discovery.models import WorldEvent, WorldEventBundle

__all__ = [
    "WorldEvent",
    "WorldEventBundle",
    "build_discovery_brief",
    "load_world_events",
]
