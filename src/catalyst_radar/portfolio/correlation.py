from __future__ import annotations

import math
from typing import Any


def correlated_exposure_notional(
    current_positions: dict[str, dict[str, Any]],
    sector: str,
    theme: str,
) -> float:
    exposure = 0.0
    for position in current_positions.values():
        notional = _notional(position)
        if notional <= 0:
            continue
        sector_matches = position.get("sector") == sector
        theme_matches = position.get("theme") == theme
        if sector_matches and theme_matches:
            exposure += notional
        elif sector_matches or theme_matches:
            exposure += notional * 0.5
    return exposure


def _notional(position: dict[str, Any]) -> float:
    try:
        value = float(position.get("notional", 0.0))
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(value) or value <= 0:
        return 0.0
    return value
