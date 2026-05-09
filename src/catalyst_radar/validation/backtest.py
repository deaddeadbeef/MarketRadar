from __future__ import annotations

from datetime import datetime


def assert_available_at_or_before_decision(
    available_at: datetime,
    decision_at: datetime,
) -> None:
    if available_at > decision_at:
        raise ValueError(
            f"future leakage: available_at={available_at.isoformat()} "
            f"is after decision_at={decision_at.isoformat()}"
        )


def label_forward_return(
    entry_price: float,
    max_forward_price: float,
    sector_return: float,
) -> dict[str, bool]:
    forward_return = (max_forward_price / entry_price) - 1
    return {
        "target_10d_15": forward_return >= 0.15,
        "target_20d_25": forward_return >= 0.25,
        "target_60d_40": forward_return >= 0.40,
        "sector_outperformance": (forward_return - sector_return) >= 0.20,
    }
