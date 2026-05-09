from __future__ import annotations

from datetime import datetime

from catalyst_radar.validation.outcomes import label_forward_return as _label_forward_return


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
    max_10d_price: float,
    max_20d_price: float,
    max_60d_price: float,
    sector_return: float,
) -> dict[str, bool]:
    return _label_forward_return(
        entry_price,
        max_10d_price,
        max_20d_price,
        max_60d_price,
        sector_return,
    )
