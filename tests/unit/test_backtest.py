from datetime import UTC, datetime

import pytest

from catalyst_radar.validation.backtest import (
    assert_available_at_or_before_decision,
    label_forward_return,
)


def test_availability_check_accepts_past_available_record() -> None:
    assert_available_at_or_before_decision(
        available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
        decision_at=datetime(2026, 5, 9, 13, 30, tzinfo=UTC),
    )


def test_availability_check_rejects_future_record() -> None:
    with pytest.raises(ValueError, match="future leakage"):
        assert_available_at_or_before_decision(
            available_at=datetime(2026, 5, 9, 14, tzinfo=UTC),
            decision_at=datetime(2026, 5, 9, 13, 30, tzinfo=UTC),
        )


def test_forward_return_labels() -> None:
    labels = label_forward_return(
        entry_price=100,
        max_10d_price=116,
        max_20d_price=126,
        max_60d_price=141,
        sector_return=0.02,
    )

    assert labels["target_10d_15"] is True
    assert labels["target_20d_25"] is True
    assert labels["target_60d_40"] is True
    assert labels["sector_outperformance"] is True


def test_forward_return_labels_are_horizon_specific() -> None:
    labels = label_forward_return(
        entry_price=100,
        max_10d_price=116,
        max_20d_price=124,
        max_60d_price=141,
        sector_return=0.22,
    )

    assert labels["target_10d_15"] is True
    assert labels["target_20d_25"] is False
    assert labels["target_60d_40"] is True
    assert labels["sector_outperformance"] is False
