from __future__ import annotations

import pytest

from catalyst_radar.validation.outcomes import (
    compute_forward_outcomes,
    label_forward_return,
    outcome_labels_as_dict,
)


def test_compute_forward_outcomes_labels_targets_excursions_and_invalidation() -> None:
    future_prices = _future_prices(
        max_by_day={5: 116.0, 18: 126.0, 55: 141.0},
        min_by_day={2: 96.0, 7: 92.0},
    )
    sector_prices = [{"close": 100.0 + (15.0 * index / 59)} for index in range(60)]

    labels = compute_forward_outcomes(
        entry_price=100,
        future_prices=future_prices,
        sector_future_prices=sector_prices,
        invalidation_price=94,
    )

    assert labels.target_10d_15 is True
    assert labels.target_20d_25 is True
    assert labels.target_60d_40 is True
    assert labels.sector_outperformance is True
    assert labels.max_adverse_excursion == pytest.approx(-0.08)
    assert labels.max_favorable_excursion == pytest.approx(0.41)
    assert labels.invalidated is True


def test_compute_forward_outcomes_is_horizon_specific() -> None:
    future_prices = _future_prices(
        max_by_day={5: 114.0, 18: 124.0, 55: 139.0},
        min_by_day={7: 95.0},
    )
    sector_prices = [{"close": 100.0 + (5.0 * index / 59)} for index in range(60)]

    labels = compute_forward_outcomes(
        entry_price=100,
        future_prices=future_prices,
        sector_future_prices=sector_prices,
        invalidation_price=90,
    )

    assert labels.target_10d_15 is False
    assert labels.target_20d_25 is False
    assert labels.target_60d_40 is False
    assert labels.sector_outperformance is True
    assert labels.invalidated is False


def test_compute_forward_outcomes_accepts_float_prices_and_handles_missing_sector() -> None:
    labels = compute_forward_outcomes(
        entry_price=100,
        future_prices=[101, 104, 99],
        sector_future_prices=[],
        invalidation_price=None,
    )

    assert labels.max_favorable_excursion == pytest.approx(0.04)
    assert labels.max_adverse_excursion == pytest.approx(-0.01)
    assert labels.sector_outperformance is False
    assert outcome_labels_as_dict(labels)["invalidated"] is False


def test_compute_forward_outcomes_rejects_non_positive_entry() -> None:
    with pytest.raises(ValueError, match="entry_price"):
        compute_forward_outcomes(entry_price=0, future_prices=[101])


def test_label_forward_return_matches_backtest_compatibility_payload() -> None:
    labels = label_forward_return(
        entry_price=100,
        max_10d_price=116,
        max_20d_price=124,
        max_60d_price=141,
        sector_return=0.22,
    )

    assert labels == {
        "target_10d_15": True,
        "target_20d_25": False,
        "target_60d_40": True,
        "sector_outperformance": False,
    }


def _future_prices(
    *,
    max_by_day: dict[int, float],
    min_by_day: dict[int, float],
) -> list[dict[str, float]]:
    prices = []
    for index in range(60):
        base = 100.0 + (index * 0.1)
        prices.append(
            {
                "close": base,
                "high": max_by_day.get(index, base),
                "low": min_by_day.get(index, base),
            }
        )
    return prices
