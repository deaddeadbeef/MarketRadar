from __future__ import annotations

from datetime import UTC, datetime

import pytest

from catalyst_radar.validation.baselines import (
    BaselineCandidate,
    event_only_watchlist,
    random_eligible_universe,
    sector_relative_momentum,
    spy_relative_momentum,
    user_watchlist,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)


def test_spy_relative_momentum_ranks_by_stored_20d_and_60d_returns() -> None:
    rows = [
        _row("BBB", ret_20d=0.12, ret_60d=0.10, spy_return_20d=0.02, spy_return_60d=0.05),
        _row("AAA", ret_20d=0.10, ret_60d=0.25, spy_return_20d=0.02, spy_return_60d=0.05),
        _row("CCC", ret_20d=0.80, ret_60d=0.80, hard_blocks=["liquidity"]),
        _row("DDD"),
    ]

    candidates = spy_relative_momentum(rows)

    assert [candidate.ticker for candidate in candidates] == ["AAA", "BBB"]
    assert [candidate.rank for candidate in candidates] == [1, 2]
    assert candidates[0].score == 0.28
    assert candidates[0].payload["relative_return_60d"] == 0.20


def test_sector_relative_momentum_ranks_by_sector_relative_score() -> None:
    rows = [
        _row("AAA", payload={"candidate": {"features": {"rs_20_sector": 55.0}}}),
        _row("BBB", sector_relative_score=82.0),
        _row("CCC", sector_relative_score=99.0, leakage_flags=["future_packet"]),
    ]

    candidates = sector_relative_momentum(rows)

    assert [candidate.ticker for candidate in candidates] == ["BBB", "AAA"]
    assert candidates[0].baseline == "sector_relative_momentum"
    assert candidates[0].score == 82.0


def test_event_only_watchlist_includes_material_event_support() -> None:
    rows = [
        _row("AAA", event_support_score=12.0),
        _row("BBB", payload={"metadata": {"material_event_count": 2}}),
        _row(
            "CCC",
            payload={
                "events": [
                    {"materiality": 0.8, "source_quality": 0.9},
                    {"materiality": 0.2, "source_quality": 0.9},
                ]
            },
        ),
        _row("DDD"),
    ]

    candidates = event_only_watchlist(rows)

    assert [candidate.ticker for candidate in candidates] == ["CCC", "AAA", "BBB"]
    assert candidates[0].score == pytest.approx(72.0)
    assert candidates[2].payload["material_event_count"] == 2.0


def test_random_eligible_universe_is_deterministic_with_seed() -> None:
    rows = [_row("AAA"), _row("BBB"), _row("CCC"), _row("DDD", eligible=False)]

    first = random_eligible_universe(rows, seed=123, limit=2)
    second = random_eligible_universe(reversed(rows), seed=123, limit=2)

    assert [(item.ticker, item.score) for item in first] == [
        (item.ticker, item.score) for item in second
    ]
    assert len(first) == 2
    assert all(item.baseline == "random_eligible_universe" for item in first)


def test_user_watchlist_returns_configured_present_tickers_and_empty_without_config() -> None:
    rows = [_row("AAA"), _row("BBB"), _row("CCC")]

    candidates = user_watchlist(rows, config={"watchlist": ["ccc", "missing", "aaa"]})

    assert [candidate.ticker for candidate in candidates] == ["CCC", "AAA"]
    assert [candidate.rank for candidate in candidates] == [1, 2]
    assert user_watchlist(rows) == ()


def test_baseline_candidate_freezes_payload_and_uppercases_ticker() -> None:
    candidate = BaselineCandidate(
        baseline="demo",
        ticker="msft",
        as_of=AS_OF,
        rank=1,
        score=1.0,
        reason="test",
        payload={"nested": {"value": 1}},
    )

    assert candidate.ticker == "MSFT"
    assert candidate.payload["nested"]["value"] == 1


def _row(ticker: str, **values: object) -> dict[str, object]:
    return {"ticker": ticker, "as_of": AS_OF, **values}
