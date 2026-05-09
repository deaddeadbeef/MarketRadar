import math

from catalyst_radar.portfolio.risk import (
    PortfolioPolicy,
    compute_position_size,
    evaluate_portfolio_impact,
)


def test_compute_position_size_caps_by_risk_budget() -> None:
    size = compute_position_size(
        account_equity=100_000,
        entry_price=100,
        invalidation_price=94,
        policy=PortfolioPolicy(max_position_pct=0.20, risk_per_trade_pct=0.01),
    )

    assert size.shares == 166
    assert size.notional == 16_600
    assert size.position_pct == 0.166
    assert size.is_capped is False


def test_compute_position_size_caps_by_max_position_pct() -> None:
    size = compute_position_size(
        account_equity=100_000,
        entry_price=100,
        invalidation_price=98,
        policy=PortfolioPolicy(max_position_pct=0.10, risk_per_trade_pct=0.01),
    )

    assert size.shares == 100
    assert size.notional == 10_000
    assert size.position_pct == 0.10
    assert size.is_capped is True


def test_compute_position_size_returns_safe_output_for_non_finite_entry() -> None:
    for entry_price in (float("nan"), float("inf")):
        size = compute_position_size(
            account_equity=100_000,
            entry_price=entry_price,
            invalidation_price=94,
        )

        assert size.shares == 0
        assert size.notional == 0.0
        assert size.position_pct == 0.0
        assert size.risk_amount == 0.0
        assert math.isfinite(size.notional)
        assert math.isfinite(size.position_pct)
        assert math.isfinite(size.risk_amount)


def test_evaluate_portfolio_impact_blocks_sector_overexposure() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="AI",
        account_equity=100_000,
        current_positions={
            "BBB": {"notional": 24_000, "sector": "Technology", "theme": "AI"},
            "CCC": {"notional": 8_000, "sector": "Healthcare", "theme": "Defensive"},
        },
        proposed_notional=10_000,
        policy=PortfolioPolicy(max_sector_pct=0.30, max_theme_pct=0.40),
    )

    assert impact.sector_after_pct == 0.34
    assert "sector_exposure_hard_block" in impact.hard_blocks
    assert impact.portfolio_penalty > 0


def test_evaluate_portfolio_impact_blocks_non_finite_existing_exposure() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="AI",
        account_equity=100_000,
        current_positions={
            "AAA": {"notional": float("inf"), "sector": "Technology", "theme": "AI"},
            "BBB": {"notional": float("nan"), "sector": "Technology", "theme": "AI"},
        },
        proposed_notional=5_000,
    )

    assert "invalid_portfolio_input" in impact.hard_blocks
    assert impact.portfolio_penalty > 0
    assert math.isfinite(impact.single_name_after_pct)
    assert math.isfinite(impact.sector_after_pct)
    assert math.isfinite(impact.theme_after_pct)


def test_evaluate_portfolio_impact_computes_before_after_fields() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="AI",
        account_equity=100_000,
        current_positions={
            "AAA": {"notional": 3_000, "sector": "Technology", "theme": "AI"},
            "BBB": {"notional": 7_000, "sector": "Technology", "theme": "Cloud"},
            "CCC": {"notional": 5_000, "sector": "Healthcare", "theme": "AI"},
        },
        proposed_notional=4_000,
        max_loss=500,
        policy=PortfolioPolicy(max_position_pct=0.10, max_sector_pct=0.30, max_theme_pct=0.35),
    )

    assert impact.single_name_before_pct == 0.03
    assert impact.single_name_after_pct == 0.07
    assert impact.sector_before_pct == 0.10
    assert impact.sector_after_pct == 0.14
    assert impact.theme_before_pct == 0.08
    assert impact.theme_after_pct == 0.12
    assert impact.correlated_before_pct == 0.09
    assert impact.correlated_after_pct == 0.13
    assert impact.proposed_notional == 4_000
    assert impact.max_loss == 500
    assert impact.hard_blocks == ()


def test_evaluate_portfolio_impact_blocks_single_name_overexposure() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="AI",
        account_equity=100_000,
        current_positions={"AAA": {"notional": 7_000, "sector": "Technology", "theme": "AI"}},
        proposed_notional=2_000,
        policy=PortfolioPolicy(max_position_pct=0.08),
    )

    assert impact.single_name_after_pct == 0.09
    assert "single_name_exposure_hard_block" in impact.hard_blocks


def test_evaluate_portfolio_impact_blocks_theme_overexposure() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="AI",
        account_equity=100_000,
        current_positions={"BBB": {"notional": 33_000, "sector": "Healthcare", "theme": "AI"}},
        proposed_notional=3_000,
        policy=PortfolioPolicy(max_position_pct=0.20, max_theme_pct=0.35),
    )

    assert impact.theme_after_pct == 0.36
    assert "theme_exposure_hard_block" in impact.hard_blocks


def test_evaluate_portfolio_impact_blocks_invalid_account_equity() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="AI",
        account_equity=0,
        current_positions={},
        proposed_notional=5_000,
    )

    assert impact.portfolio_penalty > 0
    assert impact.hard_blocks == ("invalid_portfolio_input",)


def test_evaluate_portfolio_impact_blocks_zero_proposed_notional() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="AI",
        account_equity=100_000,
        current_positions={},
        proposed_notional=0,
    )

    assert impact.proposed_notional == 0.0
    assert "invalid_portfolio_input" in impact.hard_blocks


def test_evaluate_portfolio_impact_blocks_insufficient_cash() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="AI",
        account_equity=100_000,
        current_positions={},
        proposed_notional=8_000,
        available_cash=5_000,
    )

    assert "insufficient_cash_hard_block" in impact.hard_blocks
    assert impact.portfolio_penalty > 0
