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
    assert "sector_overexposure" in impact.hard_blocks
    assert impact.portfolio_penalty > 0
