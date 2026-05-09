from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.universe.filters import UniverseFilterConfig, evaluate_universe_member


def test_universe_filter_accepts_liquid_active_common_stock() -> None:
    security = make_security("AAPL", market_cap=3_000_000_000_000, sector="Technology")
    bars = make_daily_bars("AAPL", close=214, volume=65_000_000, sessions=20)

    decision = evaluate_universe_member(
        security,
        bars,
        UniverseFilterConfig(min_price=5, min_avg_dollar_volume=10_000_000),
    )

    assert decision.included is True
    assert decision.reason == "eligible"
    assert decision.avg_dollar_volume_20d == 214 * 65_000_000


def test_universe_filter_excludes_low_liquidity_and_missing_sector() -> None:
    security = make_security("THIN", market_cap=500_000_000, sector="Unknown")
    bars = make_daily_bars("THIN", close=2.1, volume=10_000, sessions=20)

    decision = evaluate_universe_member(
        security,
        bars,
        UniverseFilterConfig(
            min_price=5,
            min_avg_dollar_volume=10_000_000,
            require_sector=True,
        ),
    )

    assert decision.included is False
    assert "low_avg_dollar_volume" in decision.exclusion_reasons
    assert "missing_sector" in decision.exclusion_reasons
    assert "low_price" in decision.exclusion_reasons


def test_universe_filter_excludes_etfs_by_default() -> None:
    security = make_security(
        "SPY",
        market_cap=500_000_000_000,
        sector="Unknown",
        metadata={"type": "ETF"},
    )
    bars = make_daily_bars("SPY", close=580, volume=80_000_000, sessions=20)

    decision = evaluate_universe_member(
        security,
        bars,
        UniverseFilterConfig(min_price=5, min_avg_dollar_volume=10_000_000),
    )

    assert decision.included is False
    assert "etf_excluded" in decision.exclusion_reasons


def test_universe_filter_excludes_unsupported_security_type() -> None:
    security = make_security(
        "PREF",
        market_cap=2_000_000_000,
        sector="Financials",
        metadata={"type": "PFD"},
    )
    bars = make_daily_bars("PREF", close=25, volume=2_000_000, sessions=20)

    decision = evaluate_universe_member(
        security,
        bars,
        UniverseFilterConfig(min_price=5, min_avg_dollar_volume=10_000_000),
    )

    assert decision.included is False
    assert "unsupported_security_type" in decision.exclusion_reasons


def make_security(
    ticker: str,
    *,
    market_cap: float,
    sector: str,
    is_active: bool = True,
    name: str | None = None,
    metadata: dict[str, object] | None = None,
) -> Security:
    return Security(
        ticker=ticker,
        name=name or f"{ticker} Corp.",
        exchange="XNAS",
        sector=sector,
        industry="Software",
        market_cap=market_cap,
        avg_dollar_volume_20d=0,
        has_options=False,
        is_active=is_active,
        updated_at=datetime(2026, 5, 8, 20, tzinfo=UTC),
        metadata=metadata or {},
    )


def make_daily_bars(
    ticker: str,
    *,
    close: float,
    volume: int,
    sessions: int,
) -> list[DailyBar]:
    start = date(2026, 5, 8) - timedelta(days=sessions - 1)
    return [
        DailyBar(
            ticker=ticker,
            date=start + timedelta(days=index),
            open=close - 1,
            high=close + 1,
            low=close - 2,
            close=close,
            volume=volume,
            vwap=close,
            adjusted=True,
            provider="test",
            source_ts=datetime(2026, 5, 8, 20, tzinfo=UTC),
            available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
        )
        for index in range(sessions)
    ]
