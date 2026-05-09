from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

import pandas as pd

from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import CandidateSnapshot, DailyBar, PolicyResult, PortfolioImpact
from catalyst_radar.features.market import compute_market_features
from catalyst_radar.portfolio.holdings import (
    PortfolioState,
    latest_portfolio_state,
    positions_by_ticker,
)
from catalyst_radar.portfolio.risk import (
    PortfolioPolicy,
    PositionSize,
    compute_position_size,
    evaluate_portfolio_impact,
)
from catalyst_radar.scoring.policy import evaluate_policy
from catalyst_radar.scoring.score import candidate_from_features
from catalyst_radar.scoring.setup_policies import select_setup_plan
from catalyst_radar.scoring.setups import SetupPlan
from catalyst_radar.storage.repositories import MarketRepository

SECTOR_ETF = {"Technology": "XLK", "Industrials": "XLI"}
EXCLUDED_SCAN_TICKERS = frozenset({"SPY", "XLK", "XLI"})
LOOKBACK_SESSIONS = 252


@dataclass(frozen=True)
class ScanResult:
    ticker: str
    candidate: CandidateSnapshot
    policy: PolicyResult


def run_scan(
    repo: MarketRepository,
    as_of: date,
    *,
    available_at: datetime | None = None,
    provider: str | None = None,
    universe_tickers: set[str] | None = None,
    config: AppConfig | None = None,
) -> list[ScanResult]:
    active_config = config or AppConfig.from_env()
    as_of_dt = datetime.combine(as_of, time(21), tzinfo=UTC)
    available_at_dt = as_of_dt if available_at is None else available_at
    portfolio_state = latest_portfolio_state(
        repo.list_holdings(),
        as_of_dt,
        fallback_value=active_config.portfolio_value,
        fallback_cash=active_config.portfolio_cash,
    )
    current_positions = positions_by_ticker(portfolio_state)
    portfolio_policy = PortfolioPolicy(
        max_position_pct=active_config.max_single_name_pct,
        risk_per_trade_pct=active_config.risk_per_trade_pct,
        max_sector_pct=active_config.max_sector_pct,
        max_theme_pct=active_config.max_theme_pct,
    )
    if universe_tickers is None:
        candidate_securities = repo.list_active_securities()
    else:
        candidate_securities = repo.list_active_securities_by_tickers(universe_tickers)
    securities = [
        security
        for security in candidate_securities
        if security.ticker not in EXCLUDED_SCAN_TICKERS
    ]
    spy_bars = repo.daily_bars(
        "SPY",
        end=as_of,
        lookback=LOOKBACK_SESSIONS,
        available_at=available_at_dt,
        provider=provider,
    )
    benchmark_cache: dict[str, pd.DataFrame] = {"SPY": _bars_frame(spy_bars)}

    results = []
    for security in securities:
        ticker_bars = repo.daily_bars(
            security.ticker,
            end=as_of,
            lookback=LOOKBACK_SESSIONS,
            available_at=available_at_dt,
            provider=provider,
        )
        if not ticker_bars:
            continue

        sector_ticker = SECTOR_ETF.get(security.sector, "SPY")
        if sector_ticker not in benchmark_cache:
            benchmark_cache[sector_ticker] = _bars_frame(
                repo.daily_bars(
                    sector_ticker,
                    end=as_of,
                    lookback=LOOKBACK_SESSIONS,
                    available_at=available_at_dt,
                    provider=provider,
                )
            )

        features = compute_market_features(
            security.ticker,
            as_of_dt,
            _bars_frame(ticker_bars),
            benchmark_cache["SPY"],
            benchmark_cache[sector_ticker],
        )
        setup_plan = select_setup_plan(ticker_bars, features)
        entry_price = _position_entry_price(setup_plan)
        invalidation_price = setup_plan.invalidation_price or 0.0
        position_size = compute_position_size(
            portfolio_state.portfolio_value,
            entry_price,
            invalidation_price,
            policy=portfolio_policy,
        )
        candidate_theme = _candidate_theme(security.industry, security.metadata)
        portfolio_impact = evaluate_portfolio_impact(
            ticker=security.ticker,
            sector=security.sector,
            theme=candidate_theme,
            account_equity=portfolio_state.portfolio_value,
            current_positions=current_positions,
            proposed_notional=position_size.notional,
            policy=portfolio_policy,
            max_loss=position_size.risk_amount,
            available_cash=portfolio_state.cash,
        )
        candidate_metadata = {
            **_setup_metadata(setup_plan),
            "position_size": _position_size_payload(position_size),
            "portfolio_impact": _portfolio_impact_payload(portfolio_impact),
            "portfolio_state": {
                "as_of": portfolio_state.as_of.isoformat(),
                "source": portfolio_state.source,
                "portfolio_value": portfolio_state.portfolio_value,
                "cash": portfolio_state.cash,
                "input_warnings": list(portfolio_state.input_warnings),
            },
            "candidate_theme": candidate_theme,
            "source_ts": _impact_source_ts(ticker_bars, portfolio_state).isoformat(),
            "available_at": _impact_available_at(
                ticker_bars,
                portfolio_state,
            ).isoformat(),
        }
        candidate = candidate_from_features(
            features,
            portfolio_penalty=portfolio_impact.portfolio_penalty,
            data_stale=_is_data_stale(ticker_bars, as_of),
            entry_zone=setup_plan.entry_zone,
            invalidation_price=setup_plan.invalidation_price,
            reward_risk=setup_plan.reward_risk,
            metadata=candidate_metadata,
        )
        results.append(
            ScanResult(
                ticker=security.ticker,
                candidate=candidate,
                policy=evaluate_policy(candidate),
            )
        )

    return sorted(results, key=lambda result: result.candidate.final_score, reverse=True)


def _bars_frame(bars: list[DailyBar]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": bar.ticker,
                "date": bar.date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "vwap": bar.vwap,
            }
            for bar in bars
        ]
    )


def _is_data_stale(bars: list[DailyBar], as_of: date) -> bool:
    return bars[-1].date < as_of


def _impact_source_ts(bars: list[DailyBar], portfolio_state: PortfolioState) -> datetime:
    if portfolio_state.source == "config_fallback":
        return bars[-1].source_ts
    return max(bars[-1].source_ts, portfolio_state.as_of)


def _impact_available_at(bars: list[DailyBar], portfolio_state: PortfolioState) -> datetime:
    if portfolio_state.source == "config_fallback":
        return bars[-1].available_at
    return max(bars[-1].available_at, portfolio_state.as_of)


def _position_entry_price(setup_plan: SetupPlan) -> float:
    if setup_plan.entry_zone is None:
        return 0.0
    return max(setup_plan.entry_zone)


def _candidate_theme(industry: str, metadata: object) -> str:
    if isinstance(metadata, Mapping):
        theme = metadata.get("theme")
        if theme:
            return str(theme)
    return industry or "unknown"


def _setup_metadata(setup_plan: SetupPlan) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "setup_type": setup_plan.setup_type.value,
        "setup_reasons": list(setup_plan.reasons),
        "chase_block": setup_plan.chase_block,
        "setup_metadata": dict(setup_plan.metadata),
    }
    if setup_plan.target_price is not None:
        metadata["target_price"] = setup_plan.target_price
    return metadata


def _position_size_payload(position_size: PositionSize) -> dict[str, Any]:
    return {
        "shares": position_size.shares,
        "notional": position_size.notional,
        "position_pct": position_size.position_pct,
        "risk_amount": position_size.risk_amount,
        "is_capped": position_size.is_capped,
    }


def _portfolio_impact_payload(portfolio_impact: PortfolioImpact) -> dict[str, Any]:
    return {
        "ticker": portfolio_impact.ticker,
        "single_name_before_pct": portfolio_impact.single_name_before_pct,
        "single_name_after_pct": portfolio_impact.single_name_after_pct,
        "sector_before_pct": portfolio_impact.sector_before_pct,
        "sector_after_pct": portfolio_impact.sector_after_pct,
        "theme_before_pct": portfolio_impact.theme_before_pct,
        "theme_after_pct": portfolio_impact.theme_after_pct,
        "correlated_before_pct": portfolio_impact.correlated_before_pct,
        "correlated_after_pct": portfolio_impact.correlated_after_pct,
        "proposed_notional": portfolio_impact.proposed_notional,
        "max_loss": portfolio_impact.max_loss,
        "portfolio_penalty": portfolio_impact.portfolio_penalty,
        "hard_blocks": list(portfolio_impact.hard_blocks),
    }
