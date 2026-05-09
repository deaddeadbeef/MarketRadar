from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time

import pandas as pd

from catalyst_radar.core.models import CandidateSnapshot, DailyBar, PolicyResult
from catalyst_radar.features.market import compute_market_features
from catalyst_radar.scoring.policy import evaluate_policy
from catalyst_radar.scoring.score import candidate_from_features
from catalyst_radar.scoring.setup_policies import select_setup_plan
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
) -> list[ScanResult]:
    as_of_dt = datetime.combine(as_of, time(21), tzinfo=UTC)
    available_at_dt = as_of_dt if available_at is None else available_at
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
        candidate = candidate_from_features(
            features,
            portfolio_penalty=0.0,
            data_stale=_is_data_stale(ticker_bars, as_of),
            entry_zone=setup_plan.entry_zone,
            invalidation_price=setup_plan.invalidation_price,
            reward_risk=setup_plan.reward_risk,
        )
        setup_metadata = {
            "setup_type": setup_plan.setup_type.value,
            "setup_reasons": setup_plan.reasons,
            "chase_block": setup_plan.chase_block,
            "setup_metadata": setup_plan.metadata,
        }
        if setup_plan.target_price is not None:
            setup_metadata["target_price"] = setup_plan.target_price
        candidate = replace(
            candidate,
            metadata={
                **candidate.metadata,
                **setup_metadata,
            },
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
