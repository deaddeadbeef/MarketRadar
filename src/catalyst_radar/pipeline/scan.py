from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

import pandas as pd

from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import CandidateSnapshot, DailyBar, PolicyResult, PortfolioImpact
from catalyst_radar.events.conflicts import detect_event_conflicts
from catalyst_radar.events.models import CanonicalEvent
from catalyst_radar.features.market import compute_market_features
from catalyst_radar.features.options import (
    OPTION_FEATURE_VERSION,
    OptionFeatureInput,
    OptionFeatureScore,
    compute_option_feature_score,
)
from catalyst_radar.features.peers import (
    PEER_FEATURE_VERSION,
    PeerReadthroughScore,
    peer_readthrough_score,
)
from catalyst_radar.features.sector import (
    SECTOR_FEATURE_VERSION,
    SectorRotationScore,
    sector_rotation_score,
)
from catalyst_radar.features.theme import (
    THEME_FEATURE_VERSION,
    load_theme_peer_config,
    theme_for_security,
    theme_velocity_score,
)
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
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.models import TextFeature

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
    event_repo: EventRepository | None = None,
    text_repo: TextRepository | None = None,
    feature_repo: FeatureRepository | None = None,
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
    events_by_ticker = _events_by_ticker(
        event_repo=event_repo,
        tickers=[security.ticker for security in securities],
        as_of=as_of_dt,
        available_at=available_at_dt,
    )
    text_features_by_ticker = _text_features_by_ticker(
        text_repo=text_repo,
        tickers=[security.ticker for security in securities],
        as_of=as_of_dt,
        available_at=available_at_dt,
    )
    option_features_by_ticker = _option_features_by_ticker(
        feature_repo=feature_repo,
        tickers=[security.ticker for security in securities],
        as_of=as_of_dt,
        available_at=available_at_dt,
    )
    theme_config = load_theme_peer_config()
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

        sector_ticker = SECTOR_ETF.get(security.sector)
        benchmark_ticker = sector_ticker or "SPY"
        if benchmark_ticker not in benchmark_cache:
            benchmark_cache[benchmark_ticker] = _bars_frame(
                repo.daily_bars(
                    benchmark_ticker,
                    end=as_of,
                    lookback=LOOKBACK_SESSIONS,
                    available_at=available_at_dt,
                    provider=provider,
                )
            )

        ticker_frame = _bars_frame(ticker_bars)
        features = compute_market_features(
            security.ticker,
            as_of_dt,
            ticker_frame,
            benchmark_cache["SPY"],
            benchmark_cache[benchmark_ticker],
        )
        material_events = events_by_ticker.get(security.ticker, [])
        text_feature = text_features_by_ticker.get(security.ticker)
        option_feature = option_features_by_ticker.get(security.ticker)
        option_score = (
            compute_option_feature_score(option_feature)
            if option_feature is not None
            else None
        )
        candidate_theme = theme_for_security(
            ticker=security.ticker,
            sector=security.sector,
            industry=security.industry,
            metadata=security.metadata,
            config=theme_config,
        )
        theme_velocity = theme_velocity_score(text_feature, candidate_theme)
        peer_score = peer_readthrough_score(
            security.ticker,
            _peer_source_theme_hits(
                text_features_by_ticker=text_features_by_ticker,
                current_ticker=security.ticker,
            ),
            theme_config,
        )
        sector_score = _sector_rotation_for_security(
            sector_ticker=sector_ticker,
            ticker_frame=ticker_frame,
            spy_frame=benchmark_cache["SPY"],
            sector_frame=benchmark_cache[benchmark_ticker],
        )
        event_conflicts = detect_event_conflicts(material_events)
        setup_plan = select_setup_plan(
            ticker_bars,
            features,
            material_events=material_events,
        )
        entry_price = _position_entry_price(setup_plan)
        invalidation_price = setup_plan.invalidation_price or 0.0
        position_size = compute_position_size(
            portfolio_state.portfolio_value,
            entry_price,
            invalidation_price,
            policy=portfolio_policy,
        )
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
            **_event_metadata(material_events, event_conflicts),
            **_text_metadata(text_feature),
            **_options_metadata(option_score),
            **_theme_sector_peer_metadata(
                candidate_theme=candidate_theme,
                theme_velocity=theme_velocity,
                peer_score=peer_score,
                sector_score=sector_score,
            ),
            "position_size": _position_size_payload(
                position_size,
                risk_per_trade_pct=portfolio_policy.risk_per_trade_pct,
                available_cash=portfolio_state.cash,
            ),
            "portfolio_impact": _portfolio_impact_payload(portfolio_impact),
            "portfolio_state": {
                "as_of": portfolio_state.as_of.isoformat(),
                "source": portfolio_state.source,
                "portfolio_value": portfolio_state.portfolio_value,
                "cash": portfolio_state.cash,
                "input_warnings": list(portfolio_state.input_warnings),
            },
            "source_ts": _impact_source_ts(ticker_bars, portfolio_state).isoformat(),
            "available_at": _impact_available_at(
                ticker_bars,
                portfolio_state,
            ).isoformat(),
            "market_provider": provider,
            "market_data_providers": sorted({bar.provider for bar in ticker_bars}),
        }
        candidate = candidate_from_features(
            features,
            portfolio_penalty=portfolio_impact.portfolio_penalty,
            data_stale=_is_data_stale(ticker_bars, as_of),
            entry_zone=setup_plan.entry_zone,
            invalidation_price=setup_plan.invalidation_price,
            reward_risk=setup_plan.reward_risk,
            metadata=candidate_metadata,
            event_support_score=_event_support_score(material_events),
            local_narrative_score=(
                text_feature.local_narrative_score if text_feature is not None else 0.0
            ),
            options_flow_score=option_score.options_flow_score if option_score else 0.0,
            options_risk_score=option_score.options_risk_score if option_score else 0.0,
            sector_rotation_score=sector_score.score,
            theme_velocity_score=theme_velocity,
            peer_readthrough_score=peer_score.score,
        )
        results.append(
            ScanResult(
                ticker=security.ticker,
                candidate=candidate,
                policy=evaluate_policy(candidate),
            )
        )

    return sorted(results, key=lambda result: result.candidate.final_score, reverse=True)


def _events_by_ticker(
    *,
    event_repo: EventRepository | None,
    tickers: list[str],
    as_of: datetime,
    available_at: datetime,
) -> dict[str, list[CanonicalEvent]]:
    if event_repo is None or not tickers:
        return {}
    return event_repo.latest_material_events_by_ticker(
        tickers,
        as_of=as_of,
        available_at=available_at,
        min_materiality=0.50,
        limit_per_ticker=5,
    )


def _text_features_by_ticker(
    *,
    text_repo: TextRepository | None,
    tickers: list[str],
    as_of: datetime,
    available_at: datetime,
) -> dict[str, TextFeature]:
    if text_repo is None or not tickers:
        return {}
    return text_repo.latest_text_features_by_ticker(
        tickers,
        as_of=as_of,
        available_at=available_at,
    )


def _option_features_by_ticker(
    *,
    feature_repo: FeatureRepository | None,
    tickers: list[str],
    as_of: datetime,
    available_at: datetime,
) -> dict[str, OptionFeatureInput]:
    if feature_repo is None or not tickers:
        return {}
    return feature_repo.latest_option_features_by_ticker(
        tickers,
        as_of=as_of,
        available_at=available_at,
    )


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


def _peer_source_theme_hits(
    *,
    text_features_by_ticker: dict[str, TextFeature],
    current_ticker: str,
) -> tuple[dict[str, Any], ...]:
    source_hits: dict[str, dict[str, Any]] = {}
    for ticker, text_feature in text_features_by_ticker.items():
        if ticker == current_ticker:
            continue
        for hit in _theme_hit_mappings(text_feature.theme_hits):
            theme_id = str(hit.get("theme_id", ""))
            if not theme_id:
                continue
            entry = source_hits.setdefault(
                theme_id,
                {"theme_id": theme_id, "count": 0.0, "terms": set(), "source_tickers": set()},
            )
            entry["count"] += _finite_float(hit.get("count", 0.0))
            entry["terms"].update(str(term) for term in _terms(hit.get("terms", ())))
            entry["source_tickers"].add(ticker)
    return tuple(
        {
            "theme_id": value["theme_id"],
            "count": value["count"],
            "terms": sorted(value["terms"]),
            "source_tickers": sorted(value["source_tickers"]),
        }
        for value in sorted(source_hits.values(), key=lambda item: str(item["theme_id"]))
    )


def _theme_hit_mappings(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, tuple | list):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, Mapping))


def _terms(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, tuple | list):
        return ()
    return tuple(str(item) for item in value)


def _finite_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number != number or number in (float("inf"), float("-inf")):
        return 0.0
    return number


def _sector_rotation_for_security(
    *,
    sector_ticker: str | None,
    ticker_frame: pd.DataFrame,
    spy_frame: pd.DataFrame,
    sector_frame: pd.DataFrame,
) -> SectorRotationScore:
    if sector_ticker is None:
        return _neutral_sector_rotation_score()
    return sector_rotation_score(ticker_frame, spy_frame, sector_frame)


def _neutral_sector_rotation_score() -> SectorRotationScore:
    return SectorRotationScore(
        score=50.0,
        ticker_return_20d=0.0,
        sector_return_20d=0.0,
        spy_return_20d=0.0,
        ticker_vs_sector=0.0,
        sector_vs_spy=0.0,
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


def _event_metadata(
    material_events: list[CanonicalEvent],
    event_conflicts: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    top_event = material_events[0] if material_events else None
    return {
        "events": [_event_payload(event) for event in material_events],
        "material_event_count": len(material_events),
        "top_event_type": top_event.event_type.value if top_event is not None else None,
        "top_event_title": top_event.title if top_event is not None else None,
        "top_event_source": top_event.source if top_event is not None else None,
        "top_event_source_url": top_event.source_url if top_event is not None else None,
        "top_event_source_quality": (
            top_event.source_quality if top_event is not None else None
        ),
        "top_event_materiality": top_event.materiality if top_event is not None else None,
        "event_support_score": _event_support_score(material_events),
        "event_source_ids": [event.id for event in material_events],
        "event_conflicts": event_conflicts,
        "has_event_conflict": bool(event_conflicts),
    }


def _event_payload(event: CanonicalEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "source_id": event.id,
        "event_type": event.event_type.value,
        "title": event.title,
        "source": event.source,
        "source_category": event.source_category.value,
        "source_quality": event.source_quality,
        "materiality": event.materiality,
        "source_ts": event.source_ts.isoformat(),
        "available_at": event.available_at.isoformat(),
        "source_url": event.source_url,
    }


def _text_metadata(text_feature: TextFeature | None) -> dict[str, Any]:
    if text_feature is None:
        return {
            "local_narrative_score": 0.0,
            "novelty_score": 0.0,
            "sentiment_score": 0.0,
            "source_quality_score": 0.0,
            "theme_match_score": 0.0,
            "theme_hits": [],
            "selected_snippet_ids": [],
            "selected_snippet_count": 0,
            "text_feature_version": None,
        }
    return {
        "local_narrative_score": text_feature.local_narrative_score,
        "novelty_score": text_feature.novelty_score,
        "sentiment_score": text_feature.sentiment_score,
        "source_quality_score": text_feature.source_quality_score,
        "theme_match_score": text_feature.theme_match_score,
        "theme_hits": text_feature.theme_hits,
        "selected_snippet_ids": list(text_feature.selected_snippet_ids),
        "selected_snippet_count": len(text_feature.selected_snippet_ids),
        "text_feature_version": text_feature.feature_version,
    }


def _options_metadata(option_score: OptionFeatureScore | None) -> dict[str, Any]:
    if option_score is None:
        return {
            "options_flow_score": 0.0,
            "options_risk_score": 0.0,
            "call_put_ratio": 0.0,
            "iv_percentile": 0.0,
            "options_feature_version": None,
        }
    return {
        "options_flow_score": option_score.options_flow_score,
        "options_risk_score": option_score.options_risk_score,
        "call_put_ratio": option_score.call_put_ratio,
        "iv_percentile": option_score.iv_percentile,
        "options_feature_version": OPTION_FEATURE_VERSION,
    }


def _theme_sector_peer_metadata(
    *,
    candidate_theme: str,
    theme_velocity: float,
    peer_score: PeerReadthroughScore,
    sector_score: SectorRotationScore,
) -> dict[str, Any]:
    return {
        "candidate_theme": candidate_theme,
        "sector_rotation_score": sector_score.score,
        "sector_rotation": {
            "ticker_return_20d": sector_score.ticker_return_20d,
            "sector_return_20d": sector_score.sector_return_20d,
            "spy_return_20d": sector_score.spy_return_20d,
            "ticker_vs_sector": sector_score.ticker_vs_sector,
            "sector_vs_spy": sector_score.sector_vs_spy,
        },
        "theme_velocity_score": theme_velocity,
        "peer_readthrough_score": peer_score.score,
        "peer_readthrough_theme": peer_score.theme_id,
        "peer_readthrough_peers": list(peer_score.peers),
        "theme_feature_version": THEME_FEATURE_VERSION,
        "peer_feature_version": PEER_FEATURE_VERSION,
        "sector_feature_version": SECTOR_FEATURE_VERSION,
    }


def _event_support_score(material_events: list[CanonicalEvent]) -> float:
    if not material_events:
        return 0.0
    return max(event.materiality * event.source_quality * 100 for event in material_events)


def _position_size_payload(
    position_size: PositionSize,
    *,
    risk_per_trade_pct: float,
    available_cash: float,
) -> dict[str, Any]:
    return {
        "risk_per_trade_pct": risk_per_trade_pct,
        "shares": position_size.shares,
        "notional": position_size.notional,
        "position_pct": position_size.position_pct,
        "risk_amount": position_size.risk_amount,
        "is_capped": position_size.is_capped,
        "cash_check": "pass" if available_cash >= position_size.notional else "insufficient",
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
