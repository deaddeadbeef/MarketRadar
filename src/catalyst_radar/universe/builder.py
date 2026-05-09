from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, time

from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.universe.filters import (
    UniverseDecision,
    UniverseFilterConfig,
    evaluate_universe_member,
)


@dataclass(frozen=True)
class UniverseSnapshotResult:
    id: str
    name: str
    as_of: datetime
    provider: str
    member_count: int
    excluded_count: int
    decisions: tuple[UniverseDecision, ...]


class UniverseBuilder:
    def __init__(
        self,
        *,
        market_repo: MarketRepository,
        provider_repo: ProviderRepository,
        config: UniverseFilterConfig,
        name: str,
        provider: str,
    ) -> None:
        self.market_repo = market_repo
        self.provider_repo = provider_repo
        self.config = config
        self.name = name
        self.provider = provider

    def build(self, *, as_of: date, available_at: datetime) -> UniverseSnapshotResult:
        as_of_dt = datetime.combine(as_of, time(21), tzinfo=UTC)
        ranked_members = []
        decisions = []
        reason_counts: Counter[str] = Counter()

        for security in self.market_repo.list_active_securities():
            bars = self.market_repo.daily_bars(
                security.ticker,
                end=as_of,
                lookback=20,
                available_at=available_at,
            )
            decision = evaluate_universe_member(security, bars, self.config, as_of=as_of)
            decisions.append(decision)
            if decision.included:
                ranked_members.append(decision)
            else:
                reason_counts.update(decision.exclusion_reasons)

        ranked_members.sort(
            key=lambda decision: (-decision.avg_dollar_volume_20d, decision.ticker)
        )
        member_rows = [
            {
                "ticker": decision.ticker,
                "reason": "eligible",
                "rank": rank,
                "metadata": {
                    "avg_dollar_volume_20d": decision.avg_dollar_volume_20d,
                    "latest_close": decision.latest_close,
                },
            }
            for rank, decision in enumerate(ranked_members, start=1)
        ]
        snapshot_id = self.provider_repo.save_universe_snapshot(
            name=self.name,
            as_of=as_of_dt,
            provider=self.provider,
            source_ts=as_of_dt,
            available_at=available_at,
            members=member_rows,
            metadata={
                "eligible_count": len(member_rows),
                "excluded_count": len(decisions) - len(member_rows),
                "exclusion_reason_counts": dict(sorted(reason_counts.items())),
                "config": {
                    "min_price": self.config.min_price,
                    "min_avg_dollar_volume": self.config.min_avg_dollar_volume,
                    "require_sector": self.config.require_sector,
                    "include_etfs": self.config.include_etfs,
                    "include_adrs": self.config.include_adrs,
                },
            },
        )
        return UniverseSnapshotResult(
            id=snapshot_id,
            name=self.name,
            as_of=as_of_dt,
            provider=self.provider,
            member_count=len(member_rows),
            excluded_count=len(decisions) - len(member_rows),
            decisions=tuple(decisions),
        )
