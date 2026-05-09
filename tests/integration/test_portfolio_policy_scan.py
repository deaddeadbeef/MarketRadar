from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import create_engine, select

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState, HoldingSnapshot
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import portfolio_impacts


def test_scan_blocks_excessive_existing_single_name_exposure() -> None:
    repo = _repo_with_fixtures()
    repo.upsert_holdings(
        [
            HoldingSnapshot(
                ticker="AAA",
                shares=70,
                market_value=7_000,
                sector="Technology",
                theme="Software",
                as_of=datetime(2026, 5, 8, 20, tzinfo=UTC),
                portfolio_value=100_000,
                cash=25_000,
            )
        ]
    )

    result = _scan_result(repo, "AAA")

    assert result.policy.state == ActionState.BLOCKED
    assert "single_name_exposure_hard_block" in result.policy.hard_blocks
    assert result.candidate.metadata["portfolio_impact"]["single_name_before_pct"] == 0.07
    assert result.candidate.metadata["portfolio_impact"]["single_name_after_pct"] > 0.08


def test_scan_blocks_excessive_sector_exposure() -> None:
    repo = _repo_with_fixtures()
    repo.upsert_holdings(
        [
            HoldingSnapshot(
                ticker="HOLD",
                shares=1,
                market_value=29_000,
                sector="Technology",
                theme="Other",
                as_of=datetime(2026, 5, 8, 20, tzinfo=UTC),
                portfolio_value=100_000,
                cash=25_000,
            )
        ]
    )

    result = _scan_result(repo, "AAA")

    assert result.policy.state == ActionState.BLOCKED
    assert "sector_exposure_hard_block" in result.policy.hard_blocks
    assert result.candidate.metadata["portfolio_impact"]["sector_after_pct"] > 0.30


def test_no_holdings_uses_config_fallback_and_records_impact() -> None:
    repo = _repo_with_fixtures()

    result = _scan_result(repo, "AAA")

    assert result.candidate.metadata["portfolio_state"]["source"] == "config_fallback"
    assert result.candidate.metadata["portfolio_state"]["portfolio_value"] == 100_000
    assert result.candidate.metadata["position_size"]["notional"] > 0
    assert result.candidate.metadata["portfolio_impact"]["proposed_notional"] > 0
    assert "portfolio_impact_missing" not in result.policy.missing_trade_plan


def test_save_scan_result_persists_portfolio_impact_evidence() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))

    result = _scan_result(repo, "AAA")
    repo.save_scan_result(result.candidate, result.policy)

    with engine.connect() as conn:
        row = conn.execute(select(portfolio_impacts)).one()

    impact = result.candidate.metadata["portfolio_impact"]
    assert row.ticker == "AAA"
    assert row.as_of == result.candidate.as_of.replace(tzinfo=None)
    assert row.setup_type == result.candidate.metadata["setup_type"]
    assert row.source_ts == result.candidate.as_of.replace(tzinfo=None)
    assert row.available_at == result.candidate.as_of.replace(tzinfo=None)
    assert row.proposed_notional == impact["proposed_notional"]
    assert row.max_loss == impact["max_loss"]
    assert row.single_name_before_pct == impact["single_name_before_pct"]
    assert row.sector_after_pct == impact["sector_after_pct"]
    assert row.portfolio_penalty == impact["portfolio_penalty"]
    assert row.hard_blocks == list(impact["hard_blocks"])
    assert row.payload["portfolio_impact"]["ticker"] == impact["ticker"]
    assert row.payload["portfolio_impact"]["hard_blocks"] == list(impact["hard_blocks"])
    assert row.payload["candidate"]["setup_type"] == result.candidate.metadata["setup_type"]
    assert row.payload["candidate"]["entry_zone"] == list(result.candidate.entry_zone)


def test_missing_account_value_blocks_buy_review_fail_closed() -> None:
    repo = _repo_with_fixtures()

    result = _scan_result(
        repo,
        "AAA",
        config=AppConfig(portfolio_value=0, portfolio_cash=0),
    )

    assert result.policy.state == ActionState.BLOCKED
    assert "invalid_portfolio_input" in result.policy.hard_blocks
    assert result.candidate.metadata["portfolio_impact"]["hard_blocks"] == (
        "invalid_portfolio_input",
    )


def _repo_with_fixtures() -> MarketRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))
    return repo


def _scan_result(
    repo: MarketRepository,
    ticker: str,
    *,
    config: AppConfig | None = None,
):
    results = run_scan(
        repo,
        as_of=date(2026, 5, 8),
        config=config or AppConfig(portfolio_value=100_000, portfolio_cash=25_000),
    )
    return next(result for result in results if result.ticker == ticker)
