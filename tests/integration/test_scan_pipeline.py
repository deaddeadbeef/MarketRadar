from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.models import DailyBar
from catalyst_radar.dashboard.data import load_candidate_rows
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository


def test_scan_pipeline_produces_candidate_states() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))

    results = run_scan(repo, as_of=date(2026, 5, 8))

    states = {result.ticker: result.policy.state.value for result in results}
    assert states["AAA"] in {"AddToWatchlist", "Warning", "EligibleForManualBuyReview"}
    assert states["CCC"] == "Blocked"


def test_scan_pipeline_excludes_future_available_same_date_bar() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))
    repo.upsert_daily_bars(
        [
            DailyBar(
                ticker="AAA",
                date=date(2026, 5, 8),
                open=900,
                high=1000,
                low=880,
                close=999,
                volume=9_999_999,
                vwap=990,
                adjusted=True,
                provider="sample",
                source_ts=datetime(2026, 5, 8, 22, tzinfo=UTC),
                available_at=datetime(2026, 5, 8, 22, tzinfo=UTC),
            )
        ]
    )

    gated_bars = repo.daily_bars(
        "AAA",
        end=date(2026, 5, 8),
        lookback=10,
        available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
    )
    results = run_scan(repo, as_of=date(2026, 5, 8))
    aaa = next(result for result in results if result.ticker == "AAA")

    assert gated_bars[-1].date == date(2026, 5, 7)
    assert gated_bars[-1].close == 105
    assert len(results) == 3
    assert aaa.candidate.data_stale is True
    assert aaa.candidate.entry_zone == (102.9, 107.1)


def test_dashboard_loads_candidate_rows() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))
    for result in run_scan(repo, as_of=date(2026, 5, 8)):
        repo.save_scan_result(result.candidate, result.policy)

    rows = load_candidate_rows(engine)

    assert rows
    assert {"ticker", "state", "final_score", "hard_blocks"}.issubset(rows[0])
