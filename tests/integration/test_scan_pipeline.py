from datetime import date
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
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
