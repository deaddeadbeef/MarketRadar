from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete, insert, select

from catalyst_radar.cli import main
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.schema import (
    candidate_packets,
    candidate_states,
    daily_bars,
    decision_cards,
    paper_trades,
    signal_features,
    validation_results,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT_TEXT = "2026-05-10T21:05:00+00:00"
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)
OUTCOME_AVAILABLE_AT_TEXT = "2026-07-15T21:05:00+00:00"
OUTCOME_AVAILABLE_AT = datetime(2026, 7, 15, 21, 5, tzinfo=UTC)


def test_validation_report_label_and_paper_cli_workflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'validation.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    _insert_warning_candidate(database_url)

    assert (
        main(["build-packets", "--as-of", "2026-05-10", "--available-at", AVAILABLE_AT_TEXT])
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            ["build-decision-cards", "--as-of", "2026-05-10", "--available-at", AVAILABLE_AT_TEXT]
        )
        == 0
    )
    capsys.readouterr()
    card_id = _scalar(database_url, select(decision_cards.c.id))
    _insert_future_daily_bars(database_url)
    assert (
        main(
            [
                "paper-decision",
                "--decision-card-id",
                card_id,
                "--decision",
                "approved",
                "--available-at",
                "2026-05-10T21:04:00+00:00",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert "decision card not found" in captured.err

    assert (
        main(
            [
                "validation-replay",
                "--as-of-start",
                "2026-05-10",
                "--as-of-end",
                "2026-05-10",
                "--available-at",
                AVAILABLE_AT_TEXT,
                "--outcome-available-at",
                OUTCOME_AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "validation_replay run_id=" in captured.out
    assert "candidate_results=1" in captured.out
    assert "baseline_results=" in captured.out
    run_id = _scalar(
        database_url,
        select(validation_results.c.run_id)
        .where(validation_results.c.baseline.is_(None))
        .limit(1),
    )
    result_card_id = _scalar(
        database_url,
        select(validation_results.c.decision_card_id)
        .where(validation_results.c.baseline.is_(None))
        .limit(1),
    )
    assert result_card_id == card_id

    assert (
        main(
            [
                "useful-label",
                "--artifact-type",
                "decision_card",
                "--artifact-id",
                card_id,
                "--ticker",
                "msft",
                "--label",
                "useful",
                "--created-at",
                AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert main(["validation-report", "--run-id", run_id, "--json"]) == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert report["candidate_count"] == 1
    assert report["precision"]["target_20d_25"] == 1.0
    assert report["useful_alert_rate"] == 1.0
    assert report["leakage_failure_count"] == 0
    assert "random_eligible_universe" in report["baseline_comparison"]
    assert report["missed_opportunity_count"] > 0
    assert main(["validation-report", "--run-id", run_id, "--available-at", AVAILABLE_AT_TEXT]) == 0
    captured = capsys.readouterr()
    assert "candidates=0" in captured.out
    assert "precision_target_20d_25=0.00" in captured.out
    assert (
        main(
            [
                "useful-label",
                "--artifact-type",
                "decision_card",
                "--artifact-id",
                card_id,
                "--ticker",
                "MSFT",
                "--label",
                "ignored",
                "--created-at",
                OUTCOME_AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert main(["validation-report", "--run-id", run_id, "--json"]) == 0
    captured = capsys.readouterr()
    relabeled_report = json.loads(captured.out)
    assert relabeled_report["useful_alert_rate"] == 0.0

    assert (
        main(
            [
                "paper-decision",
                "--decision-card-id",
                card_id,
                "--decision",
                "approved",
                "--available-at",
                AVAILABLE_AT_TEXT,
                "--entry-price",
                "100",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "state=open" in captured.out
    assert "no_execution=true" in captured.out

    assert (
        main(
            [
                "paper-update-outcomes",
                "--decision-card-id",
                card_id,
                "--available-at",
                OUTCOME_AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "target_20d_25" in captured.out
    trade_row = _scalar(
        database_url,
        select(paper_trades.c.outcome_labels)
        .where(paper_trades.c.decision_card_id == card_id)
        .order_by(paper_trades.c.available_at.desc())
        .limit(1),
    )
    assert trade_row["target_20d_25"] is True
    assert trade_row["invalidated"] is False


def test_validation_replay_counts_future_packet_and_card_leakage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'future-leakage.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    _insert_warning_candidate(database_url)
    assert (
        main(["build-packets", "--as-of", "2026-05-10", "--available-at", AVAILABLE_AT_TEXT])
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            ["build-decision-cards", "--as-of", "2026-05-10", "--available-at", AVAILABLE_AT_TEXT]
        )
        == 0
    )
    capsys.readouterr()
    _insert_future_packet_and_card(database_url)

    assert (
        main(
            [
                "validation-replay",
                "--as-of-start",
                "2026-05-10",
                "--as-of-end",
                "2026-05-10",
                "--available-at",
                AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    captured = capsys.readouterr()

    assert "leakage_failures=1" in captured.out
    leakage_flags = _scalar(
        database_url,
        select(validation_results.c.leakage_flags)
        .where(validation_results.c.baseline.is_(None))
        .limit(1),
    )
    assert "candidate_packet_future_available_at" in leakage_flags
    assert "decision_card_future_available_at" in leakage_flags


def test_validation_replay_rerun_clears_stale_deterministic_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'rerun.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    _insert_warning_candidate(database_url)

    argv = [
        "validation-replay",
        "--as-of-start",
        "2026-05-10",
        "--as-of-end",
        "2026-05-10",
        "--available-at",
        AVAILABLE_AT_TEXT,
    ]
    assert main(argv) == 0
    captured = capsys.readouterr()
    assert "candidate_results=1" in captured.out
    assert _scalar(database_url, select(validation_results.c.id).limit(1)) is not None

    engine = create_engine(database_url, future=True)
    with engine.begin() as conn:
        conn.execute(delete(candidate_states))
        conn.execute(delete(signal_features))

    assert main(argv) == 0
    captured = capsys.readouterr()
    assert "candidate_results=0" in captured.out
    with engine.connect() as conn:
        assert list(conn.execute(select(validation_results))) == []


def test_validation_replay_baselines_use_decision_cutoff_not_outcome_cutoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'baseline-cutoff.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    _insert_warning_candidate(database_url)
    _insert_future_daily_bars(database_url)
    _insert_late_baseline_only_bar(database_url)

    assert (
        main(
            [
                "validation-replay",
                "--as-of-start",
                "2026-05-10",
                "--as-of-end",
                "2026-05-10",
                "--available-at",
                AVAILABLE_AT_TEXT,
                "--outcome-available-at",
                OUTCOME_AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    capsys.readouterr()

    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        tickers = {
            row.ticker
            for row in conn.execute(
                select(validation_results.c.ticker).where(
                    validation_results.c.baseline.is_not(None)
                )
            )
        }
    assert "LATE" not in tickers


def _insert_warning_candidate(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                id="state-msft",
                ticker="MSFT",
                as_of=AS_OF,
                state=ActionState.WARNING.value,
                previous_state=None,
                final_score=78.0,
                score_delta_5d=4.0,
                hard_blocks=[],
                transition_reasons=["score_requires_manual_review"],
                feature_version="score-v4-options-theme",
                policy_version="policy-v2-events",
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(signal_features).values(
                ticker="MSFT",
                as_of=AS_OF,
                feature_version="score-v4-options-theme",
                price_strength=82.0,
                volume_score=74.0,
                liquidity_score=91.0,
                risk_penalty=4.0,
                portfolio_penalty=1.0,
                final_score=78.0,
                payload={
                    "candidate": {
                        "ticker": "MSFT",
                        "as_of": AS_OF.isoformat(),
                        "features": {
                            "ticker": "MSFT",
                            "as_of": AS_OF.isoformat(),
                            "feature_version": "score-v4-options-theme",
                        },
                        "final_score": 78.0,
                        "risk_penalty": 4.0,
                        "portfolio_penalty": 1.0,
                        "entry_zone": [100.0, 104.0],
                        "invalidation_price": 94.0,
                        "reward_risk": 2.7,
                        "metadata": {
                            "source_ts": SOURCE_TS.isoformat(),
                            "available_at": AVAILABLE_AT.isoformat(),
                            "setup_type": "breakout",
                            "target_price": 125.0,
                            "pillar_scores": {
                                "price_strength": 86.0,
                                "relative_strength": 81.0,
                                "volume_liquidity": 72.0,
                            },
                            "position_size": {
                                "risk_per_trade_pct": 0.005,
                                "shares": 20.0,
                                "notional": 2080.0,
                                "cash_check": "pass",
                            },
                            "portfolio_impact": {
                                "single_name_after_pct": 4.0,
                                "sector_after_pct": 14.0,
                                "theme_after_pct": 6.0,
                                "correlated_after_pct": 8.0,
                                "proposed_notional": 2080.0,
                                "max_loss": 200.0,
                                "portfolio_penalty": 1.0,
                                "hard_blocks": [],
                            },
                        },
                    },
                    "policy": {
                        "state": ActionState.WARNING.value,
                        "hard_blocks": [],
                        "reasons": ["score_requires_manual_review"],
                        "missing_trade_plan": [],
                        "policy_version": "policy-v2-events",
                    },
                },
            )
        )


def _insert_future_daily_bars(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    with engine.begin() as conn:
        for ticker, closes in {
            "AAA": [90.0, 93.0, 99.0],
            "BBB": [50.0, 52.0, 51.0],
            "SPY": [500.0, 505.0, 510.0],
        }.items():
            for offset, close in enumerate(closes, start=3):
                bar_date = date(2026, 5, offset)
                conn.execute(
                    insert(daily_bars).values(
                        ticker=ticker,
                        date=bar_date,
                        provider="fixture",
                        open=close - 1.0,
                        high=close + 1.0,
                        low=close - 2.0,
                        close=close,
                        volume=1_000_000,
                        vwap=close,
                        adjusted=True,
                        source_ts=datetime.combine(
                            bar_date,
                            datetime.min.time(),
                            tzinfo=UTC,
                        ),
                        available_at=AVAILABLE_AT,
                    )
                )
        for offset in range(1, 22):
            bar_date = date(2026, 5, 10) + timedelta(days=offset)
            high = 126.0 if offset == 20 else 103.0
            conn.execute(
                insert(daily_bars).values(
                    ticker="MSFT",
                    date=bar_date,
                    provider="fixture",
                    open=100.0,
                    high=high,
                    low=96.0,
                    close=102.0,
                    volume=1_000_000,
                    vwap=101.0,
                    adjusted=True,
                    source_ts=datetime.combine(bar_date, datetime.min.time(), tzinfo=UTC),
                    available_at=OUTCOME_AVAILABLE_AT,
                )
            )


def _insert_future_packet_and_card(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    future_available_at = AVAILABLE_AT + timedelta(minutes=5)
    with engine.begin() as conn:
        packet_row = dict(conn.execute(select(candidate_packets)).first()._mapping)
        packet_row["id"] = f"{packet_row['id']}:future"
        packet_row["available_at"] = future_available_at
        packet_row["created_at"] = future_available_at
        packet_payload = dict(packet_row["payload"])
        packet_audit = dict(packet_payload["audit"])
        packet_audit["available_at"] = future_available_at.isoformat()
        packet_payload["audit"] = packet_audit
        packet_row["payload"] = packet_payload
        conn.execute(insert(candidate_packets).values(**packet_row))

        card_row = dict(conn.execute(select(decision_cards)).first()._mapping)
        card_row["id"] = f"{card_row['id']}:future"
        card_row["candidate_packet_id"] = packet_row["id"]
        card_row["available_at"] = future_available_at
        card_row["created_at"] = future_available_at
        card_payload = dict(card_row["payload"])
        card_audit = dict(card_payload["audit"])
        card_audit["available_at"] = future_available_at.isoformat()
        card_audit["candidate_packet_id"] = packet_row["id"]
        card_payload["audit"] = card_audit
        card_row["payload"] = card_payload
        conn.execute(insert(decision_cards).values(**card_row))


def _insert_late_baseline_only_bar(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    with engine.begin() as conn:
        for offset, close in enumerate([20.0, 30.0], start=8):
            bar_date = date(2026, 5, offset)
            conn.execute(
                insert(daily_bars).values(
                    ticker="LATE",
                    date=bar_date,
                    provider="fixture",
                    open=close,
                    high=close + 1,
                    low=close - 1,
                    close=close,
                    volume=1_000_000,
                    vwap=close,
                    adjusted=True,
                    source_ts=datetime.combine(bar_date, datetime.min.time(), tzinfo=UTC),
                    available_at=OUTCOME_AVAILABLE_AT,
                )
            )


def _scalar(database_url: str, stmt):
    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        return conn.execute(stmt).scalar_one()
