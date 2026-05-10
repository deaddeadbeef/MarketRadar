from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, insert, select, update

from catalyst_radar.cli import main
from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard.data import load_candidate_rows
from catalyst_radar.storage.schema import candidate_states, signal_features

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT_TEXT = "2026-05-10T21:05:00+00:00"
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)


def test_candidate_packet_and_decision_card_cli_build_and_inspect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'packets.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    _insert_warning_candidate(database_url)

    assert (
        main(["build-packets", "--as-of", "2026-05-10", "--available-at", AVAILABLE_AT_TEXT])
        == 0
    )
    captured = capsys.readouterr()
    assert captured.out == "built candidate_packets=1\n"
    assert captured.err == ""

    assert (
        main(
            [
                "candidate-packet",
                "--ticker",
                "MSFT",
                "--as-of",
                "2026-05-10",
                "--available-at",
                AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "MSFT packet state=Warning supporting=" in captured.out
    assert "disconfirming=" in captured.out
    assert "[signal_features:MSFT:" in captured.out

    assert (
        main(
            [
                "candidate-packet",
                "--ticker",
                "MSFT",
                "--as-of",
                "2026-05-10",
                "--available-at",
                AVAILABLE_AT_TEXT,
                "--json",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert "provider license blocks external export: local-csv-fixture" in captured.err

    assert (
        main(
            [
                "build-decision-cards",
                "--as-of",
                "2026-05-10",
                "--available-at",
                AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert captured.out == "built decision_cards=1\n"

    assert (
        main(
            [
                "decision-card",
                "--ticker",
                "MSFT",
                "--as-of",
                "2026-05-10",
                "--available-at",
                AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "MSFT decision_card state=Warning" in captured.out
    assert "next_review_at=" in captured.out
    assert "[signal_features:MSFT:" in captured.out

    rows = load_candidate_rows(create_engine(database_url, future=True))
    msft = next(row for row in rows if row["ticker"] == "MSFT")
    assert msft["candidate_packet_id"]
    assert msft["decision_card_id"]
    assert msft["top_supporting_evidence"]["computed_feature_id"].startswith(
        "signal_features:MSFT:"
    )
    assert msft["top_disconfirming_evidence"]["computed_feature_id"]

    later_available_at = datetime(2026, 5, 10, 22, 5, tzinfo=UTC)
    later_available_at_text = later_available_at.isoformat()
    _update_signal_available_at(database_url, later_available_at)
    assert (
        main(
            [
                "build-packets",
                "--as-of",
                "2026-05-10",
                "--available-at",
                later_available_at_text,
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "build-decision-cards",
                "--as-of",
                "2026-05-10",
                "--available-at",
                later_available_at_text,
            ]
        )
        == 0
    )
    capsys.readouterr()

    rows = load_candidate_rows(create_engine(database_url, future=True))
    msft_rows = [row for row in rows if row["ticker"] == "MSFT"]
    assert len(msft_rows) == 1
    assert msft_rows[0]["candidate_packet_available_at"] == later_available_at
    assert msft_rows[0]["decision_card_available_at"] == later_available_at


def test_candidate_packet_cli_returns_nonzero_when_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'missing.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()

    exit_code = main(
        [
            "candidate-packet",
            "--ticker",
            "MSFT",
            "--as-of",
            "2026-05-10",
            "--available-at",
            AVAILABLE_AT_TEXT,
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.err == "candidate packet not found: MSFT\n"


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
                            "market_provider": "csv",
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


def _update_signal_available_at(database_url: str, available_at: datetime) -> None:
    engine = create_engine(database_url, future=True)
    with engine.begin() as conn:
        payload = conn.execute(
            select(signal_features.c.payload).where(
                signal_features.c.ticker == "MSFT",
                signal_features.c.as_of == AS_OF,
                signal_features.c.feature_version == "score-v4-options-theme",
            )
        ).scalar_one()
        payload = dict(payload)
        candidate = dict(payload["candidate"])
        metadata = dict(candidate["metadata"])
        metadata["available_at"] = available_at.isoformat()
        candidate["metadata"] = metadata
        payload["candidate"] = candidate
        conn.execute(
            update(signal_features)
            .where(
                signal_features.c.ticker == "MSFT",
                signal_features.c.as_of == AS_OF,
                signal_features.c.feature_version == "score-v4-options-theme",
            )
            .values(payload=payload)
        )
