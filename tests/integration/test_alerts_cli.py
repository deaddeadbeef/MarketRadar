from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, insert, select

from catalyst_radar.alerts.planner import plan_alerts
from catalyst_radar.cli import main
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import (
    alerts,
    candidate_packets,
    candidate_states,
    decision_cards,
)

AS_OF = datetime(2026, 5, 8, 21, tzinfo=UTC)
AS_OF_TEXT = "2026-05-08"
SOURCE_TS = datetime(2026, 5, 8, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 14, tzinfo=UTC)
AVAILABLE_AT_TEXT = "2026-05-10T14:00:00Z"
FUTURE_AT = AVAILABLE_AT + timedelta(hours=2)


def test_build_alerts_creates_visible_alert_and_lists_it(
    seeded_alert_cli,
    capsys,
) -> None:
    assert main(["build-alerts", "--as-of", AS_OF_TEXT, "--available-at", AVAILABLE_AT_TEXT]) == 0

    captured = capsys.readouterr()
    assert (
        f"built_alerts alerts=3 suppressions=1 available_at={AVAILABLE_AT.isoformat()}"
        in captured.out
    )

    assert main(["alerts-list", "--available-at", AVAILABLE_AT_TEXT]) == 0
    captured = capsys.readouterr()
    assert "MSFT alert route=immediate_manual_review status=planned dedupe_key=" in captured.out
    assert "AAPL alert route=warning_digest status=planned dedupe_key=" in captured.out
    assert "ORCL alert route=daily_digest status=planned dedupe_key=" in captured.out


def test_build_alerts_rerun_records_suppression_not_duplicate(
    seeded_alert_cli,
    capsys,
) -> None:
    assert main(["build-alerts", "--as-of", AS_OF_TEXT, "--available-at", AVAILABLE_AT_TEXT]) == 0
    assert main(["build-alerts", "--as-of", AS_OF_TEXT, "--available-at", AVAILABLE_AT_TEXT]) == 0

    captured = capsys.readouterr()
    assert "built_alerts alerts=0 suppressions=4" in captured.out

    repo = AlertRepository(create_engine(seeded_alert_cli, future=True))
    rows = repo.list_alerts(available_at=AVAILABLE_AT)
    suppressions = repo.list_suppressions(available_at=AVAILABLE_AT)
    assert len(rows) == 3
    duplicate_suppressions = [
        suppression
        for suppression in suppressions
        if suppression.reason == "duplicate_trigger"
    ]
    assert len(duplicate_suppressions) == 3
    assert {suppression.dedupe_key for suppression in duplicate_suppressions} == {
        row.dedupe_key for row in rows
    }


def test_build_alerts_does_not_use_future_decision_card(
    seeded_alert_cli,
    capsys,
) -> None:
    assert (
        main(
            [
                "build-alerts",
                "--as-of",
                AS_OF_TEXT,
                "--available-at",
                AVAILABLE_AT_TEXT,
                "--ticker",
                "MSFT",
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["alert_count"] == 1
    assert payload["alerts"][0]["candidate_packet_id"] == "packet-msft"
    assert payload["alerts"][0]["decision_card_id"] == "card-msft-visible"
    assert payload["alerts"][0]["candidate_packet_id"] != "packet-msft-created-future"
    assert payload["alerts"][0]["decision_card_id"] != "card-msft-future"
    assert payload["alerts"][0]["decision_card_id"] != "card-msft-created-future"


def test_alert_digest_groups_digest_routes(seeded_alert_cli, capsys) -> None:
    assert main(["build-alerts", "--as-of", AS_OF_TEXT, "--available-at", AVAILABLE_AT_TEXT]) == 0
    assert main(["alert-digest", "--available-at", AVAILABLE_AT_TEXT]) == 0

    captured = capsys.readouterr()
    assert "alert_digest groups=2 alerts=3 suppressed=1" in captured.out

    assert main(["alert-digest", "--available-at", AVAILABLE_AT_TEXT, "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [group["route"] for group in payload["groups"]] == [
        "warning_digest",
        "daily_digest",
    ]


def test_send_alerts_dry_run_marks_alerts_without_external_delivery(
    seeded_alert_cli,
    capsys,
) -> None:
    assert main(["build-alerts", "--as-of", AS_OF_TEXT, "--available-at", AVAILABLE_AT_TEXT]) == 0
    assert main(["send-alerts", "--available-at", AVAILABLE_AT_TEXT]) == 0

    captured = capsys.readouterr()
    assert "send_alerts dry_run=true alerts=3" in captured.out

    with create_engine(seeded_alert_cli, future=True).connect() as conn:
        statuses = [
            row.status
            for row in conn.execute(
                select(alerts.c.status).order_by(alerts.c.ticker)
            )
        ]
    assert statuses == ["dry_run", "dry_run", "dry_run"]

    assert main(["send-alerts", "--available-at", AVAILABLE_AT_TEXT]) == 0
    captured = capsys.readouterr()
    assert "send_alerts dry_run=true alerts=0" in captured.out


def test_plan_alerts_limit_prioritizes_alertable_candidate(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'limit.db'}", future=True)
    create_schema(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states),
            [
                _candidate_state("state-aaa", "AAA", "NoAction", "NoAction", 1.0, 0.0),
                _candidate_state(
                    "state-zzz",
                    "ZZZ",
                    "ExitInvalidateReview",
                    "Warning",
                    99.0,
                    22.0,
                ),
            ],
        )

    result = plan_alerts(
        AlertRepository(engine),
        as_of=AS_OF,
        available_at=AVAILABLE_AT,
        limit=1,
    )

    assert [alert.ticker for alert in result.alerts] == ["ZZZ"]
    assert result.suppressions == ()


@pytest.fixture
def seeded_alert_cli(tmp_path, monkeypatch) -> str:
    database_url = f"sqlite:///{tmp_path / 'alerts-cli.db'}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    _seed_candidates(engine)
    return database_url


def _seed_candidates(engine) -> None:
    with engine.begin() as conn:
        for row in (
            _candidate_state(
                "state-msft",
                "MSFT",
                "EligibleForManualBuyReview",
                "ResearchOnly",
                92.0,
                18.0,
            ),
            _candidate_state("state-aapl", "AAPL", "Warning", "ResearchOnly", 81.0, 15.0),
            _candidate_state("state-nvda", "NVDA", "Warning", "ResearchOnly", 72.0, 4.0),
            _candidate_state("state-orcl", "ORCL", "ResearchOnly", "NoAction", 64.0, 2.0),
        ):
            conn.execute(insert(candidate_states).values(**row))

        for row in (
            _packet("packet-msft", "MSFT", "state-msft", "EligibleForManualBuyReview"),
            _packet(
                "packet-msft-created-future",
                "MSFT",
                "state-msft",
                "EligibleForManualBuyReview",
                created_at=FUTURE_AT,
            ),
            _packet("packet-aapl", "AAPL", "state-aapl", "Warning"),
            _packet("packet-nvda", "NVDA", "state-nvda", "Warning"),
            _packet("packet-orcl", "ORCL", "state-orcl", "ResearchOnly"),
        ):
            conn.execute(insert(candidate_packets).values(**row))

        conn.execute(
            insert(decision_cards).values(
                **_card(
                    "card-msft-visible",
                    "MSFT",
                    "packet-msft",
                    "EligibleForManualBuyReview",
                    AVAILABLE_AT,
                )
            )
        )
        conn.execute(
            insert(decision_cards).values(
                **_card(
                    "card-msft-future",
                    "MSFT",
                    "packet-msft",
                    "EligibleForManualBuyReview",
                    FUTURE_AT,
                )
            )
        )
        conn.execute(
            insert(decision_cards).values(
                **_card(
                    "card-msft-created-future",
                    "MSFT",
                    "packet-msft",
                    "EligibleForManualBuyReview",
                    AVAILABLE_AT,
                    created_at=FUTURE_AT,
                )
            )
        )


def _candidate_state(
    id: str,
    ticker: str,
    state: str,
    previous_state: str,
    final_score: float,
    score_delta_5d: float,
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": ticker,
        "as_of": AS_OF,
        "state": state,
        "previous_state": previous_state,
        "final_score": final_score,
        "score_delta_5d": score_delta_5d,
        "hard_blocks": [],
        "transition_reasons": {
            "candidate": {
                "ticker": ticker,
                "as_of": AS_OF.isoformat(),
                "entry_zone": [100.0, 104.0],
                "invalidation_price": 94.0,
                "metadata": {"source_ts": SOURCE_TS.isoformat()},
            }
        },
        "feature_version": "score-v4-options-theme",
        "policy_version": "policy-v2-events",
        "created_at": AVAILABLE_AT - timedelta(minutes=5),
    }


def _packet(
    id: str,
    ticker: str,
    candidate_state_id: str,
    state: str,
    *,
    created_at: datetime = AVAILABLE_AT,
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": ticker,
        "as_of": AS_OF,
        "candidate_state_id": candidate_state_id,
        "state": state,
        "final_score": 80.0,
        "schema_version": "candidate-packet-v1",
        "source_ts": SOURCE_TS,
        "available_at": AVAILABLE_AT,
        "payload": {
            "supporting_evidence": [
                {
                    "source_id": f"event-{ticker.lower()}",
                    "title": f"{ticker} evidence update",
                    "source_url": f"https://example.com/{ticker.lower()}",
                }
            ],
            "trade_plan": {
                "entry_zone": [100.0, 104.0],
                "invalidation_price": 94.0,
            },
        },
        "created_at": created_at,
    }


def _card(
    id: str,
    ticker: str,
    candidate_packet_id: str,
    action_state: str,
    available_at: datetime,
    *,
    created_at: datetime | None = None,
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": ticker,
        "as_of": AS_OF,
        "candidate_packet_id": candidate_packet_id,
        "action_state": action_state,
        "setup_type": "breakout",
        "final_score": 92.0,
        "schema_version": "decision-card-v1",
        "source_ts": SOURCE_TS,
        "available_at": available_at,
        "next_review_at": available_at + timedelta(days=1),
        "user_decision": None,
        "payload": {
            "evidence": [{"source_id": f"card-{ticker.lower()}", "title": "visible"}],
            "trade_plan": {
                "entry_zone": [100.0, 104.0],
                "invalidation_price": 94.0,
            },
        },
        "created_at": created_at or available_at,
    }
