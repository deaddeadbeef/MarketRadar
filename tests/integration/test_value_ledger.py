from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy import func, insert, select

from apps.api.main import create_app
from catalyst_radar.cli import main
from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard.tui import render_dashboard_tui
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import (
    candidate_states,
    signal_features,
    value_ledger_entries,
)


def test_value_ledger_cli_preview_execute_and_summary(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-ledger-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _insert_candidate_state(engine)

    preview_exit = main(
        [
            "value-ledger",
            "record",
            "--artifact-type",
            "candidate_state",
            "--artifact-id",
            "state-MSFT",
            "--label",
            "good-research",
            "--supported-action",
            "research",
            "--user-decision",
            "accepted",
            "--estimated-value-usd",
            "50",
            "--confidence",
            "0.8",
            "--cost-to-produce-usd",
            "2",
            "--provider-call-count",
            "0",
            "--llm-call-count",
            "0",
            "--entry-date",
            "2026-05-15",
            "--available-at",
            "2026-05-22T12:00:00+00:00",
            "--json",
        ]
    )

    assert preview_exit == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["mode"] == "preview"
    assert preview["external_calls_required"] == 0
    assert preview["external_calls_made"] == 0
    assert preview["db_writes_required"] == 1
    assert preview["db_writes_made"] == 0
    assert preview["entry"]["ticker"] == "MSFT"
    assert preview["entry"]["candidate_state_id"] == "state-MSFT"
    assert preview["entry"]["action_state"] == "warning"
    assert preview["entry"]["final_score"] == 72.0
    with engine.connect() as conn:
        assert (
            conn.execute(select(func.count()).select_from(value_ledger_entries)).scalar_one()
            == 0
        )

    execute_exit = main(
        [
            "value-ledger",
            "record",
            "--artifact-type",
            "candidate_state",
            "--artifact-id",
            "state-MSFT",
            "--label",
            "good-research",
            "--supported-action",
            "research",
            "--user-decision",
            "accepted",
            "--estimated-value-usd",
            "50",
            "--confidence",
            "0.8",
            "--cost-to-produce-usd",
            "2",
            "--entry-date",
            "2026-05-15",
            "--available-at",
            "2026-05-22T12:00:00+00:00",
            "--execute",
            "--json",
        ]
    )

    assert execute_exit == 0
    executed = json.loads(capsys.readouterr().out)
    assert executed["mode"] == "executed"
    assert executed["db_writes_made"] == 1
    with engine.connect() as conn:
        stored = conn.execute(select(value_ledger_entries)).first()
    assert stored is not None
    stored_row = stored._mapping
    assert stored_row["ticker"] == "MSFT"
    assert stored_row["candidate_state_id"] == "state-MSFT"
    assert stored_row["supported_action"] == "research"
    assert stored_row["user_decision"] == "accepted"
    assert stored_row["estimated_value_usd"] == 50.0
    assert stored_row["confidence"] == 0.8
    assert stored_row["cost_to_produce_usd"] == 2.0

    summary_exit = main(
        [
            "value-ledger",
            "summary",
            "--available-at",
            "2026-05-22T12:00:00+00:00",
            "--json",
        ]
    )

    assert summary_exit == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["schema_version"] == "value-ledger-summary-v1"
    assert summary["external_calls_made"] == 0
    assert summary["db_writes_made"] == 0
    assert summary["entry_count"] == 1
    assert summary["confidence_weighted_value_usd"] == 40.0
    assert summary["cost_to_produce_usd"] == 2.0
    assert summary["net_confidence_weighted_value_usd"] == 38.0
    assert summary["target_coverage_pct"] == 100.0
    assert summary["chatgpt_pro_offset_pct"] == 20.0

    show_exit = main(["value-ledger", "show", executed["entry"]["id"], "--json"])

    assert show_exit == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["schema_version"] == "value-ledger-entry-v1"
    assert shown["external_calls_made"] == 0
    assert shown["db_writes_made"] == 0
    assert shown["entry"]["id"] == executed["entry"]["id"]


def test_value_ledger_coverage_reports_unlogged_surfaced_candidates(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-ledger-coverage.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _insert_candidate_state(engine, ticker="MSFT", state_id="state-MSFT")
    _insert_candidate_state(
        engine,
        ticker="AAPL",
        state_id="state-AAPL",
        state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
        final_score=91.0,
    )
    _insert_candidate_state(
        engine,
        ticker="GLW",
        state_id="state-GLW",
        state=ActionState.RESEARCH_ONLY.value,
        final_score=65.0,
    )

    assert (
        main(
            [
                "value-ledger",
                "record",
                "--artifact-type",
                "candidate_state",
                "--artifact-id",
                "state-MSFT",
                "--label",
                "good-research",
                "--supported-action",
                "research",
                "--user-decision",
                "accepted",
                "--estimated-value-usd",
                "10",
                "--confidence",
                "0.5",
                "--entry-date",
                "2026-05-15",
                "--available-at",
                "2026-05-22T12:00:00+00:00",
                "--execute",
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()
    with engine.connect() as conn:
        before = [dict(row._mapping) for row in conn.execute(select(candidate_states))]

    coverage_exit = main(
        [
            "value-ledger",
            "coverage",
            "--available-at",
            "2026-05-22T12:00:00+00:00",
            "--period-start",
            "2026-05-01",
            "--period-end",
            "2026-05-31",
            "--json",
        ]
    )

    assert coverage_exit == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "value-ledger-candidate-coverage-v1"
    assert payload["status"] == "gaps"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["surfaced_candidate_count"] == 2
    assert payload["logged_candidate_count"] == 1
    assert payload["missing_ledger_count"] == 1
    assert payload["coverage_pct"] == 50.0
    rows = {row["candidate_state_id"]: row for row in payload["rows"]}
    assert rows["state-MSFT"]["ledger_status"] == "logged"
    assert rows["state-MSFT"]["ledger_entry_id"]
    assert rows["state-AAPL"]["ledger_status"] == "missing"
    assert "--artifact-id state-AAPL" in rows["state-AAPL"]["record_command"]
    assert "--execute" not in rows["state-AAPL"]["record_command"]
    assert "state-GLW" not in rows
    with engine.connect() as conn:
        after = [dict(row._mapping) for row in conn.execute(select(candidate_states))]
        ledger_count = (
            conn.execute(select(func.count()).select_from(value_ledger_entries)).scalar_one()
        )
    assert after == before
    assert ledger_count == 1


def test_value_ledger_cli_label_command_writes_auditable_entry(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-ledger-label.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _insert_candidate_state(engine)

    label_exit = main(
        [
            "value-ledger",
            "label",
            "--artifact-type",
            "candidate_state",
            "--artifact-id",
            "state-MSFT",
            "--label",
            "useful",
            "--supported-action",
            "research",
            "--user-decision",
            "accepted",
            "--estimated-value-usd",
            "12",
            "--confidence",
            "0.5",
            "--entry-date",
            "2026-05-15",
            "--available-at",
            "2026-05-22T12:00:00+00:00",
            "--execute",
            "--json",
        ]
    )

    assert label_exit == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "executed"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 1
    assert payload["entry"]["label"] == "useful"
    assert payload["entry"]["confidence_weighted_value_usd"] == 6.0

    show_exit = main(["value-ledger", "show", payload["entry"]["id"], "--json"])

    assert show_exit == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["entry"]["label"] == "useful"


def test_value_ledger_cli_autofills_priced_in_context_from_signal_features(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-ledger-context.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _insert_candidate_state(engine)
    _insert_signal_features_with_priced_in_context(engine)
    with engine.connect() as conn:
        candidate_before = dict(conn.execute(select(candidate_states)).first()._mapping)
        signal_before = dict(conn.execute(select(signal_features)).first()._mapping)

    exit_code = main(
        [
            "value-ledger",
            "record",
            "--artifact-type",
            "candidate_state",
            "--artifact-id",
            "state-MSFT",
            "--label",
            "good-research",
            "--supported-action",
            "research",
            "--user-decision",
            "accepted",
            "--estimated-value-usd",
            "25",
            "--confidence",
            "0.5",
            "--entry-date",
            "2026-05-15",
            "--available-at",
            "2026-05-22T12:00:00+00:00",
            "--execute",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    entry = payload["entry"]
    assert entry["priced_in_status"] == "bullish_not_priced_in"
    assert entry["priced_in_direction"] == "bullish"
    assert entry["emotion_score"] == 82.0
    assert entry["reaction_score"] == 35.0
    assert entry["emotion_reaction_gap"] == 47.0
    assert entry["setup_type"] == "earnings_reaction_gap"
    with engine.connect() as conn:
        stored = conn.execute(select(value_ledger_entries)).first()
        candidate_after = dict(conn.execute(select(candidate_states)).first()._mapping)
        signal_after = dict(conn.execute(select(signal_features)).first()._mapping)
    assert stored is not None
    stored_row = stored._mapping
    assert stored_row["priced_in_status"] == "bullish_not_priced_in"
    assert stored_row["priced_in_direction"] == "bullish"
    assert stored_row["emotion_score"] == 82.0
    assert stored_row["reaction_score"] == 35.0
    assert stored_row["emotion_reaction_gap"] == 47.0
    assert stored_row["setup_type"] == "earnings_reaction_gap"
    assert candidate_after == candidate_before
    assert signal_after == signal_before


def test_value_ledger_api_preview_execute_and_read(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-ledger-api.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _insert_candidate_state(engine, ticker="AAPL", state_id="state-AAPL")
    client = TestClient(create_app())

    body = {
        "artifact_type": "candidate_state",
        "artifact_id": "state-AAPL",
        "label": "avoided-loss",
        "supported_action": "avoid",
        "user_decision": "avoided",
        "estimated_value_usd": 20,
        "confidence": 0.5,
        "entry_date": "2026-05-15",
        "available_at": "2026-05-22T12:00:00+00:00",
    }

    preview_response = client.post("/api/value-ledger/entries", json=body)

    assert preview_response.status_code == 200
    preview = preview_response.json()
    assert preview["mode"] == "preview"
    assert preview["db_writes_made"] == 0
    with engine.connect() as conn:
        assert (
            conn.execute(select(func.count()).select_from(value_ledger_entries)).scalar_one()
            == 0
        )

    execute_response = client.post(
        "/api/value-ledger/entries",
        json={**body, "execute": True},
    )

    assert execute_response.status_code == 200
    assert execute_response.json()["db_writes_made"] == 1
    list_response = client.get(
        "/api/value-ledger/entries",
        params={"available_at": "2026-05-22T12:00:00+00:00", "ticker": "AAPL"},
    )
    assert list_response.status_code == 200
    entries_payload = list_response.json()
    assert entries_payload["external_calls_made"] == 0
    assert entries_payload["db_writes_made"] == 0
    assert entries_payload["count"] == 1
    assert entries_payload["entries"][0]["ticker"] == "AAPL"
    assert entries_payload["entries"][0]["supported_action"] == "avoid"
    entry_id = execute_response.json()["entry"]["id"]
    show_response = client.get(f"/api/value-ledger/entries/{entry_id}")
    assert show_response.status_code == 200
    shown = show_response.json()
    assert shown["external_calls_made"] == 0
    assert shown["db_writes_made"] == 0
    assert shown["entry"]["id"] == entry_id
    assert shown["entry"]["ticker"] == "AAPL"

    missing_response = client.get("/api/value-ledger/entries/missing-entry")
    assert missing_response.status_code == 404

    summary_response = client.get(
        "/api/value-ledger/summary",
        params={"available_at": "2026-05-22T12:00:00+00:00"},
    )

    assert summary_response.status_code == 200
    summary = summary_response.json()
    assert summary["confidence_weighted_value_usd"] == 10.0
    assert summary["target_coverage_pct"] == 25.0
    coverage_response = client.get(
        "/api/value-ledger/coverage",
        params={"available_at": "2026-05-22T12:00:00+00:00"},
    )
    assert coverage_response.status_code == 200
    coverage = coverage_response.json()
    assert coverage["external_calls_made"] == 0
    assert coverage["db_writes_made"] == 0
    assert coverage["surfaced_candidate_count"] == 1
    assert coverage["missing_ledger_count"] == 0


def test_value_ledger_rejects_unknown_label_and_missing_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-ledger-invalid.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _insert_candidate_state(engine)
    client = TestClient(create_app())

    bad_label = client.post(
        "/api/value-ledger/entries",
        json={
            "artifact_type": "candidate_state",
            "artifact_id": "state-MSFT",
            "label": "maybe",
            "estimated_value_usd": 1,
            "confidence": 1,
        },
    )
    missing_artifact = client.post(
        "/api/value-ledger/entries",
        json={
            "artifact_type": "candidate_state",
            "artifact_id": "missing-state",
            "label": "useful",
            "estimated_value_usd": 1,
            "confidence": 1,
        },
    )

    assert bad_label.status_code == 422
    assert missing_artifact.status_code == 422
    with engine.connect() as conn:
        assert (
            conn.execute(select(func.count()).select_from(value_ledger_entries)).scalar_one()
            == 0
        )


def test_value_ledger_feedback_does_not_mutate_candidate_state(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-ledger-no-mutate.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _insert_candidate_state(engine)
    with engine.connect() as conn:
        before = conn.execute(select(candidate_states).limit(1)).first()
    assert before is not None

    response = TestClient(create_app()).post(
        "/api/value-ledger/entries",
        json={
            "artifact_type": "candidate_state",
            "artifact_id": "state-MSFT",
            "label": "useful",
            "estimated_value_usd": 5,
            "confidence": 0.5,
            "execute": True,
        },
    )

    assert response.status_code == 200
    with engine.connect() as conn:
        after = conn.execute(select(candidate_states).limit(1)).first()
        ledger_count = (
            conn.execute(select(func.count()).select_from(value_ledger_entries)).scalar_one()
        )
    assert after is not None
    assert dict(after._mapping) == dict(before._mapping)
    assert ledger_count == 1


def test_cost_page_renders_value_ledger_target_progress() -> None:
    text = render_dashboard_tui(
        {
            "costs": {
                "attempt_count": 0,
                "total_actual_cost_usd": 0.0,
                "total_estimated_cost_usd": 0.0,
                "useful_alert_count": 0,
                "cost_per_useful_alert": None,
                "status_counts": {},
            },
            "value_ledger": {
                "entry_count": 1,
                "confidence_weighted_value_usd": 40.0,
                "target_monthly_value_usd": 40.0,
                "target_coverage_pct": 100.0,
                "chatgpt_pro_offset_pct": 20.0,
                "useful_definition": "Useful means a logged artifact changed a decision.",
                "top_entries": [
                    {
                        "entry_date": "2026-05-15",
                        "ticker": "MSFT",
                        "label": "good-research",
                        "confidence_weighted_value_usd": 40.0,
                        "artifact_id": "card-MSFT",
                    }
                ],
            },
        },
        page="costs",
        width=140,
    )

    assert "Value ledger entries" in text
    assert "ChatGPT Pro offset pct" in text
    assert "good-research" in text


def _insert_candidate_state(
    engine,
    *,
    ticker: str = "MSFT",
    state_id: str = "state-MSFT",
    state: str = "warning",
    final_score: float = 72.0,
) -> None:
    as_of = datetime(2026, 5, 15, 20, 0, tzinfo=UTC)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                id=state_id,
                ticker=ticker,
                as_of=as_of,
                state=state,
                previous_state=None,
                final_score=final_score,
                score_delta_5d=4.0,
                hard_blocks=[],
                transition_reasons=["priced-in gap"],
                feature_version="test",
                policy_version="test",
                created_at=as_of,
            )
        )


def _insert_signal_features_with_priced_in_context(engine) -> None:
    as_of = datetime(2026, 5, 15, 20, 0, tzinfo=UTC)
    with engine.begin() as conn:
        conn.execute(
            insert(signal_features).values(
                ticker="MSFT",
                as_of=as_of,
                feature_version="test",
                price_strength=70.0,
                volume_score=65.0,
                liquidity_score=90.0,
                risk_penalty=0.0,
                portfolio_penalty=0.0,
                final_score=72.0,
                payload={
                    "candidate": {
                        "ticker": "MSFT",
                        "as_of": as_of.isoformat(),
                        "metadata": {
                            "setup_type": "earnings_reaction_gap",
                            "priced_in": {
                                "status": "bullish_not_priced_in",
                                "direction": "bullish",
                                "emotion_score": 82.0,
                                "reaction_score": 35.0,
                                "emotion_reaction_gap": 47.0,
                            },
                        },
                    },
                },
            )
        )
