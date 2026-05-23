from __future__ import annotations

import json
from datetime import UTC, date, datetime

from fastapi.testclient import TestClient
from sqlalchemy import insert

from apps.api.main import create_app
from catalyst_radar.cli import main
from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard.tui import render_dashboard_tui
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import candidate_states
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import (
    ValidationResult,
    ValidationRun,
    ValidationRunStatus,
    ValueOutcome,
    value_outcome_id,
)
from catalyst_radar.validation.value_ledger import build_value_ledger_entry

MISSION_BASELINES = (
    "relative_strength_screener",
    "volume_breakout_screener",
    "sector_etf_rotation_screener",
    "news_event_only_screener",
    "random_sector_matched_basket",
)


def test_value_report_cli_empty_month_is_insufficient_evidence(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-report-empty.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    create_schema(engine_from_url(database_url))

    exit_code = main(
        [
            "value-report",
            "--month",
            "2026-05",
            "--available-at",
            "2026-05-31T21:00:00+00:00",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "monthly-value-report-v1"
    assert payload["verdict"] == "insufficient_evidence"
    assert payload["entry_count"] == 0
    assert payload["plausibly_earned_at_least_40_usd"] is False
    assert payload["decision_support_value_not_profit"] is True
    validation = payload["validation_evidence"]
    assert validation["status"] == "no_validation_runs"
    assert validation["ready"] is False
    assert validation["external_calls_made"] == 0
    assert validation["db_writes_made"] == 0
    assert "validation-replay" in validation["next_action"]
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_value_report_surfaces_latest_validation_baseline_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-report-validation.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_successful_validation_run(engine)

    response = TestClient(create_app()).get(
        "/api/value-report/monthly",
        params={
            "month": "2026-05",
            "available_at": "2026-05-31T21:00:00+00:00",
        },
    )

    assert response.status_code == 200
    validation = response.json()["validation_evidence"]
    assert validation["status"] == "ready"
    assert validation["ready"] is True
    assert validation["selected_run_id"] == "validation-run-ready"
    assert validation["candidate_result_count"] == 1
    assert validation["baseline_result_count"] == 5
    assert validation["required_baselines"] == list(MISSION_BASELINES)
    assert validation["measured_baselines"] == list(MISSION_BASELINES)
    assert validation["insufficient_baselines"] == []
    assert validation["missing_baselines"] == []
    assert validation["precision_at_5"] == 1.0
    assert validation["precision_at_10"] == 1.0
    assert validation["external_calls_made"] == 0
    assert validation["db_writes_made"] == 0


def test_value_report_surfaces_missing_candidate_ledger_coverage(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-report-coverage.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_candidate_state(engine, ticker="AAPL", state_id="state-AAPL")

    exit_code = main(
        [
            "value-report",
            "--month",
            "2026-05",
            "--available-at",
            "2026-05-31T21:00:00+00:00",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    coverage = payload["candidate_ledger_coverage"]
    assert payload["verdict"] == "insufficient_evidence"
    assert payload["entry_count"] == 0
    assert coverage["status"] == "gaps"
    assert coverage["surfaced_candidate_count"] == 1
    assert coverage["logged_candidate_count"] == 0
    assert coverage["missing_ledger_count"] == 1
    assert coverage["coverage_pct"] == 0.0
    assert coverage["external_calls_made"] == 0
    assert coverage["db_writes_made"] == 0
    assert coverage["rows"][0]["candidate_state_id"] == "state-AAPL"
    assert "--artifact-id state-AAPL" in coverage["rows"][0]["record_command"]
    outcome_coverage = payload["value_outcome_coverage"]
    assert outcome_coverage["status"] == "no_ledger_entries"
    assert outcome_coverage["ledger_entry_count"] == 0
    assert outcome_coverage["coverage_pct"] is None
    assert outcome_coverage["external_calls_made"] == 0
    assert outcome_coverage["db_writes_made"] == 0
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_value_report_passes_when_useful_decision_support_covers_40_usd(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-report-pass.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_entry(
        engine,
        artifact_type="decision_card",
        artifact_id="card-MSFT",
        label="good-research",
        ticker="MSFT",
        estimated_value_usd=40,
        confidence=1,
        cost_to_produce_usd=1,
        supported_action="research",
        user_decision="accepted",
        payload={"research_time_saved_minutes": 30, "research_time_saved_usd": 25},
    )
    paper_entry_id = _seed_entry(
        engine,
        artifact_type="manual_note",
        artifact_id="note-AAPL",
        label="avoided-loss",
        ticker="AAPL",
        estimated_value_usd=20,
        confidence=0.75,
        cost_to_produce_usd=1,
        supported_action="paper_trade",
        user_decision="avoided",
        provider_call_count=1,
        payload={"operating_time_cost_usd": 2},
    )
    _seed_outcome(engine, ledger_id=paper_entry_id, ticker="AAPL")

    response = TestClient(create_app()).get(
        "/api/value-report/monthly",
        params={
            "month": "2026-05",
            "available_at": "2026-05-31T21:00:00+00:00",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["verdict"] == "pass"
    assert payload["threshold_met"] is True
    assert payload["plausibly_met_40_usd_threshold"] is True
    assert payload["useful_insights_count"] == 2
    assert payload["acted_insights_count"] == 2
    assert payload["avoided_bad_entries_count"] == 1
    assert payload["paper_trade_outcome_count"] == 1
    assert payload["confidence_weighted_value_usd"] == 55.0
    assert payload["total_cost_usd"] == 4.0
    assert payload["net_decision_support_value_usd"] == 51.0
    assert payload["provider_call_count"] == 1
    assert payload["llm_call_count"] == 0
    assert payload["llm_reviewed_entry_count"] == 0
    assert payload["useful_llm_reviewed_entry_count"] == 0
    assert payload["llm_reviewed_costs_usd"] == 0.0
    assert payload["cost_per_useful_llm_reviewed_candidate"] is None
    assert payload["outcome_status_counts"] == {"computed": 1}
    assert payload["profit_calculation_included"] is False
    assert payload["investment_advice"] is False
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_value_report_counts_useful_llm_reviewed_candidate_cost(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-report-llm.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_entry(
        engine,
        artifact_type="decision_card",
        artifact_id="card-LLM1",
        label="useful",
        ticker="LLM1",
        estimated_value_usd=30,
        confidence=1,
        cost_to_produce_usd=2.5,
        llm_call_count=1,
    )
    _seed_entry(
        engine,
        artifact_type="decision_card",
        artifact_id="card-LLM2",
        label="false-positive",
        ticker="LLM2",
        estimated_value_usd=30,
        confidence=1,
        cost_to_produce_usd=1.5,
        llm_call_count=1,
    )
    _seed_entry(
        engine,
        artifact_type="decision_card",
        artifact_id="card-NOLLM",
        label="useful",
        ticker="NOLLM",
        estimated_value_usd=20,
        confidence=1,
        cost_to_produce_usd=0.5,
        llm_call_count=0,
    )

    response = TestClient(create_app()).get(
        "/api/value-report/monthly",
        params={
            "month": "2026-05",
            "available_at": "2026-05-31T21:00:00+00:00",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["llm_call_count"] == 2
    assert payload["llm_reviewed_entry_count"] == 2
    assert payload["useful_llm_reviewed_entry_count"] == 1
    assert payload["llm_reviewed_costs_usd"] == 4.0
    assert payload["cost_per_useful_llm_reviewed_candidate"] == 4.0
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_value_report_fails_below_threshold_and_counts_noise(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-report-fail.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_entry(
        engine,
        artifact_id="note-LOW1",
        label="useful",
        ticker="LOW1",
        estimated_value_usd=10,
        confidence=0.5,
        cost_to_produce_usd=1,
    )
    _seed_entry(
        engine,
        artifact_id="note-LOW2",
        label="good-research",
        ticker="LOW2",
        estimated_value_usd=10,
        confidence=0.5,
        cost_to_produce_usd=1,
    )
    _seed_entry(
        engine,
        artifact_id="note-NOISE",
        label="false-positive",
        ticker="NOISE",
        estimated_value_usd=30,
        confidence=1,
        cost_to_produce_usd=1,
    )

    exit_code = main(
        [
            "value-report",
            "--month",
            "2026-05",
            "--available-at",
            "2026-05-31T21:00:00+00:00",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["verdict"] == "fail"
    assert payload["threshold_met"] is False
    assert payload["useful_insights_count"] == 2
    assert payload["noisy_insights_count"] == 1
    assert payload["false_positive_count"] == 1
    assert payload["missed_signal_count"] == 0
    assert payload["confidence_weighted_value_usd"] == 10.0
    assert payload["net_decision_support_value_usd"] == 7.0


def test_value_report_does_not_treat_positive_ignored_entry_as_useful(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-report-ignored.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_entry(
        engine,
        artifact_id="note-ignored",
        label="ignored",
        ticker="IGN",
        estimated_value_usd=100,
        confidence=1,
        user_decision="ignored",
    )

    response = TestClient(create_app()).get(
        "/api/value-report/monthly",
        params={
            "month": "2026-05",
            "available_at": "2026-05-31T21:00:00+00:00",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["verdict"] == "insufficient_evidence"
    assert payload["useful_insights_count"] == 0
    assert payload["ignored_insights_count"] == 1


def test_cost_page_renders_monthly_value_report() -> None:
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
                "entry_count": 2,
                "confidence_weighted_value_usd": 55.0,
                "cost_to_produce_usd": 2.0,
                "net_confidence_weighted_value_usd": 53.0,
                "target_monthly_value_usd": 40.0,
                "target_coverage_pct": 137.5,
                "chatgpt_pro_offset_pct": 27.5,
                "useful_definition": "Useful means a logged artifact changed a decision.",
                "top_entries": [],
            },
            "value_outcomes": {"outcome_count": 1, "status_counts": {"computed": 1}},
            "value_report": {
                "verdict": "pass",
                "month": "2026-05",
                "net_decision_support_value_usd": 51.0,
                "plausibly_earned_at_least_40_usd": True,
                "useful_insights_count": 2,
                "noisy_insights_count": 0,
                "false_positive_count": 0,
                "decision_support_note": "Decision-support value, not realized profit.",
                "candidate_ledger_coverage": {
                    "surfaced_candidate_count": 3,
                    "logged_candidate_count": 2,
                    "missing_ledger_count": 1,
                    "coverage_pct": 66.67,
                },
                "value_outcome_coverage": {
                    "ledger_entry_count": 2,
                    "linked_outcome_count": 1,
                    "missing_outcome_count": 1,
                    "coverage_pct": 50.0,
                },
                "validation_evidence": {
                    "status": "insufficient_evidence",
                    "selected_run_id": "validation-run-1",
                    "required_baselines": [
                        "relative_strength_screener",
                        "volume_breakout_screener",
                        "sector_etf_rotation_screener",
                        "news_event_only_screener",
                        "random_sector_matched_basket",
                    ],
                    "measured_baselines": [
                        "relative_strength_screener",
                        "volume_breakout_screener",
                    ],
                    "precision_at_5": 0.4,
                    "precision_at_10": 0.3,
                },
            },
        },
        page="costs",
        width=140,
    )

    assert "Monthly value verdict" in text
    assert "Net decision-support value" in text
    assert "Candidate ledger coverage" in text
    assert "2/3 (66.67%)" in text
    assert "Missing candidate ledgers" in text
    assert "Value outcome coverage" in text
    assert "1/2 (50.0%)" in text
    assert "Missing value outcomes" in text
    assert "Validation evidence" in text
    assert "insufficient_evidence" in text
    assert "Mission baselines measured" in text
    assert "2/5" in text
    assert "Precision at 5 / 10" in text
    assert "0.4 / 0.3" in text
    assert "Decision-support value, not realized profit." in text


def _seed_entry(
    engine,
    *,
    artifact_type: str = "manual_note",
    artifact_id: str,
    label: str,
    ticker: str,
    estimated_value_usd: float,
    confidence: float,
    cost_to_produce_usd: float = 0.0,
    supported_action: str | None = "research",
    user_decision: str | None = "accepted",
    provider_call_count: int = 0,
    llm_call_count: int = 0,
    payload: dict[str, object] | None = None,
) -> str:
    entry = build_value_ledger_entry(
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        label=label,
        ticker=ticker,
        as_of=date(2026, 5, 15),
        entry_date=date(2026, 5, 15),
        available_at=datetime(2026, 5, 15, 21, tzinfo=UTC),
        estimated_value_usd=estimated_value_usd,
        confidence=confidence,
        source="test",
        supported_action=supported_action,
        user_decision=user_decision,
        cost_to_produce_usd=cost_to_produce_usd,
        provider_call_count=provider_call_count,
        llm_call_count=llm_call_count,
        payload=payload or {},
    )
    ValidationRepository(engine).upsert_value_ledger_entry(entry)
    return entry.id


def _seed_candidate_state(
    engine,
    *,
    ticker: str,
    state_id: str,
) -> None:
    as_of = datetime(2026, 5, 15, 20, 0, tzinfo=UTC)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                id=state_id,
                ticker=ticker,
                as_of=as_of,
                state=ActionState.WARNING.value,
                previous_state=None,
                final_score=82.0,
                score_delta_5d=7.0,
                hard_blocks=[],
                transition_reasons=["priced-in gap"],
                feature_version="test",
                policy_version="test",
                created_at=as_of,
            )
        )


def _seed_successful_validation_run(engine) -> None:
    started_at = datetime(2026, 5, 31, 20, tzinfo=UTC)
    finished_at = datetime(2026, 5, 31, 20, 30, tzinfo=UTC)
    run = ValidationRun(
        id="validation-run-ready",
        run_type="replay",
        as_of_start=datetime(2026, 5, 15, 21, tzinfo=UTC),
        as_of_end=datetime(2026, 5, 15, 21, tzinfo=UTC),
        decision_available_at=datetime(2026, 5, 16, 21, tzinfo=UTC),
        status=ValidationRunStatus.SUCCESS,
        started_at=started_at,
        finished_at=finished_at,
    )
    repo = ValidationRepository(engine)
    repo.upsert_validation_run(run)
    repo.upsert_validation_results(
        [
            _validation_result(
                "validation-run-ready",
                ticker="MRDR",
                baseline=None,
                rank=None,
            ),
            *[
                _validation_result(
                    "validation-run-ready",
                    ticker=f"BL{index}",
                    baseline=baseline,
                    rank=1,
                )
                for index, baseline in enumerate(MISSION_BASELINES, start=1)
            ],
        ]
    )


def _validation_result(
    run_id: str,
    *,
    ticker: str,
    baseline: str | None,
    rank: int | None,
) -> ValidationResult:
    as_of = datetime(2026, 5, 15, 21, tzinfo=UTC)
    return ValidationResult(
        id=f"{run_id}:{baseline or 'candidate'}:{ticker}",
        run_id=run_id,
        ticker=ticker,
        as_of=as_of,
        available_at=datetime(2026, 5, 16, 21, tzinfo=UTC),
        state=ActionState.WARNING,
        final_score=90,
        baseline=baseline,
        labels={
            "target_20d_25": True,
            "max_adverse_excursion": -0.02,
            "max_favorable_excursion": 0.3,
        },
        payload={"candidate": {"rank": rank}} if rank is not None else {},
    )


def _seed_outcome(engine, *, ledger_id: str, ticker: str) -> None:
    outcome_at = datetime(2026, 5, 30, 21, tzinfo=UTC)
    outcome = ValueOutcome(
        id=value_outcome_id(
            value_ledger_entry_id=ledger_id,
            outcome_available_at=outcome_at,
        ),
        value_ledger_entry_id=ledger_id,
        ticker=ticker,
        as_of=date(2026, 5, 15),
        outcome_available_at=outcome_at,
        status="computed",
        trading_days_observed=10,
        entry_price=100,
        return_5d=0.05,
        return_10d=0.1,
    )
    ValidationRepository(engine).upsert_value_outcome(outcome)
