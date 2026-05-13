from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, insert, update

from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMSkipReason,
    LLMTaskName,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.brokers.models import (
    BrokerAccount,
    BrokerBalanceSnapshot,
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerPosition,
    broker_account_id,
    broker_balance_snapshot_id,
    broker_connection_id,
    broker_position_id,
)
from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard.data import (
    load_alert_detail,
    load_alert_rows,
    load_broker_summary,
    load_candidate_rows,
    load_cost_summary,
    load_ipo_s1_rows,
    load_ops_health,
    load_radar_run_summary,
    load_theme_rows,
    load_ticker_detail,
    load_validation_summary,
)
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import (
    alerts,
    candidate_packets,
    candidate_states,
    decision_cards,
    events,
    job_runs,
    paper_trades,
    provider_health,
    signal_features,
    text_snippets,
    useful_alert_labels,
    user_feedback,
    validation_results,
    validation_runs,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
EARLIER_AS_OF = AS_OF - timedelta(days=1)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)
NEXT_REVIEW_AT = datetime(2026, 5, 12, 21, tzinfo=UTC)
FUTURE_AT = AVAILABLE_AT + timedelta(days=30)


@pytest.fixture(autouse=True)
def _isolate_llm_config_env(monkeypatch) -> None:
    for key in (
        "CATALYST_ENABLE_PREMIUM_LLM",
        "CATALYST_LLM_DAILY_BUDGET_USD",
        "CATALYST_LLM_MONTHLY_BUDGET_USD",
        "CATALYST_LLM_TASK_DAILY_CAPS",
    ):
        monkeypatch.delenv(key, raising=False)


def test_load_candidate_rows_returns_latest_state_per_ticker(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    rows = load_candidate_rows(engine)

    assert [row["id"] for row in rows] == ["state-msft-latest", "state-aapl-latest"]
    assert [row["ticker"] for row in rows] == ["MSFT", "AAPL"]
    msft_brief = rows[0]["research_brief"]
    assert msft_brief["focus"] == "Research now"
    assert msft_brief["why_now"] == "MSFT guidance raised"
    assert msft_brief["supporting_evidence"] == "MSFT guidance raised"
    assert msft_brief["risk_or_gap"] == "Valuation stretched"
    assert msft_brief["decision_card_status"] == "available: card-msft-latest"
    assert msft_brief["audit"]["provider_license_policy"]["license_tags"] == [
        "local-csv-fixture"
    ]
    aapl_brief = rows[1]["research_brief"]
    assert aapl_brief["focus"] == "Research now"
    assert aapl_brief["why_now"] == "breakout setup with score 76.0"
    assert aapl_brief["decision_card_status"] == (
        "not generated; candidate is not in manual-buy-review state"
    )


def test_load_candidate_rows_respects_available_at_cutoff(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                _candidate_state_row(
                    id="state-msft-future",
                    ticker="MSFT",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    created_at=AVAILABLE_AT,
                )
            )
        )
        conn.execute(
            insert(signal_features).values(
                _signal_feature_row(
                    ticker="MSFT",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    theme="future_theme",
                )
            )
        )
        conn.execute(
            insert(candidate_packets).values(
                id="packet-msft-future",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_state_id="state-msft-latest",
                state=ActionState.WARNING.value,
                final_score=88.0,
                schema_version="candidate-packet-v1",
                source_ts=FUTURE_AT,
                available_at=FUTURE_AT,
                payload={"supporting_evidence": [{"title": "Future packet"}]},
                created_at=FUTURE_AT,
            )
        )

    rows = load_candidate_rows(engine, available_at=AVAILABLE_AT + timedelta(minutes=1))

    msft = next(row for row in rows if row["ticker"] == "MSFT")
    assert msft["id"] == "state-msft-latest"
    assert msft["candidate_packet_id"] == "packet-msft-latest"


def test_load_ticker_detail_returns_candidate_packet_card_events_and_validation(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    detail = load_ticker_detail(engine, "msft")

    assert detail is not None
    assert detail["ticker"] == "MSFT"
    assert detail["manual_review_only"] is True
    assert detail["latest_candidate"]["id"] == "state-msft-latest"
    assert detail["latest_candidate"]["candidate_packet_id"] == "packet-msft-latest"
    assert detail["latest_candidate"]["decision_card_id"] == "card-msft-latest"
    assert [row["id"] for row in detail["state_history"]] == [
        "state-msft-latest",
        "state-msft-earlier",
    ]
    assert detail["features"]["feature_version"] == "score-v4-options-theme"
    assert detail["events"][0]["id"] == "event-msft"
    assert detail["snippets"][0]["id"] == "snippet-msft"
    assert "event-msft-future" not in {row["id"] for row in detail["events"]}
    assert "snippet-msft-future" not in {row["id"] for row in detail["snippets"]}
    assert detail["candidate_packet"]["payload"]["supporting_evidence"][0]["title"] == (
        "MSFT guidance raised"
    )
    assert detail["decision_card"]["payload"]["disclaimer"] == "Manual review only."
    assert detail["setup_plan"]["setup_type"] == "breakout"
    assert detail["portfolio_impact"]["proposed_notional"] == 2080.0
    assert detail["validation_results"][0]["id"] == "validation-result-msft"
    assert "validation-result-msft-future" not in {
        row["id"] for row in detail["validation_results"]
    }
    assert detail["paper_trades"][0]["id"] == "paper-msft"
    assert "paper-msft-future" not in {row["id"] for row in detail["paper_trades"]}


def test_load_ticker_detail_respects_candidate_state_cutoff(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                _candidate_state_row(
                    id="state-msft-future",
                    ticker="MSFT",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    created_at=AVAILABLE_AT,
                )
            )
        )
        conn.execute(
            insert(signal_features).values(
                _signal_feature_row(
                    ticker="MSFT",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    theme="future_theme",
                )
            )
        )

    detail = load_ticker_detail(
        engine,
        "MSFT",
        available_at=AVAILABLE_AT + timedelta(minutes=1),
    )

    assert detail is not None
    assert detail["latest_candidate"]["id"] == "state-msft-latest"


def test_load_theme_rows_groups_candidate_themes(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    rows = load_theme_rows(engine)

    ai_row = next(row for row in rows if row["theme"] == "ai_infrastructure")
    assert ai_row["candidate_count"] == 2
    assert ai_row["avg_score"] == 82.0
    assert ai_row["top_tickers"] == ["MSFT", "AAPL"]
    assert ai_row["states"] == {"Warning": 2}
    assert ai_row["latest_as_of"] == AS_OF


def test_load_theme_rows_respects_available_at_cutoff(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                _candidate_state_row(
                    id="state-aapl-future",
                    ticker="AAPL",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    created_at=AVAILABLE_AT,
                )
            )
        )
        conn.execute(
            insert(signal_features).values(
                _signal_feature_row(
                    ticker="AAPL",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    theme="future_theme",
                )
            )
        )

    rows = load_theme_rows(engine, available_at=AVAILABLE_AT + timedelta(minutes=1))

    assert {row["theme"] for row in rows} == {"ai_infrastructure"}


def test_load_validation_summary_returns_latest_run_report_and_paper_trades(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful-old-same-ticker-validation",
                artifact_type="decision_card",
                artifact_id="old-card-msft-not-in-latest-run",
                ticker="MSFT",
                label="useful",
                notes="same ticker but unrelated to latest validation run",
                created_at=AVAILABLE_AT - timedelta(hours=1),
            )
        )

    summary = load_validation_summary(engine)

    assert summary["latest_run"]["id"] == "validation-run-latest"
    assert summary["report"]["candidate_count"] == 1
    assert summary["report"]["precision"]["target_20d_25"] == 1.0
    assert summary["report"]["useful_alert_rate"] == 1.0
    assert summary["report"]["leakage_failure_count"] == 0
    assert [row["id"] for row in summary["paper_trades"]] == ["paper-msft"]
    assert [row["id"] for row in summary["useful_labels"]] == ["label-useful-msft"]


def test_load_alert_rows_returns_latest_alerts_with_feedback(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    _insert_alert_fixture(engine)

    rows = load_alert_rows(engine, available_at=AVAILABLE_AT + timedelta(minutes=30))

    assert [row["id"] for row in rows] == ["alert-msft-dry-run", "alert-msft-planned"]
    assert rows[0]["ticker"] == "MSFT"
    assert rows[0]["route"] == "warning_digest"
    assert rows[0]["status"] == "dry_run"
    assert rows[0]["feedback_label"] == "acted"
    assert rows[0]["feedback_notes"] == "latest feedback"
    assert rows[0]["score_trigger"] == 88.0


def test_load_alert_detail_returns_payload_and_feedback(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    _insert_alert_fixture(engine)

    detail = load_alert_detail(engine, "alert-msft-planned", available_at=AVAILABLE_AT)

    assert detail is not None
    assert detail["id"] == "alert-msft-planned"
    assert detail["payload"]["evidence"][0]["artifact_id"] == "event-msft"
    assert detail["feedback_label"] == "useful"
    assert detail["feedback_notes"] == "worth review"
    assert detail["feedback_id"] == "feedback-alert-msft-planned"


def test_load_alert_rows_respects_available_at_cutoff(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    future_available_at = datetime.now(UTC).replace(microsecond=0) + timedelta(days=1)
    _insert_alert_fixture(engine, available_at=future_available_at)

    default_rows = load_alert_rows(engine)
    rows = load_alert_rows(
        engine,
        available_at=future_available_at + timedelta(minutes=10),
        ticker="msft",
        status="planned",
        route="immediate_manual_review",
    )

    assert default_rows == []
    assert [row["id"] for row in rows] == ["alert-msft-planned"]
    assert rows[0]["feedback_label"] == "useful"
    assert (
        load_alert_detail(engine, "alert-msft-dry-run", available_at=future_available_at)
        is None
    )


def test_load_ipo_s1_rows_returns_visible_analysis_and_filters_future_rows(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    visible_at = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)
    future_at = datetime.now(UTC).replace(microsecond=0) + timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(
            insert(events),
            [
                _ipo_event_row(
                    id="event-acme-s1",
                    ticker="ACME",
                    available_at=visible_at,
                    source_ts=visible_at,
                ),
                _ipo_event_row(
                    id="event-futr-s1",
                    ticker="FUTR",
                    available_at=future_at,
                    source_ts=future_at,
                ),
            ],
        )

    rows = load_ipo_s1_rows(engine)
    filtered = load_ipo_s1_rows(engine, ticker="acme")
    future_visible = load_ipo_s1_rows(engine, available_at=future_at + timedelta(minutes=1))

    assert [row["id"] for row in rows] == ["event-acme-s1"]
    assert [row["ticker"] for row in filtered] == ["ACME"]
    assert rows[0]["form_type"] == "S-1"
    assert rows[0]["proposed_ticker"] == "ACME"
    assert rows[0]["shares_offered"] == 12_500_000
    assert rows[0]["price_range_low"] == 17.0
    assert rows[0]["estimated_gross_proceeds"] == 225_000_000.0
    assert rows[0]["risk_flags"] == ["history_of_losses", "emerging_growth_company"]
    assert [row["id"] for row in future_visible] == ["event-futr-s1", "event-acme-s1"]


def test_load_cost_summary_defaults_to_zero_and_counts_useful_alerts(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful-old-same-ticker",
                artifact_type="decision_card",
                artifact_id="old-card-msft-not-in-latest-run",
                ticker="MSFT",
                label="useful",
                notes="same ticker but unrelated to latest validation run",
                created_at=AVAILABLE_AT - timedelta(hours=1),
            )
        )

    summary = load_cost_summary(engine)

    assert summary["source"] == "budget_ledger"
    assert summary["total_actual_cost_usd"] == 0.0
    assert summary["total_estimated_cost_usd"] == 0.0
    assert summary["validation_total_cost_usd"] == 0.0
    assert summary["useful_alert_count"] == 1
    assert summary["cost_per_useful_alert"] == 0.0
    assert summary["attempt_count"] == 0
    assert summary["status_counts"] == {}
    assert summary["rows"] == []


def test_load_cost_summary_counts_useful_alert_feedback(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    _insert_alert_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(useful_alert_labels),
            [
                {
                    "id": "label-alert-msft-useful",
                    "artifact_type": "alert",
                    "artifact_id": "alert-msft-planned",
                    "ticker": "MSFT",
                    "label": "useful",
                    "notes": "alert mapped to validation result by candidate ids",
                    "created_at": AVAILABLE_AT,
                },
                {
                    "id": "label-alert-aapl-useful",
                    "artifact_type": "alert",
                    "artifact_id": "alert-aapl-unmatched",
                    "ticker": "AAPL",
                    "label": "useful",
                    "notes": "not part of latest validation result rows",
                    "created_at": AVAILABLE_AT,
                },
            ],
        )

    summary = load_cost_summary(engine)

    assert summary["useful_alert_count"] == 2
    assert {row["id"] for row in summary["useful_labels"]} == {
        "label-useful-msft",
        "label-alert-msft-useful",
    }


def test_load_cost_summary_uses_budget_ledger_rows(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    _insert_budget_ledger_fixture(engine, available_at=AVAILABLE_AT)

    summary = load_cost_summary(engine, available_at=AVAILABLE_AT + timedelta(minutes=1))

    assert summary["source"] == "budget_ledger"
    assert summary["currency"] == "USD"
    assert summary["total_actual_cost_usd"] == 0.19
    assert summary["total_estimated_cost_usd"] == 0.22
    assert summary["validation_total_cost_usd"] == 0.0
    assert summary["attempt_count"] == 2
    assert summary["status_counts"] == {"completed": 1, "skipped": 1}
    assert summary["useful_alert_count"] == 1
    assert summary["cost_per_useful_alert"] == 0.19
    assert summary["by_task"][0]["task"] == "mid_review"
    assert summary["by_model"][0]["model"] == "model-review"
    assert [row["status"] for row in summary["rows"]] == ["skipped", "completed"]
    assert summary["caps"] == {
        "premium_llm_enabled": False,
        "daily_budget_usd": 0.0,
        "monthly_budget_usd": 0.0,
        "task_daily_caps": {},
    }


def test_load_cost_summary_keeps_validation_cost_separate(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            update(validation_runs)
            .where(validation_runs.c.id == "validation-run-latest")
            .values(metrics={"total_cost_usd": 7.25})
        )

    summary = load_cost_summary(engine, available_at=AVAILABLE_AT)

    assert summary["total_actual_cost_usd"] == 0.0
    assert summary["total_estimated_cost_usd"] == 0.0
    assert summary["validation_total_cost_usd"] == 7.25
    assert summary["cost_per_useful_alert"] == 0.0


def test_load_cost_summary_validation_context_respects_requested_cutoff(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            update(validation_runs)
            .where(validation_runs.c.id == "validation-run-latest")
            .values(metrics={"total_cost_usd": 7.25})
        )

    summary = load_cost_summary(engine, available_at=AVAILABLE_AT - timedelta(minutes=1))

    assert summary["validation_total_cost_usd"] == 0.0
    assert summary["useful_alert_count"] == 0
    assert summary["useful_labels"] == []


def test_load_cost_summary_hides_future_ledger_rows_by_default(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    visible_at = datetime.now(UTC) - timedelta(hours=1)
    future_at = datetime.now(UTC) + timedelta(days=1)
    _insert_budget_ledger_fixture(engine, available_at=visible_at, future_at=future_at)

    summary = load_cost_summary(engine)

    assert summary["attempt_count"] == 1
    assert summary["total_actual_cost_usd"] == 0.19
    assert summary["status_counts"] == {"completed": 1}
    assert [row["id"] for row in summary["rows"]] == [
        budget_ledger_id(
            task=LLMTaskName.MID_REVIEW.value,
            ticker="msft",
            candidate_packet_id="candidate-packet-MSFT",
            status=LLMCallStatus.COMPLETED.value,
            available_at=visible_at,
            prompt_version="evidence_review_v1",
        )
    ]


def test_load_ops_health_reports_provider_status_and_database(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    health = load_ops_health(engine, now=AVAILABLE_AT)

    assert health["database"]["status"] == "ok"
    assert health["database"]["candidate_state_count"] == 3
    assert [row["provider"] for row in health["providers"]] == ["news", "polygon"]
    assert health["providers"][0]["status"] == "stale"
    assert health["stale_data"]["detected"] is True
    assert health["stale_data"]["providers"] == ["news"]
    assert health["jobs"][0]["id"] == "job-ingest"
    assert "provider_banners" in health
    assert "degraded_mode" in health
    assert "metrics" in health
    assert "score_drift" in health


def test_load_radar_run_summary_returns_latest_daily_step_group(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    old_decision_at = AVAILABLE_AT - timedelta(days=1)
    latest_decision_at = AVAILABLE_AT
    old_metadata = {
        "as_of": "2026-05-09",
        "decision_available_at": old_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "old-universe",
        "tickers": ["AAPL"],
    }
    latest_metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": latest_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "liquid-us",
        "tickers": ["MSFT", "NVDA"],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "old-feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=old_decision_at,
                    metadata=old_metadata,
                ),
                _job_run_row(
                    "latest-scoring",
                    job_type="scoring_policy",
                    status="failed",
                    started_at=latest_decision_at + timedelta(seconds=2),
                    metadata=latest_metadata,
                    error_summary="policy input missing",
                ),
                _job_run_row(
                    "latest-feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=1),
                    metadata=latest_metadata,
                    requested_count=4,
                    raw_count=3,
                    normalized_count=3,
                ),
                _job_run_row(
                    "provider-job",
                    job_type="provider_ingest",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=3),
                    metadata={"ignored": True},
                ),
            ],
        )

    summary = load_radar_run_summary(engine)

    assert summary["status"] == "partial_success"
    assert summary["as_of"] == "2026-05-10"
    assert summary["decision_available_at"] == latest_decision_at.isoformat()
    assert summary["provider"] == "csv"
    assert summary["universe"] == "liquid-us"
    assert summary["tickers"] == ["MSFT", "NVDA"]
    assert summary["step_count"] == 2
    assert summary["status_counts"] == {"failed": 1, "success": 1}
    assert summary["requested_count"] == 4
    assert [row["step"] for row in summary["steps"]] == ["feature_scan", "scoring_policy"]
    assert summary["steps"][1]["error_summary"] == "policy input missing"


def test_load_radar_run_summary_marks_limited_analysis_skips_partial(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    latest_decision_at = AVAILABLE_AT
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": latest_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": None,
        "universe": None,
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=1),
                    metadata={
                        **metadata,
                        "result_status": "success",
                        "result_reason": None,
                        "result_payload": {"scan_result_count": 2},
                    },
                    requested_count=3,
                    raw_count=2,
                    normalized_count=2,
                ),
                _job_run_row(
                    "candidate-packets",
                    job_type="candidate_packets",
                    status="skipped",
                    started_at=latest_decision_at + timedelta(seconds=2),
                    metadata={
                        **metadata,
                        "result_status": "skipped",
                        "result_reason": "degraded_mode_blocks_high_state_work",
                        "result_payload": {"degraded_mode": {"enabled": True}},
                    },
                ),
            ],
        )

    summary = load_radar_run_summary(engine)

    assert summary["status"] == "partial_success"
    assert summary["status_counts"] == {"skipped": 1, "success": 1}
    assert summary["steps"][1]["reason"] == "degraded_mode_blocks_high_state_work"
    assert summary["steps"][1]["payload"] == {"degraded_mode": {"enabled": True}}


def test_load_broker_summary_returns_portfolio_context(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    repo = BrokerRepository(engine)
    now = datetime.now(UTC).replace(microsecond=0)
    connection_id = broker_connection_id()
    account_id = broker_account_id("schwab", "account-hash-123")
    repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.CONNECTED,
            created_at=now,
            updated_at=now,
            last_successful_sync_at=now,
            metadata={"mode": "read_only"},
        )
    )
    repo.upsert_accounts(
        [
            BrokerAccount(
                id=account_id,
                connection_id=connection_id,
                broker="schwab",
                broker_account_id="12345678",
                account_hash="account-hash-123",
                created_at=now,
                updated_at=now,
                display_name="MARGIN ending 5678",
            )
        ]
    )
    repo.upsert_balance_snapshots(
        [
            BrokerBalanceSnapshot(
                id=broker_balance_snapshot_id(account_id, now),
                account_id=account_id,
                as_of=now,
                cash=50000.0,
                buying_power=100000.0,
                liquidation_value=250000.0,
                equity=250000.0,
                raw_payload={},
                created_at=now,
            )
        ]
    )
    repo.upsert_positions(
        [
            BrokerPosition(
                id=broker_position_id(account_id, "GLW", now),
                account_id=account_id,
                as_of=now,
                ticker="GLW",
                quantity=100,
                market_value=9500.0,
                raw_payload={},
                created_at=now,
            )
        ]
    )

    summary = load_broker_summary(engine)

    assert summary["snapshot"]["connection_status"] == "connected"
    assert summary["snapshot"]["account_count"] == 1
    assert summary["positions"][0]["ticker"] == "GLW"
    assert summary["balances"][0]["cash"] == 50000.0
    assert summary["exposure"]["broker_data_stale"] is False
    assert summary["exposure"]["exposure_before"]["single_name"] == {"GLW": 0.038}
    assert summary["rate_limit_config"]["portfolio_sync_min_interval_seconds"] == 900
    assert summary["rate_limits"][0]["operation"] == "portfolio_sync"


def _engine(tmp_path: Path) -> Engine:
    engine = create_engine(f"sqlite:///{(tmp_path / 'dashboard.db').as_posix()}", future=True)
    create_schema(engine)
    return engine


def _insert_dashboard_fixture(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states),
            [
                _candidate_state_row(
                    id="state-msft-earlier",
                    ticker="MSFT",
                    as_of=EARLIER_AS_OF,
                    state=ActionState.ADD_TO_WATCHLIST.value,
                    final_score=74.0,
                ),
                _candidate_state_row(
                    id="state-msft-latest",
                    ticker="MSFT",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=88.0,
                ),
                _candidate_state_row(
                    id="state-aapl-latest",
                    ticker="AAPL",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=76.0,
                ),
            ],
        )
        conn.execute(
            insert(signal_features),
            [
                _signal_feature_row(
                    ticker="MSFT",
                    as_of=EARLIER_AS_OF,
                    state=ActionState.ADD_TO_WATCHLIST.value,
                    final_score=74.0,
                    theme="ai_infrastructure",
                ),
                _signal_feature_row(
                    ticker="MSFT",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=88.0,
                    theme="ai_infrastructure",
                ),
                _signal_feature_row(
                    ticker="AAPL",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=76.0,
                    theme="ai_infrastructure",
                ),
            ],
        )
        conn.execute(
            insert(candidate_packets).values(
                id="packet-msft-latest",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_state_id="state-msft-latest",
                state=ActionState.WARNING.value,
                final_score=88.0,
                schema_version="candidate-packet-v1",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload=_candidate_packet_payload(),
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(decision_cards).values(
                id="card-msft-latest",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_packet_id="packet-msft-latest",
                action_state=ActionState.WARNING.value,
                setup_type="breakout",
                final_score=88.0,
                schema_version="decision-card-v1",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                next_review_at=NEXT_REVIEW_AT,
                user_decision=None,
                payload=_decision_card_payload(),
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(events).values(
                id="event-msft",
                ticker="MSFT",
                event_type="guidance",
                provider="news_fixture",
                source="Reuters",
                source_category="reputable_news",
                source_url="https://news.example.com/msft",
                title="MSFT guidance raised",
                body_hash="body-msft",
                dedupe_key="MSFT:event-msft",
                source_quality=0.9,
                materiality=0.85,
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload={"summary": "Raised cloud guidance."},
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(events).values(
                id="event-msft-future",
                ticker="MSFT",
                event_type="guidance",
                provider="news_fixture",
                source="Reuters",
                source_category="reputable_news",
                source_url="https://news.example.com/msft-future",
                title="MSFT future guidance",
                body_hash="body-msft-future",
                dedupe_key="MSFT:event-msft-future",
                source_quality=0.9,
                materiality=0.9,
                source_ts=FUTURE_AT,
                available_at=FUTURE_AT,
                payload={"summary": "Future row must not leak into decision-time detail."},
                created_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(text_snippets).values(
                id="snippet-msft",
                ticker="MSFT",
                event_id="event-msft",
                snippet_hash="snippet-hash",
                section="body",
                text="Azure demand improved.",
                source="Reuters",
                source_url="https://news.example.com/msft",
                source_quality=0.9,
                event_type="guidance",
                materiality=0.8,
                ontology_hits=[{"theme": "ai_infrastructure"}],
                sentiment=0.7,
                embedding=[0.1, 0.2],
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload={"rank": 1},
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(text_snippets).values(
                id="snippet-msft-future",
                ticker="MSFT",
                event_id="event-msft-future",
                snippet_hash="snippet-hash-future",
                section="body",
                text="Future detail should be hidden.",
                source="Reuters",
                source_url="https://news.example.com/msft-future",
                source_quality=0.9,
                event_type="guidance",
                materiality=0.9,
                ontology_hits=[{"theme": "ai_infrastructure"}],
                sentiment=0.7,
                embedding=[0.1, 0.2],
                source_ts=FUTURE_AT,
                available_at=FUTURE_AT,
                payload={"rank": 1},
                created_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(validation_runs),
            [
                _validation_run_row("validation-run-earlier", EARLIER_AS_OF),
                _validation_run_row("validation-run-latest", AS_OF),
                _validation_run_row(
                    "validation-run-failed",
                    AS_OF + timedelta(days=1),
                    status="failed",
                ),
            ],
        )
        conn.execute(
            insert(validation_results).values(
                id="validation-result-msft",
                run_id="validation-run-latest",
                ticker="MSFT",
                as_of=AS_OF,
                available_at=AVAILABLE_AT,
                state=ActionState.WARNING.value,
                final_score=88.0,
                candidate_state_id="state-msft-latest",
                candidate_packet_id="packet-msft-latest",
                decision_card_id="card-msft-latest",
                baseline=None,
                labels={"target_20d_25": True},
                leakage_flags=[],
                payload={"audit": {"external_calls": False}},
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(validation_results).values(
                id="validation-result-msft-future",
                run_id="validation-run-latest",
                ticker="MSFT",
                as_of=AS_OF,
                available_at=FUTURE_AT,
                state=ActionState.WARNING.value,
                final_score=88.0,
                candidate_state_id="state-msft-latest",
                candidate_packet_id="packet-msft-latest",
                decision_card_id="card-msft-latest",
                baseline=None,
                labels={"target_20d_25": False},
                leakage_flags=[],
                payload={"audit": {"external_calls": False}},
                created_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(paper_trades).values(
                id="paper-msft",
                decision_card_id="card-msft-latest",
                ticker="MSFT",
                as_of=AS_OF,
                decision="approved",
                state="open",
                entry_price=101.0,
                entry_at=AVAILABLE_AT,
                invalidation_price=94.0,
                shares=20.0,
                notional=2080.0,
                max_loss=200.0,
                outcome_labels={"target_20d_25": True},
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload={"no_execution": True},
                created_at=AVAILABLE_AT,
                updated_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(paper_trades).values(
                id="paper-msft-future",
                decision_card_id="card-msft-latest",
                ticker="MSFT",
                as_of=AS_OF,
                decision="approved",
                state="closed",
                entry_price=101.0,
                entry_at=AVAILABLE_AT,
                invalidation_price=94.0,
                shares=20.0,
                notional=2080.0,
                max_loss=200.0,
                outcome_labels={"target_20d_25": False},
                source_ts=SOURCE_TS,
                available_at=FUTURE_AT,
                payload={"no_execution": True},
                created_at=FUTURE_AT,
                updated_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful-msft",
                artifact_type="decision_card",
                artifact_id="card-msft-latest",
                ticker="MSFT",
                label="useful",
                notes="worth review",
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful-future-msft",
                artifact_type="decision_card",
                artifact_id="card-msft-latest",
                ticker="MSFT",
                label="useful",
                notes="future label should not change latest-run cost summary",
                created_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(provider_health),
            [
                {
                    "id": "provider-polygon",
                    "provider": "polygon",
                    "status": "healthy",
                    "checked_at": AVAILABLE_AT,
                    "reason": "ok",
                    "latency_ms": 123.0,
                },
                {
                    "id": "provider-news",
                    "provider": "news",
                    "status": "stale",
                    "checked_at": AVAILABLE_AT,
                    "reason": "last record exceeded freshness window",
                    "latency_ms": None,
                },
            ],
        )
        conn.execute(
            insert(job_runs).values(
                id="job-ingest",
                job_type="provider_ingest",
                provider="polygon",
                status="success",
                started_at=SOURCE_TS,
                finished_at=AVAILABLE_AT,
                requested_count=1,
                raw_count=1,
                normalized_count=1,
                error_summary=None,
                metadata={"dry_run": True},
            )
        )


def _job_run_row(
    job_id: str,
    *,
    job_type: str,
    status: str,
    started_at: datetime,
    metadata: dict[str, object],
    requested_count: int = 0,
    raw_count: int = 0,
    normalized_count: int = 0,
    error_summary: str | None = None,
) -> dict[str, object]:
    return {
        "id": job_id,
        "job_type": job_type,
        "provider": metadata.get("provider"),
        "status": status,
        "started_at": started_at,
        "finished_at": started_at + timedelta(seconds=1),
        "requested_count": requested_count,
        "raw_count": raw_count,
        "normalized_count": normalized_count,
        "error_summary": error_summary,
        "metadata": metadata,
    }


def _insert_alert_fixture(engine: Engine, *, available_at: datetime = AVAILABLE_AT) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(alerts),
            [
                _alert_row(
                    id="alert-msft-planned",
                    route="immediate_manual_review",
                    status="planned",
                    priority="high",
                    available_at=available_at,
                    created_at=available_at,
                ),
                _alert_row(
                    id="alert-msft-dry-run",
                    route="warning_digest",
                    status="dry_run",
                    priority="critical",
                    available_at=available_at + timedelta(minutes=20),
                    created_at=available_at + timedelta(minutes=20),
                    dedupe_key="alert-dedupe-v1:MSFT:warning_digest:Warning:score:88",
                ),
            ],
        )
        conn.execute(
            insert(user_feedback),
            [
                {
                    "id": "feedback-alert-msft-planned",
                    "artifact_type": "alert",
                    "artifact_id": "alert-msft-planned",
                    "ticker": "MSFT",
                    "label": "useful",
                    "notes": "worth review",
                    "source": "dashboard",
                    "payload": {"alert_id": "alert-msft-planned"},
                    "created_at": available_at,
                },
                {
                    "id": "feedback-alert-msft-dry-run-old",
                    "artifact_type": "alert",
                    "artifact_id": "alert-msft-dry-run",
                    "ticker": "MSFT",
                    "label": "useful",
                    "notes": "old feedback",
                    "source": "dashboard",
                    "payload": {"alert_id": "alert-msft-dry-run"},
                    "created_at": available_at + timedelta(minutes=21),
                },
                {
                    "id": "feedback-alert-msft-dry-run-latest",
                    "artifact_type": "alert",
                    "artifact_id": "alert-msft-dry-run",
                    "ticker": "MSFT",
                    "label": "acted",
                    "notes": "latest feedback",
                    "source": "dashboard",
                    "payload": {"alert_id": "alert-msft-dry-run"},
                    "created_at": available_at + timedelta(minutes=22),
                },
            ],
        )


def _insert_budget_ledger_fixture(
    engine: Engine,
    *,
    available_at: datetime,
    future_at: datetime | None = None,
) -> None:
    repo = BudgetLedgerRepository(engine)
    repo.upsert_entry(
        BudgetLedgerEntry(
            **_budget_ledger_entry_kwargs(
                ticker="msft",
                status=LLMCallStatus.COMPLETED,
                available_at=available_at,
                estimated_cost=0.22,
                actual_cost=0.19,
            )
        )
    )
    repo.upsert_entry(
        BudgetLedgerEntry(
            **_budget_ledger_entry_kwargs(
                ticker="aapl",
                status=LLMCallStatus.SKIPPED,
                available_at=future_at or available_at + timedelta(seconds=30),
                estimated_cost=0.0,
                actual_cost=0.0,
                skip_reason=LLMSkipReason.PREMIUM_LLM_DISABLED,
            )
        )
    )


def _budget_ledger_entry_kwargs(
    *,
    ticker: str,
    status: LLMCallStatus,
    available_at: datetime,
    estimated_cost: float,
    actual_cost: float,
    skip_reason: LLMSkipReason | None = None,
) -> dict[str, object]:
    return {
        "id": budget_ledger_id(
            task=LLMTaskName.MID_REVIEW.value,
            ticker=ticker,
            candidate_packet_id=f"candidate-packet-{ticker.upper()}",
            status=status.value,
            available_at=available_at,
            prompt_version="evidence_review_v1",
        ),
        "ts": available_at - timedelta(minutes=5),
        "available_at": available_at,
        "ticker": ticker,
        "candidate_state_id": f"candidate-state-{ticker.upper()}",
        "candidate_packet_id": f"candidate-packet-{ticker.upper()}",
        "decision_card_id": f"decision-card-{ticker.upper()}",
        "task": LLMTaskName.MID_REVIEW,
        "model": "model-review",
        "provider": "openai",
        "status": status,
        "skip_reason": skip_reason,
        "token_usage": TokenUsage(
            input_tokens=1_000,
            cached_input_tokens=100,
            output_tokens=250,
        ),
        "tool_calls": [{"name": "evidence_review", "arguments": {"ticker": ticker}}],
        "estimated_cost": estimated_cost,
        "actual_cost": actual_cost,
        "currency": "USD",
        "candidate_state": "Warning",
        "prompt_version": "evidence_review_v1",
        "schema_version": "evidence-review-v1",
        "outcome_label": "reviewed",
        "payload": {"ticker": ticker, "status": status.value},
        "created_at": available_at,
    }


def _alert_row(
    *,
    id: str,
    route: str,
    status: str,
    priority: str,
    available_at: datetime,
    created_at: datetime,
    dedupe_key: str = (
        "alert-dedupe-v1:MSFT:immediate_manual_review:Warning:"
        "state_transition:AddToWatchlist->Warning"
    ),
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": "MSFT",
        "as_of": AS_OF,
        "source_ts": SOURCE_TS,
        "available_at": available_at,
        "candidate_state_id": "state-msft-latest",
        "candidate_packet_id": "packet-msft-latest",
        "decision_card_id": "card-msft-latest",
        "action_state": ActionState.WARNING.value,
        "route": route,
        "channel": "dashboard",
        "priority": priority,
        "status": status,
        "dedupe_key": dedupe_key,
        "trigger_kind": "state_transition",
        "trigger_fingerprint": "AddToWatchlist->Warning",
        "title": "MSFT alert review",
        "summary": "MSFT candidate has new review evidence.",
        "feedback_url": "/api/alerts/feedback/alert-msft-planned",
        "payload": {
            "score": 88.0,
            "evidence": [{"kind": "event", "artifact_id": "event-msft"}],
        },
        "created_at": created_at,
        "sent_at": None,
    }


def _ipo_event_row(
    *,
    id: str,
    ticker: str,
    available_at: datetime,
    source_ts: datetime,
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": ticker,
        "event_type": "financing",
        "provider": "sec",
        "source": "SEC EDGAR",
        "source_category": "primary_source",
        "source_url": f"https://www.sec.gov/Archives/{ticker.lower()}-s1.htm",
        "title": f"{ticker} S-1 IPO registration statement",
        "body_hash": f"body-{id}",
        "dedupe_key": f"{ticker}:{id}",
        "source_quality": 1.0,
        "materiality": 0.9,
        "source_ts": source_ts,
        "available_at": available_at,
        "payload": {
            "form_type": "S-1",
            "filing_date": source_ts.date().isoformat(),
            "accession_number": f"000-{id}",
            "document_url": f"https://www.sec.gov/Archives/{ticker.lower()}-s1.htm",
            "document_text_hash": f"hash-{id}",
            "summary": f"{ticker} proposed IPO terms.",
            "ipo_analysis": {
                "analysis_version": "ipo-s1-analysis-v1",
                "company_name": f"{ticker} Robotics, Inc.",
                "form_type": "S-1",
                "source_url": f"https://www.sec.gov/Archives/{ticker.lower()}-s1.htm",
                "proposed_ticker": ticker,
                "exchange": "Nasdaq Global Select Market",
                "shares_offered": 12_500_000,
                "price_range_low": 17.0,
                "price_range_high": 19.0,
                "price_range_midpoint": 18.0,
                "estimated_gross_proceeds": 225_000_000.0,
                "underwriters": ["Morgan Stanley & Co. LLC"],
                "use_of_proceeds_summary": "working capital and research and development",
                "risk_flags": ["history_of_losses", "emerging_growth_company"],
                "sections_found": ["prospectus summary", "risk factors"],
            },
        },
        "created_at": available_at,
    }


def _candidate_state_row(
    *,
    id: str,
    ticker: str,
    as_of: datetime,
    state: str,
    final_score: float,
    created_at: datetime = AVAILABLE_AT,
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": ticker,
        "as_of": as_of,
        "state": state,
        "previous_state": None,
        "final_score": final_score,
        "score_delta_5d": 4.0,
        "hard_blocks": [],
        "transition_reasons": ["score_requires_manual_review"],
        "feature_version": "score-v4-options-theme",
        "policy_version": "policy-v2-events",
        "created_at": created_at,
    }


def _signal_feature_row(
    *,
    ticker: str,
    as_of: datetime,
    state: str,
    final_score: float,
    theme: str,
) -> dict[str, object]:
    return {
        "ticker": ticker,
        "as_of": as_of,
        "feature_version": "score-v4-options-theme",
        "price_strength": 82.0,
        "volume_score": 74.0,
        "liquidity_score": 91.0,
        "risk_penalty": 4.0,
        "portfolio_penalty": 1.0,
        "final_score": final_score,
        "payload": {
            "candidate": {
                "ticker": ticker,
                "as_of": as_of.isoformat(),
                "features": {
                    "ticker": ticker,
                    "as_of": as_of.isoformat(),
                    "feature_version": "score-v4-options-theme",
                },
                "final_score": final_score,
                "entry_zone": [100.0, 104.0],
                "invalidation_price": 94.0,
                "metadata": {
                    "source_ts": SOURCE_TS.isoformat(),
                    "available_at": AVAILABLE_AT.isoformat(),
                    "setup_type": "breakout",
                    "candidate_theme": theme,
                    "theme_hits": [{"theme_id": theme, "count": 2}],
                    "portfolio_impact": {
                        "proposed_notional": 2080.0,
                        "max_loss": 200.0,
                        "hard_blocks": [],
                    },
                },
            },
            "policy": {
                "state": state,
                "hard_blocks": [],
                "reasons": ["score_requires_manual_review"],
                "missing_trade_plan": [],
                "policy_version": "policy-v2-events",
            },
        },
    }


def _candidate_packet_payload() -> dict[str, object]:
    return {
        "identity": {"ticker": "MSFT", "as_of": AS_OF.isoformat()},
        "scores": {"final": 88.0},
        "trade_plan": {
            "entry_zone": [100.0, 104.0],
            "invalidation_price": 94.0,
            "reward_risk": 2.7,
        },
        "setup_plan": {"setup_type": "breakout", "review_focus": "volume confirmation"},
        "portfolio_impact": {
            "proposed_notional": 2080.0,
            "max_loss": 200.0,
            "hard_blocks": [],
        },
        "supporting_evidence": [
            {
                "kind": "event",
                "title": "MSFT guidance raised",
                "source_id": "event-msft",
                "source_url": "https://news.example.com/msft",
                "strength": 0.8,
            }
        ],
        "disconfirming_evidence": [
            {
                "kind": "risk",
                "title": "Valuation stretched",
                "computed_feature_id": "signal_features:MSFT:risk",
                "strength": 0.4,
            }
        ],
        "hard_blocks": [],
        "audit": {
            "source_ts": SOURCE_TS.isoformat(),
            "available_at": AVAILABLE_AT.isoformat(),
            "provider_license_policy": {
                "license_tags": ["local-csv-fixture"],
                "metadata_complete": True,
                "prompt_allowed": True,
                "external_export_allowed": False,
                "attribution_required": False,
                "policies": [],
            },
        },
    }


def _decision_card_payload() -> dict[str, object]:
    return {
        "disclaimer": "Manual review only.",
        "manual_review_only": True,
        "trade_plan": {
            "entry_zone": [100.0, 104.0],
            "invalidation_price": 94.0,
            "reward_risk": 2.7,
        },
        "setup_plan": {"setup_type": "breakout", "next_step": "review evidence"},
        "portfolio_impact": {
            "proposed_notional": 2080.0,
            "max_loss": 200.0,
            "hard_blocks": [],
        },
    }


def _validation_run_row(
    id: str,
    as_of: datetime,
    *,
    status: str = "success",
) -> dict[str, object]:
    return {
        "id": id,
        "run_type": "point_in_time_replay",
        "as_of_start": as_of,
        "as_of_end": as_of,
        "decision_available_at": AVAILABLE_AT,
        "status": status,
        "config": {"states": [ActionState.WARNING.value]},
        "metrics": {},
        "started_at": as_of,
        "finished_at": as_of + timedelta(minutes=5),
        "created_at": as_of + timedelta(minutes=10),
    }
