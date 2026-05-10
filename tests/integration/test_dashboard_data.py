from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import Engine, create_engine, insert

from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard.data import (
    load_alert_detail,
    load_alert_rows,
    load_candidate_rows,
    load_cost_summary,
    load_ops_health,
    load_theme_rows,
    load_ticker_detail,
    load_validation_summary,
)
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


def test_load_candidate_rows_returns_latest_state_per_ticker(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    rows = load_candidate_rows(engine)

    assert [row["id"] for row in rows] == ["state-msft-latest", "state-aapl-latest"]
    assert [row["ticker"] for row in rows] == ["MSFT", "AAPL"]


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
    _insert_alert_fixture(engine)

    default_rows = load_alert_rows(engine)
    rows = load_alert_rows(
        engine,
        available_at=AVAILABLE_AT + timedelta(minutes=10),
        ticker="msft",
        status="planned",
        route="immediate_manual_review",
    )

    assert default_rows == []
    assert [row["id"] for row in rows] == ["alert-msft-planned"]
    assert rows[0]["feedback_label"] == "useful"
    assert load_alert_detail(engine, "alert-msft-dry-run", available_at=AVAILABLE_AT) is None


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

    assert summary["total_cost_usd"] == 0.0
    assert summary["useful_alert_count"] == 1
    assert summary["cost_per_useful_alert"] == 0.0
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


def test_load_ops_health_reports_provider_status_and_database(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    health = load_ops_health(engine)

    assert health["database"]["status"] == "ok"
    assert health["database"]["candidate_state_count"] == 3
    assert [row["provider"] for row in health["providers"]] == ["news", "polygon"]
    assert health["providers"][0]["status"] == "stale"
    assert health["stale_data"]["detected"] is True
    assert health["stale_data"]["providers"] == ["news"]
    assert health["jobs"][0]["id"] == "job-ingest"


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


def _insert_alert_fixture(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(alerts),
            [
                _alert_row(
                    id="alert-msft-planned",
                    route="immediate_manual_review",
                    status="planned",
                    priority="high",
                    available_at=AVAILABLE_AT,
                    created_at=AVAILABLE_AT,
                ),
                _alert_row(
                    id="alert-msft-dry-run",
                    route="warning_digest",
                    status="dry_run",
                    priority="critical",
                    available_at=AVAILABLE_AT + timedelta(minutes=20),
                    created_at=AVAILABLE_AT + timedelta(minutes=20),
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
                    "created_at": AVAILABLE_AT,
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
                    "created_at": AVAILABLE_AT + timedelta(minutes=21),
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
                    "created_at": AVAILABLE_AT + timedelta(minutes=22),
                },
            ],
        )


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


def _candidate_state_row(
    *,
    id: str,
    ticker: str,
    as_of: datetime,
    state: str,
    final_score: float,
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
        "created_at": AVAILABLE_AT,
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
        "audit": {"source_ts": SOURCE_TS.isoformat(), "available_at": AVAILABLE_AT.isoformat()},
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
