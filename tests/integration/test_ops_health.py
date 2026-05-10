from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import Engine, create_engine, insert

from catalyst_radar.core.models import ActionState
from catalyst_radar.ops.health import load_ops_health
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import (
    budget_ledger,
    candidate_states,
    data_quality_incidents,
    job_runs,
    provider_health,
    useful_alert_labels,
    validation_runs,
)


def test_ops_health_enables_degraded_mode_for_stale_core_data() -> None:
    engine = _engine()
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    stale_as_of = now - timedelta(days=3)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                _candidate_state_row(
                    id="state-old",
                    ticker="AAA",
                    as_of=stale_as_of,
                    state=ActionState.WARNING.value,
                    final_score=88.0,
                    created_at=stale_as_of,
                )
            )
        )

    health = load_ops_health(engine, now=now, stale_after=timedelta(hours=36))

    assert health["degraded_mode"]["enabled"] is True
    assert health["degraded_mode"]["max_action_state"] == ActionState.ADD_TO_WATCHLIST.value
    assert ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value in health["degraded_mode"][
        "disabled_states"
    ]
    assert health["stale_data"]["detected"] is True
    assert health["stale_data"]["core_data"] is True


def test_ops_health_reports_metrics_banners_incidents_drift_and_runbooks() -> None:
    engine = _engine()
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    prev = datetime(2026, 5, 8, 21, 0, tzinfo=UTC)
    latest = datetime(2026, 5, 9, 21, 0, tzinfo=UTC)
    with engine.begin() as conn:
        conn.execute(
            insert(provider_health),
            [
                {
                    "id": "provider-polygon",
                    "provider": "polygon",
                    "status": "healthy",
                    "checked_at": now - timedelta(minutes=10),
                    "reason": "ok",
                    "latency_ms": 25.0,
                },
                {
                    "id": "provider-sec",
                    "provider": "sec",
                    "status": "degraded",
                    "checked_at": now - timedelta(minutes=5),
                    "reason": "schema rejects spiked",
                    "latency_ms": None,
                },
            ],
        )
        conn.execute(
            insert(candidate_states),
            [
                _candidate_state_row(
                    id="prev-aaa",
                    ticker="AAA",
                    as_of=prev,
                    state=ActionState.ADD_TO_WATCHLIST.value,
                    final_score=50.0,
                    created_at=prev,
                ),
                _candidate_state_row(
                    id="prev-bbb",
                    ticker="BBB",
                    as_of=prev,
                    state=ActionState.ADD_TO_WATCHLIST.value,
                    final_score=52.0,
                    created_at=prev,
                ),
                _candidate_state_row(
                    id="latest-aaa",
                    ticker="AAA",
                    as_of=latest,
                    state=ActionState.WARNING.value,
                    final_score=90.0,
                    created_at=latest,
                ),
                _candidate_state_row(
                    id="latest-bbb",
                    ticker="BBB",
                    as_of=latest,
                    state=ActionState.WARNING.value,
                    final_score=92.0,
                    created_at=latest,
                ),
            ],
        )
        conn.execute(
            insert(job_runs),
            [
                {
                    "id": "job-scan",
                    "job_type": "feature_scan",
                    "provider": None,
                    "status": "success",
                    "started_at": now - timedelta(hours=2),
                    "finished_at": now - timedelta(hours=1, minutes=50),
                    "requested_count": 2,
                    "raw_count": 2,
                    "normalized_count": 2,
                    "error_summary": None,
                    "metadata": {},
                },
                {
                    "id": "job-llm",
                    "job_type": "llm_review",
                    "provider": "openai",
                    "status": "failed",
                    "started_at": now - timedelta(hours=1),
                    "finished_at": now - timedelta(minutes=55),
                    "requested_count": 2,
                    "raw_count": 0,
                    "normalized_count": 0,
                    "error_summary": "schema failure",
                    "metadata": {},
                },
            ],
        )
        conn.execute(
            insert(data_quality_incidents),
            [
                {
                    "id": "incident-schema",
                    "provider": "sec",
                    "severity": "error",
                    "kind": "schema_validation",
                    "affected_tickers": ["AAA"],
                    "reason": "schema mismatch",
                    "fail_closed_action": "disable escalation",
                    "payload": {"field": "filing_date"},
                    "detected_at": now - timedelta(minutes=30),
                    "source_ts": now - timedelta(hours=2),
                    "available_at": now - timedelta(minutes=30),
                },
                {
                    "id": "incident-stale",
                    "provider": "polygon",
                    "severity": "warning",
                    "kind": "stale_data",
                    "affected_tickers": ["BBB"],
                    "reason": "freshness window exceeded",
                    "fail_closed_action": "watchlist only",
                    "payload": {},
                    "detected_at": now - timedelta(minutes=20),
                    "source_ts": now - timedelta(days=2),
                    "available_at": now - timedelta(minutes=20),
                },
            ],
        )
        conn.execute(
            insert(budget_ledger),
            [
                _budget_row(
                    id="budget-ok",
                    status="completed",
                    skip_reason=None,
                    actual_cost=0.25,
                    available_at=now - timedelta(minutes=15),
                    payload={"result": "ok"},
                ),
                _budget_row(
                    id="budget-rejected",
                    status="schema_rejected",
                    skip_reason="schema_validation_failed",
                    actual_cost=0.05,
                    available_at=now - timedelta(minutes=10),
                    payload={"error": "source_faithfulness violation"},
                ),
            ],
        )
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful",
                artifact_type="decision_card",
                artifact_id="card-aaa",
                ticker="AAA",
                label="useful",
                notes="acted",
                created_at=now - timedelta(minutes=5),
            )
        )
        conn.execute(
            insert(validation_runs).values(
                id="validation-latest",
                run_type="point_in_time_replay",
                as_of_start=prev,
                as_of_end=latest,
                decision_available_at=now - timedelta(hours=1),
                status="success",
                config={},
                metrics={"false_positive_count": 1, "candidate_count": 4},
                started_at=now - timedelta(minutes=45),
                finished_at=now - timedelta(minutes=40),
                created_at=now - timedelta(minutes=45),
            )
        )

    health = load_ops_health(engine, now=now)

    assert health["provider_banners"] == [
        {
            "provider": "sec",
            "status": "degraded",
            "reason": "schema rejects spiked",
            "runbook": "docs/runbooks/provider-failure.md",
        }
    ]
    assert health["degraded_mode"]["enabled"] is True
    assert health["runbooks"]["provider_failure"] == "docs/runbooks/provider-failure.md"
    assert health["runbooks"]["llm_failure"] == "docs/runbooks/llm-failure.md"
    assert health["runbooks"]["score_drift"] == "docs/runbooks/score-drift.md"
    assert [row["id"] for row in health["incidents"]] == ["incident-stale", "incident-schema"]
    assert health["score_drift"]["detected"] is True
    assert health["score_drift"]["reason"] == "mean_shift"

    metrics = health["metrics"]
    assert metrics["stage_counts"]["jobs_by_type"]["llm_review"]["failed"] == 1
    assert metrics["stage_counts"]["candidate_states"] == {ActionState.WARNING.value: 2}
    assert metrics["cost"]["total_actual_cost_usd"] == 0.3
    assert metrics["cost"]["cost_per_useful_alert"] == 0.3
    assert metrics["useful_alert_count"] == 1
    assert metrics["stale_incident_count"] == 1
    assert metrics["schema_failure_count"] == 1
    assert metrics["unsupported_claim_count"] == 1
    assert metrics["unsupported_claim_rate"] == 0.5
    assert metrics["false_positive_rate"] == 0.25


def _engine() -> Engine:
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    return engine


def _candidate_state_row(
    *,
    id: str,
    ticker: str,
    as_of: datetime,
    state: str,
    final_score: float,
    created_at: datetime,
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": ticker,
        "as_of": as_of,
        "state": state,
        "previous_state": None,
        "final_score": final_score,
        "score_delta_5d": 0.0,
        "hard_blocks": [],
        "transition_reasons": [],
        "feature_version": "test-features",
        "policy_version": "test-policy",
        "created_at": created_at,
    }


def _budget_row(
    *,
    id: str,
    status: str,
    skip_reason: str | None,
    actual_cost: float,
    available_at: datetime,
    payload: dict[str, object],
) -> dict[str, object]:
    return {
        "id": id,
        "ts": available_at - timedelta(minutes=1),
        "available_at": available_at,
        "ticker": "AAA",
        "candidate_state_id": "latest-aaa",
        "candidate_packet_id": None,
        "decision_card_id": None,
        "task": "mid_review",
        "model": "model-review",
        "provider": "openai",
        "status": status,
        "skip_reason": skip_reason,
        "input_tokens": 100,
        "cached_input_tokens": 0,
        "output_tokens": 25,
        "tool_calls": [],
        "estimated_cost": actual_cost,
        "actual_cost": actual_cost,
        "currency": "USD",
        "candidate_state": ActionState.WARNING.value,
        "prompt_version": "test",
        "schema_version": "evidence-review-v1",
        "outcome_label": None,
        "payload": payload,
        "created_at": available_at,
    }
