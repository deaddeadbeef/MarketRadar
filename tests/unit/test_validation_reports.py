from __future__ import annotations

import pytest

from catalyst_radar.validation.baselines import BaselineCandidate
from catalyst_radar.validation.reports import (
    build_validation_report,
    validation_report_payload,
)


def test_report_builder_computes_core_metrics() -> None:
    rows = [
        _result("r1", "AAA", "Warning", {"target_20d_25": True, "target_10d_15": True}),
        _result("r2", "BBB", "Warning", {"target_20d_25": False}, ["future_packet"]),
        _result("r3", "CCC", "Blocked", {"target_20d_25": True}),
    ]
    useful_labels = [
        {"artifact_id": "r1", "ticker": "AAA", "label": "useful"},
        {"artifact_id": "r2", "ticker": "BBB", "label": "ignored"},
        {"artifact_id": "r3", "ticker": "CCC", "label": "acted"},
    ]

    report = build_validation_report(
        "run-1",
        rows,
        useful_alert_labels=useful_labels,
        total_cost=12.0,
    )
    payload = validation_report_payload(report)

    assert payload["candidate_count"] == 3
    assert payload["precision"]["target_20d_25"] == pytest.approx(2 / 3)
    assert payload["precision"]["target_10d_15"] == pytest.approx(1 / 3)
    assert payload["false_positive_count"] == 1
    assert payload["useful_alert_rate"] == pytest.approx(2 / 3)
    assert payload["cost_per_useful_alert"] == pytest.approx(6.0)
    assert payload["leakage_failure_count"] == 1
    assert payload["state_mix"] == {"Blocked": 1, "Warning": 2}


def test_report_builder_handles_zero_cost_without_division_failure() -> None:
    report = build_validation_report(
        "run-1",
        [_result("r1", "AAA", "Warning", {"target_20d_25": False})],
        useful_alert_labels=[],
        total_cost=0.0,
    )

    assert report.cost_per_useful_alert == 0.0
    assert report.useful_alert_rate == 0.0


def test_report_builder_counts_missed_opportunities_from_baseline_winners() -> None:
    rows = [
        _result("r1", "AAA", "Warning", {"target_20d_25": True}),
        _result("r2", "BBB", "Warning", {"target_20d_25": False}),
    ]
    baselines = [
        _baseline("spy_relative_momentum", "AAA", 1),
        _baseline("spy_relative_momentum", "DDD", 2),
        _baseline("sector_relative_momentum", "EEE", 1),
    ]

    report = validation_report_payload(
        build_validation_report("run-1", rows, baseline_candidates=baselines)
    )

    assert report["missed_opportunity_count"] == 2
    assert report["baseline_comparison"]["spy_relative_momentum"] == {
        "baseline_candidate_count": 2,
        "overlap_count": 1,
        "missed_opportunity_count": 1,
        "overlap_tickers": ["AAA"],
        "missed_tickers": ["DDD"],
        "overlap_keys": ["AAA:unknown"],
        "missed_keys": ["DDD:unknown"],
    }
    assert report["baseline_comparison"]["sector_relative_momentum"][
        "missed_tickers"
    ] == ["EEE"]


def test_report_builder_can_treat_baseline_rows_in_results_as_comparisons() -> None:
    rows = [
        _result("r1", "AAA", "Warning", {"target_20d_25": True}),
        {
            "baseline": "event_only_watchlist",
            "ticker": "ZZZ",
            "rank": 1,
            "labels": {"target_20d_25": True},
        },
    ]

    report = validation_report_payload(build_validation_report("run-1", rows))

    assert report["candidate_count"] == 1
    assert report["missed_opportunity_count"] == 1
    assert report["baseline_comparison"]["event_only_watchlist"]["missed_tickers"] == [
        "ZZZ"
    ]


def test_report_builder_compares_baselines_by_ticker_and_as_of() -> None:
    rows = [
        _result("r1", "AAA", "Warning", {"target_20d_25": True}, as_of="2026-05-10"),
    ]
    baselines = [
        _baseline("spy_relative_momentum", "AAA", 1, as_of="2026-05-10"),
        _baseline("spy_relative_momentum", "AAA", 2, as_of="2026-05-11"),
    ]

    report = validation_report_payload(
        build_validation_report("run-1", rows, baseline_candidates=baselines)
    )

    comparison = report["baseline_comparison"]["spy_relative_momentum"]
    assert comparison["overlap_keys"] == ["AAA:2026-05-10"]
    assert comparison["missed_keys"] == ["AAA:2026-05-11"]
    assert comparison["missed_opportunity_count"] == 1


def _result(
    row_id: str,
    ticker: str,
    state: str,
    labels: dict[str, bool],
    leakage_flags: list[str] | None = None,
    as_of: object | None = None,
) -> dict[str, object]:
    return {
        "id": row_id,
        "ticker": ticker,
        "as_of": as_of,
        "state": state,
        "labels": labels,
        "leakage_flags": leakage_flags or [],
    }


def _baseline(
    name: str,
    ticker: str,
    rank: int,
    *,
    as_of: object | None = None,
) -> BaselineCandidate:
    return BaselineCandidate(
        baseline=name,
        ticker=ticker,
        as_of=as_of,
        rank=rank,
        score=float(10 - rank),
        reason="test",
    )
