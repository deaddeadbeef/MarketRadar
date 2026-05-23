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
    assert payload["cost_per_candidate"] == pytest.approx(4.0)
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
    assert report.cost_per_candidate == 0.0
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
    spy_comparison = report["baseline_comparison"]["spy_relative_momentum"]
    assert {
        key: spy_comparison[key]
        for key in (
            "baseline_candidate_count",
            "overlap_count",
            "missed_opportunity_count",
            "overlap_tickers",
            "missed_tickers",
            "overlap_keys",
            "missed_keys",
        )
    } == {
        "baseline_candidate_count": 2,
        "overlap_count": 1,
        "missed_opportunity_count": 1,
        "overlap_tickers": ["AAA"],
        "missed_tickers": ["DDD"],
        "overlap_keys": ["AAA:unknown"],
        "missed_keys": ["DDD:unknown"],
    }
    assert spy_comparison["sample_status"] == "insufficient_evidence"
    assert spy_comparison["result_vs_market_radar"] == "insufficient_evidence"
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


def test_report_builder_marks_baseline_comparison_insufficient_without_labels() -> None:
    report = validation_report_payload(
        build_validation_report(
            "run-1",
            [_result("r1", "AAA", "Warning", {})],
            baseline_candidates=[_baseline("volume_breakout_screener", "BBB", 1)],
        )
    )

    comparison = report["baseline_comparison"]["volume_breakout_screener"]
    assert comparison["baseline_precision_at_5"] is None
    assert comparison["sample_status"] == "insufficient_evidence"
    assert comparison["result_vs_market_radar"] == "insufficient_evidence"


def test_report_builder_compares_labeled_baseline_precision() -> None:
    rows = [
        _result(
            "r1",
            "AAA",
            "Warning",
            {
                "target_20d_25": True,
                "return_20d": 0.30,
                "spy_relative_return_20d": 0.22,
                "sector_return_20d": 0.10,
                "sector_outperformance": True,
                "max_adverse_excursion": -0.02,
                "max_favorable_excursion": 0.36,
            },
        ),
        _result(
            "r2",
            "BBB",
            "Warning",
            {
                "target_20d_25": False,
                "return_20d": -0.05,
                "spy_return_20d": 0.02,
                "sector_relative_return_20d": -0.10,
                "sector_outperformance": False,
                "max_adverse_excursion": -0.11,
                "max_favorable_excursion": 0.04,
            },
        ),
        {
            "baseline": "volume_breakout_screener",
            "ticker": "CCC",
            "rank": 1,
            "labels": {
                "target_20d_25": True,
                "return_20d": 0.42,
                "spy_return_20d": 0.10,
                "sector_relative_return_20d": 0.27,
                "sector_outperformance": True,
                "max_adverse_excursion": -0.03,
                "max_favorable_excursion": 0.32,
            },
        },
    ]

    report = validation_report_payload(build_validation_report("run-1", rows, total_cost=4.0))

    comparison = report["baseline_comparison"]["volume_breakout_screener"]
    assert comparison["marketradar_precision_at_10"] == pytest.approx(0.5)
    assert comparison["marketradar_false_positive_rate"] == pytest.approx(0.5)
    assert comparison["marketradar_max_adverse_excursion_avg"] == pytest.approx(-0.065)
    assert comparison["marketradar_max_favorable_excursion_avg"] == pytest.approx(0.2)
    assert comparison["marketradar_labeled_count"] == 2
    assert comparison["marketradar_cost_per_candidate"] == pytest.approx(2.0)
    assert comparison["baseline_precision_at_10"] == 1.0
    assert comparison["baseline_false_positive_rate"] == 0.0
    assert comparison["baseline_max_favorable_excursion_avg"] == 0.32
    assert comparison["baseline_cost_per_candidate"] == 0.0
    assert comparison["marketradar_return_20d_avg"] == pytest.approx(0.125)
    assert comparison["marketradar_spy_relative_return_20d_avg"] == pytest.approx(
        0.075
    )
    assert comparison["marketradar_sector_relative_return_20d_avg"] == pytest.approx(0.05)
    assert comparison["marketradar_sector_outperformance_rate"] == pytest.approx(0.5)
    assert comparison["baseline_return_20d_avg"] == pytest.approx(0.42)
    assert comparison["baseline_spy_relative_return_20d_avg"] == pytest.approx(0.32)
    assert comparison["baseline_sector_relative_return_20d_avg"] == pytest.approx(0.27)
    assert comparison["baseline_sector_outperformance_rate"] == pytest.approx(1.0)
    assert comparison["sample_status"] == "measured"
    assert comparison["result_vs_market_radar"] == "baseline_wins"


def test_report_builder_adds_score_calibration_buckets() -> None:
    rows = [
        _result(
            "r1",
            "AAA",
            "Warning",
            {
                "target_20d_25": True,
                "sector_outperformance": True,
                "max_adverse_excursion": -0.02,
                "max_favorable_excursion": 0.35,
                "return_20d": 0.31,
                "spy_return_20d": 0.08,
                "sector_relative_return_20d": 0.14,
            },
            final_score=92,
            payload={
                "sector": "Technology",
                "market_regime": "risk_on",
                "setup_type": "breakout",
                "priced_in_status": "bullish_not_priced_in",
                "source_coverage": {"sources": {"market_bars": {"missing": 0}}},
            },
        ),
        _result(
            "r2",
            "BBB",
            "Warning",
            {
                "target_20d_25": False,
                "sector_outperformance": False,
                "max_adverse_excursion": -0.12,
                "max_favorable_excursion": 0.08,
                "return_20d": -0.04,
                "spy_relative_return_20d": -0.09,
                "sector_return_20d": 0.03,
            },
            final_score=72,
            payload={
                "sector": "Technology",
                "market_regime": "risk_on",
                "setup_type": "breakout",
                "priced_in_status": "bullish_not_priced_in",
                "source_coverage": {"weak_sources": ["options"]},
            },
        ),
        _result(
            "r3",
            "CCC",
            "Research",
            {
                "target_20d_25": True,
                "max_adverse_excursion": -0.01,
                "return_20d": 0.18,
                "spy_relative_return_20d": 0.05,
            },
            final_score=67,
            payload={
                "sector": "Industrials",
                "market_regime": "risk_off",
                "setup_type": "pullback",
                "priced_in_status": "neutral",
                "source_coverage": {"sources": {"market_bars": {"missing": 0}}},
            },
        ),
    ]
    useful_labels = [{"artifact_id": "r1", "ticker": "AAA", "label": "useful"}]

    report = validation_report_payload(
        build_validation_report("run-1", rows, useful_alert_labels=useful_labels)
    )

    calibration = report["score_calibration"]
    assert calibration["thresholds_changed"] is False
    assert calibration["sample_status"] == "measured"
    buckets = {row["bucket"]: row for row in calibration["buckets"]}
    assert buckets["90_plus"]["precision"] == 1.0
    assert buckets["90_plus"]["useful_label_rate"] == 1.0
    assert buckets["90_plus"]["return_20d_avg"] == pytest.approx(0.31)
    assert buckets["90_plus"]["spy_relative_return_20d_avg"] == pytest.approx(0.23)
    assert buckets["90_plus"]["sector_relative_return_20d_avg"] == pytest.approx(0.14)
    assert buckets["70_79"]["false_positive_rate"] == 1.0
    assert buckets["70_79"]["return_20d_avg"] == pytest.approx(-0.04)
    assert buckets["70_79"]["spy_relative_return_20d_avg"] == pytest.approx(-0.09)
    assert buckets["70_79"]["sector_relative_return_20d_avg"] == pytest.approx(-0.07)
    assert buckets["60_69"]["positive_count"] == 1
    assert buckets["60_69"]["return_20d_avg"] == pytest.approx(0.18)
    assert buckets["50_59"]["sample_status"] == "insufficient_evidence"
    distribution = calibration["score_distribution"]
    assert set(distribution) == {
        "sector",
        "market_regime",
        "setup_type",
        "priced_in_status",
        "action_state",
        "source_coverage",
        "usefulness_label",
    }
    sectors = {row["value"]: row for row in distribution["sector"]["groups"]}
    assert sectors["technology"]["bucket_counts"]["90_plus"] == 1
    assert sectors["technology"]["return_20d_avg"] == pytest.approx(0.135)
    assert sectors["technology"]["sector_outperformance_rate"] == pytest.approx(0.5)
    assert sectors["industrials"]["precision"] == 1.0
    assert sectors["industrials"]["return_20d_avg"] == pytest.approx(0.18)
    labels = {row["value"]: row for row in distribution["usefulness_label"]["groups"]}
    assert labels["useful"]["candidate_count"] == 1
    assert labels["unlabeled"]["candidate_count"] == 2
    coverage = {row["value"]: row for row in distribution["source_coverage"]["groups"]}
    assert coverage["complete"]["candidate_count"] == 2
    assert coverage["gaps:options"]["candidate_count"] == 1


def test_report_builder_flags_high_false_positive_score_bucket() -> None:
    rows = [
        _result(f"r{index}", f"T{index}", "Warning", {"target_20d_25": False}, final_score=75)
        for index in range(3)
    ]

    report = validation_report_payload(build_validation_report("run-1", rows))

    flags = report["score_calibration"]["threshold_review_flags"]
    assert flags == [
        {
            "bucket": "70_79",
            "reason": "false_positive_rate_at_or_above_50pct",
            "false_positive_rate": 1.0,
            "labeled_count": 3,
            "action": "review_threshold_with_more_evidence_before_changing_policy",
        }
    ]


def test_report_builder_measures_local_text_intelligence() -> None:
    rows = [
        _result(
            "r1",
            "AAA",
            "Warning",
            {"target_20d_25": True},
            final_score=82,
            payload={
                "local_narrative_score": 88.0,
                "novelty_score": 80.0,
                "source_quality_score": 95.0,
                "sentiment_score": 45.0,
                "theme_match_score": 75.0,
                "theme_velocity_score": 60.0,
                "theme_hits": [{"theme_id": "ai_infrastructure"}],
            },
        ),
        _result(
            "r2",
            "BBB",
            "Warning",
            {"target_20d_25": False},
            final_score=68,
            payload={
                "local_narrative_score": 20.0,
                "novelty_score": 15.0,
                "source_quality_score": 35.0,
                "sentiment_score": -30.0,
                "theme_match_score": 0.0,
                "theme_velocity_score": 0.0,
                "theme_hits": [],
            },
        ),
    ]
    useful_labels = [{"artifact_id": "r1", "ticker": "AAA", "label": "useful"}]

    report = validation_report_payload(
        build_validation_report("run-1", rows, useful_alert_labels=useful_labels)
    )

    local_text = report["local_text_intelligence"]
    assert local_text["thresholds_changed"] is False
    assert local_text["models_changed"] is False
    assert local_text["sample_status"] == "measured"
    narrative = local_text["features"]["local_narrative_score"]
    buckets = {row["bucket"]: row for row in narrative["buckets"]}
    assert buckets["high"]["precision"] == 1.0
    assert buckets["low"]["false_positive_rate"] == 1.0
    assert narrative["high_vs_low"]["precision_delta"] == 1.0
    assert narrative["high_vs_low"]["false_positive_reduction_delta"] == 1.0
    assert narrative["high_vs_low"]["useful_label_rate_delta"] == 1.0
    assert narrative["high_vs_low"]["interpretation"] == (
        "supports_existing_local_text_signal"
    )
    theme_presence = local_text["features"]["theme_hit_presence"]
    presence = {row["bucket"]: row for row in theme_presence["buckets"]}
    assert presence["present"]["useful_label_rate"] == 1.0
    assert theme_presence["present_vs_absent"]["precision_delta"] == 1.0


def test_report_builder_marks_local_text_measurement_insufficient_without_features() -> None:
    rows = [_result("r1", "AAA", "Warning", {"target_20d_25": True}, final_score=82)]

    report = validation_report_payload(build_validation_report("run-1", rows))

    local_text = report["local_text_intelligence"]
    assert local_text["sample_status"] == "insufficient_evidence"
    assert local_text["measured_feature_count"] == 0
    assert local_text["upgrade_recommendation"] == "insufficient_evidence"
    narrative = local_text["features"]["local_narrative_score"]
    assert narrative["sample_count"] == 0
    assert narrative["missing_count"] == 1


def _result(
    row_id: str,
    ticker: str,
    state: str,
    labels: dict[str, bool],
    leakage_flags: list[str] | None = None,
    as_of: object | None = None,
    final_score: float = 75.0,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "id": row_id,
        "ticker": ticker,
        "as_of": as_of,
        "state": state,
        "final_score": final_score,
        "labels": labels,
        "leakage_flags": leakage_flags or [],
        "payload": payload or {},
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
