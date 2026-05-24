from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping
from catalyst_radar.validation.baselines import (
    NEWS_EVENT_ONLY_SCREENER,
    RANDOM_SECTOR_MATCHED_BASKET,
    RELATIVE_STRENGTH_SCREENER,
    SECTOR_ETF_ROTATION_SCREENER,
    VOLUME_BREAKOUT_SCREENER,
    BaselineCandidate,
)
from catalyst_radar.validation.outcomes import OutcomeLabels

DEFAULT_POSITIVE_LABEL = "target_20d_25"
USEFUL_ALERT_LABELS = frozenset({"useful", "acted"})
KNOWN_BOOLEAN_LABELS = (
    "target_10d_15",
    "target_20d_25",
    "target_60d_40",
    "sector_outperformance",
    "invalidated",
)
MISSION_BRIEF_BASELINES = (
    RELATIVE_STRENGTH_SCREENER,
    VOLUME_BREAKOUT_SCREENER,
    SECTOR_ETF_ROTATION_SCREENER,
    NEWS_EVENT_ONLY_SCREENER,
    RANDOM_SECTOR_MATCHED_BASKET,
)
FORWARD_RETURN_HORIZONS = (5, 10, 20, 60)
RELATIVE_RETURN_BENCHMARKS = ("spy", "sector")
COMPARISON_RETURN_METRICS = (
    *(f"return_{horizon}d_avg" for horizon in FORWARD_RETURN_HORIZONS),
    *(
        f"{benchmark}_relative_return_{horizon}d_avg"
        for benchmark in RELATIVE_RETURN_BENCHMARKS
        for horizon in FORWARD_RETURN_HORIZONS
    ),
    "sector_outperformance_rate",
)


@dataclass(frozen=True)
class ValidationReport:
    """Summary metrics for a validation run."""

    run_id: str
    candidate_count: int
    precision: Mapping[str, float]
    false_positive_count: int
    useful_alert_rate: float
    cost_per_useful_alert: float | None
    cost_per_candidate: float | None
    missed_opportunity_count: int
    leakage_failure_count: int
    state_mix: Mapping[str, int]
    baseline_comparison: Mapping[str, Any]
    backtest_summary: Mapping[str, Any]
    score_calibration: Mapping[str, Any]
    local_text_intelligence: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not str(self.run_id).strip():
            msg = "run_id must not be blank"
            raise ValueError(msg)
        object.__setattr__(self, "precision", freeze_mapping(self.precision, "precision"))
        object.__setattr__(self, "state_mix", freeze_mapping(self.state_mix, "state_mix"))
        object.__setattr__(
            self,
            "baseline_comparison",
            freeze_mapping(self.baseline_comparison, "baseline_comparison"),
        )
        object.__setattr__(
            self,
            "backtest_summary",
            freeze_mapping(self.backtest_summary, "backtest_summary"),
        )
        object.__setattr__(
            self,
            "score_calibration",
            freeze_mapping(self.score_calibration, "score_calibration"),
        )
        object.__setattr__(
            self,
            "local_text_intelligence",
            freeze_mapping(self.local_text_intelligence, "local_text_intelligence"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "candidate_count": self.candidate_count,
            "precision": dict(self.precision),
            "false_positive_count": self.false_positive_count,
            "useful_alert_rate": self.useful_alert_rate,
            "cost_per_useful_alert": self.cost_per_useful_alert,
            "cost_per_candidate": self.cost_per_candidate,
            "missed_opportunity_count": self.missed_opportunity_count,
            "leakage_failure_count": self.leakage_failure_count,
            "state_mix": dict(self.state_mix),
            "baseline_comparison": _thaw(self.baseline_comparison),
            "backtest_summary": _thaw(self.backtest_summary),
            "score_calibration": _thaw(self.score_calibration),
            "local_text_intelligence": _thaw(self.local_text_intelligence),
        }


def build_validation_report(
    run_id: str,
    results: Iterable[Any],
    *,
    useful_alert_labels: Iterable[Any] | None = None,
    baseline_candidates: Iterable[Any] | None = None,
    total_cost: float = 0.0,
    positive_label: str = DEFAULT_POSITIVE_LABEL,
    baseline_top_n: int | None = None,
) -> ValidationReport:
    """Build deterministic report metrics from validation result-like rows."""

    result_rows = tuple(results)
    useful_label_rows = tuple(useful_alert_labels or ())
    candidate_rows = tuple(row for row in result_rows if _baseline_name(row) is None)
    baseline_rows = tuple(row for row in result_rows if _baseline_name(row) is not None)
    baselines = (*baseline_rows, *(baseline_candidates or ()))

    candidate_count = len(candidate_rows)
    precision = _precision_by_label(candidate_rows)
    true_positive_count = _positive_count(candidate_rows, positive_label)
    false_positive_count = candidate_count - true_positive_count
    useful_count = _useful_alert_count(
        candidate_rows,
        useful_label_rows,
        fallback_count=true_positive_count,
    )
    useful_alert_rate = _safe_rate(useful_count, candidate_count)
    cost_per_useful_alert = _cost_per_useful_alert(total_cost, useful_count)
    cost_per_candidate = _cost_per_candidate(total_cost, candidate_count)
    baseline_comparison = _baseline_comparison(
        candidate_rows,
        baselines,
        baseline_top_n=baseline_top_n,
        positive_label=positive_label,
        total_cost=total_cost,
    )
    backtest_summary = _backtest_summary(
        candidate_rows,
        baseline_comparison=baseline_comparison,
        positive_label=positive_label,
        total_cost=total_cost,
    )
    score_calibration = _score_calibration(
        candidate_rows,
        useful_alert_labels=useful_label_rows,
        positive_label=positive_label,
    )
    local_text_intelligence = _local_text_intelligence(
        candidate_rows,
        feedback_label_rows=useful_label_rows,
        positive_label=positive_label,
    )

    return ValidationReport(
        run_id=run_id,
        candidate_count=candidate_count,
        precision=precision,
        false_positive_count=false_positive_count,
        useful_alert_rate=useful_alert_rate,
        cost_per_useful_alert=cost_per_useful_alert,
        cost_per_candidate=cost_per_candidate,
        missed_opportunity_count=len(_all_missed_tickers(baseline_comparison)),
        leakage_failure_count=sum(1 for row in candidate_rows if _leakage_flags(row)),
        state_mix=_state_mix(candidate_rows),
        baseline_comparison=baseline_comparison,
        backtest_summary=backtest_summary,
        score_calibration=score_calibration,
        local_text_intelligence=local_text_intelligence,
    )


def validation_report_payload(report: ValidationReport | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(report, ValidationReport):
        return report.as_dict()
    return {str(key): _thaw(value) for key, value in report.items()}


def _precision_by_label(rows: tuple[Any, ...]) -> dict[str, float]:
    labels = set(KNOWN_BOOLEAN_LABELS)
    for row in rows:
        labels.update(key for key, value in _labels(row).items() if isinstance(value, bool))
    return {
        label: _safe_rate(sum(1 for row in rows if _labels(row).get(label) is True), len(rows))
        for label in sorted(labels)
    }


def _positive_count(rows: tuple[Any, ...], positive_label: str) -> int:
    return sum(1 for row in rows if _labels(row).get(positive_label) is True)


def _useful_alert_count(
    candidate_rows: tuple[Any, ...],
    useful_alert_labels: Iterable[Any] | None,
    *,
    fallback_count: int,
) -> int:
    if useful_alert_labels is None:
        return fallback_count
    candidate_keys = _candidate_artifact_keys(candidate_rows)
    useful_keys = set()
    for label in useful_alert_labels:
        label_value = str(_read(label, "label") or "").lower()
        if label_value not in USEFUL_ALERT_LABELS:
            continue
        key = str(_first_present(_read(label, "artifact_id"), _read(label, "ticker"), ""))
        ticker = _maybe_ticker(_read(label, "ticker"))
        if key in candidate_keys or (ticker is not None and ticker in candidate_keys):
            useful_keys.add(key or ticker)
    return min(len(useful_keys), len(candidate_rows))


def _candidate_artifact_keys(rows: tuple[Any, ...]) -> set[str]:
    keys = set()
    for row in rows:
        for name in ("id", "candidate_packet_id", "decision_card_id", "artifact_id"):
            value = _read(row, name)
            if value is not None:
                keys.add(str(value))
        ticker = _maybe_ticker(_read(row, "ticker"))
        if ticker is not None:
            keys.add(ticker)
    return keys


def _baseline_comparison(
    candidate_rows: tuple[Any, ...],
    baselines: Iterable[Any],
    *,
    baseline_top_n: int | None,
    positive_label: str,
    total_cost: float,
) -> dict[str, Any]:
    candidate_keys = {
        key for row in candidate_rows if (key := _ticker_as_of_key(row)) is not None
    }
    marketradar_rows = _ranked_market_radar_rows(candidate_rows, limit=baseline_top_n)
    marketradar_stats = _selection_stats(marketradar_rows, positive_label=positive_label)
    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in baselines:
        name = _baseline_name(row)
        key = _ticker_as_of_key(row)
        if name is None or key is None:
            continue
        rank = _rank(row)
        if baseline_top_n is not None and (rank is None or rank > baseline_top_n):
            continue
        grouped[name].append(row)

    comparison = {}
    for name, rows in sorted(grouped.items()):
        sorted_rows = tuple(
            sorted(rows, key=lambda item: (_rank(item) or 10**9, _read(item, "ticker")))
        )
        baseline_keys = [
            key
            for row in sorted_rows
            if (key := _ticker_as_of_key(row)) is not None
        ]
        unique_keys = tuple(dict.fromkeys(baseline_keys))
        overlap = tuple(key for key in unique_keys if key in candidate_keys)
        missed = tuple(key for key in unique_keys if key not in candidate_keys)
        baseline_stats = _selection_stats(sorted_rows, positive_label=positive_label)
        comparison[name] = {
            "baseline_candidate_count": len(unique_keys),
            "overlap_count": len(overlap),
            "missed_opportunity_count": len(missed),
            "overlap_tickers": [key[0] for key in overlap],
            "missed_tickers": [key[0] for key in missed],
            "overlap_keys": [_format_ticker_as_of_key(key) for key in overlap],
            "missed_keys": [_format_ticker_as_of_key(key) for key in missed],
            "marketradar_candidate_count": len(marketradar_rows),
            "marketradar_precision_at_5": marketradar_stats["precision_at_5"],
            "marketradar_precision_at_10": marketradar_stats["precision_at_10"],
            "marketradar_false_positive_rate": marketradar_stats[
                "false_positive_rate"
            ],
            "marketradar_max_adverse_excursion_avg": marketradar_stats[
                "max_adverse_excursion_avg"
            ],
            "marketradar_max_favorable_excursion_avg": marketradar_stats[
                "max_favorable_excursion_avg"
            ],
            "marketradar_labeled_count": marketradar_stats["labeled_count"],
            "marketradar_cost_per_candidate": _cost_per_candidate(
                total_cost,
                marketradar_stats["candidate_count"],
            ),
            "baseline_precision_at_5": baseline_stats["precision_at_5"],
            "baseline_precision_at_10": baseline_stats["precision_at_10"],
            "baseline_false_positive_rate": baseline_stats["false_positive_rate"],
            "baseline_max_adverse_excursion_avg": baseline_stats[
                "max_adverse_excursion_avg"
            ],
            "baseline_max_favorable_excursion_avg": baseline_stats[
                "max_favorable_excursion_avg"
            ],
            **_prefixed_metrics("marketradar", marketradar_stats),
            **_prefixed_metrics("baseline", baseline_stats),
            "baseline_labeled_count": baseline_stats["labeled_count"],
            "baseline_cost_per_candidate": 0.0,
            "sample_status": _comparison_sample_status(marketradar_stats, baseline_stats),
            "result_vs_market_radar": _comparison_result(
                marketradar_stats,
                baseline_stats,
            ),
        }
    for name in MISSION_BRIEF_BASELINES:
        if name not in comparison:
            comparison[name] = _empty_baseline_comparison(
                marketradar_stats,
                total_cost=total_cost,
            )
    return comparison


def _empty_baseline_comparison(
    marketradar_stats: Mapping[str, Any],
    *,
    total_cost: float,
) -> dict[str, Any]:
    return {
        "baseline_candidate_count": 0,
        "overlap_count": 0,
        "missed_opportunity_count": 0,
        "overlap_tickers": [],
        "missed_tickers": [],
        "overlap_keys": [],
        "missed_keys": [],
        "marketradar_candidate_count": marketradar_stats["candidate_count"],
        "marketradar_precision_at_5": marketradar_stats["precision_at_5"],
        "marketradar_precision_at_10": marketradar_stats["precision_at_10"],
        "marketradar_false_positive_rate": marketradar_stats["false_positive_rate"],
        "marketradar_max_adverse_excursion_avg": marketradar_stats[
            "max_adverse_excursion_avg"
        ],
        "marketradar_max_favorable_excursion_avg": marketradar_stats[
            "max_favorable_excursion_avg"
        ],
        "marketradar_labeled_count": marketradar_stats["labeled_count"],
        "marketradar_cost_per_candidate": _cost_per_candidate(
            total_cost,
            marketradar_stats["candidate_count"],
        ),
        "baseline_precision_at_5": None,
        "baseline_precision_at_10": None,
        "baseline_false_positive_rate": 0.0,
        "baseline_max_adverse_excursion_avg": None,
        "baseline_max_favorable_excursion_avg": None,
        **_prefixed_metrics("marketradar", marketradar_stats),
        **_prefixed_metrics("baseline", {}),
        "baseline_labeled_count": 0,
        "baseline_cost_per_candidate": 0.0,
        "sample_status": "insufficient_evidence",
        "result_vs_market_radar": "insufficient_evidence",
    }


def _backtest_summary(
    candidate_rows: tuple[Any, ...],
    *,
    baseline_comparison: Mapping[str, Any],
    positive_label: str,
    total_cost: float,
) -> dict[str, Any]:
    marketradar_rows = _ranked_market_radar_rows(candidate_rows, limit=None)
    stats = _selection_stats(marketradar_rows, positive_label=positive_label)
    baseline_result_counts = Counter(
        str(_read(row, "result_vs_market_radar") or "unknown")
        for row in baseline_comparison.values()
    )
    adverse_values = _label_values(marketradar_rows, "max_adverse_excursion")
    favorable_values = _label_values(marketradar_rows, "max_favorable_excursion")
    return {
        "schema_version": "validation-backtest-summary-v1",
        "scope": "marketradar_candidates",
        "positive_label": positive_label,
        "candidate_count": stats["candidate_count"],
        "labeled_count": stats["labeled_count"],
        "positive_count": stats["positive_count"],
        "hit_rate": _safe_rate(stats["positive_count"], stats["labeled_count"]),
        "precision_at_5": stats["precision_at_5"],
        "precision_at_10": stats["precision_at_10"],
        "false_positive_rate": stats["false_positive_rate"],
        "max_adverse_excursion_avg": stats["max_adverse_excursion_avg"],
        "max_favorable_excursion_avg": stats["max_favorable_excursion_avg"],
        "drawdown_proxy": {
            "metric": "abs_max_adverse_excursion",
            "value": _max_abs(adverse_values),
            "note": (
                "Validation is signal-quality evidence, not realized execution P&L. "
                "This uses stored max adverse excursion as the drawdown proxy."
            ),
        },
        "max_favorable_excursion_abs": _max_abs(favorable_values),
        "return_5d_avg": stats["return_5d_avg"],
        "return_10d_avg": stats["return_10d_avg"],
        "return_20d_avg": stats["return_20d_avg"],
        "return_60d_avg": stats["return_60d_avg"],
        "spy_relative_return_20d_avg": stats["spy_relative_return_20d_avg"],
        "sector_relative_return_20d_avg": stats["sector_relative_return_20d_avg"],
        "sector_outperformance_rate": stats["sector_outperformance_rate"],
        "slippage_assumption": {
            "round_trip_bps": 0.0,
            "applied_to_returns": False,
            "note": (
                "No trade execution P&L is claimed here; returns are raw "
                "point-in-time outcome labels. Apply explicit slippage in a "
                "paper/live P&L model before using this as execution evidence."
            ),
        },
        "benchmark_comparison": {
            "required_baseline_count": len(MISSION_BRIEF_BASELINES),
            "measured_baseline_count": int(baseline_result_counts.get("marketradar_wins") or 0)
            + int(baseline_result_counts.get("baseline_wins") or 0)
            + int(baseline_result_counts.get("tie") or 0),
            "marketradar_wins": int(baseline_result_counts.get("marketradar_wins") or 0),
            "baseline_wins": int(baseline_result_counts.get("baseline_wins") or 0),
            "ties": int(baseline_result_counts.get("tie") or 0),
            "insufficient_evidence": int(
                baseline_result_counts.get("insufficient_evidence") or 0
            ),
            "missing": int(baseline_result_counts.get("missing") or 0),
            "result_counts": dict(sorted(baseline_result_counts.items())),
        },
        "cost_per_candidate": _cost_per_candidate(total_cost, stats["candidate_count"]),
    }


def _ranked_market_radar_rows(rows: tuple[Any, ...], *, limit: int | None) -> tuple[Any, ...]:
    ordered = sorted(
        rows,
        key=lambda row: (
            -(_finite_float(_read(row, "final_score")) or 0.0),
            _maybe_ticker(_read(row, "ticker")) or "",
            _as_of_key(_read(row, "as_of")),
        ),
    )
    if limit is not None:
        ordered = ordered[: max(0, int(limit))]
    return tuple(ordered)


def _selection_stats(rows: Iterable[Any], *, positive_label: str) -> dict[str, Any]:
    row_tuple = tuple(rows)
    labeled_rows = tuple(row for row in row_tuple if positive_label in _labels(row))
    positive_count = sum(1 for row in labeled_rows if _labels(row).get(positive_label) is True)
    return {
        "candidate_count": len(row_tuple),
        "labeled_count": len(labeled_rows),
        "positive_count": positive_count,
        "precision_at_5": _precision_at(row_tuple, positive_label=positive_label, n=5),
        "precision_at_10": _precision_at(row_tuple, positive_label=positive_label, n=10),
        "false_positive_rate": _safe_rate(
            len(labeled_rows) - positive_count,
            len(labeled_rows),
        ),
        "max_adverse_excursion_avg": _average_label(
            labeled_rows,
            "max_adverse_excursion",
        ),
        "max_favorable_excursion_avg": _average_label(
            labeled_rows,
            "max_favorable_excursion",
        ),
        **_selection_return_stats(row_tuple),
    }


def _precision_at(rows: tuple[Any, ...], *, positive_label: str, n: int) -> float | None:
    top_rows = rows[:n]
    labeled_rows = tuple(row for row in top_rows if positive_label in _labels(row))
    if not labeled_rows:
        return None
    return _safe_rate(
        sum(1 for row in labeled_rows if _labels(row).get(positive_label) is True),
        len(labeled_rows),
    )


def _average_label(rows: tuple[Any, ...], label: str) -> float | None:
    values = _label_values(rows, label)
    if not values:
        return None
    return sum(values) / len(values)


def _label_values(rows: tuple[Any, ...], label: str) -> list[float]:
    return [
        number
        for row in rows
        if (number := _finite_float(_labels(row).get(label))) is not None
    ]


def _max_abs(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return max(abs(value) for value in values)


def _selection_return_stats(rows: tuple[Any, ...]) -> dict[str, float | None]:
    stats: dict[str, float | None] = {}
    for horizon in FORWARD_RETURN_HORIZONS:
        stats[f"return_{horizon}d_avg"] = _average_label(
            rows,
            f"return_{horizon}d",
        )
    for benchmark in RELATIVE_RETURN_BENCHMARKS:
        for horizon in FORWARD_RETURN_HORIZONS:
            stats[f"{benchmark}_relative_return_{horizon}d_avg"] = (
                _average_relative_return(rows, benchmark=benchmark, horizon=horizon)
            )
    stats["sector_outperformance_rate"] = _boolean_label_rate(
        rows,
        "sector_outperformance",
    )
    return stats


def _average_relative_return(
    rows: tuple[Any, ...],
    *,
    benchmark: str,
    horizon: int,
) -> float | None:
    label = f"{benchmark}_relative_return_{horizon}d"
    values = []
    for row in rows:
        labels = _labels(row)
        value = _finite_float(labels.get(label))
        if value is None:
            candidate_return = _finite_float(labels.get(f"return_{horizon}d"))
            benchmark_return = _finite_float(labels.get(f"{benchmark}_return_{horizon}d"))
            if candidate_return is not None and benchmark_return is not None:
                value = candidate_return - benchmark_return
        if value is not None:
            values.append(value)
    if not values:
        return None
    return sum(values) / len(values)


def _prefixed_metrics(prefix: str, stats: Mapping[str, Any]) -> dict[str, Any]:
    return {
        f"{prefix}_{metric}": stats.get(metric)
        for metric in COMPARISON_RETURN_METRICS
    }


def _comparison_sample_status(
    marketradar_stats: Mapping[str, Any],
    baseline_stats: Mapping[str, Any],
) -> str:
    if not marketradar_stats.get("labeled_count") or not baseline_stats.get("labeled_count"):
        return "insufficient_evidence"
    return "measured"


def _comparison_result(
    marketradar_stats: Mapping[str, Any],
    baseline_stats: Mapping[str, Any],
) -> str:
    if _comparison_sample_status(marketradar_stats, baseline_stats) != "measured":
        return "insufficient_evidence"
    market_precision = _finite_float(marketradar_stats.get("precision_at_10"))
    baseline_precision = _finite_float(baseline_stats.get("precision_at_10"))
    if market_precision is None or baseline_precision is None:
        return "insufficient_evidence"
    if market_precision > baseline_precision:
        return "marketradar_wins"
    if market_precision < baseline_precision:
        return "baseline_wins"
    return "tie"


def _local_text_intelligence(
    candidate_rows: tuple[Any, ...],
    *,
    feedback_label_rows: Iterable[Any],
    positive_label: str,
) -> dict[str, Any]:
    feedback_labels = _feedback_label_keys(feedback_label_rows)
    useful_keys = _useful_label_keys(feedback_labels)
    numeric_features = {
        "local_narrative_score": ("local_narrative_score",),
        "novelty_score": ("novelty_score",),
        "source_quality_score": ("source_quality_score",),
        "sentiment_score": ("sentiment_score",),
        "theme_match_score": ("theme_match_score",),
        "theme_velocity_score": ("theme_velocity_score",),
    }
    features = {
        name: _local_text_numeric_feature_payload(
            candidate_rows,
            feature_name=name,
            field_names=field_names,
            useful_keys=useful_keys,
            positive_label=positive_label,
        )
        for name, field_names in numeric_features.items()
    }
    features["theme_hit_presence"] = _local_text_presence_feature_payload(
        candidate_rows,
        feature_name="theme_hit_presence",
        useful_keys=useful_keys,
        positive_label=positive_label,
    )
    measured_features = [
        payload for payload in features.values() if payload["sample_status"] == "measured"
    ]
    return {
        "schema_version": "local-text-measurement-v1",
        "sample_status": "measured" if measured_features else "insufficient_evidence",
        "feature_count": len(features),
        "measured_feature_count": len(measured_features),
        "features": features,
        "upgrade_recommendation": _local_text_upgrade_recommendation(measured_features),
        "thresholds_changed": False,
        "models_changed": False,
        "note": (
            "Report-only local text evidence. No sentiment model, embedding model, "
            "ontology, scoring weight, policy threshold, trade plan, or action gate "
            "was changed."
        ),
    }


def _local_text_numeric_feature_payload(
    rows: tuple[Any, ...],
    *,
    feature_name: str,
    field_names: tuple[str, ...],
    useful_keys: set[str],
    positive_label: str,
) -> dict[str, Any]:
    rows_with_values = tuple(
        (row, value)
        for row in rows
        if (value := _local_text_feature_float(row, field_names)) is not None
    )
    bucket_rows = {
        "low": tuple(row for row, value in rows_with_values if value < 50.0),
        "mid": tuple(row for row, value in rows_with_values if 50.0 <= value < 75.0),
        "high": tuple(row for row, value in rows_with_values if value >= 75.0),
    }
    buckets = [
        _local_text_bucket_payload(
            name,
            bucket_rows[name],
            useful_keys=useful_keys,
            positive_label=positive_label,
        )
        for name in ("low", "mid", "high")
    ]
    high = buckets[2]
    low = buckets[0]
    return {
        "feature": feature_name,
        "kind": "numeric_score",
        "sample_count": len(rows_with_values),
        "missing_count": len(rows) - len(rows_with_values),
        "sample_status": "measured" if rows_with_values else "insufficient_evidence",
        "buckets": buckets,
        "high_vs_low": _local_text_high_vs_low(high, low),
    }


def _local_text_presence_feature_payload(
    rows: tuple[Any, ...],
    *,
    feature_name: str,
    useful_keys: set[str],
    positive_label: str,
) -> dict[str, Any]:
    present_rows = tuple(row for row in rows if _theme_hits_present(row) is True)
    absent_rows = tuple(row for row in rows if _theme_hits_present(row) is False)
    unknown_count = len(rows) - len(present_rows) - len(absent_rows)
    buckets = [
        _local_text_bucket_payload(
            "absent",
            absent_rows,
            useful_keys=useful_keys,
            positive_label=positive_label,
        ),
        _local_text_bucket_payload(
            "present",
            present_rows,
            useful_keys=useful_keys,
            positive_label=positive_label,
        ),
    ]
    return {
        "feature": feature_name,
        "kind": "presence",
        "sample_count": len(present_rows) + len(absent_rows),
        "missing_count": unknown_count,
        "sample_status": "measured"
        if present_rows or absent_rows
        else "insufficient_evidence",
        "buckets": buckets,
        "present_vs_absent": _local_text_high_vs_low(buckets[1], buckets[0]),
    }


def _local_text_bucket_payload(
    name: str,
    rows: tuple[Any, ...],
    *,
    useful_keys: set[str],
    positive_label: str,
) -> dict[str, Any]:
    labeled_rows = tuple(row for row in rows if positive_label in _labels(row))
    positive_count = sum(1 for row in labeled_rows if _labels(row).get(positive_label) is True)
    useful_count = sum(1 for row in rows if _row_matches_useful_key(row, useful_keys))
    return {
        "bucket": name,
        "candidate_count": len(rows),
        "labeled_count": len(labeled_rows),
        "positive_count": positive_count,
        "precision": _safe_rate(positive_count, len(labeled_rows))
        if labeled_rows
        else None,
        "false_positive_rate": _safe_rate(
            len(labeled_rows) - positive_count,
            len(labeled_rows),
        )
        if labeled_rows
        else None,
        "useful_label_count": useful_count,
        "useful_label_rate": _safe_rate(useful_count, len(rows)) if rows else None,
        "sample_status": "measured" if labeled_rows else "insufficient_evidence",
    }


def _local_text_high_vs_low(
    high: Mapping[str, Any],
    low: Mapping[str, Any],
) -> dict[str, Any]:
    precision_delta = _optional_delta(high.get("precision"), low.get("precision"))
    false_positive_delta = _optional_delta(
        low.get("false_positive_rate"),
        high.get("false_positive_rate"),
    )
    useful_delta = _optional_delta(high.get("useful_label_rate"), low.get("useful_label_rate"))
    return {
        "precision_delta": precision_delta,
        "false_positive_reduction_delta": false_positive_delta,
        "useful_label_rate_delta": useful_delta,
        "sample_status": "measured"
        if precision_delta is not None or useful_delta is not None
        else "insufficient_evidence",
        "interpretation": _local_text_delta_interpretation(
            precision_delta,
            false_positive_delta,
            useful_delta,
        ),
    }


def _local_text_delta_interpretation(
    precision_delta: float | None,
    false_positive_delta: float | None,
    useful_delta: float | None,
) -> str:
    values = [value for value in (precision_delta, false_positive_delta, useful_delta) if value]
    if not values:
        return "insufficient_evidence"
    positive = sum(1 for value in values if value > 0)
    negative = sum(1 for value in values if value < 0)
    if positive and not negative:
        return "supports_existing_local_text_signal"
    if negative and not positive:
        return "possible_noise_or_overweighting"
    return "mixed"


def _local_text_upgrade_recommendation(measured_features: list[Mapping[str, Any]]) -> str:
    if not measured_features:
        return "insufficient_evidence"
    supportive = 0
    noisy = 0
    for payload in measured_features:
        comparison = payload.get("high_vs_low") or payload.get("present_vs_absent") or {}
        interpretation = str(_read(comparison, "interpretation") or "")
        if interpretation == "supports_existing_local_text_signal":
            supportive += 1
        elif interpretation == "possible_noise_or_overweighting":
            noisy += 1
    if supportive and not noisy:
        return "collect_more_samples_before_upgrading"
    if noisy:
        return "review_local_text_weighting_before_upgrading"
    return "continue_measuring"


def _local_text_feature_float(row: Any, field_names: tuple[str, ...]) -> float | None:
    for name in field_names:
        value = _finite_float(_read(row, name))
        if value is not None:
            return value
    for source in _dimension_sources(row):
        for name in field_names:
            value = _finite_float(source.get(name))
            if value is not None:
                return value
        for path in _dimension_nested_paths(field_names):
            value = _finite_float(_nested_mapping_value(source, *path))
            if value is not None:
                return value
    return None


def _theme_hits_present(row: Any) -> bool | None:
    for source in _dimension_sources(row):
        for name in ("theme_hits", "ontology_hits"):
            value = source.get(name)
            if value is not None:
                return bool(_text_list(value))
        theme_match = _finite_float(source.get("theme_match_score"))
        if theme_match is not None:
            return theme_match > 0
    return None


def _optional_delta(left: Any, right: Any) -> float | None:
    left_value = _finite_float(left)
    right_value = _finite_float(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _score_calibration(
    candidate_rows: tuple[Any, ...],
    *,
    useful_alert_labels: Iterable[Any],
    positive_label: str,
) -> dict[str, Any]:
    feedback_labels = _feedback_label_keys(useful_alert_labels)
    useful_keys = _useful_label_keys(feedback_labels)
    buckets = []
    for name, lower, upper in (
        ("below_50", None, 49.9999),
        ("50_59", 50.0, 59.9999),
        ("60_69", 60.0, 69.9999),
        ("70_79", 70.0, 79.9999),
        ("80_89", 80.0, 89.9999),
        ("90_plus", 90.0, None),
    ):
        rows = tuple(
            row
            for row in candidate_rows
            if _score_in_bucket(_finite_float(_read(row, "final_score")), lower, upper)
        )
        buckets.append(
            _score_bucket_payload(
                name,
                rows,
                useful_keys=useful_keys,
                positive_label=positive_label,
            )
        )
    measured = [bucket for bucket in buckets if bucket["labeled_count"] > 0]
    monotonic_precision = _monotonic_precision(measured)
    threshold_review_flags = _threshold_review_flags(buckets)
    return {
        "schema_version": "score-calibration-v1",
        "positive_label": positive_label,
        "bucket_count": len(buckets),
        "sample_status": "measured" if measured else "insufficient_evidence",
        "monotonic_precision": monotonic_precision,
        "score_ordering_verdict": _score_ordering_verdict(
            monotonic_precision=monotonic_precision,
            threshold_review_flags=threshold_review_flags,
        ),
        "higher_scores_correlate_with_outcomes": _higher_scores_correlate_with_outcomes(
            monotonic_precision
        ),
        "buckets": buckets,
        "score_distribution": _score_distribution_dimensions(
            candidate_rows,
            feedback_labels=feedback_labels,
            useful_keys=useful_keys,
            positive_label=positive_label,
        ),
        "threshold_review_flags": threshold_review_flags,
        "threshold_review_required": bool(threshold_review_flags),
        "thresholds_changed": False,
        "note": (
            "Report-only calibration evidence. No scoring weights, policy thresholds, "
            "trade plans, or action gates were changed."
        ),
    }


def _score_bucket_payload(
    name: str,
    rows: tuple[Any, ...],
    *,
    useful_keys: set[str],
    positive_label: str,
) -> dict[str, Any]:
    labeled_rows = tuple(row for row in rows if positive_label in _labels(row))
    positive_count = sum(1 for row in labeled_rows if _labels(row).get(positive_label) is True)
    useful_count = sum(1 for row in rows if _row_matches_useful_key(row, useful_keys))
    return {
        "bucket": name,
        "score_min": _bucket_min(name),
        "score_max": _bucket_max(name),
        "candidate_count": len(rows),
        "labeled_count": len(labeled_rows),
        "positive_count": positive_count,
        "precision": _safe_rate(positive_count, len(labeled_rows))
        if labeled_rows
        else None,
        "false_positive_rate": _safe_rate(
            len(labeled_rows) - positive_count,
            len(labeled_rows),
        )
        if labeled_rows
        else None,
        "useful_label_count": useful_count,
        "useful_label_rate": _safe_rate(useful_count, len(rows)) if rows else None,
        "max_adverse_excursion_avg": _average_label(
            labeled_rows,
            "max_adverse_excursion",
        ),
        "max_favorable_excursion_avg": _average_label(
            labeled_rows,
            "max_favorable_excursion",
        ),
        **_selection_return_stats(labeled_rows),
        "sample_status": "measured" if labeled_rows else "insufficient_evidence",
    }


def _score_distribution_dimensions(
    candidate_rows: tuple[Any, ...],
    *,
    feedback_labels: Mapping[str, str],
    useful_keys: set[str],
    positive_label: str,
) -> dict[str, Any]:
    return {
        dimension: _score_distribution_for_dimension(
            candidate_rows,
            dimension=dimension,
            feedback_labels=feedback_labels,
            useful_keys=useful_keys,
            positive_label=positive_label,
        )
        for dimension in (
            "sector",
            "market_regime",
            "setup_type",
            "priced_in_status",
            "action_state",
            "source_coverage",
            "usefulness_label",
        )
    }


def _score_distribution_for_dimension(
    candidate_rows: tuple[Any, ...],
    *,
    dimension: str,
    feedback_labels: Mapping[str, str],
    useful_keys: set[str],
    positive_label: str,
) -> dict[str, Any]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in candidate_rows:
        grouped[
            _dimension_value(row, dimension=dimension, feedback_labels=feedback_labels)
        ].append(row)
    groups = [
        _score_distribution_group_payload(
            value,
            tuple(rows),
            useful_keys=useful_keys,
            positive_label=positive_label,
        )
        for value, rows in sorted(grouped.items())
    ]
    return {
        "group_count": len(groups),
        "sample_status": "measured"
        if any(group["labeled_count"] > 0 for group in groups)
        else "insufficient_evidence",
        "groups": groups,
    }


def _score_distribution_group_payload(
    value: str,
    rows: tuple[Any, ...],
    *,
    useful_keys: set[str],
    positive_label: str,
) -> dict[str, Any]:
    labeled_rows = tuple(row for row in rows if positive_label in _labels(row))
    positive_count = sum(1 for row in labeled_rows if _labels(row).get(positive_label) is True)
    useful_count = sum(1 for row in rows if _row_matches_useful_key(row, useful_keys))
    scores = [
        score
        for row in rows
        if (score := _finite_float(_read(row, "final_score"))) is not None
    ]
    return {
        "value": value,
        "candidate_count": len(rows),
        "labeled_count": len(labeled_rows),
        "positive_count": positive_count,
        "average_score": (sum(scores) / len(scores)) if scores else None,
        "precision": _safe_rate(positive_count, len(labeled_rows))
        if labeled_rows
        else None,
        "false_positive_rate": _safe_rate(
            len(labeled_rows) - positive_count,
            len(labeled_rows),
        )
        if labeled_rows
        else None,
        "useful_label_count": useful_count,
        "useful_label_rate": _safe_rate(useful_count, len(rows)) if rows else None,
        "bucket_counts": _score_bucket_counts(rows),
        **_selection_return_stats(labeled_rows),
        "sample_status": "measured" if labeled_rows else "insufficient_evidence",
    }


def _score_bucket_counts(rows: tuple[Any, ...]) -> dict[str, int]:
    counts = {name: 0 for name, _, _ in _score_bucket_defs()}
    for row in rows:
        score = _finite_float(_read(row, "final_score"))
        for name, lower, upper in _score_bucket_defs():
            if _score_in_bucket(score, lower, upper):
                counts[name] += 1
                break
    return counts


def _score_bucket_defs() -> tuple[tuple[str, float | None, float | None], ...]:
    return (
        ("below_50", None, 49.9999),
        ("50_59", 50.0, 59.9999),
        ("60_69", 60.0, 69.9999),
        ("70_79", 70.0, 79.9999),
        ("80_89", 80.0, 89.9999),
        ("90_plus", 90.0, None),
    )


def _score_in_bucket(score: float | None, lower: float | None, upper: float | None) -> bool:
    if score is None:
        return False
    if lower is not None and score < lower:
        return False
    if upper is not None and score > upper:
        return False
    return True


def _bucket_min(name: str) -> float | None:
    if name == "below_50":
        return None
    if name == "90_plus":
        return 90.0
    return float(name.split("_", maxsplit=1)[0])


def _bucket_max(name: str) -> float | None:
    if name == "below_50":
        return 49.9999
    if name == "90_plus":
        return None
    return float(name.split("_", maxsplit=1)[1]) + 0.9999


def _boolean_label_rate(rows: tuple[Any, ...], label: str) -> float | None:
    labeled_rows = tuple(row for row in rows if label in _labels(row))
    if not labeled_rows:
        return None
    return _safe_rate(
        sum(1 for row in labeled_rows if _labels(row).get(label) is True),
        len(labeled_rows),
    )


def _feedback_label_keys(feedback_labels: Iterable[Any]) -> dict[str, str]:
    keys: dict[str, str] = {}
    for label in feedback_labels:
        label_value = str(_read(label, "label") or "").strip().lower()
        if not label_value:
            continue
        for field_name in ("artifact_id", "ticker"):
            value = _read(label, field_name)
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            keys[text] = label_value
            keys[text.upper()] = label_value
    return keys


def _useful_label_keys(feedback_labels: Mapping[str, str]) -> set[str]:
    return {
        key
        for key, label in feedback_labels.items()
        if str(label).lower() in USEFUL_ALERT_LABELS
    }


def _dimension_value(
    row: Any,
    *,
    dimension: str,
    feedback_labels: Mapping[str, str],
) -> str:
    if dimension == "usefulness_label":
        for key in _candidate_artifact_keys((row,)):
            label = feedback_labels.get(key) or feedback_labels.get(key.upper())
            if label:
                return label
        return "unlabeled"
    if dimension == "action_state":
        return _first_dimension_text(row, ("action_state", "state")) or "unknown"
    if dimension == "source_coverage":
        return _source_coverage_dimension(row)
    lookup = {
        "sector": ("sector", "gics_sector", "sector_etf"),
        "market_regime": ("market_regime", "regime"),
        "setup_type": ("setup_type", "setup"),
        "priced_in_status": ("priced_in_status", "priced_in"),
    }
    return _first_dimension_text(row, lookup.get(dimension, (dimension,))) or "unknown"


def _first_dimension_text(row: Any, names: tuple[str, ...]) -> str | None:
    for name in names:
        text = _dimension_text(_read(row, name))
        if text is not None:
            return text
    for source in _dimension_sources(row):
        for name in names:
            text = _dimension_text(source.get(name))
            if text is not None:
                return text
        for path in _dimension_nested_paths(names):
            text = _dimension_text(_nested_mapping_value(source, *path))
            if text is not None:
                return text
    return None


def _dimension_nested_paths(names: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    return tuple(
        path
        for name in names
        for path in (
            ("candidate", name),
            ("candidate", "metadata", name),
            ("candidate", "features", name),
            ("identity", name),
            ("metadata", name),
            ("trade_plan", name),
            ("priced_in", name),
            ("source_coverage", name),
        )
    )


def _source_coverage_dimension(row: Any) -> str:
    for source in _dimension_sources(row):
        coverage = _first_mapping(source.get("source_coverage"), source)
        if coverage:
            weak_sources = _text_list(coverage.get("weak_sources"))
            if weak_sources:
                return f"gaps:{','.join(weak_sources[:3])}"
            sources = coverage.get("sources")
            if isinstance(sources, Mapping):
                gap_sources = [
                    str(name)
                    for name, value in sources.items()
                    if _source_row_has_gap(value)
                ]
                if gap_sources:
                    return f"gaps:{','.join(sorted(gap_sources)[:3])}"
                if sources:
                    return "complete"
            summary = _dimension_text(coverage.get("summary"))
            if summary is not None:
                return summary
    for name in ("source_coverage", "source_coverage_summary", "source_status"):
        text = _dimension_text(_read(row, name))
        if text is not None:
            return text
    return "unknown"


def _source_row_has_gap(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    for name in ("missing", "stale", "gap_count", "missing_count", "stale_count"):
        number = _finite_float(value.get(name))
        if number is not None and number > 0:
            return True
    status = str(value.get("status") or "").strip().lower()
    return status in {"missing", "stale", "blocked", "gap"}


def _dimension_sources(row: Any) -> tuple[Mapping[str, Any], ...]:
    row_mapping = row if isinstance(row, Mapping) else {}
    payload = _mapping_value(_read(row, "payload"))
    replay_payload = _mapping_value(payload.get("payload"))
    signal_payload = _mapping_value(replay_payload.get("signal_payload"))
    candidate = _mapping_value(signal_payload.get("candidate"))
    packet = _mapping_value(replay_payload.get("packet"))
    decision_card = _mapping_value(replay_payload.get("decision_card"))
    baseline_candidate = _mapping_value(payload.get("candidate"))
    values = (
        row_mapping,
        payload,
        replay_payload,
        signal_payload,
        candidate,
        _mapping_value(candidate.get("metadata")),
        _mapping_value(candidate.get("features")),
        packet,
        _mapping_value(packet.get("metadata")),
        _mapping_value(packet.get("payload")),
        decision_card,
        _mapping_value(decision_card.get("payload")),
        _mapping_value(decision_card.get("trade_plan")),
        baseline_candidate,
        _mapping_value(baseline_candidate.get("metadata")),
        _mapping_value(baseline_candidate.get("features")),
        _mapping_value(payload.get("metadata")),
    )
    return tuple(value for value in values if value)


def _dimension_text(value: Any) -> str | None:
    if value is None or isinstance(value, Mapping):
        return None
    if isinstance(value, Iterable) and not isinstance(value, str | bytes):
        values = [text for item in value if (text := _dimension_text(item)) is not None]
        return ",".join(values) if values else None
    text = str(value).strip()
    return text.lower() if text else None


def _text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip().lower()] if value.strip() else []
    if isinstance(value, Iterable):
        return [str(item).strip().lower() for item in value if str(item).strip()]
    return [str(value).strip().lower()] if str(value).strip() else []


def _first_mapping(*values: Any) -> Mapping[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return value
    return {}


def _mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _nested_mapping_value(source: Mapping[str, Any], *keys: str) -> Any:
    value: Any = source
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _row_matches_useful_key(row: Any, useful_keys: set[str]) -> bool:
    for key in _candidate_artifact_keys((row,)):
        if key in useful_keys or key.upper() in useful_keys:
            return True
    return False


def _monotonic_precision(buckets: list[Mapping[str, Any]]) -> str:
    values = [
        _finite_float(bucket.get("precision"))
        for bucket in buckets
        if _finite_float(bucket.get("precision")) is not None
    ]
    if len(values) < 2:
        return "insufficient_evidence"
    if all(left <= right for left, right in zip(values, values[1:], strict=False)):
        return "increasing"
    if all(left >= right for left, right in zip(values, values[1:], strict=False)):
        return "decreasing"
    return "mixed"


def _threshold_review_flags(buckets: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    flags = []
    for bucket in buckets:
        false_positive_rate = _finite_float(bucket.get("false_positive_rate"))
        if (
            false_positive_rate is not None
            and false_positive_rate >= 0.5
            and int(bucket.get("labeled_count") or 0) >= 3
        ):
            flags.append(
                {
                    "bucket": bucket.get("bucket"),
                    "reason": "false_positive_rate_at_or_above_50pct",
                    "false_positive_rate": false_positive_rate,
                    "labeled_count": bucket.get("labeled_count"),
                    "action": "review_threshold_with_more_evidence_before_changing_policy",
                }
            )
    return flags


def _score_ordering_verdict(
    *,
    monotonic_precision: str,
    threshold_review_flags: Sequence[Mapping[str, Any]],
) -> str:
    if threshold_review_flags:
        return "review_thresholds"
    if monotonic_precision == "increasing":
        return "supports_higher_scores"
    if monotonic_precision == "decreasing":
        return "contradicts_higher_scores"
    if monotonic_precision == "mixed":
        return "mixed_evidence"
    return "insufficient_evidence"


def _higher_scores_correlate_with_outcomes(monotonic_precision: str) -> bool | None:
    if monotonic_precision == "increasing":
        return True
    if monotonic_precision == "decreasing":
        return False
    return None


def _ticker_as_of_key(row: Any) -> tuple[str, str] | None:
    ticker = _maybe_ticker(_read(row, "ticker"))
    if ticker is None:
        return None
    return ticker, _as_of_key(_read(row, "as_of"))


def _as_of_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if value is None:
        return "unknown"
    text = str(value)
    if "T" in text:
        return text.split("T", maxsplit=1)[0]
    return text


def _format_ticker_as_of_key(key: tuple[str, str]) -> str:
    return f"{key[0]}:{key[1]}"


def _all_missed_tickers(comparison: Mapping[str, Any]) -> set[str]:
    missed = set()
    for value in comparison.values():
        for ticker in _read(value, "missed_tickers") or ():
            missed.add(str(ticker).upper())
    return missed


def _state_mix(rows: tuple[Any, ...]) -> dict[str, int]:
    counts = Counter(str(_first_present(_read(row, "state"), "unknown")) for row in rows)
    return dict(sorted(counts.items()))


def _cost_per_useful_alert(total_cost: float, useful_count: int) -> float | None:
    cost = _finite_float(total_cost) or 0.0
    if cost <= 0:
        return 0.0
    if useful_count <= 0:
        return None
    return cost / useful_count


def _cost_per_candidate(total_cost: float, candidate_count: int) -> float | None:
    cost = _finite_float(total_cost) or 0.0
    if cost <= 0:
        return 0.0
    if candidate_count <= 0:
        return None
    return cost / candidate_count


def _safe_rate(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _labels(row: Any) -> Mapping[str, Any]:
    labels = _first_present(_read(row, "labels"), _read(row, "outcome_labels"))
    if isinstance(labels, OutcomeLabels):
        return labels.as_dict()
    if isinstance(labels, Mapping):
        return labels
    return {}


def _leakage_flags(row: Any) -> tuple[str, ...]:
    flags = _read(row, "leakage_flags")
    if flags is None:
        return ()
    if isinstance(flags, str):
        return (flags,) if flags else ()
    if isinstance(flags, Iterable):
        return tuple(str(flag) for flag in flags if str(flag))
    return (str(flags),)


def _baseline_name(row: Any) -> str | None:
    if isinstance(row, BaselineCandidate):
        return row.baseline
    value = _read(row, "baseline")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _rank(row: Any) -> int | None:
    value = _read(row, "rank")
    try:
        rank = int(value)
    except (TypeError, ValueError):
        return None
    return rank if rank > 0 else None


def _read(source: Any, key: str) -> Any:
    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(key)
    if is_dataclass(source) and not isinstance(source, type):
        return getattr(source, key, None)
    keys = getattr(source, "keys", None)
    if callable(keys):
        try:
            return source[key]
        except (KeyError, TypeError):
            return None
    return getattr(source, key, None)


def _maybe_ticker(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text.upper() if text else None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _thaw(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    if isinstance(value, list):
        return [_thaw(item) for item in value]
    return value


__all__ = [
    "DEFAULT_POSITIVE_LABEL",
    "USEFUL_ALERT_LABELS",
    "ValidationReport",
    "build_validation_report",
    "validation_report_payload",
]
