from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping
from catalyst_radar.validation.baselines import BaselineCandidate
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


@dataclass(frozen=True)
class ValidationReport:
    """Summary metrics for a validation run."""

    run_id: str
    candidate_count: int
    precision: Mapping[str, float]
    false_positive_count: int
    useful_alert_rate: float
    cost_per_useful_alert: float | None
    missed_opportunity_count: int
    leakage_failure_count: int
    state_mix: Mapping[str, int]
    baseline_comparison: Mapping[str, Any]

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

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "candidate_count": self.candidate_count,
            "precision": dict(self.precision),
            "false_positive_count": self.false_positive_count,
            "useful_alert_rate": self.useful_alert_rate,
            "cost_per_useful_alert": self.cost_per_useful_alert,
            "missed_opportunity_count": self.missed_opportunity_count,
            "leakage_failure_count": self.leakage_failure_count,
            "state_mix": dict(self.state_mix),
            "baseline_comparison": _thaw(self.baseline_comparison),
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
    candidate_rows = tuple(row for row in result_rows if _baseline_name(row) is None)
    baseline_rows = tuple(row for row in result_rows if _baseline_name(row) is not None)
    baselines = (*baseline_rows, *(baseline_candidates or ()))

    candidate_count = len(candidate_rows)
    precision = _precision_by_label(candidate_rows)
    true_positive_count = _positive_count(candidate_rows, positive_label)
    false_positive_count = candidate_count - true_positive_count
    useful_count = _useful_alert_count(
        candidate_rows,
        useful_alert_labels,
        fallback_count=true_positive_count,
    )
    useful_alert_rate = _safe_rate(useful_count, candidate_count)
    cost_per_useful_alert = _cost_per_useful_alert(total_cost, useful_count)
    baseline_comparison = _baseline_comparison(
        candidate_rows,
        baselines,
        baseline_top_n=baseline_top_n,
    )

    return ValidationReport(
        run_id=run_id,
        candidate_count=candidate_count,
        precision=precision,
        false_positive_count=false_positive_count,
        useful_alert_rate=useful_alert_rate,
        cost_per_useful_alert=cost_per_useful_alert,
        missed_opportunity_count=len(_all_missed_tickers(baseline_comparison)),
        leakage_failure_count=sum(1 for row in candidate_rows if _leakage_flags(row)),
        state_mix=_state_mix(candidate_rows),
        baseline_comparison=baseline_comparison,
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
) -> dict[str, Any]:
    candidate_keys = {
        key for row in candidate_rows if (key := _ticker_as_of_key(row)) is not None
    }
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
        baseline_keys = [
            key
            for row in sorted(rows, key=lambda item: (_rank(item) or 10**9, _read(item, "ticker")))
            if (key := _ticker_as_of_key(row)) is not None
        ]
        unique_keys = tuple(dict.fromkeys(baseline_keys))
        overlap = tuple(key for key in unique_keys if key in candidate_keys)
        missed = tuple(key for key in unique_keys if key not in candidate_keys)
        comparison[name] = {
            "baseline_candidate_count": len(unique_keys),
            "overlap_count": len(overlap),
            "missed_opportunity_count": len(missed),
            "overlap_tickers": [key[0] for key in overlap],
            "missed_tickers": [key[0] for key in missed],
            "overlap_keys": [_format_ticker_as_of_key(key) for key in overlap],
            "missed_keys": [_format_ticker_as_of_key(key) for key in missed],
        }
    return comparison


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
