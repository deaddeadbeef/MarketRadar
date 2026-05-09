from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlparse


@dataclass(frozen=True)
class SourceQualityScore:
    score: float
    reasons: tuple[str, ...]


_CATEGORY_BASE = {
    "primary_source": 1.00,
    "regulatory": 0.95,
    "reputable_news": 0.85,
    "company_press_release": 0.75,
    "analyst_provider": 0.70,
    "aggregator": 0.55,
    "social": 0.25,
    "promotional": 0.10,
    "unknown": 0.40,
}

_CATEGORY_REASONS = {
    "primary_source": "primary_source",
    "regulatory": "regulatory_source",
    "reputable_news": "reputable_news",
    "company_press_release": "company_press_release",
    "analyst_provider": "analyst_provider",
    "aggregator": "aggregator_source",
    "social": "social_source",
    "promotional": "promotional_source",
    "unknown": "unknown_source",
}

_PROMOTIONAL_HOST_MARKERS = ("promo", "sponsored", "stockpick")


def score_source_quality(
    *,
    source: str,
    category: object,
    url: str | None = None,
) -> SourceQualityScore:
    category_value = _category_value(category)
    reasons = [_CATEGORY_REASONS.get(category_value, "unknown_source")]
    score = _CATEGORY_BASE.get(category_value, _CATEGORY_BASE["unknown"])

    host = urlparse(url or "").hostname or ""
    host = host.lower()
    if host == "sec.gov" or host.endswith(".sec.gov"):
        reasons.append("primary_source_domain")
        score = max(score, 0.95)
    if any(marker in host for marker in _PROMOTIONAL_HOST_MARKERS):
        reasons.append("promotional_domain")
        score = min(score, 0.20)

    if _looks_promotional(source):
        reasons.append("promotional_source_name")
        score = min(score, 0.20)

    return SourceQualityScore(score=_clamp(score, 0.0, 1.0), reasons=tuple(reasons))


def _category_value(category: object) -> str:
    if isinstance(category, StrEnum):
        return str(category.value)
    value = getattr(category, "value", category)
    return str(value).lower()


def _looks_promotional(source: str) -> bool:
    normalized = source.casefold()
    return any(marker in normalized for marker in ("sponsored", "promo", "stockpick"))


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, float(value)))
