from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from catalyst_radar.core.immutability import freeze_json_value, freeze_mapping


@dataclass(frozen=True)
class OntologyTheme:
    theme_id: str
    terms: Sequence[str]
    sectors: Sequence[str] = ()
    read_through: Sequence[str] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "theme_id", _required_text(self.theme_id, "theme_id"))
        object.__setattr__(self, "terms", _required_text_tuple(self.terms, "terms"))
        object.__setattr__(self, "sectors", tuple(str(sector) for sector in self.sectors))
        object.__setattr__(
            self,
            "read_through",
            tuple(str(item) for item in self.read_through),
        )


@dataclass(frozen=True)
class OntologyMatch:
    theme_id: str
    terms: Sequence[str]
    score: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "theme_id", _required_text(self.theme_id, "theme_id"))
        object.__setattr__(self, "terms", _required_text_tuple(self.terms, "terms"))
        object.__setattr__(self, "score", _clamp(self.score, 0.0, 1.0))


@dataclass(frozen=True)
class SentimentResult:
    score: float
    positive_terms: Sequence[str] = ()
    negative_terms: Sequence[str] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "score", _clamp(self.score, -1.0, 1.0))
        object.__setattr__(
            self,
            "positive_terms",
            tuple(str(term) for term in self.positive_terms),
        )
        object.__setattr__(
            self,
            "negative_terms",
            tuple(str(term) for term in self.negative_terms),
        )


@dataclass(frozen=True)
class EmbeddingVector:
    values: Sequence[float]
    model: str = "hashing-v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", tuple(float(value) for value in self.values))
        object.__setattr__(self, "model", _required_text(self.model, "model"))


@dataclass(frozen=True)
class NoveltyResult:
    score: float
    max_similarity: float
    prior_snippet_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "score", _clamp(self.score, 0.0, 100.0))
        object.__setattr__(self, "max_similarity", _clamp(self.max_similarity, 0.0, 1.0))
        if self.prior_snippet_id is not None:
            object.__setattr__(
                self,
                "prior_snippet_id",
                _required_text(self.prior_snippet_id, "prior_snippet_id"),
            )


@dataclass(frozen=True)
class TextSnippet:
    id: str
    ticker: str
    event_id: str
    snippet_hash: str
    section: str
    text: str
    source: str
    source_url: str | None
    source_quality: float
    event_type: str
    materiality: float
    ontology_hits: Sequence[Mapping[str, Any]]
    sentiment: float
    embedding: Sequence[float]
    source_ts: datetime
    available_at: datetime
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "event_id", _required_text(self.event_id, "event_id"))
        object.__setattr__(
            self,
            "snippet_hash",
            _required_text(self.snippet_hash, "snippet_hash"),
        )
        object.__setattr__(self, "section", _required_text(self.section, "section"))
        object.__setattr__(self, "text", _required_text(self.text, "text"))
        object.__setattr__(self, "source", _required_text(self.source, "source"))
        if self.source_url is not None:
            object.__setattr__(self, "source_url", str(self.source_url).strip() or None)
        object.__setattr__(self, "source_quality", _clamp(self.source_quality, 0.0, 1.0))
        object.__setattr__(self, "event_type", _required_text(self.event_type, "event_type"))
        object.__setattr__(self, "materiality", _clamp(self.materiality, 0.0, 1.0))
        object.__setattr__(self, "ontology_hits", freeze_json_value(self.ontology_hits))
        object.__setattr__(self, "sentiment", _clamp(self.sentiment, -1.0, 1.0))
        object.__setattr__(
            self,
            "embedding",
            tuple(float(value) for value in self.embedding),
        )
        object.__setattr__(self, "source_ts", _require_aware_utc(self.source_ts, "source_ts"))
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        _reject_available_before_source(self.source_ts, self.available_at)
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


@dataclass(frozen=True)
class TextFeature:
    id: str
    ticker: str
    as_of: datetime
    feature_version: str
    local_narrative_score: float
    novelty_score: float
    sentiment_score: float
    source_quality_score: float
    theme_match_score: float
    conflict_penalty: float
    selected_snippet_ids: Sequence[str]
    theme_hits: Any
    source_ts: datetime
    available_at: datetime
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _require_aware_utc(self.as_of, "as_of"))
        object.__setattr__(
            self,
            "feature_version",
            _required_text(self.feature_version, "feature_version"),
        )
        object.__setattr__(
            self,
            "local_narrative_score",
            _clamp(self.local_narrative_score, 0.0, 100.0),
        )
        object.__setattr__(self, "novelty_score", _clamp(self.novelty_score, 0.0, 100.0))
        object.__setattr__(
            self,
            "sentiment_score",
            _clamp(self.sentiment_score, -100.0, 100.0),
        )
        object.__setattr__(
            self,
            "source_quality_score",
            _clamp(self.source_quality_score, 0.0, 100.0),
        )
        object.__setattr__(
            self,
            "theme_match_score",
            _clamp(self.theme_match_score, 0.0, 100.0),
        )
        object.__setattr__(
            self,
            "conflict_penalty",
            _clamp(self.conflict_penalty, 0.0, 100.0),
        )
        object.__setattr__(
            self,
            "selected_snippet_ids",
            _text_tuple(self.selected_snippet_ids, "selected_snippet_ids"),
        )
        object.__setattr__(self, "theme_hits", freeze_json_value(self.theme_hits))
        object.__setattr__(self, "source_ts", _require_aware_utc(self.source_ts, "source_ts"))
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        _reject_available_before_source(self.source_ts, self.available_at)
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


def _required_text(value: str, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _required_text_tuple(values: Sequence[str], field_name: str) -> tuple[str, ...]:
    if isinstance(values, str):
        values = (values,)
    texts = tuple(_required_text(value, field_name) for value in values)
    if not texts:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return texts


def _text_tuple(values: Sequence[str], field_name: str) -> tuple[str, ...]:
    if isinstance(values, str):
        values = (values,)
    return tuple(_required_text(value, field_name) for value in values)


def _require_aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _reject_available_before_source(source_ts: datetime, available_at: datetime) -> None:
    if available_at < source_ts:
        msg = "available_at must be greater than or equal to source_ts"
        raise ValueError(msg)


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(upper, max(lower, float(value)))
