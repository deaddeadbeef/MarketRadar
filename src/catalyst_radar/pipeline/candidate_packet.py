from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, is_dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal

from catalyst_radar.core.immutability import freeze_mapping, thaw_json_value
from catalyst_radar.core.models import ActionState

CANDIDATE_PACKET_SCHEMA_VERSION = "candidate-packet-v1"

EvidencePolarity = Literal["supporting", "disconfirming", "neutral"]
_RecordInput = Mapping[str, Any] | object
_RecordIterable = Iterable[_RecordInput] | _RecordInput | None

_MANUAL_PACKET_STATES = {
    ActionState.BLOCKED,
    ActionState.WARNING,
    ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
    ActionState.THESIS_WEAKENING,
    ActionState.EXIT_INVALIDATE_REVIEW,
}


@dataclass(frozen=True)
class EvidenceItem:
    kind: str
    title: str
    summary: str
    polarity: EvidencePolarity
    strength: float
    source_id: str | None = None
    source_url: str | None = None
    computed_feature_id: str | None = None
    source_quality: float | None = None
    source_ts: datetime | None = None
    available_at: datetime | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", _required_text(self.kind, "kind"))
        object.__setattr__(self, "title", _required_text(self.title, "title"))
        object.__setattr__(self, "summary", _required_text(self.summary, "summary"))
        object.__setattr__(self, "polarity", _coerce_polarity(self.polarity))
        object.__setattr__(self, "strength", _finite_score(self.strength, "strength"))
        if self.source_id is not None:
            object.__setattr__(self, "source_id", _optional_text(self.source_id))
        if self.source_url is not None:
            object.__setattr__(self, "source_url", _optional_text(self.source_url))
        if self.computed_feature_id is not None:
            object.__setattr__(
                self,
                "computed_feature_id",
                _optional_text(self.computed_feature_id),
            )
        if not (self.source_id or self.source_url or self.computed_feature_id):
            msg = "evidence requires source_id, source_url, or computed_feature_id"
            raise ValueError(msg)
        if self.source_quality is not None:
            object.__setattr__(
                self,
                "source_quality",
                _finite_score(self.source_quality, "source_quality"),
            )
        if self.source_ts is not None:
            object.__setattr__(
                self,
                "source_ts",
                _require_aware_utc(self.source_ts, "source_ts"),
            )
        if self.available_at is not None:
            object.__setattr__(
                self,
                "available_at",
                _require_aware_utc(self.available_at, "available_at"),
            )
        if (
            self.source_ts is not None
            and self.available_at is not None
            and self.available_at < self.source_ts
        ):
            msg = "available_at must be greater than or equal to source_ts"
            raise ValueError(msg)
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> EvidenceItem:
        return cls(
            kind=str(value.get("kind", "")),
            title=str(value.get("title", "")),
            summary=str(value.get("summary", "")),
            polarity=_coerce_polarity(value.get("polarity", "neutral")),
            strength=_float_value(value.get("strength"), default=0.0),
            source_id=_maybe_text(value.get("source_id")),
            source_url=_maybe_text(value.get("source_url")),
            computed_feature_id=_maybe_text(value.get("computed_feature_id")),
            source_quality=_maybe_float(value.get("source_quality")),
            source_ts=_optional_aware_utc(value.get("source_ts"), "source_ts"),
            available_at=_optional_aware_utc(value.get("available_at"), "available_at"),
            payload=_mapping_value(value.get("payload")),
        )


@dataclass(frozen=True)
class CandidatePacket:
    id: str
    ticker: str
    as_of: datetime
    candidate_state_id: str | None
    state: ActionState
    final_score: float
    supporting_evidence: Sequence[EvidenceItem]
    disconfirming_evidence: Sequence[EvidenceItem]
    conflicts: Sequence[Mapping[str, Any]]
    hard_blocks: Sequence[str]
    payload: Mapping[str, Any]
    source_ts: datetime
    available_at: datetime
    schema_version: str = CANDIDATE_PACKET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _require_aware_utc(self.as_of, "as_of"))
        if self.candidate_state_id is not None:
            object.__setattr__(
                self,
                "candidate_state_id",
                _optional_text(self.candidate_state_id),
            )
        object.__setattr__(self, "state", _coerce_action_state(self.state))
        object.__setattr__(
            self,
            "final_score",
            _finite_float(self.final_score, "final_score"),
        )
        object.__setattr__(
            self,
            "supporting_evidence",
            tuple(_coerce_evidence_items(self.supporting_evidence)),
        )
        object.__setattr__(
            self,
            "disconfirming_evidence",
            tuple(_coerce_evidence_items(self.disconfirming_evidence)),
        )
        object.__setattr__(
            self,
            "conflicts",
            tuple(
                freeze_mapping(_mapping_value(conflict), "conflict")
                for conflict in self.conflicts
            ),
        )
        object.__setattr__(
            self,
            "hard_blocks",
            tuple(_required_text(block, "hard_block") for block in self.hard_blocks),
        )
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))
        object.__setattr__(
            self,
            "source_ts",
            _require_aware_utc(self.source_ts, "source_ts"),
        )
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        if self.available_at < self.source_ts:
            msg = "available_at must be greater than or equal to source_ts"
            raise ValueError(msg)
        if self.schema_version != CANDIDATE_PACKET_SCHEMA_VERSION:
            msg = f"schema_version must be {CANDIDATE_PACKET_SCHEMA_VERSION}"
            raise ValueError(msg)
        if self.state in _MANUAL_PACKET_STATES and not self.supporting_evidence:
            msg = f"{self.state.value} candidate packet requires supporting evidence"
            raise ValueError(msg)
        if self.state in _MANUAL_PACKET_STATES and not self.disconfirming_evidence:
            msg = f"{self.state.value} candidate packet requires disconfirming evidence"
            raise ValueError(msg)


@dataclass(frozen=True)
class _PacketContext:
    ticker: str
    as_of: datetime
    state: ActionState
    final_score: float
    candidate_state_id: str | None
    feature_version: str
    policy_version: str | None
    score_delta_5d: float | None
    signal_payload: Mapping[str, Any]
    candidate_payload: Mapping[str, Any]
    policy_payload: Mapping[str, Any]
    metadata: Mapping[str, Any]
    features_payload: Mapping[str, Any]
    portfolio_payload: Mapping[str, Any] | None
    portfolio_row: Mapping[str, Any] | None
    events: tuple[Mapping[str, Any], ...]
    snippets: tuple[Mapping[str, Any], ...]
    text_features: tuple[Mapping[str, Any], ...]
    option_features: tuple[Mapping[str, Any], ...]
    hard_blocks: tuple[str, ...]
    transition_reasons: tuple[str, ...]
    missing_trade_plan: tuple[str, ...]
    source_ts: datetime
    available_at: datetime


def build_candidate_packet(
    *,
    candidate_state: _RecordInput,
    signal_features_payload: _RecordInput,
    portfolio_row: _RecordInput | None = None,
    snippets: Iterable[_RecordInput] | None = None,
    events: Iterable[_RecordInput] | None = None,
    text_features: _RecordIterable = None,
    option_features: _RecordIterable = None,
    requested_available_at: datetime | None = None,
) -> CandidatePacket:
    """Build a deterministic candidate packet from persisted scan payloads.

    The builder copies persisted scores and metadata. It does not call scoring,
    policy, LLM, or execution systems.
    """

    requested_at = (
        _require_aware_utc(requested_available_at, "requested_available_at")
        if requested_available_at is not None
        else None
    )
    ctx = _build_context(
        candidate_state=candidate_state,
        signal_features_payload=signal_features_payload,
        portfolio_row=portfolio_row,
        snippets=snippets,
        events=events,
        text_features=text_features,
        option_features=option_features,
        requested_available_at=requested_at,
    )
    supporting = _ordered_evidence(_supporting_evidence(ctx))
    if ctx.state in _MANUAL_PACKET_STATES and not supporting:
        supporting = (
            _score_threshold_support(ctx),
        )
    disconfirming = _ordered_evidence(_disconfirming_evidence(ctx))
    if ctx.state in _MANUAL_PACKET_STATES and not disconfirming:
        disconfirming = (_evidence_gap(ctx),)

    payload = _packet_payload(ctx, supporting, disconfirming)
    packet = CandidatePacket(
        id=candidate_packet_id(
            ctx.ticker,
            ctx.as_of,
            ctx.state,
            available_at=ctx.available_at,
        ),
        ticker=ctx.ticker,
        as_of=ctx.as_of,
        candidate_state_id=ctx.candidate_state_id,
        state=ctx.state,
        final_score=ctx.final_score,
        supporting_evidence=supporting,
        disconfirming_evidence=disconfirming,
        conflicts=tuple(_json_safe(conflict) for conflict in _conflicts(ctx)),
        hard_blocks=ctx.hard_blocks,
        payload=payload,
        source_ts=ctx.source_ts,
        available_at=ctx.available_at,
    )
    return packet


def candidate_packet_id(
    ticker: str,
    as_of: datetime,
    state: ActionState | str,
    *,
    available_at: datetime,
    schema_version: str = CANDIDATE_PACKET_SCHEMA_VERSION,
) -> str:
    normalized_as_of = _require_aware_utc(as_of, "as_of").isoformat()
    normalized_available_at = _require_aware_utc(
        available_at,
        "available_at",
    ).isoformat()
    normalized_state = _coerce_action_state(state).value
    normalized_ticker = _required_text(ticker, "ticker").upper()
    return (
        f"{schema_version}:{normalized_ticker}:{normalized_as_of}:"
        f"{normalized_state}:{normalized_available_at}"
    )


def evidence_item_payload(item: EvidenceItem) -> dict[str, Any]:
    return {
        "kind": item.kind,
        "title": item.title,
        "summary": item.summary,
        "polarity": item.polarity,
        "strength": item.strength,
        "source_id": item.source_id,
        "source_url": item.source_url,
        "computed_feature_id": item.computed_feature_id,
        "source_quality": item.source_quality,
        "source_ts": item.source_ts.isoformat() if item.source_ts else None,
        "available_at": item.available_at.isoformat() if item.available_at else None,
        "payload": _json_safe(item.payload),
    }


def packet_payload(packet: CandidatePacket) -> dict[str, Any]:
    return _json_safe(packet.payload)


def canonical_packet_json(packet: CandidatePacket | Mapping[str, Any]) -> str:
    payload = packet_payload(packet) if isinstance(packet, CandidatePacket) else _json_safe(packet)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _build_context(
    *,
    candidate_state: Mapping[str, Any] | object,
    signal_features_payload: Mapping[str, Any] | object,
    portfolio_row: Mapping[str, Any] | object | None,
    snippets: Iterable[Mapping[str, Any] | object] | None,
    events: Iterable[Mapping[str, Any] | object] | None,
    text_features: Iterable[Mapping[str, Any] | object] | Mapping[str, Any] | object | None,
    option_features: Iterable[Mapping[str, Any] | object] | Mapping[str, Any] | object | None,
    requested_available_at: datetime | None,
) -> _PacketContext:
    state_row = _record_mapping(candidate_state)
    signal_row = _record_mapping(signal_features_payload)
    signal_payload = _signal_payload(signal_row)
    if not isinstance(signal_payload.get("candidate"), Mapping):
        msg = "signal_features payload with candidate data is required"
        raise ValueError(msg)
    candidate_payload = _mapping_value(signal_payload.get("candidate", signal_payload))
    policy_payload = _mapping_value(signal_payload.get("policy", {}))
    metadata = _mapping_value(candidate_payload.get("metadata", signal_payload.get("metadata", {})))
    features_payload = _mapping_value(candidate_payload.get("features", {}))

    ticker = _first_text(
        state_row.get("ticker"),
        candidate_payload.get("ticker"),
        features_payload.get("ticker"),
        field_name="ticker",
    ).upper()
    as_of = _first_datetime(
        state_row.get("as_of"),
        candidate_payload.get("as_of"),
        features_payload.get("as_of"),
        field_name="as_of",
    )
    state = _coerce_action_state(
        _first_present(
            state_row.get("state"),
            policy_payload.get("state"),
            candidate_payload.get("state"),
        )
    )
    final_score = _float_value(
        _first_present(
            state_row.get("final_score"),
            candidate_payload.get("final_score"),
            signal_row.get("final_score"),
        ),
        default=0.0,
    )
    candidate_state_id = _maybe_text(state_row.get("id"))
    feature_version = _first_text(
        state_row.get("feature_version"),
        signal_row.get("feature_version"),
        features_payload.get("feature_version"),
        metadata.get("feature_version"),
        "unknown",
        field_name="feature_version",
    )
    policy_version = _maybe_text(
        _first_present(state_row.get("policy_version"), policy_payload.get("policy_version"))
    )
    score_delta_5d = _maybe_float(state_row.get("score_delta_5d"))

    base_source_ts = _first_datetime(
        metadata.get("source_ts"),
        signal_payload.get("source_ts"),
        candidate_payload.get("source_ts"),
        as_of,
        field_name="source_ts",
    )
    base_available_at = _first_datetime(
        metadata.get("available_at"),
        signal_payload.get("available_at"),
        candidate_payload.get("available_at"),
        state_row.get("created_at"),
        requested_available_at,
        base_source_ts,
        field_name="available_at",
    )
    _reject_future_required_input(base_available_at, requested_available_at)

    selected_events = _point_in_time_records(
        (*_records(events), *_metadata_events(metadata)),
        requested_available_at,
        fallback_available_at=base_available_at,
    )
    selected_snippets = _point_in_time_records(
        _records(snippets),
        requested_available_at,
        fallback_available_at=base_available_at,
    )
    selected_text_features = _point_in_time_records(
        _records(text_features),
        requested_available_at,
        fallback_available_at=base_available_at,
    )
    selected_option_features = _point_in_time_records(
        _records(option_features),
        requested_available_at,
        fallback_available_at=base_available_at,
    )

    portfolio_mapping = _record_mapping(portfolio_row) if portfolio_row is not None else None
    portfolio_payload = _portfolio_payload(metadata, portfolio_mapping)
    portfolio_records = (portfolio_mapping,) if portfolio_mapping else ()

    timestamps = [(base_source_ts, base_available_at)]
    for records in (
        selected_events,
        selected_snippets,
        selected_text_features,
        selected_option_features,
        portfolio_records,
    ):
        timestamps.extend(
            _record_timestamps(record, base_source_ts, base_available_at)
            for record in records
        )
    source_ts = max(item[0] for item in timestamps)
    available_at = max(item[1] for item in timestamps)
    _reject_future_required_input(available_at, requested_available_at)

    state_blocks = _text_tuple(state_row.get("hard_blocks", ()))
    policy_blocks = _text_tuple(policy_payload.get("hard_blocks", ()))
    portfolio_blocks = _text_tuple((portfolio_payload or {}).get("hard_blocks", ()))
    hard_blocks = tuple(dict.fromkeys((*state_blocks, *policy_blocks, *portfolio_blocks)))
    transition_reasons = _text_tuple(
        _first_present(state_row.get("transition_reasons"), policy_payload.get("reasons"), ())
    )
    missing_trade_plan = tuple(
        dict.fromkeys(
            (
                *_text_tuple(policy_payload.get("missing_trade_plan", ())),
                *_derived_missing_trade_plan(candidate_payload),
            )
        )
    )

    return _PacketContext(
        ticker=ticker,
        as_of=as_of,
        state=state,
        final_score=final_score,
        candidate_state_id=candidate_state_id,
        feature_version=feature_version,
        policy_version=policy_version,
        score_delta_5d=score_delta_5d,
        signal_payload=freeze_mapping(signal_payload, "signal_payload"),
        candidate_payload=freeze_mapping(candidate_payload, "candidate_payload"),
        policy_payload=freeze_mapping(policy_payload, "policy_payload"),
        metadata=freeze_mapping(metadata, "metadata"),
        features_payload=freeze_mapping(features_payload, "features_payload"),
        portfolio_payload=freeze_mapping(portfolio_payload, "portfolio_payload")
        if portfolio_payload
        else None,
        portfolio_row=freeze_mapping(portfolio_mapping, "portfolio_row")
        if portfolio_mapping
        else None,
        events=tuple(freeze_mapping(record, "event") for record in selected_events),
        snippets=tuple(freeze_mapping(record, "snippet") for record in selected_snippets),
        text_features=tuple(
            freeze_mapping(record, "text_feature") for record in selected_text_features
        ),
        option_features=tuple(
            freeze_mapping(record, "option_feature") for record in selected_option_features
        ),
        hard_blocks=hard_blocks,
        transition_reasons=transition_reasons,
        missing_trade_plan=missing_trade_plan,
        source_ts=source_ts,
        available_at=available_at,
    )


def _supporting_evidence(ctx: _PacketContext) -> list[EvidenceItem]:
    evidence: list[EvidenceItem] = []
    top_event = _top_event(ctx.events)
    if top_event is not None:
        evidence.append(_event_support(ctx, top_event))

    snippet = _top_supporting_snippet(ctx.snippets)
    if snippet is not None:
        evidence.append(_snippet_support(snippet))

    theme_hits = _sequence_of_mappings(ctx.metadata.get("theme_hits", ()))
    if theme_hits:
        evidence.append(
            EvidenceItem(
                kind="theme_hit",
                title="Persisted text theme hits",
                summary=_theme_hit_summary(theme_hits),
                polarity="supporting",
                strength=_score_to_strength(ctx.metadata.get("theme_match_score"), default=0.55),
                computed_feature_id=_signal_feature_id(ctx, "local_narrative_score"),
                source_quality=_score_to_strength(
                    ctx.metadata.get("source_quality_score"),
                    default=None,
                ),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"theme_hits": _json_safe(theme_hits[:5])},
            )
        )

    pillar_scores = _mapping_value(ctx.metadata.get("pillar_scores", {}))
    strong_pillars = {
        str(name): _float_value(score, default=0.0)
        for name, score in pillar_scores.items()
        if _float_value(score, default=0.0) >= 70.0
    }
    if strong_pillars:
        evidence.append(
            EvidenceItem(
                kind="computed_feature",
                title="Strong persisted market pillars",
                summary=_pillar_summary(strong_pillars),
                polarity="supporting",
                strength=min(max(strong_pillars.values()) / 100.0, 1.0),
                computed_feature_id=_signal_feature_id(ctx, "pillar_scores"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"pillar_scores": strong_pillars},
            )
        )

    local_score = _maybe_float(ctx.metadata.get("local_narrative_score"))
    if local_score is not None and local_score >= 60.0:
        evidence.append(
            EvidenceItem(
                kind="computed_feature",
                title="Positive local narrative score",
                summary=f"Persisted local narrative score is {local_score:.2f}.",
                polarity="supporting",
                strength=_score_to_strength(local_score),
                computed_feature_id=_signal_feature_id(ctx, "local_narrative_score"),
                source_quality=_score_to_strength(
                    ctx.metadata.get("source_quality_score"),
                    default=None,
                ),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={
                    "local_narrative_score": local_score,
                    "selected_snippet_ids": _json_safe(
                        _text_tuple(ctx.metadata.get("selected_snippet_ids", ()))
                    ),
                },
            )
        )

    options_flow = _maybe_float(ctx.metadata.get("options_flow_score"))
    if options_flow is not None and options_flow >= 60.0:
        evidence.append(
            EvidenceItem(
                kind="computed_feature",
                title="Positive aggregate options flow",
                summary=f"Persisted aggregate options flow score is {options_flow:.2f}.",
                polarity="supporting",
                strength=_score_to_strength(options_flow),
                computed_feature_id=_signal_feature_id(ctx, "options_flow_score"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={
                    "options_flow_score": options_flow,
                    "call_put_ratio": _json_safe(ctx.metadata.get("call_put_ratio")),
                    "iv_percentile": _json_safe(ctx.metadata.get("iv_percentile")),
                },
            )
        )

    sector_theme_support = _sector_theme_support(ctx)
    if sector_theme_support:
        evidence.append(sector_theme_support)

    if not _derived_missing_trade_plan(ctx.candidate_payload):
        evidence.append(
            EvidenceItem(
                kind="setup_plan",
                title="Complete persisted setup plan",
                summary=_trade_plan_summary(ctx.candidate_payload),
                polarity="supporting",
                strength=_reward_risk_strength(ctx.candidate_payload.get("reward_risk")),
                computed_feature_id=_signal_feature_id(ctx, "setup_plan"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload=_trade_plan_payload(ctx),
            )
        )

    if ctx.portfolio_payload is not None and not _text_tuple(
        ctx.portfolio_payload.get("hard_blocks", ())
    ):
        evidence.append(
            EvidenceItem(
                kind="portfolio_impact",
                title="Portfolio impact within stored limits",
                summary=_portfolio_summary(ctx.portfolio_payload),
                polarity="supporting",
                strength=0.7,
                computed_feature_id=_portfolio_feature_id(ctx),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload=_json_safe(ctx.portfolio_payload),
            )
        )

    return evidence


def _disconfirming_evidence(ctx: _PacketContext) -> list[EvidenceItem]:
    evidence: list[EvidenceItem] = []

    if ctx.hard_blocks:
        evidence.append(
            EvidenceItem(
                kind="hard_block",
                title="Hard policy block",
                summary=f"Persisted hard blocks: {', '.join(ctx.hard_blocks)}.",
                polarity="disconfirming",
                strength=1.0,
                computed_feature_id=_candidate_feature_id(ctx, "hard_blocks"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"hard_blocks": list(ctx.hard_blocks)},
            )
        )

    if ctx.missing_trade_plan:
        evidence.append(
            EvidenceItem(
                kind="missing_trade_plan",
                title="Trade plan is incomplete",
                summary=f"Missing or weak fields: {', '.join(ctx.missing_trade_plan)}.",
                polarity="disconfirming",
                strength=0.85,
                computed_feature_id=_signal_feature_id(ctx, "missing_trade_plan"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"missing_trade_plan": list(ctx.missing_trade_plan)},
            )
        )

    if bool(ctx.candidate_payload.get("data_stale", False)):
        evidence.append(
            EvidenceItem(
                kind="data_stale",
                title="Candidate data is stale",
                summary="Persisted candidate payload marks source data as stale.",
                polarity="disconfirming",
                strength=0.9,
                computed_feature_id=_candidate_feature_id(ctx, "transition_reasons"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"data_stale": True},
            )
        )

    if _conflicts(ctx):
        evidence.append(
            EvidenceItem(
                kind="event_conflict",
                title="Unresolved event conflict",
                summary=f"Persisted scan has {len(_conflicts(ctx))} unresolved event conflict(s).",
                polarity="disconfirming",
                strength=0.8,
                computed_feature_id=_candidate_feature_id(ctx, "transition_reasons"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"conflicts": _json_safe(_conflicts(ctx))},
            )
        )

    narrative_score = _maybe_float(ctx.metadata.get("local_narrative_score"))
    if narrative_score is None or narrative_score < 50.0:
        evidence.append(
            EvidenceItem(
                kind="weak_local_narrative",
                title="Weak or missing local narrative",
                summary=_weak_narrative_summary(narrative_score),
                polarity="disconfirming",
                strength=(
                    0.65
                    if narrative_score is None
                    else max(0.45, (50.0 - narrative_score) / 50.0)
                ),
                computed_feature_id=_signal_feature_id(ctx, "local_narrative_score"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"local_narrative_score": narrative_score},
            )
        )

    risk_penalty = _maybe_float(ctx.candidate_payload.get("risk_penalty"))
    if risk_penalty is not None and risk_penalty >= 15.0:
        evidence.append(
            EvidenceItem(
                kind="risk_penalty",
                title="Elevated risk penalty",
                summary=f"Persisted risk penalty is {risk_penalty:.2f}.",
                polarity="disconfirming",
                strength=min(risk_penalty / 30.0, 1.0),
                computed_feature_id=_signal_feature_id(ctx, "risk_penalty"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"risk_penalty": risk_penalty},
            )
        )

    options_risk = _maybe_float(ctx.metadata.get("options_risk_score"))
    if options_risk is not None and options_risk >= 60.0:
        evidence.append(
            EvidenceItem(
                kind="options_risk",
                title="Elevated options risk score",
                summary=f"Persisted aggregate options risk score is {options_risk:.2f}.",
                polarity="disconfirming",
                strength=_score_to_strength(options_risk),
                computed_feature_id=_signal_feature_id(ctx, "risk_penalty"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"options_risk_score": options_risk},
            )
        )

    if ctx.metadata.get("chase_block") is True:
        evidence.append(
            EvidenceItem(
                kind="chase_risk",
                title="Chase risk block is set",
                summary="Persisted setup metadata marks this candidate as extended chase risk.",
                polarity="disconfirming",
                strength=0.9,
                computed_feature_id=_candidate_feature_id(ctx, "transition_reasons"),
                source_ts=ctx.source_ts,
                available_at=ctx.available_at,
                payload={"chase_block": True},
            )
        )

    negative_snippet = _top_negative_snippet(ctx.snippets)
    if negative_snippet is not None:
        evidence.append(_snippet_disconfirming(negative_snippet))

    return evidence


def _packet_payload(
    ctx: _PacketContext,
    supporting: Sequence[EvidenceItem],
    disconfirming: Sequence[EvidenceItem],
) -> dict[str, Any]:
    return {
        "identity": {
            "ticker": ctx.ticker,
            "as_of": ctx.as_of.isoformat(),
            "state": ctx.state.value,
            "candidate_state_id": ctx.candidate_state_id,
            "schema_version": CANDIDATE_PACKET_SCHEMA_VERSION,
        },
        "scores": {
            "final": ctx.final_score,
            "pillars": _json_safe(ctx.metadata.get("pillar_scores", {})),
            "risk_penalty": _json_safe(ctx.candidate_payload.get("risk_penalty")),
            "portfolio_penalty": _json_safe(ctx.candidate_payload.get("portfolio_penalty")),
            "score_delta_5d": ctx.score_delta_5d,
            "stored_score_components": {
                "event_bonus": _json_safe(ctx.metadata.get("event_bonus")),
                "local_narrative_bonus": _json_safe(ctx.metadata.get("local_narrative_bonus")),
                "options_bonus": _json_safe(ctx.metadata.get("options_bonus")),
                "sector_theme_bonus": _json_safe(ctx.metadata.get("sector_theme_bonus")),
                "options_risk_penalty": _json_safe(ctx.metadata.get("options_risk_penalty")),
            },
        },
        "trade_plan": _trade_plan_payload(ctx),
        "portfolio_impact": _json_safe(ctx.portfolio_payload),
        "supporting_evidence": [evidence_item_payload(item) for item in supporting],
        "disconfirming_evidence": [evidence_item_payload(item) for item in disconfirming],
        "conflicts": _json_safe(_conflicts(ctx)),
        "hard_blocks": list(ctx.hard_blocks),
        "features": {
            "feature_version": ctx.feature_version,
            "market": _json_safe(ctx.features_payload),
            "text": {
                "text_feature_version": _json_safe(ctx.metadata.get("text_feature_version")),
                "local_narrative_score": _json_safe(ctx.metadata.get("local_narrative_score")),
                "selected_snippet_ids": _json_safe(
                    _text_tuple(ctx.metadata.get("selected_snippet_ids", ()))
                ),
            },
            "options": {
                "options_feature_version": _json_safe(ctx.metadata.get("options_feature_version")),
                "options_flow_score": _json_safe(ctx.metadata.get("options_flow_score")),
                "options_risk_score": _json_safe(ctx.metadata.get("options_risk_score")),
            },
            "sector_theme_peer": {
                "candidate_theme": _json_safe(ctx.metadata.get("candidate_theme")),
                "sector_rotation_score": _json_safe(ctx.metadata.get("sector_rotation_score")),
                "theme_velocity_score": _json_safe(ctx.metadata.get("theme_velocity_score")),
                "peer_readthrough_score": _json_safe(ctx.metadata.get("peer_readthrough_score")),
                "sector_theme_bonus": _json_safe(ctx.metadata.get("sector_theme_bonus")),
            },
        },
        "policy": {
            "policy_version": ctx.policy_version,
            "hard_blocks": list(ctx.hard_blocks),
            "transition_reasons": list(ctx.transition_reasons),
            "missing_trade_plan": list(ctx.missing_trade_plan),
        },
        "escalation": _escalation_payload(ctx),
        "audit": {
            "source_ts": ctx.source_ts.isoformat(),
            "available_at": ctx.available_at.isoformat(),
            "feature_version": ctx.feature_version,
            "policy_version": ctx.policy_version,
            "schema_version": CANDIDATE_PACKET_SCHEMA_VERSION,
            "builder": "deterministic_candidate_packet_builder",
            "score_recomputed": False,
            "llm_calls": False,
        },
    }


def _escalation_payload(ctx: _PacketContext) -> dict[str, Any]:
    packet_required = ctx.state in _MANUAL_PACKET_STATES
    decision_card_required = ctx.state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW
    llm_review_candidate = ctx.state in _MANUAL_PACKET_STATES
    reasons = []
    if packet_required:
        reasons.append("packet_required")
    if decision_card_required:
        reasons.append("decision_card_required")
    if llm_review_candidate:
        reasons.append("llm_review_candidate")
    return {
        "packet_required": packet_required,
        "decision_card_required": decision_card_required,
        "llm_review_candidate": llm_review_candidate,
        "llm_review_status": "not_configured_phase_8",
        "no_trade_execution": True,
        "reasons": reasons,
    }


def _event_support(ctx: _PacketContext, event: Mapping[str, Any]) -> EvidenceItem:
    title = _first_text(event.get("title"), "Material event", field_name="event_title")
    source = _maybe_text(event.get("source"))
    event_type = _maybe_text(event.get("event_type")) or "event"
    materiality = _float_value(event.get("materiality"), default=0.5)
    source_quality = _float_value(event.get("source_quality"), default=0.5)
    return EvidenceItem(
        kind="material_event",
        title=title,
        summary=f"Top persisted {event_type} from {source or 'source'} supports review context.",
        polarity="supporting",
        strength=max(0.01, min(materiality * source_quality, 1.0)),
        source_id=_maybe_text(_first_present(event.get("source_id"), event.get("id"))),
        source_url=_maybe_text(event.get("source_url")),
        source_quality=source_quality,
        source_ts=_optional_aware_utc(event.get("source_ts"), "source_ts") or ctx.source_ts,
        available_at=_optional_aware_utc(event.get("available_at"), "available_at")
        or ctx.available_at,
        payload=_json_safe(event),
    )


def _snippet_support(snippet: Mapping[str, Any]) -> EvidenceItem:
    text = _first_text(snippet.get("text"), "Selected text snippet", field_name="snippet_text")
    source_quality = _float_value(snippet.get("source_quality"), default=0.5)
    materiality = _float_value(snippet.get("materiality"), default=0.5)
    return EvidenceItem(
        kind="text_snippet",
        title=f"Selected {snippet.get('section', 'text')} snippet",
        summary=_truncate(text),
        polarity="supporting",
        strength=max(0.01, min(source_quality * materiality, 1.0)),
        source_id=_maybe_text(_first_present(snippet.get("source_id"), snippet.get("id"))),
        source_url=_maybe_text(snippet.get("source_url")),
        source_quality=source_quality,
        source_ts=_optional_aware_utc(snippet.get("source_ts"), "source_ts"),
        available_at=_optional_aware_utc(snippet.get("available_at"), "available_at"),
        payload=_json_safe(snippet),
    )


def _snippet_disconfirming(snippet: Mapping[str, Any]) -> EvidenceItem:
    text = _first_text(snippet.get("text"), "Selected text snippet", field_name="snippet_text")
    sentiment = abs(_float_value(snippet.get("sentiment"), default=-0.3))
    return EvidenceItem(
        kind="negative_text_snippet",
        title=f"Negative {snippet.get('section', 'text')} snippet",
        summary=_truncate(text),
        polarity="disconfirming",
        strength=min(max(sentiment, 0.3), 1.0),
        source_id=_maybe_text(_first_present(snippet.get("source_id"), snippet.get("id"))),
        source_url=_maybe_text(snippet.get("source_url")),
        source_quality=_maybe_float(snippet.get("source_quality")),
        source_ts=_optional_aware_utc(snippet.get("source_ts"), "source_ts"),
        available_at=_optional_aware_utc(snippet.get("available_at"), "available_at"),
        payload=_json_safe(snippet),
    )


def _sector_theme_support(ctx: _PacketContext) -> EvidenceItem | None:
    sector = _maybe_float(ctx.metadata.get("sector_rotation_score"))
    theme = _maybe_float(ctx.metadata.get("theme_velocity_score"))
    peer = _maybe_float(ctx.metadata.get("peer_readthrough_score"))
    supporting_scores = {
        "sector_rotation_score": sector,
        "theme_velocity_score": theme,
        "peer_readthrough_score": peer,
    }
    positive = {
        name: score
        for name, score in supporting_scores.items()
        if score is not None and score > (55.0 if name == "sector_rotation_score" else 0.0)
    }
    if not positive:
        return None
    return EvidenceItem(
        kind="computed_feature",
        title="Sector, theme, or peer support",
        summary=_sector_theme_summary(positive),
        polarity="supporting",
        strength=min(max(positive.values()) / 100.0, 1.0),
        computed_feature_id=_signal_feature_id(ctx, "sector_theme_bonus"),
        source_ts=ctx.source_ts,
        available_at=ctx.available_at,
        payload=_json_safe(positive),
    )


def _score_threshold_support(ctx: _PacketContext) -> EvidenceItem:
    return EvidenceItem(
        kind="computed_feature",
        title="Persisted score crossed manual review threshold",
        summary=f"Persisted final score is {ctx.final_score:.2f} with state {ctx.state.value}.",
        polarity="supporting",
        strength=min(max(ctx.final_score / 100.0, 0.01), 1.0),
        computed_feature_id=_signal_feature_id(ctx, "pillar_scores"),
        source_ts=ctx.source_ts,
        available_at=ctx.available_at,
        payload={"final_score": ctx.final_score, "state": ctx.state.value},
    )


def _evidence_gap(ctx: _PacketContext) -> EvidenceItem:
    return EvidenceItem(
        kind="evidence_gap",
        title="No stronger deterministic disconfirming evidence found",
        summary="Builder added an explicit gap so manual review sees missing bear-case coverage.",
        polarity="disconfirming",
        strength=0.25,
        computed_feature_id=_candidate_feature_id(ctx, "evidence_gap"),
        source_ts=ctx.source_ts,
        available_at=ctx.available_at,
        payload={"reason": "required_disconfirming_evidence_gap"},
    )


def _trade_plan_payload(ctx: _PacketContext) -> dict[str, Any]:
    return {
        "setup_type": _json_safe(ctx.metadata.get("setup_type")),
        "entry_zone": _json_safe(ctx.candidate_payload.get("entry_zone")),
        "invalidation_price": _json_safe(ctx.candidate_payload.get("invalidation_price")),
        "target_price": _json_safe(ctx.metadata.get("target_price")),
        "reward_risk": _json_safe(ctx.candidate_payload.get("reward_risk")),
        "missing_fields": list(ctx.missing_trade_plan),
        "position_size": _json_safe(ctx.metadata.get("position_size", {})),
    }


def _trade_plan_summary(candidate_payload: Mapping[str, Any]) -> str:
    entry_zone = candidate_payload.get("entry_zone")
    invalidation = candidate_payload.get("invalidation_price")
    reward_risk = _maybe_float(candidate_payload.get("reward_risk"))
    rr_text = f"{reward_risk:.2f}" if reward_risk is not None else "unknown"
    return (
        f"Entry zone {entry_zone}, invalidation {invalidation}, "
        f"stored reward/risk {rr_text}."
    )


def _portfolio_summary(portfolio_payload: Mapping[str, Any]) -> str:
    notional = _maybe_float(portfolio_payload.get("proposed_notional"))
    max_loss = _maybe_float(portfolio_payload.get("max_loss"))
    notional_text = f"{notional:.2f}" if notional is not None else "unknown"
    max_loss_text = f"{max_loss:.2f}" if max_loss is not None else "unknown"
    return f"Stored proposed notional {notional_text} with max loss {max_loss_text}."


def _weak_narrative_summary(score: float | None) -> str:
    if score is None:
        return "No persisted local narrative score is available."
    return f"Persisted local narrative score is weak at {score:.2f}."


def _pillar_summary(strong_pillars: Mapping[str, float]) -> str:
    parts = [f"{name}={score:.2f}" for name, score in sorted(strong_pillars.items())]
    return "Strong persisted pillar scores: " + ", ".join(parts) + "."


def _theme_hit_summary(theme_hits: Sequence[Mapping[str, Any]]) -> str:
    themes = [
        _maybe_text(hit.get("theme_id")) or _maybe_text(hit.get("theme")) or "unknown"
        for hit in theme_hits[:3]
    ]
    return "Persisted text themes: " + ", ".join(themes) + "."


def _sector_theme_summary(scores: Mapping[str, float]) -> str:
    parts = [f"{name}={score:.2f}" for name, score in sorted(scores.items())]
    return "Persisted sector/theme/peer support: " + ", ".join(parts) + "."


def _reward_risk_strength(value: Any) -> float:
    reward_risk = _float_value(value, default=0.0)
    return min(max(reward_risk / 3.0, 0.5), 1.0)


def _score_to_strength(value: Any, *, default: float | None = 0.0) -> float | None:
    score = _maybe_float(value)
    if score is None:
        return default
    return min(max(score / 100.0, 0.0), 1.0)


def _signal_feature_id(ctx: _PacketContext, suffix: str) -> str:
    return f"signal_features:{ctx.ticker}:{ctx.as_of.isoformat()}:{ctx.feature_version}:{suffix}"


def _candidate_feature_id(ctx: _PacketContext, suffix: str) -> str:
    identity = ctx.candidate_state_id or f"{ctx.ticker}:{ctx.as_of.isoformat()}"
    return f"candidate_states:{identity}:{suffix}"


def _portfolio_feature_id(ctx: _PacketContext) -> str:
    setup_type = _maybe_text(ctx.metadata.get("setup_type")) or "unknown"
    return f"portfolio_impacts:{ctx.ticker}:{ctx.as_of.isoformat()}:{setup_type}"


def _conflicts(ctx: _PacketContext) -> tuple[Mapping[str, Any], ...]:
    conflicts = _sequence_of_mappings(ctx.metadata.get("event_conflicts", ()))
    if conflicts:
        return tuple(conflicts)
    if ctx.metadata.get("has_event_conflict") is True:
        return ({"kind": "event_conflict", "source": "scan_metadata"},)
    return ()


def _top_event(events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    if not events:
        return None
    return sorted(
        events,
        key=lambda event: (
            -_float_value(event.get("materiality"), default=0.0)
            * _float_value(event.get("source_quality"), default=0.0),
            str(_first_present(event.get("source_ts"), "")),
            str(_first_present(event.get("id"), event.get("source_id"), "")),
        ),
    )[0]


def _top_supporting_snippet(snippets: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    positive = [
        snippet
        for snippet in snippets
        if _float_value(snippet.get("sentiment"), default=0.0) >= 0.0
    ]
    if not positive:
        return None
    return sorted(
        positive,
        key=lambda snippet: (
            -_float_value(snippet.get("source_quality"), default=0.0)
            * _float_value(snippet.get("materiality"), default=0.0),
            str(snippet.get("id", "")),
        ),
    )[0]


def _top_negative_snippet(snippets: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    negative = [
        snippet
        for snippet in snippets
        if _float_value(snippet.get("sentiment"), default=0.0) <= -0.25
    ]
    if not negative:
        return None
    return sorted(
        negative,
        key=lambda snippet: (
            _float_value(snippet.get("sentiment"), default=0.0),
            str(snippet.get("id", "")),
        ),
    )[0]


def _ordered_evidence(items: Iterable[EvidenceItem]) -> tuple[EvidenceItem, ...]:
    deduped: dict[tuple[str, str, str], EvidenceItem] = {}
    for item in items:
        link = item.source_id or item.source_url or item.computed_feature_id or ""
        deduped.setdefault((item.kind, item.polarity, link), item)
    return tuple(
        sorted(
            deduped.values(),
            key=lambda item: (
                -item.strength,
                item.kind,
                item.title,
                item.source_id or item.source_url or item.computed_feature_id or "",
            ),
        )
    )


def _derived_missing_trade_plan(candidate_payload: Mapping[str, Any]) -> tuple[str, ...]:
    missing = []
    if candidate_payload.get("entry_zone") in (None, (), []):
        missing.append("entry_zone")
    invalidation = _maybe_float(candidate_payload.get("invalidation_price"))
    if invalidation is None or invalidation <= 0:
        missing.append("invalidation_price")
    reward_risk = _maybe_float(candidate_payload.get("reward_risk"))
    if reward_risk is None or reward_risk <= 0:
        missing.append("reward_risk")
    elif reward_risk < 2.0:
        missing.append("reward_risk_too_low")
    return tuple(missing)


def _signal_payload(signal_row: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = signal_row.get("payload")
    if isinstance(payload, Mapping):
        return _mapping_value(payload)
    if "candidate" in signal_row or "policy" in signal_row:
        return _mapping_value(signal_row)
    return {}


def _portfolio_payload(
    metadata: Mapping[str, Any],
    portfolio_row: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if portfolio_row:
        row_payload = portfolio_row.get("payload")
        if isinstance(row_payload, Mapping):
            payload_mapping = _mapping_value(row_payload)
            if isinstance(payload_mapping.get("portfolio_impact"), Mapping):
                return _mapping_value(payload_mapping["portfolio_impact"])
            return payload_mapping
        values = {
            key: portfolio_row.get(key)
            for key in (
                "proposed_notional",
                "max_loss",
                "single_name_before_pct",
                "single_name_after_pct",
                "sector_before_pct",
                "sector_after_pct",
                "theme_before_pct",
                "theme_after_pct",
                "correlated_before_pct",
                "correlated_after_pct",
                "portfolio_penalty",
                "hard_blocks",
            )
            if key in portfolio_row
        }
        if values:
            return _mapping_value(values)
    metadata_impact = metadata.get("portfolio_impact")
    if isinstance(metadata_impact, Mapping):
        return _mapping_value(metadata_impact)
    return None


def _metadata_events(metadata: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return tuple(_sequence_of_mappings(metadata.get("events", ())))


def _point_in_time_records(
    records: Iterable[Mapping[str, Any]],
    requested_available_at: datetime | None,
    *,
    fallback_available_at: datetime,
) -> tuple[Mapping[str, Any], ...]:
    selected = []
    for record in records:
        available_at = (
            _optional_aware_utc(record.get("available_at"), "available_at")
            or fallback_available_at
        )
        if requested_available_at is None or available_at <= requested_available_at:
            selected.append(record)
    return tuple(selected)


def _record_timestamps(
    record: Mapping[str, Any],
    fallback_source_ts: datetime,
    fallback_available_at: datetime,
) -> tuple[datetime, datetime]:
    source_ts = _optional_aware_utc(record.get("source_ts"), "source_ts") or fallback_source_ts
    available_at = (
        _optional_aware_utc(record.get("available_at"), "available_at")
        or fallback_available_at
    )
    return source_ts, available_at


def _reject_future_required_input(
    available_at: datetime,
    requested_available_at: datetime | None,
) -> None:
    if requested_available_at is not None and available_at > requested_available_at:
        msg = "required packet input is not available at requested_available_at"
        raise ValueError(msg)


def _records(
    value: Iterable[Mapping[str, Any] | object] | Mapping[str, Any] | object | None,
) -> tuple[Mapping[str, Any], ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping) or hasattr(value, "_mapping") or is_dataclass(value):
        return (_record_mapping(value),)
    if isinstance(value, str):
        return ()
    return tuple(_record_mapping(item) for item in value)


def _record_mapping(value: _RecordInput) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return _mapping_value(value)
    if hasattr(value, "_mapping"):
        return _mapping_value(value._mapping)
    if is_dataclass(value) and not isinstance(value, type):
        return _mapping_value(
            {
                field_name: getattr(value, field_name)
                for field_name in value.__dataclass_fields__
            }
        )
    keys = getattr(value, "keys", None)
    if callable(keys):
        return _mapping_value({key: value[key] for key in keys()})
    attrs = {
        name: getattr(value, name)
        for name in dir(value)
        if not name.startswith("_") and not callable(getattr(value, name))
    }
    return _mapping_value(attrs)


def _mapping_value(value: Any) -> Mapping[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return {}


def _sequence_of_mappings(value: Any) -> tuple[Mapping[str, Any], ...]:
    if isinstance(value, Mapping):
        return (_mapping_value(value),)
    if isinstance(value, str) or not isinstance(value, Iterable):
        return ()
    return tuple(_mapping_value(item) for item in value if isinstance(item, Mapping))


def _coerce_evidence_items(items: Sequence[EvidenceItem]) -> tuple[EvidenceItem, ...]:
    evidence = []
    for item in items:
        if isinstance(item, EvidenceItem):
            evidence.append(item)
        elif isinstance(item, Mapping):
            evidence.append(EvidenceItem.from_mapping(item))
        else:
            msg = "evidence items must be EvidenceItem or mapping instances"
            raise TypeError(msg)
    return tuple(evidence)


def _coerce_action_state(value: Any) -> ActionState:
    if isinstance(value, ActionState):
        return value
    text = _required_text(str(value), "state")
    try:
        return ActionState(text)
    except ValueError:
        try:
            return ActionState[text]
        except KeyError as exc:
            msg = f"unknown action state: {text}"
            raise ValueError(msg) from exc


def _coerce_polarity(value: Any) -> EvidencePolarity:
    text = _required_text(str(value), "polarity")
    if text not in {"supporting", "disconfirming", "neutral"}:
        msg = "polarity must be supporting, disconfirming, or neutral"
        raise ValueError(msg)
    return text  # type: ignore[return-value]


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _first_text(*values: Any, field_name: str) -> str:
    for value in values:
        text = _maybe_text(value)
        if text is not None:
            return text
    msg = f"{field_name} must not be blank"
    raise ValueError(msg)


def _first_datetime(*values: Any, field_name: str) -> datetime:
    for value in values:
        if value is None:
            continue
        return _require_aware_utc(value, field_name)
    msg = f"{field_name} is required"
    raise ValueError(msg)


def _text_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_required_text(value, "text"),)
    if not isinstance(value, Iterable):
        text = _maybe_text(value)
        return (text,) if text is not None else ()
    return tuple(text for item in value if (text := _maybe_text(item)) is not None)


def _maybe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_text(value: Any) -> str | None:
    return _maybe_text(value)


def _required_text(value: Any, field_name: str) -> str:
    text = _maybe_text(value)
    if text is None:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _optional_aware_utc(value: Any, field_name: str) -> datetime | None:
    if value is None:
        return None
    return _require_aware_utc(value, field_name)


def _require_aware_utc(value: Any, field_name: str) -> datetime:
    if isinstance(value, str):
        value = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            msg = f"{field_name} must be an ISO datetime"
            raise ValueError(msg) from exc
        value = parsed
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _finite_score(value: Any, field_name: str) -> float:
    number = _finite_float(value, field_name)
    if number < 0.0 or number > 1.0:
        msg = f"{field_name} must be between 0 and 1"
        raise ValueError(msg)
    return number


def _finite_float(value: Any, field_name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        msg = f"{field_name} must be finite"
        raise ValueError(msg) from exc
    if not math.isfinite(number):
        msg = f"{field_name} must be finite"
        raise ValueError(msg)
    return number


def _float_value(value: Any, *, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return number


def _maybe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _truncate(value: str, limit: int = 180) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "."


def _json_safe(value: Any) -> Any:
    if isinstance(value, MappingProxyType):
        value = dict(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, datetime):
        return _require_aware_utc(value, "datetime").isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple | list):
        return [_json_safe(item) for item in value]
    return thaw_json_value(value)


__all__ = [
    "CANDIDATE_PACKET_SCHEMA_VERSION",
    "CandidatePacket",
    "EvidenceItem",
    "build_candidate_packet",
    "candidate_packet_id",
    "canonical_packet_json",
    "evidence_item_payload",
    "packet_payload",
]
