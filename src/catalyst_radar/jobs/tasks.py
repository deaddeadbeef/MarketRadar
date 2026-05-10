from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from typing import Any

from sqlalchemy import Engine, func, select

from catalyst_radar.alerts.digest import build_alert_digest, digest_payload
from catalyst_radar.core.models import ActionState, JobStatus
from catalyst_radar.decision_cards.builder import build_decision_card
from catalyst_radar.pipeline.candidate_packet import build_candidate_packet
from catalyst_radar.pipeline.scan import ScanResult, run_scan
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.candidate_packet_repositories import CandidatePacketRepository
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import events
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.textint.pipeline import run_text_pipeline
from catalyst_radar.validation.models import ValidationRun, ValidationRunStatus
from catalyst_radar.validation.replay import build_replay_results, deterministic_replay_run_id
from catalyst_radar.validation.reports import build_validation_report, validation_report_payload

DAILY_STEP_ORDER = (
    "daily_bar_ingest",
    "event_ingest",
    "local_text_triage",
    "feature_scan",
    "scoring_policy",
    "candidate_packets",
    "decision_cards",
    "llm_review",
    "digest",
    "validation_update",
)


@dataclass(frozen=True)
class DailyRunSpec:
    as_of: date
    decision_available_at: datetime
    outcome_available_at: datetime | None = None
    provider: str | None = None
    universe: str | None = None
    tickers: tuple[str, ...] = ()
    dry_run_alerts: bool = True
    run_llm: bool = False
    llm_dry_run: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "decision_available_at",
            _aware_utc(self.decision_available_at, "decision_available_at"),
        )
        if self.outcome_available_at is not None:
            object.__setattr__(
                self,
                "outcome_available_at",
                _aware_utc(self.outcome_available_at, "outcome_available_at"),
            )
        object.__setattr__(self, "tickers", _normalize_tickers(self.tickers))


@dataclass(frozen=True)
class JobStepResult:
    name: str
    status: str
    job_id: str
    requested_count: int = 0
    raw_count: int = 0
    normalized_count: int = 0
    reason: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DailyRunResult:
    status: str
    spec: DailyRunSpec
    steps: tuple[JobStepResult, ...]

    def step(self, name: str) -> JobStepResult:
        for item in self.steps:
            if item.name == name:
                return item
        raise KeyError(name)


@dataclass(frozen=True)
class _StepOutcome:
    status: str
    requested_count: int = 0
    raw_count: int = 0
    normalized_count: int = 0
    reason: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class _DailyRunContext:
    spec: DailyRunSpec
    market_repo: MarketRepository
    event_repo: EventRepository
    text_repo: TextRepository
    feature_repo: FeatureRepository
    packet_repo: CandidatePacketRepository
    alert_repo: AlertRepository
    validation_repo: ValidationRepository
    scan_results: tuple[ScanResult, ...] = ()
    candidate_packets: tuple[Any, ...] = ()
    decision_cards: tuple[Any, ...] = ()

    @property
    def as_of_datetime(self) -> datetime:
        return datetime.combine(self.spec.as_of, time(21), tzinfo=UTC)


_StepHandler = Callable[[_DailyRunContext], _StepOutcome]


def run_daily(spec: DailyRunSpec, *, engine: Engine) -> DailyRunResult:
    provider_repo = ProviderRepository(engine)
    context = _DailyRunContext(
        spec=spec,
        market_repo=MarketRepository(engine),
        event_repo=EventRepository(engine),
        text_repo=TextRepository(engine),
        feature_repo=FeatureRepository(engine),
        packet_repo=CandidatePacketRepository(engine),
        alert_repo=AlertRepository(engine),
        validation_repo=ValidationRepository(engine),
    )

    steps = tuple(
        _run_step(
            step_name,
            provider_repo=provider_repo,
            context=context,
        )
        for step_name in DAILY_STEP_ORDER
    )
    return DailyRunResult(status=_daily_status(steps), spec=spec, steps=steps)


def _run_step(
    step_name: str,
    *,
    provider_repo: ProviderRepository,
    context: _DailyRunContext,
) -> JobStepResult:
    job_id = provider_repo.start_job(
        step_name,
        context.spec.provider,
        metadata=_step_metadata(context.spec, step_name),
    )
    try:
        outcome = _STEP_HANDLERS[step_name](context)
    except Exception as exc:
        reason = _truncate_reason(str(exc) or exc.__class__.__name__)
        provider_repo.finish_job(
            job_id,
            JobStatus.FAILED.value,
            requested_count=0,
            raw_count=0,
            normalized_count=0,
            error_summary=reason,
        )
        return JobStepResult(
            name=step_name,
            status=JobStatus.FAILED.value,
            job_id=job_id,
            reason=reason,
            payload={"error_type": exc.__class__.__name__},
        )

    provider_repo.finish_job(
        job_id,
        outcome.status,
        requested_count=outcome.requested_count,
        raw_count=outcome.raw_count,
        normalized_count=outcome.normalized_count,
        error_summary=outcome.reason if outcome.status == JobStatus.FAILED.value else None,
    )
    return JobStepResult(
        name=step_name,
        status=outcome.status,
        job_id=job_id,
        requested_count=outcome.requested_count,
        raw_count=outcome.raw_count,
        normalized_count=outcome.normalized_count,
        reason=outcome.reason,
        payload=outcome.payload,
    )


def _daily_bar_ingest(_: _DailyRunContext) -> _StepOutcome:
    return _skipped("no_scheduled_provider_input")


def _event_ingest(_: _DailyRunContext) -> _StepOutcome:
    return _skipped("no_scheduled_event_provider")


def _local_text_triage(context: _DailyRunContext) -> _StepOutcome:
    event_count = _visible_event_count(context)
    if event_count == 0:
        return _skipped("no_text_inputs")
    result = run_text_pipeline(
        context.event_repo,
        context.text_repo,
        as_of=context.as_of_datetime,
        available_at=context.spec.decision_available_at,
        tickers=context.spec.tickers or None,
    )
    if result.feature_count == 0 and result.snippet_count == 0:
        return _skipped("no_text_inputs", requested_count=event_count)
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=event_count,
        raw_count=result.snippet_count,
        normalized_count=result.feature_count,
        payload={
            "snippet_count": result.snippet_count,
            "feature_count": result.feature_count,
        },
    )


def _feature_scan(context: _DailyRunContext) -> _StepOutcome:
    securities = (
        context.market_repo.list_active_securities_by_tickers(context.spec.tickers)
        if context.spec.tickers
        else context.market_repo.list_active_securities()
    )
    if not securities:
        return _skipped("no_active_securities")

    results = tuple(
        run_scan(
            context.market_repo,
            context.spec.as_of,
            available_at=context.spec.decision_available_at,
            provider=context.spec.provider,
            universe_tickers=set(context.spec.tickers) if context.spec.tickers else None,
            event_repo=context.event_repo,
            text_repo=context.text_repo,
            feature_repo=context.feature_repo,
        )
    )
    context.scan_results = results
    if not results:
        return _skipped(
            "no_feature_inputs",
            requested_count=len(securities),
            payload={"active_security_count": len(securities)},
        )
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=len(securities),
        raw_count=len(results),
        normalized_count=len(results),
        payload={
            "active_security_count": len(securities),
            "scan_result_count": len(results),
        },
    )


def _scoring_policy(context: _DailyRunContext) -> _StepOutcome:
    if not context.scan_results:
        return _skipped("no_candidate_inputs")
    for result in context.scan_results:
        context.market_repo.save_scan_result(result.candidate, result.policy)
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=len(context.scan_results),
        raw_count=len(context.scan_results),
        normalized_count=len(context.scan_results),
        payload={"candidate_state_count": len(context.scan_results)},
    )


def _candidate_packets(context: _DailyRunContext) -> _StepOutcome:
    inputs = context.packet_repo.list_candidate_inputs(
        as_of=context.as_of_datetime,
        available_at=context.spec.decision_available_at,
        tickers=context.spec.tickers or None,
        states=_states_at_or_above(ActionState.WARNING),
    )
    if not inputs:
        return _skipped("no_warning_or_higher_candidates")

    packets = []
    for item in inputs:
        candidate_state = item["candidate_state"]
        ticker = str(candidate_state["ticker"]).upper()
        text_features = context.text_repo.latest_text_features_by_ticker(
            [ticker],
            as_of=context.as_of_datetime,
            available_at=context.spec.decision_available_at,
        )
        option_features = context.feature_repo.latest_option_features_by_ticker(
            [ticker],
            as_of=context.as_of_datetime,
            available_at=context.spec.decision_available_at,
        )
        packet = build_candidate_packet(
            candidate_state=candidate_state,
            signal_features_payload=item["signal_payload"],
            events=context.event_repo.list_events_for_ticker(
                ticker,
                as_of=context.as_of_datetime,
                available_at=context.spec.decision_available_at,
            ),
            snippets=context.text_repo.list_snippets_for_ticker(
                ticker,
                as_of=context.as_of_datetime,
                available_at=context.spec.decision_available_at,
            ),
            text_features=text_features.get(ticker),
            option_features=option_features.get(ticker),
            requested_available_at=context.spec.decision_available_at,
        )
        context.packet_repo.upsert_candidate_packet(packet)
        packets.append(packet)

    context.candidate_packets = tuple(packets)
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=len(inputs),
        raw_count=len(packets),
        normalized_count=len(packets),
        payload={"candidate_packet_count": len(packets)},
    )


def _decision_cards(context: _DailyRunContext) -> _StepOutcome:
    eligible_packets = tuple(
        packet
        for packet in context.candidate_packets
        if packet.state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW
    )
    if not eligible_packets:
        return _skipped("no_manual_buy_review_inputs")

    cards = []
    for packet in eligible_packets:
        card = build_decision_card(
            packet,
            available_at=context.spec.decision_available_at,
        )
        context.packet_repo.upsert_decision_card(card)
        cards.append(card)

    context.decision_cards = tuple(cards)
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=len(eligible_packets),
        raw_count=len(cards),
        normalized_count=len(cards),
        payload={"decision_card_count": len(cards)},
    )


def _llm_review(context: _DailyRunContext) -> _StepOutcome:
    if not context.spec.run_llm:
        return _skipped("llm_disabled")
    if not context.decision_cards:
        return _skipped("no_llm_review_inputs")
    if context.spec.llm_dry_run:
        return _StepOutcome(
            status=JobStatus.SUCCESS.value,
            requested_count=len(context.decision_cards),
            raw_count=0,
            normalized_count=len(context.decision_cards),
            reason="dry_run_only",
            payload={
                "dry_run": True,
                "reviewed_card_count": len(context.decision_cards),
            },
        )
    return _StepOutcome(
        status=JobStatus.FAILED.value,
        requested_count=len(context.decision_cards),
        reason="real_llm_review_not_configured",
        payload={"dry_run": False},
    )


def _digest(context: _DailyRunContext) -> _StepOutcome:
    alerts = tuple(
        _filter_alerts_by_tickers(
            context.alert_repo.list_alerts(
                available_at=context.spec.decision_available_at,
                limit=500,
            ),
            context.spec.tickers,
        )
    )
    if not alerts:
        return _skipped("no_alerts")

    suppressions = tuple(
        context.alert_repo.list_suppressions(
            available_at=context.spec.decision_available_at,
            limit=500,
        )
    )
    digest = build_alert_digest(
        alerts,
        suppressions,
        generated_at=context.spec.decision_available_at,
    )
    payload = digest_payload(digest)
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=len(alerts),
        raw_count=len(alerts),
        normalized_count=int(payload["group_count"]),
        payload={
            "dry_run": context.spec.dry_run_alerts,
            "digest": payload,
        },
    )


def _validation_update(context: _DailyRunContext) -> _StepOutcome:
    if context.spec.outcome_available_at is None:
        return _skipped("outcome_available_at_not_supplied")
    if context.spec.outcome_available_at < context.spec.decision_available_at:
        return _StepOutcome(
            status=JobStatus.FAILED.value,
            reason="outcome_available_at_before_decision_available_at",
            payload={
                "decision_available_at": context.spec.decision_available_at.isoformat(),
                "outcome_available_at": context.spec.outcome_available_at.isoformat(),
            },
        )

    states = _states_at_or_above(ActionState.WARNING)
    run_id = deterministic_replay_run_id(
        as_of_start=context.as_of_datetime,
        as_of_end=context.as_of_datetime,
        decision_available_at=context.spec.decision_available_at,
        states=states,
        tickers=context.spec.tickers,
    )
    run = ValidationRun(
        id=run_id,
        run_type="point_in_time_replay",
        as_of_start=context.as_of_datetime,
        as_of_end=context.as_of_datetime,
        decision_available_at=context.spec.decision_available_at,
        status=ValidationRunStatus.RUNNING,
        config={
            "states": [state.value for state in states],
            "tickers": list(context.spec.tickers),
            "outcome_available_at": context.spec.outcome_available_at.isoformat(),
            "no_external_calls": True,
            "source": "daily_validation_update",
        },
    )
    context.validation_repo.upsert_validation_run(run)
    try:
        results = build_replay_results(
            context.packet_repo,
            context.validation_repo,
            as_of_start=context.as_of_datetime,
            as_of_end=context.as_of_datetime,
            decision_available_at=context.spec.decision_available_at,
            states=states,
            tickers=context.spec.tickers or None,
            run_id=run_id,
        )
        count = context.validation_repo.upsert_validation_results(results)
        report = build_validation_report(
            run_id,
            results,
            useful_alert_labels=context.validation_repo.list_useful_alert_labels(
                available_at=context.spec.outcome_available_at,
            ),
        )
        metrics = validation_report_payload(report)
        context.validation_repo.finish_validation_run(
            run_id,
            ValidationRunStatus.SUCCESS,
            metrics,
            finished_at=context.spec.outcome_available_at,
        )
    except Exception as exc:
        reason = _truncate_reason(str(exc) or exc.__class__.__name__)
        context.validation_repo.finish_validation_run(
            run_id,
            ValidationRunStatus.FAILED,
            {"error": reason, "error_type": exc.__class__.__name__},
            finished_at=context.spec.outcome_available_at,
        )
        return _StepOutcome(
            status=JobStatus.FAILED.value,
            reason=reason,
            payload={
                "run_id": run_id,
                "error_type": exc.__class__.__name__,
                "outcome_available_at": context.spec.outcome_available_at.isoformat(),
            },
        )
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=len(results),
        raw_count=len(results),
        normalized_count=count,
        payload={
            "run_id": run_id,
            "result_count": count,
            "candidate_count": metrics["candidate_count"],
            "outcome_available_at": context.spec.outcome_available_at.isoformat(),
        },
    )


def _step_metadata(spec: DailyRunSpec, step_name: str) -> dict[str, Any]:
    return {
        "step": step_name,
        "as_of": spec.as_of.isoformat(),
        "decision_available_at": spec.decision_available_at.isoformat(),
        "outcome_available_at": (
            spec.outcome_available_at.isoformat()
            if spec.outcome_available_at is not None
            else None
        ),
        "dry_run_alerts": spec.dry_run_alerts,
        "run_llm": spec.run_llm,
        "llm_dry_run": spec.llm_dry_run,
        "provider": spec.provider,
        "universe": spec.universe,
        "tickers": list(spec.tickers),
    }


def _skipped(
    reason: str,
    *,
    requested_count: int = 0,
    raw_count: int = 0,
    normalized_count: int = 0,
    payload: Mapping[str, Any] | None = None,
) -> _StepOutcome:
    return _StepOutcome(
        status="skipped",
        requested_count=requested_count,
        raw_count=raw_count,
        normalized_count=normalized_count,
        reason=reason,
        payload=payload or {},
    )


def _states_at_or_above(min_state: ActionState) -> tuple[ActionState, ...]:
    floor = _state_rank(min_state)
    return tuple(state for state in ActionState if _state_rank(state) >= floor)


def _state_rank(state: ActionState) -> int:
    return {
        ActionState.NO_ACTION: 0,
        ActionState.RESEARCH_ONLY: 1,
        ActionState.ADD_TO_WATCHLIST: 2,
        ActionState.WARNING: 3,
        ActionState.BLOCKED: 3,
        ActionState.THESIS_WEAKENING: 3,
        ActionState.EXIT_INVALIDATE_REVIEW: 3,
        ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW: 4,
    }[state]


def _filter_alerts_by_tickers(alerts: list[Any], tickers: tuple[str, ...]) -> tuple[Any, ...]:
    if not tickers:
        return tuple(alerts)
    allowed = set(tickers)
    return tuple(alert for alert in alerts if alert.ticker in allowed)


def _visible_event_count(context: _DailyRunContext) -> int:
    filters = [
        events.c.source_ts <= context.as_of_datetime,
        events.c.available_at <= context.spec.decision_available_at,
    ]
    if context.spec.tickers:
        filters.append(events.c.ticker.in_(context.spec.tickers))
    with context.event_repo.engine.connect() as conn:
        return int(
            conn.scalar(select(func.count()).select_from(events).where(*filters)) or 0
        )


def _daily_status(steps: tuple[JobStepResult, ...]) -> str:
    if any(step.status == JobStatus.FAILED.value for step in steps):
        if any(step.status != JobStatus.FAILED.value for step in steps):
            return JobStatus.PARTIAL_SUCCESS.value
        return JobStatus.FAILED.value
    return JobStatus.SUCCESS.value


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _normalize_tickers(tickers: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(ticker).strip().upper() for ticker in tickers if str(ticker).strip())


def _truncate_reason(value: str, limit: int = 500) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "."


_STEP_HANDLERS: Mapping[str, _StepHandler] = {
    "daily_bar_ingest": _daily_bar_ingest,
    "event_ingest": _event_ingest,
    "local_text_triage": _local_text_triage,
    "feature_scan": _feature_scan,
    "scoring_policy": _scoring_policy,
    "candidate_packets": _candidate_packets,
    "decision_cards": _decision_cards,
    "llm_review": _llm_review,
    "digest": _digest,
    "validation_update": _validation_update,
}


__all__ = [
    "DAILY_STEP_ORDER",
    "DailyRunResult",
    "DailyRunSpec",
    "JobStepResult",
    "run_daily",
]
