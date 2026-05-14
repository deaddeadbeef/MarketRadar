from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from typing import Any

from sqlalchemy import Engine, func, select

from catalyst_radar.alerts.digest import build_alert_digest, digest_payload
from catalyst_radar.brokers.portfolio_context import latest_broker_portfolio_context
from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.http import (
    HeaderInjectingTransport,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.connectors.news import NewsJsonConnector
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ingest_provider_records,
)
from catalyst_radar.connectors.sec import SecSubmissionsConnector
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState, JobStatus, PolicyResult, Security
from catalyst_radar.decision_cards.builder import build_decision_card
from catalyst_radar.jobs.step_outcomes import (
    BLOCKING_SKIP_REASONS,
    classify_step_outcome,
)
from catalyst_radar.ops.health import DISABLED_DEGRADED_STATES, load_ops_health
from catalyst_radar.ops.telemetry import record_telemetry_event
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

LIMITED_ANALYSIS_SKIP_REASONS = BLOCKING_SKIP_REASONS
CSV_SCHEDULED_PROVIDER_NAMES = frozenset({"csv", "sample"})
POLYGON_SCHEDULED_PROVIDER_NAMES = frozenset({"polygon"})
MARKET_SCHEDULED_PROVIDER_NAMES = (
    CSV_SCHEDULED_PROVIDER_NAMES | POLYGON_SCHEDULED_PROVIDER_NAMES
)
NEWS_SCHEDULED_EVENT_PROVIDER_NAMES = frozenset({"news_fixture", "sample", "fixture"})
SEC_SCHEDULED_EVENT_PROVIDER_NAMES = frozenset({"sec", "sec_submissions"})
EVENT_SCHEDULED_PROVIDER_NAMES = (
    NEWS_SCHEDULED_EVENT_PROVIDER_NAMES | SEC_SCHEDULED_EVENT_PROVIDER_NAMES
)
DISABLED_SCHEDULED_PROVIDER_NAMES = frozenset({"", "none", "off", "disabled"})
logger = logging.getLogger("catalyst_radar.jobs.tasks")


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
        if self.run_llm and not self.llm_dry_run:
            msg = "real daily LLM review is not supported; use run-llm-review per candidate"
            raise ValueError(msg)
        if not self.dry_run_alerts:
            msg = "daily alert delivery is not supported; use send-alerts dry-run"
            raise ValueError(msg)


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
    engine: Engine
    spec: DailyRunSpec
    market_repo: MarketRepository
    event_repo: EventRepository
    text_repo: TextRepository
    feature_repo: FeatureRepository
    packet_repo: CandidatePacketRepository
    alert_repo: AlertRepository
    validation_repo: ValidationRepository
    config: AppConfig
    scan_results: tuple[ScanResult, ...] = ()
    candidate_packets: tuple[Any, ...] = ()
    decision_cards: tuple[Any, ...] = ()
    step_results: dict[str, JobStepResult] = field(default_factory=dict)
    degraded_mode: Mapping[str, Any] | None = None

    @property
    def as_of_datetime(self) -> datetime:
        return datetime.combine(self.spec.as_of, time(21), tzinfo=UTC)


_StepHandler = Callable[[_DailyRunContext], _StepOutcome]


def run_daily(
    spec: DailyRunSpec,
    *,
    engine: Engine,
    abort_event: Any | None = None,
) -> DailyRunResult:
    provider_repo = ProviderRepository(engine)
    context = _DailyRunContext(
        engine=engine,
        spec=spec,
        config=AppConfig.from_env(),
        market_repo=MarketRepository(engine),
        event_repo=EventRepository(engine),
        text_repo=TextRepository(engine),
        feature_repo=FeatureRepository(engine),
        packet_repo=CandidatePacketRepository(engine),
        alert_repo=AlertRepository(engine),
        validation_repo=ValidationRepository(engine),
    )

    steps = []
    for step_name in DAILY_STEP_ORDER:
        step = _run_step(
            step_name,
            provider_repo=provider_repo,
            context=context,
            abort_event=abort_event,
        )
        context.step_results[step_name] = step
        steps.append(step)
    step_tuple = tuple(steps)
    return DailyRunResult(status=_daily_status(step_tuple), spec=spec, steps=step_tuple)


def _run_step(
    step_name: str,
    *,
    provider_repo: ProviderRepository,
    context: _DailyRunContext,
    abort_event: Any | None = None,
) -> JobStepResult:
    job_id = provider_repo.start_job(
        step_name,
        context.spec.provider,
        metadata=_step_metadata(context.spec, step_name),
    )
    _log_step_started(context, step_name, job_id)
    try:
        if abort_event is not None and abort_event.is_set():
            outcome = _StepOutcome(
                status=JobStatus.FAILED.value,
                reason="lock_heartbeat_lost",
            )
        else:
            failed_dependency = _failed_dependency(step_name, context)
            if failed_dependency is not None:
                outcome = _skipped(f"blocked_by_failed_dependency:{failed_dependency}")
            else:
                outcome = _STEP_HANDLERS[step_name](context)
                if (
                    abort_event is not None
                    and abort_event.is_set()
                    and outcome.status != JobStatus.FAILED.value
                ):
                    outcome = _StepOutcome(
                        status=JobStatus.FAILED.value,
                        requested_count=outcome.requested_count,
                        raw_count=outcome.raw_count,
                        normalized_count=outcome.normalized_count,
                        reason="lock_heartbeat_lost",
                        payload={
                            "completed_status_before_abort": outcome.status,
                            **dict(outcome.payload),
                        },
                    )
    except Exception as exc:
        reason = _truncate_reason(str(exc) or exc.__class__.__name__)
        provider_repo.finish_job(
            job_id,
            JobStatus.FAILED.value,
            requested_count=0,
            raw_count=0,
            normalized_count=0,
            error_summary=reason,
            metadata_update={
                "result_status": JobStatus.FAILED.value,
                "result_reason": reason,
                "result_payload": {"error_type": exc.__class__.__name__},
                **classify_step_outcome(JobStatus.FAILED.value, reason).as_metadata(),
            },
        )
        failed_step = JobStepResult(
            name=step_name,
            status=JobStatus.FAILED.value,
            job_id=job_id,
            reason=reason,
            payload={"error_type": exc.__class__.__name__},
        )
        _record_step_finished(context, failed_step)
        return failed_step

    provider_repo.finish_job(
        job_id,
        outcome.status,
        requested_count=outcome.requested_count,
        raw_count=outcome.raw_count,
        normalized_count=outcome.normalized_count,
        error_summary=outcome.reason if outcome.status == JobStatus.FAILED.value else None,
        metadata_update={
            "result_status": outcome.status,
            "result_reason": outcome.reason,
            "result_payload": outcome.payload,
            **classify_step_outcome(outcome.status, outcome.reason).as_metadata(),
        },
    )
    step_result = JobStepResult(
        name=step_name,
        status=outcome.status,
        job_id=job_id,
        requested_count=outcome.requested_count,
        raw_count=outcome.raw_count,
        normalized_count=outcome.normalized_count,
        reason=outcome.reason,
        payload=outcome.payload,
    )
    _record_step_finished(context, step_result)
    return step_result


def _daily_bar_ingest(context: _DailyRunContext) -> _StepOutcome:
    provider = _scheduled_market_provider(context)
    if provider in DISABLED_SCHEDULED_PROVIDER_NAMES:
        return _skipped("no_scheduled_provider_input")
    if provider not in MARKET_SCHEDULED_PROVIDER_NAMES:
        return _skipped(
            "scheduled_provider_not_supported",
            payload={
                "provider": provider,
                "supported_providers": sorted(MARKET_SCHEDULED_PROVIDER_NAMES),
            },
        )
    if provider in POLYGON_SCHEDULED_PROVIDER_NAMES:
        return _daily_polygon_bar_ingest(context, provider)

    return _daily_csv_bar_ingest(context, provider)


def _daily_csv_bar_ingest(context: _DailyRunContext, provider: str) -> _StepOutcome:
    connector = CsvMarketDataConnector(
        securities_path=context.config.csv_securities_path,
        daily_bars_path=context.config.csv_daily_bars_path,
        holdings_path=context.config.csv_holdings_path,
        provider=provider,
    )
    metadata = {
        "scheduled_provider": provider,
        "scan_provider": context.spec.provider,
        "securities": context.config.csv_securities_path,
        "daily_bars": context.config.csv_daily_bars_path,
        "holdings": context.config.csv_holdings_path,
    }
    request = ConnectorRequest(
        provider=connector.provider,
        endpoint="scheduled_csv_ingest",
        params=metadata,
        requested_at=context.spec.decision_available_at,
        idempotency_key=(
            f"daily_bar_ingest:{provider}:{context.spec.as_of.isoformat()}:"
            f"{context.spec.decision_available_at.isoformat()}"
        ),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=context.market_repo,
            provider_repo=ProviderRepository(context.engine),
            job_type="scheduled_csv_ingest",
            metadata=metadata,
        )
    except ProviderIngestError as exc:
        return _StepOutcome(
            status=JobStatus.FAILED.value,
            reason=_truncate_reason(str(exc) or exc.__class__.__name__),
            payload={"provider": provider, "endpoint": request.endpoint},
        )
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=result.requested_count,
        raw_count=result.raw_count,
        normalized_count=result.normalized_count,
        payload={
            "provider": result.provider,
            "ingest_job_id": result.job_id,
            "security_count": result.security_count,
            "daily_bar_count": result.daily_bar_count,
            "holding_count": result.holding_count,
            "rejected_count": result.rejected_count,
        },
    )


def _daily_polygon_bar_ingest(context: _DailyRunContext, provider: str) -> _StepOutcome:
    endpoint = PolygonEndpoint.GROUPED_DAILY
    connector = PolygonMarketDataConnector(
        api_key=context.config.polygon_api_key,
        client=JsonHttpClient(
            transport=UrlLibHttpTransport(),
            timeout_seconds=context.config.http_timeout_seconds,
        ),
        base_url=context.config.polygon_base_url,
        availability_policy=context.config.provider_availability_policy,
    )
    metadata = {
        "scheduled_provider": provider,
        "scan_provider": context.spec.provider or provider,
        "endpoint": endpoint.value,
        "date": context.spec.as_of.isoformat(),
        "adjusted": True,
        "include_otc": False,
        "availability_policy": context.config.provider_availability_policy,
    }
    request = ConnectorRequest(
        provider=connector.provider,
        endpoint=endpoint.value,
        params={
            "date": context.spec.as_of.isoformat(),
            "adjusted": True,
            "include_otc": False,
        },
        requested_at=context.spec.decision_available_at,
        idempotency_key=(
            f"daily_bar_ingest:{provider}:{endpoint.value}:"
            f"{context.spec.as_of.isoformat()}:"
            f"{context.spec.decision_available_at.isoformat()}"
        ),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=context.market_repo,
            provider_repo=ProviderRepository(context.engine),
            job_type=endpoint.value,
            metadata=metadata,
        )
    except ProviderIngestError as exc:
        return _StepOutcome(
            status=JobStatus.FAILED.value,
            reason=_truncate_reason(str(exc) or exc.__class__.__name__),
            payload={"provider": provider, "endpoint": request.endpoint},
        )
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=result.requested_count,
        raw_count=result.raw_count,
        normalized_count=result.normalized_count,
        payload={
            "provider": result.provider,
            "endpoint": endpoint.value,
            "ingest_job_id": result.job_id,
            "security_count": result.security_count,
            "daily_bar_count": result.daily_bar_count,
            "holding_count": result.holding_count,
            "rejected_count": result.rejected_count,
        },
    )


def _event_ingest(context: _DailyRunContext) -> _StepOutcome:
    provider = _scheduled_event_provider(context)
    if provider in DISABLED_SCHEDULED_PROVIDER_NAMES:
        return _skipped("no_scheduled_event_provider")
    if provider not in EVENT_SCHEDULED_PROVIDER_NAMES:
        return _skipped(
            "scheduled_event_provider_not_supported",
            payload={
                "provider": provider,
                "supported_providers": sorted(EVENT_SCHEDULED_PROVIDER_NAMES),
            },
        )
    if provider in SEC_SCHEDULED_EVENT_PROVIDER_NAMES:
        return _daily_sec_event_ingest(context, provider)

    return _daily_news_fixture_ingest(context, provider)


def _daily_news_fixture_ingest(context: _DailyRunContext, provider: str) -> _StepOutcome:
    connector = NewsJsonConnector(
        fixture_path=context.config.news_fixture_path,
        provider=provider,
    )
    metadata = {
        "scheduled_event_provider": provider,
        "fixture": context.config.news_fixture_path,
    }
    request = ConnectorRequest(
        provider=connector.provider,
        endpoint="scheduled_news_fixture_ingest",
        params=metadata,
        requested_at=context.spec.decision_available_at,
        idempotency_key=(
            f"event_ingest:{provider}:{context.spec.as_of.isoformat()}:"
            f"{context.spec.decision_available_at.isoformat()}"
        ),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=context.market_repo,
            provider_repo=ProviderRepository(context.engine),
            event_repo=context.event_repo,
            job_type="scheduled_news_fixture_ingest",
            metadata=metadata,
        )
    except ProviderIngestError as exc:
        return _StepOutcome(
            status=JobStatus.FAILED.value,
            reason=_truncate_reason(str(exc) or exc.__class__.__name__),
            payload={"provider": provider, "endpoint": request.endpoint},
        )
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=result.requested_count,
        raw_count=result.raw_count,
        normalized_count=result.normalized_count,
        payload={
            "provider": result.provider,
            "ingest_job_id": result.job_id,
            "event_count": result.event_count,
            "rejected_count": result.rejected_count,
        },
    )


def _daily_sec_event_ingest(context: _DailyRunContext, provider: str) -> _StepOutcome:
    if not context.config.sec_enable_live:
        return _StepOutcome(
            status=JobStatus.FAILED.value,
            reason="CATALYST_SEC_ENABLE_LIVE=1 required for scheduled SEC ingest",
            payload={"provider": provider, "endpoint": "submissions"},
        )
    if not context.config.sec_user_agent:
        return _StepOutcome(
            status=JobStatus.FAILED.value,
            reason="CATALYST_SEC_USER_AGENT is required for scheduled SEC ingest",
            payload={"provider": provider, "endpoint": "submissions"},
        )

    candidates = _scheduled_sec_targets(context)
    if not candidates:
        return _skipped(
            "no_sec_cik_targets",
            payload={
                "provider": provider,
                "endpoint": "submissions",
                "max_tickers": context.config.sec_daily_max_tickers,
            },
        )

    transport = HeaderInjectingTransport(
        UrlLibHttpTransport(),
        {"User-Agent": context.config.sec_user_agent or ""},
    )
    connector = SecSubmissionsConnector(
        client=JsonHttpClient(
            transport=transport,
            timeout_seconds=context.config.http_timeout_seconds,
        ),
        base_url=context.config.sec_base_url,
    )
    job_ids: list[str] = []
    requested_count = 0
    raw_count = 0
    normalized_count = 0
    event_count = 0
    rejected_count = 0
    try:
        for security, cik in candidates:
            metadata = {
                "scheduled_event_provider": provider,
                "endpoint": "submissions",
                "ticker": security.ticker,
                "cik": cik,
                "live": True,
                "max_tickers": context.config.sec_daily_max_tickers,
            }
            request = ConnectorRequest(
                provider=connector.provider,
                endpoint="submissions",
                params={"ticker": security.ticker, "cik": cik},
                requested_at=context.spec.decision_available_at,
                idempotency_key=(
                    f"event_ingest:{provider}:submissions:{security.ticker}:"
                    f"{context.spec.as_of.isoformat()}:"
                    f"{context.spec.decision_available_at.isoformat()}"
                ),
            )
            result = ingest_provider_records(
                connector=connector,
                request=request,
                market_repo=context.market_repo,
                provider_repo=ProviderRepository(context.engine),
                event_repo=context.event_repo,
                job_type="scheduled_sec_submissions",
                metadata=metadata,
            )
            job_ids.append(result.job_id)
            requested_count += result.requested_count
            raw_count += result.raw_count
            normalized_count += result.normalized_count
            event_count += result.event_count
            rejected_count += result.rejected_count
    except ProviderIngestError as exc:
        return _StepOutcome(
            status=JobStatus.FAILED.value,
            reason=_truncate_reason(str(exc) or exc.__class__.__name__),
            payload={"provider": provider, "endpoint": "submissions"},
        )

    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=requested_count,
        raw_count=raw_count,
        normalized_count=normalized_count,
        payload={
            "provider": "sec",
            "endpoint": "submissions",
            "target_count": len(candidates),
            "job_ids": job_ids,
            "event_count": event_count,
            "rejected_count": rejected_count,
        },
    )


def _local_text_triage(context: _DailyRunContext) -> _StepOutcome:
    event_ingest = context.step_results.get("event_ingest")
    if event_ingest is not None and event_ingest.status == JobStatus.FAILED.value:
        return _skipped("blocked_by_failed_dependency:event_ingest")
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
    daily_ingest = context.step_results.get("daily_bar_ingest")
    if daily_ingest is not None and daily_ingest.status == JobStatus.FAILED.value:
        return _skipped("blocked_by_failed_dependency:daily_bar_ingest")
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
            provider=_scan_provider(context),
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
    capped_count = 0
    for result in context.scan_results:
        policy = _degraded_policy_cap(context, result.policy)
        if policy.state != result.policy.state:
            capped_count += 1
        context.market_repo.save_scan_result(result.candidate, policy)
    return _StepOutcome(
        status=JobStatus.SUCCESS.value,
        requested_count=len(context.scan_results),
        raw_count=len(context.scan_results),
        normalized_count=len(context.scan_results),
        payload={
            "candidate_state_count": len(context.scan_results),
            "degraded_state_cap_count": capped_count,
            "degraded_mode": _degraded_payload(context) if capped_count else None,
        },
    )


def _candidate_packets(context: _DailyRunContext) -> _StepOutcome:
    if _degraded_mode_enabled(context):
        disabled_inputs = context.packet_repo.list_candidate_inputs(
            as_of=context.as_of_datetime,
            available_at=context.spec.decision_available_at,
            tickers=context.spec.tickers or None,
            states=DISABLED_DEGRADED_STATES,
        )
        return _skipped(
            "degraded_mode_blocks_high_state_work",
            requested_count=len(disabled_inputs),
            payload={"degraded_mode": _degraded_payload(context)},
        )
    if not context.scan_results:
        return _skipped("no_current_scan_results")
    inputs = context.packet_repo.list_candidate_inputs(
        as_of=context.as_of_datetime,
        available_at=context.spec.decision_available_at,
        tickers=context.spec.tickers or None,
        states=_states_at_or_above(ActionState.WARNING),
    )
    if not inputs:
        return _skipped(
            "no_warning_or_higher_candidates",
            requested_count=len(context.scan_results),
            payload=_scan_result_summary_payload(context),
        )

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
    if _degraded_mode_enabled(context):
        return _skipped(
            "degraded_mode_blocks_decision_cards",
            payload={"degraded_mode": _degraded_payload(context)},
        )
    eligible_packets = tuple(
        packet
        for packet in context.candidate_packets
        if packet.state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW
    )
    if not context.candidate_packets:
        packet_step = context.step_results.get("candidate_packets")
        return _skipped(
            "no_candidate_packets",
            payload={
                "candidate_packets_status": (
                    packet_step.status if packet_step is not None else None
                ),
                "candidate_packets_reason": (
                    packet_step.reason if packet_step is not None else None
                ),
                "candidate_packets_requested_count": (
                    packet_step.requested_count if packet_step is not None else 0
                ),
                "candidate_packets_normalized_count": (
                    packet_step.normalized_count if packet_step is not None else 0
                ),
            },
        )
    if not eligible_packets:
        return _skipped("no_manual_buy_review_inputs")

    cards = []
    for packet in eligible_packets:
        card = build_decision_card(
            packet,
            available_at=context.spec.decision_available_at,
            broker_portfolio_context=latest_broker_portfolio_context(
                context.engine,
                ticker=packet.ticker,
                available_at=context.spec.decision_available_at,
            ),
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
    if _degraded_mode_enabled(context):
        return _skipped(
            "degraded_mode_blocks_llm_review",
            payload={"degraded_mode": _degraded_payload(context)},
        )
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


def _log_step_started(context: _DailyRunContext, step_name: str, job_id: str) -> None:
    metadata = {
        **_step_metadata(context.spec, step_name),
        "job_id": job_id,
    }
    logger.info(
        "radar_step_started",
        extra={
            "catalyst_radar": metadata,
        },
    )
    try:
        record_telemetry_event(
            context.engine,
            event_name="radar_run.step_started",
            status="started",
            actor_source="radar_pipeline",
            artifact_type="job_run",
            artifact_id=job_id,
            metadata=metadata,
            available_at=context.spec.decision_available_at,
        )
    except Exception:
        logger.exception(
            "radar_step_started_telemetry_failed",
            extra={"catalyst_radar": metadata},
        )


def _record_step_finished(context: _DailyRunContext, step: JobStepResult) -> None:
    metadata = {
        **_step_metadata(context.spec, step.name),
        "job_id": step.job_id,
        "result_status": step.status,
        "result_reason": step.reason,
        "requested_count": step.requested_count,
        "raw_count": step.raw_count,
        "normalized_count": step.normalized_count,
        **classify_step_outcome(step.status, step.reason).as_metadata(),
    }
    log_payload = {
        **metadata,
        "result_payload": dict(step.payload),
    }
    logger.info("radar_step_finished", extra={"catalyst_radar": log_payload})
    try:
        record_telemetry_event(
            context.engine,
            event_name="radar_run.step_finished",
            status=step.status,
            actor_source="radar_pipeline",
            artifact_type="job_run",
            artifact_id=step.job_id,
            reason=step.reason,
            metadata=metadata,
            after_payload={"step": step.name, "payload": dict(step.payload)},
            available_at=context.spec.decision_available_at,
        )
    except Exception:
        logger.exception(
            "radar_step_telemetry_failed",
            extra={"catalyst_radar": log_payload},
        )


def _failed_dependency(step_name: str, context: _DailyRunContext) -> str | None:
    for dependency in _STEP_DEPENDENCIES.get(step_name, ()):
        result = context.step_results.get(dependency)
        if result is None:
            continue
        if result.status == JobStatus.FAILED.value or _is_blocked_step(result):
            return dependency
    return None


def _is_blocked_step(result: JobStepResult) -> bool:
    return result.status == "skipped" and str(result.reason or "").startswith(
        "blocked_by_failed_dependency:"
    )


def _degraded_policy_cap(
    context: _DailyRunContext,
    policy: PolicyResult,
) -> PolicyResult:
    if not _degraded_mode_enabled(context) or policy.state not in DISABLED_DEGRADED_STATES:
        return policy
    reasons = tuple(
        dict.fromkeys((*policy.reasons, "degraded_mode_state_cap")).keys()
    )
    return PolicyResult(
        state=ActionState.ADD_TO_WATCHLIST,
        hard_blocks=policy.hard_blocks,
        reasons=reasons,
        missing_trade_plan=policy.missing_trade_plan,
    )


def _degraded_mode_enabled(context: _DailyRunContext) -> bool:
    payload = _degraded_payload(context)
    return bool(payload.get("enabled"))


def _degraded_payload(context: _DailyRunContext) -> dict[str, Any]:
    if context.degraded_mode is None:
        health = load_ops_health(context.market_repo.engine, now=context.spec.decision_available_at)
        degraded_mode = health.get("degraded_mode")
        scoped = degraded_mode if isinstance(degraded_mode, Mapping) else {}
        context.degraded_mode = _scoped_degraded_payload(
            scoped,
            relevant_providers=_relevant_degraded_providers(context),
        )
    return dict(context.degraded_mode)


def _scheduled_market_provider(context: _DailyRunContext) -> str:
    provider = context.config.daily_market_provider.strip().lower()
    return provider if provider else "csv"


def _scan_provider(context: _DailyRunContext) -> str | None:
    if context.spec.provider:
        return context.spec.provider.strip().lower()
    scheduled = _scheduled_market_provider(context)
    if scheduled in DISABLED_SCHEDULED_PROVIDER_NAMES:
        return None
    if scheduled in POLYGON_SCHEDULED_PROVIDER_NAMES:
        return scheduled
    if scheduled == "sample":
        return scheduled
    return None


def _scheduled_event_provider(context: _DailyRunContext) -> str:
    return context.config.daily_event_provider.strip().lower()


def _scheduled_sec_targets(context: _DailyRunContext) -> tuple[tuple[Security, str], ...]:
    securities = (
        context.market_repo.list_active_securities_by_tickers(context.spec.tickers)
        if context.spec.tickers
        else context.market_repo.list_active_securities()
    )
    targets: list[tuple[Security, str]] = []
    for security in securities:
        cik = _security_cik(security)
        if cik is None:
            continue
        targets.append((security, cik))
    return tuple(targets[: context.config.sec_daily_max_tickers])


def _security_cik(security: Security) -> str | None:
    for key in ("cik", "cik_str", "central_index_key"):
        value = security.metadata.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().zfill(10)
    return None


def _relevant_degraded_providers(context: _DailyRunContext) -> set[str]:
    providers = {_scheduled_market_provider(context), _scheduled_event_provider(context)}
    if context.spec.provider:
        providers.add(context.spec.provider.strip().lower())
    return {provider for provider in providers if provider not in DISABLED_SCHEDULED_PROVIDER_NAMES}


def _scoped_degraded_payload(
    degraded_mode: Mapping[str, Any],
    *,
    relevant_providers: set[str],
) -> dict[str, Any]:
    reasons = [
        str(reason)
        for reason in degraded_mode.get("reasons", [])
        if not str(reason).startswith("provider:")
        or str(reason).removeprefix("provider:").strip().lower() in relevant_providers
    ]
    return {
        **dict(degraded_mode),
        "enabled": bool(reasons),
        "reasons": reasons,
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


def _scan_result_summary_payload(context: _DailyRunContext) -> dict[str, Any]:
    state_counts: dict[str, int] = {}
    top_candidates: list[dict[str, Any]] = []
    max_score: float | None = None
    max_state: str | None = None
    for result in context.scan_results:
        state = result.policy.state.value
        state_counts[state] = state_counts.get(state, 0) + 1
        score = float(result.candidate.final_score)
        if max_score is None or score > max_score:
            max_score = score
            max_state = state
        if len(top_candidates) < 5:
            top_candidates.append(
                {
                    "ticker": result.ticker,
                    "state": state,
                    "score": round(score, 2),
                    "hard_blocks": list(result.policy.hard_blocks),
                    "reasons": list(result.policy.reasons),
                }
            )
    return {
        "scored_candidate_count": len(context.scan_results),
        "warning_threshold_state": ActionState.WARNING.value,
        "state_counts": state_counts,
        "max_score": round(max_score, 2) if max_score is not None else None,
        "max_state": max_state,
        "top_scored_candidates": top_candidates,
    }


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
    if any(
        step.status == "skipped" and step.reason in LIMITED_ANALYSIS_SKIP_REASONS
        for step in steps
    ):
        return JobStatus.PARTIAL_SUCCESS.value
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

_STEP_DEPENDENCIES: Mapping[str, tuple[str, ...]] = {
    "scoring_policy": ("feature_scan",),
    "candidate_packets": ("feature_scan", "scoring_policy"),
    "decision_cards": ("candidate_packets",),
    "llm_review": ("decision_cards",),
    "digest": ("candidate_packets",),
    "validation_update": ("candidate_packets",),
}


__all__ = [
    "DAILY_STEP_ORDER",
    "DailyRunResult",
    "DailyRunSpec",
    "JobStepResult",
    "run_daily",
]
