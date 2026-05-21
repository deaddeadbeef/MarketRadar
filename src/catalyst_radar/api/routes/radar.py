from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, time, timedelta
from datetime import date as Date
from math import ceil
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.options import (
    OptionsAggregateConnector,
    validate_options_fixture_json,
)
from catalyst_radar.connectors.polygon_fixture import (
    capture_polygon_grouped_daily_response_with_preview,
    ingest_polygon_grouped_daily_fixture,
    preview_polygon_grouped_daily_fixture,
)
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ingest_provider_records,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.dashboard.source_batches import (
    execute_priced_in_source_batch,
    execute_priced_in_source_batches,
)
from catalyst_radar.events.sec_cik import (
    apply_sec_cik_overrides,
    refresh_sec_cik_metadata,
    validate_sec_cik_overrides,
)
from catalyst_radar.events.sec_ingest import (
    SecSubmissionTarget,
    ingest_sec_submissions_batch,
)
from catalyst_radar.jobs.scheduler import (
    SchedulerConfig,
    run_once,
    scheduler_run_payload,
)
from catalyst_radar.jobs.step_outcomes import classify_step_outcome
from catalyst_radar.market.manual_bars import (
    import_manual_market_bars,
    manual_market_bars_repair_plan,
    saved_capture_approval_guard_expected_payload,
    saved_capture_approval_guard_payload,
    write_manual_market_bars_template,
)
from catalyst_radar.market.status import (
    market_bars_import_verification_payload,
    market_bars_post_capture_verification_payload,
    market_bars_status_payload,
)
from catalyst_radar.ops.telemetry import record_telemetry_event
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.security.licenses import (
    provider_license_report_from_payload,
    redact_restricted_external_payload,
)
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.job_repositories import JobLockRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.pipeline import run_text_pipeline
from catalyst_radar.universe.seed import seed_polygon_tickers

router = APIRouter(prefix="/api/radar", tags=["radar"])
RADAR_RUN_COOLDOWN_LOCK_NAME = "manual_radar_run_cooldown"
UNIVERSE_SEED_LOCK_NAME = "polygon_ticker_seed"
SHORTLIST_REDACTED_TEXT_FIELDS = (
    "why_now",
    "top_catalyst",
    "evidence",
    "risk_or_gap",
    "source",
)
SHORTLIST_RESTRICTED_SAFE_FIELDS = (
    "priority",
    "ticker",
    "decision_status",
    "state",
    "score",
    "setup",
    "decision_card_id",
)


def _engine():
    engine = engine_from_url(AppConfig.from_env().database_url)
    create_schema(engine)
    return engine


class RadarRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: Date | None = None
    decision_available_at: datetime | None = None
    outcome_available_at: datetime | None = None
    provider: str | None = None
    universe: str | None = None
    tickers: list[str] = Field(default_factory=list)
    run_llm: bool = False
    llm_dry_run: bool = True
    dry_run_alerts: bool = True


class UniverseSeedRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str = "polygon"
    date: Date | None = None
    max_pages: int | None = Field(default=None, ge=1)


class SecSubmissionTargetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str
    cik: str


class SecSubmissionsBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    targets: list[SecSubmissionTargetRequest] = Field(default_factory=list)


class SecCikOverrideRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str
    cik: str
    sec_company_name: str | None = None


class SecCikOverridesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    overrides: list[SecCikOverrideRequest] = Field(default_factory=list)


class OptionsFixtureValidateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fixture_path: str
    expected_as_of: Date | None = None


class OptionsFixtureImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fixture_path: str
    expected_as_of: Date | None = None
    execute: bool = False


class TextFeaturesBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: Date
    available_at: datetime | None = None
    tickers: list[str] = Field(default_factory=list)


class MarketBarsTemplateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_as_of: Date
    output_path: str
    provider: str = "manual_csv"
    missing_only: bool = False
    stocks_only: bool = False
    overwrite: bool = False


class MarketBarsImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    daily_bars_path: str
    expected_as_of: Date | None = None
    stocks_only: bool = False
    complete_rows_only: bool = False
    execute: bool = False


class MarketBarsRepairPlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_as_of: Date
    stocks_only: bool = False


class MarketBarsProviderFixturePreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_as_of: Date
    fixture_path: str


class MarketBarsProviderFixtureCaptureRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_as_of: Date
    output_path: str
    fixture_path: str | None = None
    confirm_external_call: bool = False
    stocks_only: bool = False
    expected_active_security_count: int | None = None
    expected_existing_as_of_bar_count: int | None = None
    expected_missing_as_of_bar_count: int | None = None


class MarketBarsProviderFixtureImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_as_of: Date
    fixture_path: str
    execute: bool = False


def _market_bars_capture_approval_context(
    engine: Any,
    *,
    expected_as_of: Date,
    output_path: Path,
    stocks_only: bool = False,
):
    config = AppConfig.from_env()
    provider_health = ProviderRepository(engine).latest_health("polygon")
    target_date = expected_as_of.isoformat()
    fixture_path = str(output_path)
    validate_body = {
        "expected_as_of": target_date,
        "fixture_path": fixture_path,
    }
    import_preview_body = {**validate_body, "execute": False}
    import_body = {**validate_body, "execute": True}
    try:
        repair = manual_market_bars_repair_plan(
            engine,
            expected_as_of=expected_as_of,
            stocks_only=stocks_only,
            provider_key_configured=config.polygon_api_key_configured,
            provider_health_status=(
                provider_health.status.value if provider_health is not None else None
            ),
            provider_health_reason=(
                provider_health.reason if provider_health is not None else None
            ),
            provider_health_checked_at=(
                provider_health.checked_at if provider_health is not None else None
            ),
        ).as_payload()
    except ValueError as exc:
        return {
            "approval_context_status": "unavailable",
            "approval_context_reason": str(exc),
            "approval_context_external_calls_made": 0,
            "provider_saved_file_validate_request_body": validate_body,
            "provider_saved_file_import_preview_request_body": import_preview_body,
            "provider_saved_file_import_request_body": import_body,
        }
    active_count = repair.get("active_security_count")
    existing_count = repair.get("existing_as_of_bar_count")
    missing_count = repair.get("missing_as_of_bar_count")
    capture_body = {
        "expected_as_of": target_date,
        "output_path": str(output_path),
        "confirm_external_call": False,
        "stocks_only": bool(stocks_only),
        "expected_active_security_count": active_count,
        "expected_existing_as_of_bar_count": existing_count,
        "expected_missing_as_of_bar_count": missing_count,
    }
    return {
        "approval_context_status": "ready",
        "approval_context_external_calls_made": repair.get("external_calls_made", 0),
        "coverage_scope": repair.get("coverage_scope"),
        "active_security_count": active_count,
        "existing_as_of_bar_count": existing_count,
        "missing_as_of_bar_count": missing_count,
        "missing_as_of_bar_ticker_sample": repair.get(
            "missing_as_of_bar_ticker_sample",
        )
        or [],
        "missing_as_of_bar_ticker_more": repair.get(
            "missing_as_of_bar_ticker_more",
        )
        or 0,
        "missing_security_type_counts": repair.get("missing_security_type_counts")
        or {},
        "missing_universe_diagnostic": repair.get("missing_universe_diagnostic")
        or {},
        "provider_saved_file_status": repair.get("provider_saved_file_status"),
        "provider_saved_file_capture_request_body": capture_body,
        "provider_saved_file_capture_confirm_request_body": {
            **capture_body,
            "confirm_external_call": True,
        },
        "approval_guard": saved_capture_approval_guard_expected_payload(
            expected_as_of=expected_as_of,
            stocks_only=stocks_only,
            active_security_count=active_count,
            existing_as_of_bar_count=existing_count,
            missing_as_of_bar_count=int(missing_count or 0),
        ),
        "provider_saved_file_validate_command": (
            "catalyst-radar market-bars saved-validate "
            f"--expected-as-of {target_date} --fixture {output_path}"
        ),
        "provider_saved_file_import_command": (
            "catalyst-radar market-bars saved-import "
            f"--expected-as-of {target_date} --fixture {output_path}"
        ),
        "provider_saved_file_validate_request_body": validate_body,
        "provider_saved_file_import_preview_request_body": import_preview_body,
        "provider_saved_file_import_request_body": import_body,
        "next_zero_call_after_capture": (
            "After capture, preview the saved file from disk, then import only "
            "if the preview covers the intended missing market bars."
        ),
    }


class SourceBatchExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    available_at: datetime | None = None
    status: str | None = None
    usefulness: str | None = None
    decision_gap: list[str] = Field(default_factory=list)
    min_gap: float | None = Field(default=None, ge=0)
    stocks_only: bool = False
    max_batches: int = Field(default=1, ge=1, le=50)


def _candidate_api_scope(latest_run: object) -> dict[str, object]:
    summary = latest_run if isinstance(latest_run, Mapping) else {}
    return {
        "source": "latest_radar_run" if summary else "latest_candidate_state",
        "as_of": summary.get("as_of"),
        "decision_available_at": summary.get("decision_available_at"),
        "finished_at": summary.get("finished_at"),
    }


def _latest_run_detail_cutoff(latest_run: object) -> datetime | None:
    if not isinstance(latest_run, Mapping):
        return None
    for key in ("finished_at", "decision_available_at"):
        parsed = _parse_api_datetime(latest_run.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_api_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _dashboard_helper(name: str) -> Callable[..., Any]:
    try:
        return getattr(dashboard_data, name)
    except AttributeError as exc:
        msg = f"dashboard data helper is unavailable: {name}"
        raise RuntimeError(msg) from exc


@router.get("/candidates", dependencies=[Depends(require_role(Role.VIEWER))])
def candidates() -> dict[str, object]:
    engine = _engine()
    load_radar_run_summary = _dashboard_helper("load_radar_run_summary")
    load_radar_run_candidate_rows = _dashboard_helper("load_radar_run_candidate_rows")
    load_candidate_rows = _dashboard_helper("load_candidate_rows")
    latest_run = load_radar_run_summary(engine)
    rows = (
        load_radar_run_candidate_rows(
            engine,
            latest_run,
            include_post_run_artifacts=True,
        )
        if isinstance(latest_run, Mapping) and latest_run
        else load_candidate_rows(engine)
    )
    return {
        "scope": _candidate_api_scope(latest_run),
        "items": redact_restricted_external_payload(rows),
    }


@router.get("/candidates/{ticker}", dependencies=[Depends(require_role(Role.VIEWER))])
def candidate_detail(ticker: str) -> dict[str, object]:
    engine = _engine()
    load_radar_run_summary = _dashboard_helper("load_radar_run_summary")
    load_ticker_detail = _dashboard_helper("load_ticker_detail")
    latest_run = load_radar_run_summary(engine)
    cutoff = _latest_run_detail_cutoff(latest_run)
    detail = (
        load_ticker_detail(engine, ticker.upper(), available_at=cutoff)
        if cutoff is not None
        else load_ticker_detail(engine, ticker.upper())
    )
    if detail is None:
        raise HTTPException(status_code=404, detail="candidate not found")
    return redact_restricted_external_payload(detail)


@router.post("/runs", dependencies=[Depends(require_role(Role.ANALYST))])
def run_radar(
    request: RadarRunRequest,
    x_catalyst_actor: str | None = Header(default=None),
    x_catalyst_role: str | None = Header(default=None),
) -> dict[str, object]:
    engine = _engine()
    app_config = AppConfig.from_env()
    run_artifact_id = f"radar-run-api:{uuid4().hex}"
    request_metadata = _radar_run_request_metadata(request, app_config)
    record_telemetry_event(
        engine,
        event_name="radar_run.requested",
        status="received",
        actor_source="api",
        actor_id=x_catalyst_actor,
        actor_role=x_catalyst_role,
        artifact_type="radar_run",
        artifact_id=run_artifact_id,
        metadata=request_metadata,
    )
    try:
        _validate_radar_run_request(request, app_config)
        config = SchedulerConfig(
            owner="api-radar-run",
            as_of=request.as_of,
            decision_available_at=request.decision_available_at,
            outcome_available_at=request.outcome_available_at,
            provider=request.provider,
            universe=request.universe,
            tickers=tuple(request.tickers),
            run_llm=request.run_llm,
            llm_dry_run=request.llm_dry_run,
            dry_run_alerts=request.dry_run_alerts,
        )
        _acquire_radar_run_slot(engine, config=app_config, metadata=request_metadata)
        result = run_once(engine=engine, config=config)
    except _RadarRunRateLimited as exc:
        payload = exc.as_payload()
        record_telemetry_event(
            engine,
            event_name="radar_run.rate_limited",
            status="blocked",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="radar_run",
            artifact_id=run_artifact_id,
            reason="rate_limited",
            metadata={**request_metadata, **payload},
        )
        raise HTTPException(
            status_code=429,
            detail=payload,
            headers={"Retry-After": str(payload["retry_after_seconds"])},
        ) from exc
    except ValueError as exc:
        record_telemetry_event(
            engine,
            event_name="radar_run.rejected",
            status="rejected",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="radar_run",
            artifact_id=run_artifact_id,
            reason=str(exc),
            metadata=request_metadata,
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        record_telemetry_event(
            engine,
            event_name="radar_run.error",
            status="failed",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="radar_run",
            artifact_id=run_artifact_id,
            reason=str(exc) or exc.__class__.__name__,
            metadata={**request_metadata, "error_type": exc.__class__.__name__},
        )
        raise

    payload = scheduler_run_payload(result)
    if not result.acquired_lock:
        record_telemetry_event(
            engine,
            event_name="radar_run.lock_contention",
            status="blocked",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="radar_run",
            artifact_id=run_artifact_id,
            reason=result.reason,
            metadata={
                **request_metadata,
                "lock_expires_at": (
                    result.lock_expires_at.isoformat()
                    if result.lock_expires_at is not None
                    else None
                ),
            },
            after_payload=payload,
        )
        raise HTTPException(status_code=409, detail=payload)
    payload = _with_discovery_snapshot(engine, config=app_config, payload=payload)
    record_telemetry_event(
        engine,
        event_name="radar_run.completed",
        status=(result.daily_result.status if result.daily_result is not None else "success"),
        actor_source="api",
        actor_id=x_catalyst_actor,
        actor_role=x_catalyst_role,
        artifact_type="radar_run",
        artifact_id=run_artifact_id,
        metadata={
            **request_metadata,
            **_radar_run_result_metadata(payload),
        },
        after_payload=payload,
    )
    return payload


@router.get("/runs/latest", dependencies=[Depends(require_role(Role.VIEWER))])
def latest_radar_run() -> dict[str, object]:
    load_radar_run_summary = _dashboard_helper("load_radar_run_summary")
    engine = _engine()
    summary = load_radar_run_summary(engine)
    return _with_discovery_snapshot(
        engine,
        config=AppConfig.from_env(),
        payload=summary,
        radar_run_summary=summary,
    )


@router.get("/readiness", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_readiness() -> dict[str, object]:
    readiness_payload = _dashboard_helper("radar_readiness_payload")
    return redact_restricted_external_payload(
        readiness_payload(_engine(), AppConfig.from_env())
    )


@router.get("/live-activation", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_live_activation() -> dict[str, object]:
    activation_payload = _dashboard_helper("live_data_activation_contract_payload")
    load_radar_run_summary = _dashboard_helper("load_radar_run_summary")
    load_broker_summary = _dashboard_helper("load_broker_summary")
    engine = _engine()
    return activation_payload(
        AppConfig.from_env(),
        radar_run_summary=load_radar_run_summary(engine),
        broker_summary=load_broker_summary(engine),
    )


@router.get("/research-shortlist", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_research_shortlist(
    limit: int = Query(default=8, ge=1, le=50),
) -> dict[str, object]:
    shortlist_payload = _dashboard_helper("radar_research_shortlist_payload")
    return _redact_restricted_research_shortlist(
        shortlist_payload(_engine(), AppConfig.from_env(), limit=limit)
    )


@router.get("/priced-in", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_priced_in_queue(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    all_rows: bool = Query(default=False),
    decision_ready: bool = Query(default=False),
    available_at: datetime | None = None,
    status: str | None = Query(default=None),
    usefulness: str | None = Query(default=None),
    source_gap: str | None = Query(default=None),
    decision_gap: str | None = Query(default=None),
    min_gap: float | None = Query(default=None, ge=0),
    stocks_only: bool = Query(default=False),
) -> dict[str, object]:
    priced_in_payload = _dashboard_helper("priced_in_queue_payload")
    resolved_status = "actionable" if decision_ready else status
    resolved_usefulness = "decision_useful" if decision_ready else usefulness
    return redact_restricted_external_payload(
        priced_in_payload(
            _engine(),
            AppConfig.from_env(),
            limit=1_000_000 if all_rows else limit,
            offset=0 if all_rows else offset,
            available_at=_parse_api_datetime(available_at),
            status=resolved_status,
            usefulness=resolved_usefulness,
            source_gap=source_gap,
            decision_gap=decision_gap,
            min_gap=min_gap,
            stocks_only=stocks_only,
        )
    )


@router.get("/priced-in/preflight", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_priced_in_preflight(
    stocks_only: bool = Query(default=False),
) -> dict[str, object]:
    preflight_payload = _dashboard_helper("priced_in_preflight_payload")
    return redact_restricted_external_payload(
        preflight_payload(
            _engine(),
            AppConfig.from_env(),
            stocks_only=stocks_only,
        )
    )


@router.get("/priced-in/answer", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_priced_in_answer(
    limit: int = Query(default=5, ge=1, le=50),
    available_at: datetime | None = None,
    status: str | None = Query(default=None),
    usefulness: str | None = Query(default=None),
    source_gap: str | None = Query(default=None),
    decision_gap: str | None = Query(default=None),
    min_gap: float | None = Query(default=None, ge=0),
    stocks_only: bool = Query(default=False),
) -> dict[str, object]:
    answer_payload = _dashboard_helper("priced_in_answer_payload")
    return redact_restricted_external_payload(
        answer_payload(
            _engine(),
            AppConfig.from_env(),
            limit=limit,
            available_at=_parse_api_datetime(available_at),
            status=status,
            usefulness=usefulness,
            source_gap=source_gap,
            decision_gap=decision_gap,
            min_gap=min_gap,
            stocks_only=stocks_only,
        )
    )


@router.get("/priced-in/audit", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_priced_in_audit(
    available_at: datetime | None = None,
    source_gap: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    all_rows: bool = Query(default=False),
    stocks_only: bool = Query(default=False),
) -> dict[str, object]:
    audit_payload = _dashboard_helper("priced_in_full_scan_audit_payload")
    return redact_restricted_external_payload(
        audit_payload(
            _engine(),
            AppConfig.from_env(),
            available_at=_parse_api_datetime(available_at),
            source_gap=source_gap,
            preview_limit=1_000_000 if all_rows else limit,
            preview_offset=0 if all_rows else offset,
            all_rows=all_rows,
            stocks_only=stocks_only,
        )
    )


@router.get("/priced-in/source-batches", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_priced_in_source_batches(
    source: str = Query(...),
    batch_limit: int = Query(default=5, ge=1, le=50),
    batch_offset: int = Query(default=0, ge=0),
    batch_size: int | None = Query(default=None, ge=1, le=50),
    all_batches: bool = Query(default=False),
    available_at: datetime | None = None,
    status: str | None = Query(default=None),
    usefulness: str | None = Query(default=None),
    decision_gap: str | None = Query(default=None),
    min_gap: float | None = Query(default=None, ge=0),
    stocks_only: bool = Query(default=False),
) -> dict[str, object]:
    source_name = source.strip().lower()
    if source_name in {"all", "*"}:
        overview_payload = _dashboard_helper("priced_in_all_source_gap_batches_payload")
        return redact_restricted_external_payload(
            overview_payload(
                _engine(),
                AppConfig.from_env(),
                batch_size=batch_size,
                available_at=_parse_api_datetime(available_at),
                status=status,
                usefulness=usefulness,
                decision_gap=decision_gap,
                min_gap=min_gap,
                stocks_only=stocks_only,
            )
        )
    batches_payload = _dashboard_helper("priced_in_source_gap_batches_payload")
    try:
        payload = batches_payload(
            _engine(),
            AppConfig.from_env(),
            source=source,
            batch_limit=batch_limit,
            batch_offset=batch_offset,
            batch_size=batch_size,
            all_batches=all_batches,
            available_at=_parse_api_datetime(available_at),
            status=status,
            usefulness=usefulness,
            decision_gap=decision_gap,
            min_gap=min_gap,
            stocks_only=stocks_only,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(payload)


@router.post(
    "/priced-in/source-batches/execute-next",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_priced_in_source_batch_execute_next(
    request: SourceBatchExecuteRequest,
) -> dict[str, object]:
    try:
        if request.max_batches > 1:
            payload = execute_priced_in_source_batches(
                _engine(),
                AppConfig.from_env(),
                source=request.source,
                max_batches=request.max_batches,
                available_at=_parse_api_datetime(request.available_at),
                status=request.status,
                usefulness=request.usefulness,
                decision_gap=request.decision_gap,
                min_gap=request.min_gap,
                stocks_only=request.stocks_only,
            )
        else:
            payload = execute_priced_in_source_batch(
                _engine(),
                AppConfig.from_env(),
                source=request.source,
                available_at=_parse_api_datetime(request.available_at),
                status=request.status,
                usefulness=request.usefulness,
                decision_gap=request.decision_gap,
                min_gap=request.min_gap,
                stocks_only=request.stocks_only,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(payload)


@router.get(
    "/market-bars/status",
    dependencies=[Depends(require_role(Role.VIEWER))],
)
def radar_market_bars_status(
    expected_as_of: Date | None = None,
    stocks_only: bool = False,
):
    try:
        payload = market_bars_status_payload(
            _engine(),
            AppConfig.from_env(),
            expected_as_of=expected_as_of,
            stocks_only=stocks_only,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(payload)


@router.post(
    "/market-bars/template",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_market_bars_template(
    request: MarketBarsTemplateRequest,
) -> dict[str, object]:
    try:
        result = write_manual_market_bars_template(
            _engine(),
            output_path=request.output_path,
            expected_as_of=request.expected_as_of,
            provider=request.provider,
            missing_only=request.missing_only,
            stocks_only=request.stocks_only,
            overwrite=request.overwrite,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(result.as_payload())


@router.post(
    "/market-bars/import",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_market_bars_import(
    request: MarketBarsImportRequest,
) -> dict[str, object]:
    try:
        result = import_manual_market_bars(
            _engine(),
            daily_bars_path=request.daily_bars_path,
            expected_as_of=request.expected_as_of,
            stocks_only=request.stocks_only,
            complete_rows_only=request.complete_rows_only,
            execute=request.execute,
        )
    except (FileNotFoundError, KeyError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    payload = result.as_payload()
    if result.expected_as_of is not None:
        payload["post_import_verification"] = market_bars_import_verification_payload(
            _engine(),
            AppConfig.from_env(),
            expected_as_of=result.expected_as_of,
            stocks_only=result.stocks_only,
            executed=result.executed,
            source="manual_csv",
            db_changes_made=1 if result.executed else 0,
            projected_missing_after_import_count=(
                None if result.executed else len(result.missing_expected_tickers)
            ),
            projected_db_changes_made=None if result.executed else 1,
        )
    return redact_restricted_external_payload(payload)


@router.post(
    "/market-bars/repair-plan",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_market_bars_repair_plan(
    request: MarketBarsRepairPlanRequest,
) -> dict[str, object]:
    engine = _engine()
    provider_health = ProviderRepository(engine).latest_health("polygon")
    try:
        result = manual_market_bars_repair_plan(
            engine,
            expected_as_of=request.expected_as_of,
            stocks_only=request.stocks_only,
            provider_key_configured=AppConfig.from_env().polygon_api_key_configured,
            provider_health_status=(
                provider_health.status.value if provider_health is not None else None
            ),
            provider_health_reason=(
                provider_health.reason if provider_health is not None else None
            ),
            provider_health_checked_at=(
                provider_health.checked_at if provider_health is not None else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(result.as_payload())


@router.post(
    "/market-bars/provider-fixture-preview",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_market_bars_provider_fixture_preview(
    request: MarketBarsProviderFixturePreviewRequest,
) -> dict[str, object]:
    engine = _engine()
    try:
        payload = preview_polygon_grouped_daily_fixture(
            config=AppConfig.from_env(),
            market_repo=MarketRepository(engine),
            date_value=request.expected_as_of,
            fixture_path=Path(request.fixture_path),
        )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(payload)


@router.post(
    "/market-bars/provider-fixture-capture",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_market_bars_provider_fixture_capture(
    request: MarketBarsProviderFixtureCaptureRequest,
) -> dict[str, object]:
    engine = _engine()
    config = AppConfig.from_env()
    output_path = Path(request.output_path)
    if request.fixture_path is None and not request.confirm_external_call:
        target_date = request.expected_as_of.isoformat()
        approval_context = _market_bars_capture_approval_context(
            engine,
            expected_as_of=request.expected_as_of,
            output_path=output_path,
            stocks_only=request.stocks_only,
        )
        active_count = approval_context.get("active_security_count")
        existing_count = approval_context.get("existing_as_of_bar_count")
        missing_count = approval_context.get("missing_as_of_bar_count")
        stock_flag = " --stocks-only" if request.stocks_only else ""
        return redact_restricted_external_payload(
            {
                "schema_version": "polygon-grouped-daily-response-capture-v1",
                "status": "approval_required",
                "provider": "polygon",
                "date": target_date,
                "output_path": str(output_path),
                "stocks_only": bool(request.stocks_only),
                "capture_external_call_count": 1,
                "external_calls_made": 0,
                "db_writes_made": 0,
                "capture_command": (
                    "catalyst-radar market-bars saved-capture "
                    f"--expected-as-of {target_date} --out {output_path} "
                    f"--expect-active-count {active_count} "
                    f"--expect-existing-count {existing_count} "
                    f"--expect-missing-count {missing_count} "
                    "--confirm-external-call"
                    f"{stock_flag}"
                ),
                "approval_boundary": (
                    "Live grouped-daily capture makes one Polygon/Massive "
                    "provider call and requires explicit operator approval."
                ),
                "next_action": (
                    "Set confirm_external_call=true only if you approve the "
                    "single provider call, or provide fixture_path for a local "
                    "fixture-backed capture."
                ),
                **approval_context,
            },
        )
    if request.fixture_path is None and request.confirm_external_call:
        guard = saved_capture_approval_guard_payload(
            engine,
            expected_as_of=request.expected_as_of,
            stocks_only=request.stocks_only,
            expected_active_security_count=request.expected_active_security_count,
            expected_existing_as_of_bar_count=(
                request.expected_existing_as_of_bar_count
            ),
            expected_missing_as_of_bar_count=request.expected_missing_as_of_bar_count,
        )
        if guard.get("status") != "ready":
            raise HTTPException(status_code=422, detail=guard)
    try:
        payload = capture_polygon_grouped_daily_response_with_preview(
            config=config,
            market_repo=MarketRepository(engine),
            date_value=request.expected_as_of,
            output_path=output_path,
            fixture_path=Path(request.fixture_path) if request.fixture_path else None,
            confirm_external_call=request.confirm_external_call,
        )
        payload["post_capture_verification"] = market_bars_post_capture_verification_payload(
            engine,
            config,
            expected_as_of=request.expected_as_of,
            capture_payload=payload,
            stocks_only=request.stocks_only,
        )
    except (FileNotFoundError, PermissionError, RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(payload)


@router.post(
    "/market-bars/provider-fixture-import",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_market_bars_provider_fixture_import(
    request: MarketBarsProviderFixtureImportRequest,
) -> dict[str, object]:
    engine = _engine()
    config = AppConfig.from_env()
    market_repo = MarketRepository(engine)
    provider_repo = ProviderRepository(engine)
    fixture_path = Path(request.fixture_path)
    try:
        preview = preview_polygon_grouped_daily_fixture(
            config=config,
            market_repo=market_repo,
            date_value=request.expected_as_of,
            fixture_path=fixture_path,
        )
        if not request.execute:
            return redact_restricted_external_payload(
                {
                    **preview,
                    "schema_version": "polygon-grouped-daily-fixture-import-v1",
                    "executed": False,
                    "post_import_verification": market_bars_import_verification_payload(
                        engine,
                        config,
                        expected_as_of=request.expected_as_of,
                        executed=False,
                        source="saved_provider_file",
                        db_changes_made=0,
                        projected_missing_after_import_count=int(
                            (
                                preview.get("coverage")
                                if isinstance(preview.get("coverage"), dict)
                                else {}
                            ).get("missing_after_import_count", 0)
                            or 0
                        ),
                        projected_db_changes_made=1,
                    ),
                    "db_writes_made": 0,
                    "write_boundary": (
                        "Preview only; set execute=true to import the saved "
                        "fixture. This path reads from disk and makes 0 "
                        "provider calls."
                    ),
                },
            )
        if preview.get("status") == "invalid":
            raise ValueError(str(preview.get("next_action") or "fixture is invalid"))
        result = ingest_polygon_grouped_daily_fixture(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            date_value=request.expected_as_of,
            fixture_path=fixture_path,
        )
    except (FileNotFoundError, RuntimeError, ValueError, ProviderIngestError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(
        {
            "schema_version": "polygon-grouped-daily-fixture-import-v1",
            "status": (
                "imported_with_rejections"
                if result.rejected_count > 0
                else "imported"
            ),
            "executed": True,
            "provider": result.provider,
            "job_id": result.job_id,
            "requested_count": result.requested_count,
            "raw_count": result.raw_count,
            "normalized_count": result.normalized_count,
            "security_count": result.security_count,
            "daily_bar_count": result.daily_bar_count,
            "holding_count": result.holding_count,
            "event_count": result.event_count,
            "option_feature_count": result.option_feature_count,
            "rejected_count": result.rejected_count,
            "external_calls_made": 0,
            "db_writes_made": 1,
            "post_import_verification": market_bars_import_verification_payload(
                engine,
                config,
                expected_as_of=request.expected_as_of,
                executed=True,
                source="saved_provider_file",
                db_changes_made=1,
            ),
            "preview": preview,
            "write_boundary": (
                "Imported from a saved fixture on disk. This path made 0 "
                "provider calls."
            ),
        },
    )


@router.post(
    "/sec/submissions-batch",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_sec_submissions_batch(
    request: SecSubmissionsBatchRequest,
) -> dict[str, object]:
    config = AppConfig.from_env()
    targets = _sec_submission_targets_from_request(request, config)
    engine = _engine()
    try:
        result = ingest_sec_submissions_batch(
            config=config,
            market_repo=MarketRepository(engine),
            provider_repo=ProviderRepository(engine),
            event_repo=EventRepository(engine),
            targets=targets,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ProviderIngestError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return redact_restricted_external_payload(result.as_payload())


@router.post(
    "/sec/company-tickers",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_sec_company_tickers() -> dict[str, object]:
    config = AppConfig.from_env()
    try:
        result = refresh_sec_cik_metadata(_engine(), config)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return redact_restricted_external_payload(result.as_payload())


@router.get(
    "/options/fixture-template",
    dependencies=[Depends(require_role(Role.VIEWER))],
)
def radar_options_fixture_template(
    stocks_only: bool = Query(default=False),
) -> dict[str, object]:
    payload = dashboard_data.options_fixture_template_payload(
        _engine(),
        AppConfig.from_env(),
        stocks_only=stocks_only,
    )
    return redact_restricted_external_payload(payload)


@router.post(
    "/options/fixture-validate",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_options_fixture_validate(
    request: OptionsFixtureValidateRequest,
) -> dict[str, object]:
    result = validate_options_fixture_json(
        Path(request.fixture_path),
        expected_as_of=request.expected_as_of,
    )
    return redact_restricted_external_payload(result.as_payload())


@router.post(
    "/options/fixture-import",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_options_fixture_import(
    request: OptionsFixtureImportRequest,
):
    fixture_path = Path(request.fixture_path)
    validation = validate_options_fixture_json(
        fixture_path,
        expected_as_of=request.expected_as_of,
    ).as_payload()
    if not request.execute:
        return redact_restricted_external_payload(
            {
                "schema_version": "options-fixture-import-v1",
                "status": validation.get("status"),
                "executed": False,
                "provider": "options_fixture",
                "fixture_path": str(fixture_path),
                "external_calls_made": 0,
                "db_writes_made": 0,
                "validation": validation,
                "write_boundary": (
                    "Preview only; set execute=true to import the validated "
                    "point-in-time options fixture. This path reads from disk "
                    "and makes 0 provider calls."
                ),
                "next_action": (
                    "Set execute=true only after validation is ready and the "
                    "scan date matches your intent."
                    if validation.get("status") == "ready"
                    else validation.get("next_action")
                ),
            },
        )
    if validation.get("status") != "ready":
        detail = validation.get("next_action") or "options fixture is invalid"
        raise HTTPException(status_code=422, detail=str(detail))
    engine = _engine()
    connector = OptionsAggregateConnector(fixture_path=fixture_path)
    request_model = ConnectorRequest(
        provider="options_fixture",
        endpoint="fixture",
        params={"fixture": str(fixture_path)},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request_model,
            market_repo=MarketRepository(engine),
            provider_repo=ProviderRepository(engine),
            job_type="options_fixture",
            metadata={
                "provider": "options_fixture",
                "endpoint": "fixture",
                "fixture": str(fixture_path),
            },
            feature_repo=FeatureRepository(engine),
        )
    except (ProviderIngestError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return redact_restricted_external_payload(
        {
            "schema_version": "options-fixture-import-v1",
            "status": "imported",
            "executed": True,
            "provider": result.provider,
            "job_id": result.job_id,
            "requested_count": result.requested_count,
            "raw_count": result.raw_count,
            "normalized_count": result.normalized_count,
            "security_count": result.security_count,
            "daily_bar_count": result.daily_bar_count,
            "holding_count": result.holding_count,
            "event_count": result.event_count,
            "option_feature_count": result.option_feature_count,
            "rejected_count": result.rejected_count,
            "fixture_path": str(fixture_path),
            "external_calls_made": 0,
            "db_writes_made": 1,
            "validation": validation,
            "write_boundary": (
                "Imported from a local point-in-time options fixture. This "
                "path made 0 provider calls."
            ),
        },
    )


@router.get(
    "/sec/cik-overrides-template",
    dependencies=[Depends(require_role(Role.VIEWER))],
)
def radar_sec_cik_overrides_template(
    stocks_only: bool = Query(default=False),
) -> dict[str, object]:
    payload = dashboard_data.sec_cik_override_template_payload(
        _engine(),
        AppConfig.from_env(),
        stocks_only=stocks_only,
    )
    return redact_restricted_external_payload(payload)


@router.post(
    "/sec/cik-overrides",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_sec_cik_overrides(request: SecCikOverridesRequest) -> dict[str, object]:
    records = [
        {
            "ticker": item.ticker,
            "cik": item.cik,
            "sec_company_name": item.sec_company_name,
        }
        for item in request.overrides
    ]
    result = apply_sec_cik_overrides(_engine(), records)
    return redact_restricted_external_payload(result.as_payload())


@router.post(
    "/sec/cik-overrides/validate",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_sec_cik_overrides_validate(
    request: SecCikOverridesRequest,
) -> dict[str, object]:
    records = [
        {
            "ticker": item.ticker,
            "cik": item.cik,
            "sec_company_name": item.sec_company_name,
        }
        for item in request.overrides
    ]
    result = validate_sec_cik_overrides(_engine(), records)
    return redact_restricted_external_payload(result.as_payload())


@router.post(
    "/text/features-batch",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def radar_text_features_batch(request: TextFeaturesBatchRequest) -> dict[str, object]:
    tickers = _text_feature_batch_tickers(request)
    available_at = _parse_api_datetime(request.available_at) or datetime.now(UTC)
    engine = _engine()
    result = run_text_pipeline(
        EventRepository(engine),
        TextRepository(engine),
        as_of=datetime.combine(request.as_of, time(21), tzinfo=UTC),
        available_at=available_at,
        tickers=tickers,
    )
    return {
        "schema_version": "text-features-batch-result-v1",
        "provider": "local_text",
        "endpoint": "features-batch",
        "as_of": request.as_of.isoformat(),
        "available_at": available_at.isoformat(),
        "tickers": list(tickers),
        "ticker_count": len(tickers),
        "feature_count": result.feature_count,
        "snippet_count": result.snippet_count,
        "external_calls_made": 0,
    }


@router.post("/runs/call-plan", dependencies=[Depends(require_role(Role.VIEWER))])
def radar_run_call_plan(request: RadarRunRequest) -> dict[str, object]:
    call_plan_payload = _dashboard_helper("radar_run_call_plan_payload")
    return call_plan_payload(
        _engine(),
        AppConfig.from_env(),
        as_of=request.as_of,
        provider=request.provider,
        universe=request.universe,
        tickers=request.tickers,
        run_llm=request.run_llm,
        llm_dry_run=request.llm_dry_run,
        dry_run_alerts=request.dry_run_alerts,
    )


def _sec_submission_targets_from_request(
    request: SecSubmissionsBatchRequest,
    config: AppConfig,
) -> tuple[SecSubmissionTarget, ...]:
    if not request.targets:
        raise HTTPException(status_code=400, detail="At least one SEC target is required")
    if len(request.targets) > config.sec_daily_max_tickers:
        raise HTTPException(
            status_code=400,
            detail=(
                "Too many SEC submissions targets; maximum is "
                f"{config.sec_daily_max_tickers}"
            ),
        )
    targets = []
    for item in request.targets:
        ticker = item.ticker.strip().upper()
        raw_cik = item.cik.strip()
        if not ticker or not raw_cik:
            raise HTTPException(
                status_code=422,
                detail="SEC targets must include both ticker and CIK",
            )
        cik = raw_cik.zfill(10)
        targets.append(SecSubmissionTarget(ticker=ticker, cik=cik))
    return tuple(targets)


def _text_feature_batch_tickers(
    request: TextFeaturesBatchRequest,
) -> tuple[str, ...]:
    if not request.tickers:
        raise HTTPException(
            status_code=400,
            detail="At least one local text ticker is required",
        )
    tickers = tuple(
        dict.fromkeys(
            ticker.strip().upper()
            for ticker in request.tickers
            if ticker.strip()
        )
    )
    if not tickers:
        raise HTTPException(
            status_code=422,
            detail="Local text tickers must not be blank",
        )
    if len(tickers) > 50:
        raise HTTPException(
            status_code=400,
            detail="Too many local text tickers; maximum is 50",
        )
    return tickers


def _redact_restricted_research_shortlist(
    payload: Mapping[str, object],
) -> dict[str, object]:
    rows = payload.get("rows")
    base = {
        str(key): redact_restricted_external_payload(value)
        for key, value in payload.items()
        if key != "rows"
    }
    base["rows"] = []
    if isinstance(rows, list | tuple):
        base["rows"] = [
            _redact_restricted_research_shortlist_row(row)
            for row in rows
            if isinstance(row, Mapping)
        ]
    return base


def _redact_restricted_research_shortlist_row(
    row: Mapping[str, object],
) -> dict[str, object]:
    report = provider_license_report_from_payload(row)
    if report["metadata_complete"] and not report["external_export_allowed"]:
        safe = {
            key: row[key]
            for key in SHORTLIST_RESTRICTED_SAFE_FIELDS
            if row.get(key) not in (None, "")
        }
        safe["next_step"] = (
            "Review source details in the local dashboard; provider text is withheld "
            "by export policy."
        )
        safe["external_export_blocked"] = True
        safe["license_tags"] = report["license_tags"]
        safe["attribution_required"] = report["attribution_required"]
        safe["restricted_fields"] = [
            key
            for key in SHORTLIST_REDACTED_TEXT_FIELDS
            if row.get(key) not in (None, "")
        ]
        return safe
    return redact_restricted_external_payload(row)


@router.post("/universe/seed", dependencies=[Depends(require_role(Role.ANALYST))])
def seed_universe(
    request: UniverseSeedRequest,
    x_catalyst_actor: str | None = Header(default=None),
    x_catalyst_role: str | None = Header(default=None),
) -> dict[str, object]:
    engine = _engine()
    config = AppConfig.from_env()
    artifact_id = f"universe-seed-api:{uuid4().hex}"
    metadata = _universe_seed_request_metadata(request, config)
    record_telemetry_event(
        engine,
        event_name="universe_seed.requested",
        status="received",
        actor_source="api",
        actor_id=x_catalyst_actor,
        actor_role=x_catalyst_role,
        artifact_type="universe_seed",
        artifact_id=artifact_id,
        metadata=metadata,
    )
    try:
        _validate_universe_seed_request(request, config)
        _acquire_universe_seed_slot(engine, config=config, metadata=metadata)
        result = seed_polygon_tickers(
            engine,
            config=config,
            max_pages=request.max_pages,
            date_value=request.date,
        )
    except _UniverseSeedRateLimited as exc:
        payload = exc.as_payload()
        record_telemetry_event(
            engine,
            event_name="universe_seed.rate_limited",
            status="blocked",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="universe_seed",
            artifact_id=artifact_id,
            reason="rate_limited",
            metadata={**metadata, **payload},
        )
        raise HTTPException(
            status_code=429,
            detail=payload,
            headers={"Retry-After": str(payload["retry_after_seconds"])},
        ) from exc
    except ValueError as exc:
        record_telemetry_event(
            engine,
            event_name="universe_seed.rejected",
            status="rejected",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="universe_seed",
            artifact_id=artifact_id,
            reason=str(exc),
            metadata=metadata,
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        reason = str(exc) or exc.__class__.__name__
        record_telemetry_event(
            engine,
            event_name="universe_seed.rejected",
            status="failed",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="universe_seed",
            artifact_id=artifact_id,
            reason=reason,
            metadata={**metadata, "error_type": exc.__class__.__name__},
        )
        raise HTTPException(status_code=503, detail=reason) from exc

    payload = result.as_payload()
    record_telemetry_event(
        engine,
        event_name="universe_seed.completed",
        status="success",
        actor_source="api",
        actor_id=x_catalyst_actor,
        actor_role=x_catalyst_role,
        artifact_type="universe_seed",
        artifact_id=artifact_id,
        metadata={**metadata, "job_id": result.job_id},
        after_payload=payload,
    )
    return payload


def _radar_run_request_metadata(
    request: RadarRunRequest,
    config: AppConfig,
) -> dict[str, object]:
    return {
        "lock_name": "daily-run",
        "cooldown_lock_name": RADAR_RUN_COOLDOWN_LOCK_NAME,
        "min_interval_seconds": config.radar_run_min_interval_seconds,
        "as_of": request.as_of.isoformat() if request.as_of is not None else None,
        "decision_available_at": (
            request.decision_available_at.isoformat()
            if request.decision_available_at is not None
            else None
        ),
        "outcome_available_at": (
            request.outcome_available_at.isoformat()
            if request.outcome_available_at is not None
            else None
        ),
        "provider": request.provider,
        "universe": request.universe,
        "tickers": [ticker.upper() for ticker in request.tickers],
        "run_llm": request.run_llm,
        "llm_dry_run": request.llm_dry_run,
        "dry_run_alerts": request.dry_run_alerts,
    }


def _validate_radar_run_request(
    request: RadarRunRequest,
    config: AppConfig,
) -> None:
    override = str(request.provider or "").strip().lower()
    if not override:
        return
    scheduled_provider = (config.daily_market_provider or "").strip().lower() or "csv"
    if override == scheduled_provider:
        return
    msg = (
        f"provider override {override} does not match "
        f"CATALYST_DAILY_MARKET_PROVIDER={scheduled_provider}; remove the override "
        "or align CATALYST_DAILY_MARKET_PROVIDER and CATALYST_DAILY_PROVIDER"
    )
    raise ValueError(msg)


def _with_discovery_snapshot(
    engine,
    *,
    config: AppConfig,
    payload: dict[str, object],
    radar_run_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    summary = (
        radar_run_summary
        if radar_run_summary is not None
        else dashboard_data.load_radar_run_summary(engine)
    )
    return {
        **payload,
        "discovery_snapshot": redact_restricted_external_payload(
            dashboard_data.radar_discovery_snapshot_payload(
                engine,
                config,
                radar_run_summary=summary,
            )
        ),
    }


def _universe_seed_request_metadata(
    request: UniverseSeedRequest,
    config: AppConfig,
) -> dict[str, object]:
    return {
        "provider": request.provider,
        "date": request.date.isoformat() if request.date is not None else None,
        "requested_max_pages": request.max_pages,
        "configured_max_pages": config.polygon_tickers_max_pages,
        "min_interval_seconds": config.polygon_ticker_seed_min_interval_seconds,
    }


def _validate_universe_seed_request(
    request: UniverseSeedRequest,
    config: AppConfig,
) -> None:
    provider = str(request.provider or "").strip().lower()
    if provider != "polygon":
        msg = "only provider=polygon is supported for universe seed"
        raise ValueError(msg)
    if (
        request.max_pages is not None
        and request.max_pages > config.polygon_tickers_max_pages
    ):
        msg = (
            "max_pages exceeds configured cap "
            f"CATALYST_POLYGON_TICKERS_MAX_PAGES={config.polygon_tickers_max_pages}"
        )
        raise ValueError(msg)


class _UniverseSeedRateLimited(RuntimeError):
    def __init__(self, *, retry_after_seconds: int, reset_at: datetime | None) -> None:
        self.retry_after_seconds = retry_after_seconds
        self.reset_at = reset_at
        super().__init__(f"universe seed is rate limited for {retry_after_seconds}s")

    def as_payload(self) -> dict[str, object]:
        return {
            "operation": "polygon_ticker_seed",
            "retry_after_seconds": self.retry_after_seconds,
            "reset_at": self.reset_at.isoformat() if self.reset_at is not None else None,
        }


class _RadarRunRateLimited(RuntimeError):
    def __init__(self, *, retry_after_seconds: int, reset_at: datetime | None) -> None:
        self.retry_after_seconds = retry_after_seconds
        self.reset_at = reset_at
        super().__init__(f"radar run is rate limited for {retry_after_seconds}s")

    def as_payload(self) -> dict[str, object]:
        return {
            "operation": "manual_radar_run",
            "retry_after_seconds": self.retry_after_seconds,
            "reset_at": self.reset_at.isoformat() if self.reset_at is not None else None,
        }


def _acquire_radar_run_slot(
    engine,
    *,
    config: AppConfig,
    metadata: dict[str, object],
) -> None:
    now = datetime.now(UTC)
    result = JobLockRepository(engine).acquire(
        RADAR_RUN_COOLDOWN_LOCK_NAME,
        owner=f"api-radar-run-cooldown:{uuid4().hex}",
        ttl=timedelta(seconds=config.radar_run_min_interval_seconds),
        now=now,
        metadata={
            "operation": "manual_radar_run",
            **metadata,
        },
    )
    if result.acquired:
        return
    raise _RadarRunRateLimited(
        retry_after_seconds=_retry_after_seconds(result.expires_at, now),
        reset_at=result.expires_at,
    )


def _acquire_universe_seed_slot(
    engine,
    *,
    config: AppConfig,
    metadata: dict[str, object],
) -> None:
    now = datetime.now(UTC)
    result = JobLockRepository(engine).acquire(
        UNIVERSE_SEED_LOCK_NAME,
        owner=f"api-universe-seed:{uuid4().hex}",
        ttl=timedelta(seconds=config.polygon_ticker_seed_min_interval_seconds),
        now=now,
        metadata={
            "operation": "polygon_ticker_seed",
            **metadata,
        },
    )
    if result.acquired:
        return
    raise _UniverseSeedRateLimited(
        retry_after_seconds=_retry_after_seconds(result.expires_at, now),
        reset_at=result.expires_at,
    )


def _retry_after_seconds(reset_at: datetime | None, now: datetime) -> int:
    if reset_at is None:
        return 1
    return max(1, int(ceil((reset_at.astimezone(UTC) - now).total_seconds())))


def _radar_run_result_metadata(payload: dict[str, object]) -> dict[str, object]:
    daily_result = payload.get("daily_result")
    if not isinstance(daily_result, dict):
        return {
            "daily_status": None,
            "step_counts": {},
            "outcome_category_counts": {},
            "skip_reason_counts": {},
            "blocked_steps": [],
            "expected_gate_steps": [],
            "skipped_steps": [],
        }
    steps = daily_result.get("steps")
    if not isinstance(steps, dict):
        return {
            "daily_status": daily_result.get("status"),
            "step_counts": {},
            "outcome_category_counts": {},
            "skip_reason_counts": {},
            "blocked_steps": [],
            "expected_gate_steps": [],
            "skipped_steps": [],
        }
    counts: dict[str, int] = {}
    outcome_category_counts: dict[str, int] = {}
    skip_reason_counts: dict[str, int] = {}
    skipped_steps: list[dict[str, object]] = []
    blocked_steps: list[dict[str, object]] = []
    expected_gate_steps: list[dict[str, object]] = []
    for step_name, step in steps.items():
        status = str(step.get("status") if isinstance(step, dict) else "unknown")
        counts[status] = counts.get(status, 0) + 1
        if isinstance(step, dict):
            reason = str(step.get("reason") or "unspecified")
            category = str(step.get("category") or "")
            classification = (
                classify_step_outcome(status, None if reason == "unspecified" else reason)
                if not category
                else None
            )
            outcome_category = category or classification.category
            outcome_category_counts[outcome_category] = (
                outcome_category_counts.get(outcome_category, 0) + 1
            )
            step_summary = {
                "step": str(step.get("name") or step_name),
                "reason": None if reason == "unspecified" else reason,
                "category": outcome_category,
                "label": step.get("label")
                or (classification.label if classification is not None else None),
                "requested_count": step.get("requested_count"),
                "raw_count": step.get("raw_count"),
                "normalized_count": step.get("normalized_count"),
            }
            if bool(
                step.get("blocks_reliance")
                if "blocks_reliance" in step
                else (
                    classification.blocks_reliance
                    if classification is not None
                    else outcome_category in {"blocked_input", "failed", "needs_review"}
                )
            ):
                blocked_steps.append(step_summary)
            if outcome_category == "expected_gate":
                expected_gate_steps.append(step_summary)
        if status == "skipped" and isinstance(step, dict):
            skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + 1
            skipped_steps.append(step_summary)
    return {
        "daily_status": daily_result.get("status"),
        "step_counts": dict(sorted(counts.items())),
        "outcome_category_counts": dict(sorted(outcome_category_counts.items())),
        "skip_reason_counts": dict(sorted(skip_reason_counts.items())),
        "blocked_steps": blocked_steps,
        "expected_gate_steps": expected_gate_steps,
        "skipped_steps": skipped_steps,
    }
