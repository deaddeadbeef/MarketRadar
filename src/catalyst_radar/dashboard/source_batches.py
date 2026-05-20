from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time

from fastapi import HTTPException
from sqlalchemy.engine import Engine

from catalyst_radar.api.routes.brokers import (
    _acquire_schwab_rate_limit_slot,
    _active_access_token,
)
from catalyst_radar.brokers.interactive import (
    market_snapshot_payload,
    normalize_tickers,
    sync_market_context,
    upsert_schwab_option_features,
)
from catalyst_radar.brokers.models import BrokerConnectionStatus
from catalyst_radar.brokers.rate_limit import SCHWAB_MARKET_SYNC_OPERATION
from catalyst_radar.brokers.schwab import SchwabClient
from catalyst_radar.connectors.http import JsonHttpClient, UrlLibHttpTransport
from catalyst_radar.connectors.provider_ingest import ProviderIngestError
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import priced_in_source_gap_batches_payload
from catalyst_radar.events.sec_ingest import (
    SecSubmissionTarget,
    ingest_sec_submissions_batch,
)
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.pipeline import run_text_pipeline

MAX_PRICED_IN_SOURCE_BATCH_RUN_CHUNKS = 50


def execute_priced_in_source_batch(
    engine: Engine,
    config: AppConfig,
    *,
    source: str,
    available_at: datetime | None = None,
    status: str | None = None,
    usefulness: str | None = None,
    decision_gap: str | Sequence[str] | None = None,
    min_gap: float | None = None,
    stocks_only: bool = False,
) -> dict[str, object]:
    plan = priced_in_source_gap_batches_payload(
        engine,
        config,
        source=source,
        batch_limit=1,
        available_at=available_at,
        status=status,
        usefulness=usefulness,
        decision_gap=decision_gap,
        min_gap=min_gap,
        stocks_only=stocks_only,
    )
    source_name = str(plan.get("source") or source).strip()
    coverage_blocker = _market_bar_execution_blocker(
        engine,
        config,
        source_name=source_name,
        available_at=available_at,
        status=status,
        usefulness=usefulness,
        decision_gap=decision_gap,
        min_gap=min_gap,
        stocks_only=stocks_only,
    )
    if coverage_blocker is not None:
        return _execution_payload(
            source_name=source_name,
            status="blocked",
            plan=plan,
            reason=str(coverage_blocker.get("reason") or ""),
            execution_blocker=coverage_blocker,
        )
    batches = _rows(plan.get("batches"))
    if str(plan.get("status") or "") == "blocked":
        return _execution_payload(
            source_name=source_name,
            status="blocked",
            plan=plan,
            reason=_plan_block_reason(plan, f"{source_name} source batch is blocked."),
        )
    if not batches:
        status_value = str(plan.get("status") or "no_action")
        return _execution_payload(
            source_name=source_name,
            status="no_action" if status_value in {"no_gaps", "ready"} else "blocked",
            plan=plan,
            reason=str(
                plan.get("next_action")
                or _plan_block_reason(plan, f"No runnable {source_name} source batch.")
            ),
        )
    batch = batches[0]
    call_plan_status = str(batch.get("call_plan_status") or "").strip()
    if call_plan_status == "blocked":
        return _execution_payload(
            source_name=source_name,
            status="blocked",
            plan=plan,
            batch=batch,
            reason=str(
                batch.get("call_plan_next_action")
                or batch.get("call_plan_headline")
                or f"{source_name} source batch is blocked by call-plan guardrails."
            ),
        )
    if source_name == "local_text":
        result = _execute_local_text_source_batch(engine, batch)
    elif source_name == "catalyst_events":
        result = _execute_sec_source_batch(engine, config, batch)
    elif source_name in {"options", "broker_context"}:
        result = _execute_schwab_source_batch(engine, config, source_name, batch)
    else:
        return _execution_payload(
            source_name=source_name,
            status="blocked",
            plan=plan,
            batch=batch,
            reason=f"{source_name} is not executable by source batch.",
        )
    result_status = str(result.get("status") or "executed")
    post_execution = None
    if result_status == "executed":
        post_plan = priced_in_source_gap_batches_payload(
            engine,
            config,
            source=source,
            batch_limit=1,
            available_at=available_at,
            status=status,
            usefulness=usefulness,
            decision_gap=decision_gap,
            min_gap=min_gap,
            stocks_only=stocks_only,
        )
        post_execution = _post_execution_check_payload(
            source_name=source_name,
            before_plan=plan,
            after_plan=post_plan,
        )
    return _execution_payload(
        source_name=source_name,
        status=result_status,
        plan=plan,
        batch=batch,
        result=result,
        reason=str(result.get("reason") or ""),
        external_calls_made=int(_number_or_zero(result.get("external_calls_made"))),
        post_execution=post_execution,
    )


def execute_priced_in_source_batches(
    engine: Engine,
    config: AppConfig,
    *,
    source: str,
    max_batches: int,
    available_at: datetime | None = None,
    status: str | None = None,
    usefulness: str | None = None,
    decision_gap: str | Sequence[str] | None = None,
    min_gap: float | None = None,
    stocks_only: bool = False,
) -> dict[str, object]:
    if int(max_batches) <= 0:
        raise ValueError("max_batches must be positive")
    if int(max_batches) > MAX_PRICED_IN_SOURCE_BATCH_RUN_CHUNKS:
        raise ValueError(
            "max_batches must be <= "
            f"{MAX_PRICED_IN_SOURCE_BATCH_RUN_CHUNKS}"
        )
    source_name = str(source or "").strip().lower()
    if source_name in {"all", "*"}:
        raise ValueError("source all is plan-only; choose one executable source")
    before_plan = priced_in_source_gap_batches_payload(
        engine,
        config,
        source=source,
        batch_limit=1,
        available_at=available_at,
        status=status,
        usefulness=usefulness,
        decision_gap=decision_gap,
        min_gap=min_gap,
        stocks_only=stocks_only,
    )
    source_name = str(before_plan.get("source") or source_name).strip()
    executions: list[dict[str, object]] = []
    external_calls_made = 0
    stop_reason = ""
    for _index in range(int(max_batches)):
        execution = execute_priced_in_source_batch(
            engine,
            config,
            source=source_name,
            available_at=available_at,
            status=status,
            usefulness=usefulness,
            decision_gap=decision_gap,
            min_gap=min_gap,
            stocks_only=stocks_only,
        )
        executions.append(execution)
        external_calls_made += int(_number_or_zero(execution.get("external_calls_made")))
        execution_status = str(execution.get("status") or "").strip()
        if execution_status != "executed":
            stop_reason = str(
                execution.get("reason")
                or f"{source_name} batch stopped with status={execution_status}"
            )
            break
    else:
        stop_reason = f"Reached max_batches={int(max_batches)}."

    after_plan = priced_in_source_gap_batches_payload(
        engine,
        config,
        source=source_name,
        batch_limit=1,
        available_at=available_at,
        status=status,
        usefulness=usefulness,
        decision_gap=decision_gap,
        min_gap=min_gap,
        stocks_only=stocks_only,
    )
    before_summary = _source_plan_summary(before_plan)
    after_summary = _source_plan_summary(after_plan)
    executed_batches = sum(
        1 for item in executions if str(item.get("status") or "") == "executed"
    )
    failed = next(
        (
            item
            for item in executions
            if str(item.get("status") or "") not in {"executed", "no_action"}
        ),
        None,
    )
    status_value = _source_batch_run_status(
        executed_batches=executed_batches,
        requested_batches=int(max_batches),
        failed=failed,
        after_summary=after_summary,
    )
    next_command = (
        f"catalyst-radar priced-in-source-batches --source {source_name} "
        f"--execute-batches {int(max_batches)}"
        if int(_number_or_zero(after_summary.get("plannable_gap_rows"))) > 0
        else None
    )
    payload: dict[str, object] = {
        "schema_version": "priced-in-source-batch-run-v1",
        "source": source_name,
        "status": status_value,
        "requested_batches": int(max_batches),
        "executed_batches": executed_batches,
        "stopped_reason": stop_reason,
        "external_calls_made": external_calls_made,
        "execution_boundary": (
            "Executes up to the requested number of source-fill chunks, stopping "
            "early on blocked, failed, or no-action results. Each chunk still uses "
            "the existing provider-specific guardrails."
        ),
        "before_plan": before_summary,
        "after_plan": after_summary,
        "gap_rows_resolved": int(_number_or_zero(before_summary.get("total_gap_rows")))
        - int(_number_or_zero(after_summary.get("total_gap_rows"))),
        "plannable_rows_resolved": int(
            _number_or_zero(before_summary.get("plannable_gap_rows"))
        )
        - int(_number_or_zero(after_summary.get("plannable_gap_rows"))),
        "executions": executions,
        "next_action": _source_batch_run_next_action(
            source_name=source_name,
            executed_batches=executed_batches,
            requested_batches=int(max_batches),
            failed=failed,
            after_summary=after_summary,
            stop_reason=stop_reason,
        ),
        "next_command": next_command,
    }
    return payload


def source_batch_run_summary(payload: Mapping[str, object]) -> str:
    source = str(payload.get("source") or "source").strip()
    status = str(payload.get("status") or "unknown").strip()
    before = _mapping(payload.get("before_plan"))
    after = _mapping(payload.get("after_plan"))
    executed = int(_number_or_zero(payload.get("executed_batches")))
    requested = int(_number_or_zero(payload.get("requested_batches")))
    resolved = int(_number_or_zero(payload.get("gap_rows_resolved")))
    calls = int(_number_or_zero(payload.get("external_calls_made")))
    before_gaps = int(_number_or_zero(before.get("total_gap_rows")))
    after_gaps = int(_number_or_zero(after.get("total_gap_rows")))
    next_action = str(payload.get("next_action") or "").strip()
    return (
        f"{source} batch run {status}: executed {executed}/{requested} chunk(s), "
        f"gap rows {before_gaps}->{after_gaps} (resolved {resolved}), "
        f"external calls={calls}. {next_action}"
    ).strip()


def source_batch_execution_summary(payload: Mapping[str, object]) -> str:
    source = str(payload.get("source") or "source").strip()
    status = str(payload.get("status") or "unknown").strip()
    batch = _mapping(payload.get("batch"))
    result = _mapping(payload.get("result"))
    reason = str(payload.get("reason") or "").strip()
    if status != "executed":
        return reason or f"{source} source batch status={status}."
    row_start = int(_number_or_zero(batch.get("row_start"))) or 1
    row_end = int(_number_or_zero(batch.get("row_end"))) or row_start
    tickers = _batch_tickers(_mapping(batch.get("api_payload")), batch)
    details: list[str] = [
        (
            f"Executed {source} chunk "
            f"{int(_number_or_zero(batch.get('number'))) or 1} "
            f"(rows {row_start}-{row_end}):"
        ),
        f"tickers={len(tickers)}",
    ]
    if source == "local_text":
        details.extend(
            [
                f"features={result.get('feature_count')}",
                f"snippets={result.get('snippet_count')}",
            ]
        )
    elif source == "catalyst_events":
        details.extend(
            [
                f"targets={result.get('target_count')}",
                f"events={result.get('event_count')}",
            ]
        )
    elif source in {"options", "broker_context"}:
        items = result.get("items")
        details.extend(
            [
                f"snapshots={len(items) if isinstance(items, list | tuple) else 0}",
                f"option_features={result.get('option_features_upserted', 0)}",
            ]
        )
    details.append(f"external_calls={payload.get('external_calls_made', 0)}")
    post_execution = _mapping(payload.get("post_execution"))
    if post_execution:
        next_action = str(post_execution.get("next_action") or "").strip()
        return (
            " ".join(details)
            + f". Post-check: {next_action or _post_execution_fallback(post_execution)}"
        )
    return " ".join(details) + ". Refresh to see updated full-scan coverage."


def _market_bar_execution_blocker(
    engine: Engine,
    config: AppConfig,
    *,
    source_name: str,
    available_at: datetime | None,
    status: str | None,
    usefulness: str | None,
    decision_gap: str | Sequence[str] | None,
    min_gap: float | None,
    stocks_only: bool,
) -> dict[str, object] | None:
    if source_name == "market_bars":
        return None
    market_plan = priced_in_source_gap_batches_payload(
        engine,
        config,
        source="market_bars",
        batch_limit=1,
        available_at=available_at,
        status="all",
        usefulness="all",
        decision_gap=None,
        min_gap=None,
        stocks_only=stocks_only,
    )
    gaps = int(_number_or_zero(market_plan.get("total_gap_rows")))
    if gaps <= 0:
        return None
    diagnostic = _mapping(market_plan.get("diagnostic"))
    coverage_basis = str(
        _mapping(market_plan.get("scan_scope")).get("coverage_basis") or ""
    )
    row_label = (
        "stock-like row(s)"
        if coverage_basis == "stock_like_active_as_of_bars"
        else "active row(s)"
    )
    scan_label = "stocks-only scan" if stocks_only else "full scan"
    command = (
        market_plan.get("review_rows_command")
        or diagnostic.get("manual_template_command")
        or diagnostic.get("fix_command")
    )
    return {
        "schema_version": "priced-in-source-execution-blocker-v1",
        "status": "blocked",
        "blocked_by": "market_bars",
        "blocked_gap_rows": gaps,
        "source": source_name,
        "reason": (
            "market_bars must be complete before executing "
            f"{source_name} source batches for a {scan_label}; {gaps} "
            f"{row_label} still lack scan-date price reaction."
        ),
        "command": command,
        "external_calls_made": 0,
    }


def _execute_local_text_source_batch(
    engine: Engine,
    batch: Mapping[str, object],
) -> dict[str, object]:
    payload = _mapping(batch.get("api_payload"))
    tickers = _batch_tickers(payload, batch)
    if not tickers:
        return {"status": "blocked", "reason": "No local_text tickers are available."}
    as_of = _date_or_none(payload.get("as_of"))
    if as_of is None:
        return {"status": "blocked", "reason": "Local text batch is missing scan date."}
    available_at = _datetime_or_none(payload.get("available_at")) or datetime.now(UTC)
    result = run_text_pipeline(
        EventRepository(engine),
        TextRepository(engine),
        as_of=datetime.combine(as_of, time(21), tzinfo=UTC),
        available_at=available_at,
        tickers=tuple(tickers),
    )
    return {
        "status": "executed",
        "provider": "local_text",
        "endpoint": "features-batch",
        "as_of": as_of.isoformat(),
        "available_at": available_at.isoformat(),
        "tickers": tickers,
        "ticker_count": len(tickers),
        "feature_count": result.feature_count,
        "snippet_count": result.snippet_count,
        "external_calls_made": 0,
    }


def _execute_sec_source_batch(
    engine: Engine,
    config: AppConfig,
    batch: Mapping[str, object],
) -> dict[str, object]:
    payload = _mapping(batch.get("api_payload"))
    targets: list[SecSubmissionTarget] = []
    for target in _rows(payload.get("targets")):
        ticker = str(target.get("ticker") or "").strip().upper()
        cik = str(target.get("cik") or "").strip().zfill(10)
        if ticker and cik:
            targets.append(SecSubmissionTarget(ticker=ticker, cik=cik))
    if not targets:
        return {"status": "blocked", "reason": "No SEC targets with CIKs are available."}
    try:
        result = ingest_sec_submissions_batch(
            config=config,
            market_repo=MarketRepository(engine),
            provider_repo=ProviderRepository(engine),
            event_repo=EventRepository(engine),
            targets=tuple(targets),
        )
    except ValueError as exc:
        return {"status": "blocked", "reason": f"SEC batch blocked: {exc}"}
    except ProviderIngestError as exc:
        return {"status": "failed", "reason": f"SEC batch failed: {exc}"}
    return {"status": "executed", **result.as_payload()}


def _execute_schwab_source_batch(
    engine: Engine,
    config: AppConfig,
    source_name: str,
    batch: Mapping[str, object],
) -> dict[str, object]:
    payload = _mapping(batch.get("api_payload"))
    tickers = normalize_tickers(_batch_tickers(payload, batch))
    if not tickers:
        return {"status": "blocked", "reason": f"No {source_name} tickers are available."}
    if len(tickers) > config.schwab_market_sync_max_tickers:
        return {
            "status": "blocked",
            "reason": (
                "Too many Schwab market-sync tickers; maximum is "
                f"{config.schwab_market_sync_max_tickers}"
            ),
        }
    repo = BrokerRepository(engine)
    connection = repo.latest_connection()
    token = repo.latest_token(connection.id) if connection is not None else None
    if connection is None or token is None:
        return {"status": "blocked", "reason": "Schwab connection token is missing"}
    if connection.status != BrokerConnectionStatus.CONNECTED:
        return {
            "status": "blocked",
            "reason": f"Schwab connection is {connection.status.value}",
        }
    try:
        _acquire_schwab_rate_limit_slot(
            engine,
            operation=SCHWAB_MARKET_SYNC_OPERATION,
            min_interval_seconds=config.schwab_market_sync_min_interval_seconds,
            metadata={
                "endpoint": "/api/radar/priced-in/source-batches/execute-next",
                "source": source_name,
                "tickers": tickers,
            },
        )
        access_token = _active_access_token(config, repo, connection, token)
    except HTTPException as exc:
        return {"status": "blocked", "reason": f"Schwab batch blocked: {exc.detail}"}
    except ValueError as exc:
        return {"status": "blocked", "reason": f"Schwab batch blocked: {exc}"}
    client = SchwabClient(
        client=JsonHttpClient(UrlLibHttpTransport(), config.http_timeout_seconds),
        access_token=access_token,
        base_url=config.schwab_base_url,
    )
    try:
        snapshots = sync_market_context(
            client=client,
            repo=repo,
            tickers=tickers,
            include_history=bool(payload.get("include_history", True)),
            include_options=bool(payload.get("include_options", True)),
        )
    except RuntimeError as exc:
        return {"status": "failed", "reason": f"Schwab batch failed: {exc}"}
    option_feature_count = (
        upsert_schwab_option_features(
            feature_repo=FeatureRepository(engine),
            snapshots=snapshots,
        )
        if bool(payload.get("include_options", True))
        else 0
    )
    return {
        "status": "executed",
        "provider": "schwab",
        "endpoint": "market-sync",
        "items": [market_snapshot_payload(row) for row in snapshots],
        "option_features_upserted": option_feature_count,
        "external_calls_made": 1,
    }


def _execution_payload(
    *,
    source_name: str,
    status: str,
    plan: Mapping[str, object],
    reason: str = "",
    batch: Mapping[str, object] | None = None,
    result: Mapping[str, object] | None = None,
    external_calls_made: int = 0,
    post_execution: Mapping[str, object] | None = None,
    execution_blocker: Mapping[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "schema_version": "priced-in-source-batch-execution-v1",
        "source": source_name,
        "status": status,
        "reason": reason or None,
        "external_calls_made": max(0, external_calls_made),
        "execution_boundary": (
            "Executes at most one planned source-fill chunk. Use the plan endpoint "
            "to inspect the full-scan batch list before repeating."
        ),
        "plan": {
            "status": plan.get("status"),
            "total_gap_rows": plan.get("total_gap_rows"),
            "plannable_gap_rows": plan.get("plannable_gap_rows"),
            "batch_count": plan.get("batch_count"),
            "batch_size": plan.get("batch_size"),
            "review_rows_command": plan.get("review_rows_command"),
            "all_batches_command": plan.get("all_batches_command"),
        },
        "batch": dict(batch or {}),
        "result": dict(result or {}),
    }
    if execution_blocker is not None:
        payload["execution_blocker"] = dict(execution_blocker)
    if post_execution is not None:
        payload["post_execution"] = dict(post_execution)
    return payload


def _source_plan_summary(plan: Mapping[str, object]) -> dict[str, object]:
    return {
        "status": plan.get("status"),
        "total_gap_rows": int(_number_or_zero(plan.get("total_gap_rows"))),
        "plannable_gap_rows": int(_number_or_zero(plan.get("plannable_gap_rows"))),
        "unplannable_gap_rows": int(_number_or_zero(plan.get("unplannable_gap_rows"))),
        "batch_count": int(_number_or_zero(plan.get("batch_count"))),
        "batch_size": int(_number_or_zero(plan.get("batch_size"))),
        "next_action": plan.get("next_action"),
        "review_rows_command": plan.get("review_rows_command"),
        "all_batches_command": plan.get("all_batches_command"),
    }


def _source_batch_run_status(
    *,
    executed_batches: int,
    requested_batches: int,
    failed: Mapping[str, object] | None,
    after_summary: Mapping[str, object],
) -> str:
    if int(_number_or_zero(after_summary.get("total_gap_rows"))) <= 0:
        return "complete"
    if failed is not None:
        return "partial" if executed_batches else str(failed.get("status") or "blocked")
    if executed_batches > 0:
        return "executed"
    if requested_batches <= 0:
        return "blocked"
    return "no_action"


def _source_batch_run_next_action(
    *,
    source_name: str,
    executed_batches: int,
    requested_batches: int,
    failed: Mapping[str, object] | None,
    after_summary: Mapping[str, object],
    stop_reason: str,
) -> str:
    after_gaps = int(_number_or_zero(after_summary.get("total_gap_rows")))
    after_plannable = int(_number_or_zero(after_summary.get("plannable_gap_rows")))
    if after_gaps <= 0:
        return f"No full-scan {source_name} source gaps remain."
    if failed is not None:
        reason = str(failed.get("reason") or stop_reason or "Review the failed chunk.")
        return f"Stopped after {executed_batches} chunk(s): {reason}"
    if executed_batches > 0:
        return (
            f"Executed {executed_batches}/{requested_batches} capped chunk(s). "
            f"{after_gaps} full-scan gap row(s) remain; {after_plannable} are "
            "currently plannable. Review the next batch plan before continuing."
        )
    return str(after_summary.get("next_action") or stop_reason or "No chunk executed.")


def _post_execution_check_payload(
    *,
    source_name: str,
    before_plan: Mapping[str, object],
    after_plan: Mapping[str, object],
) -> dict[str, object]:
    before_gap_rows = int(_number_or_zero(before_plan.get("total_gap_rows")))
    after_gap_rows = int(_number_or_zero(after_plan.get("total_gap_rows")))
    before_plannable = int(_number_or_zero(before_plan.get("plannable_gap_rows")))
    after_plannable = int(_number_or_zero(after_plan.get("plannable_gap_rows")))
    before_batches = int(_number_or_zero(before_plan.get("batch_count")))
    after_batches = int(_number_or_zero(after_plan.get("batch_count")))
    gap_rows_resolved = before_gap_rows - after_gap_rows
    plannable_rows_resolved = before_plannable - after_plannable
    if after_gap_rows <= 0:
        status = "complete"
        next_action = f"No full-scan {source_name} source gaps remain."
    elif gap_rows_resolved > 0 or plannable_rows_resolved > 0:
        status = "improved"
        next_action = (
            f"Full-scan {source_name} coverage improved; "
            f"{max(gap_rows_resolved, 0)} gap row(s) and "
            f"{max(plannable_rows_resolved, 0)} plannable row(s) cleared. "
            "Review the updated next batch before executing another chunk."
        )
    else:
        status = "unchanged"
        next_action = (
            f"No full-scan {source_name} source-gap delta detected yet; "
            "refresh the dashboard or inspect the updated batch plan before repeating."
        )
    return {
        "schema_version": "priced-in-source-batch-post-execution-v1",
        "source": source_name,
        "status": status,
        "external_calls_made": 0,
        "before_gap_rows": before_gap_rows,
        "after_gap_rows": after_gap_rows,
        "gap_rows_resolved": gap_rows_resolved,
        "before_plannable_rows": before_plannable,
        "after_plannable_rows": after_plannable,
        "plannable_rows_resolved": plannable_rows_resolved,
        "before_batch_count": before_batches,
        "after_batch_count": after_batches,
        "review_rows_command": after_plan.get("review_rows_command"),
        "all_batches_command": after_plan.get("all_batches_command"),
        "next_action": next_action,
    }


def _post_execution_fallback(post_execution: Mapping[str, object]) -> str:
    source = str(post_execution.get("source") or "source").strip()
    before = int(_number_or_zero(post_execution.get("before_gap_rows")))
    after = int(_number_or_zero(post_execution.get("after_gap_rows")))
    return f"{source} full-scan source gaps moved {before}->{after}."


def _plan_block_reason(plan: Mapping[str, object], fallback: str) -> str:
    diagnostic = _mapping(plan.get("diagnostic"))
    return str(plan.get("next_action") or diagnostic.get("reason") or fallback)


def _batch_tickers(
    payload: Mapping[str, object],
    batch: Mapping[str, object],
) -> list[str]:
    raw_tickers = payload.get("tickers") or batch.get("tickers") or []
    if not isinstance(raw_tickers, list | tuple):
        return []
    return list(
        dict.fromkeys(
            str(ticker).strip().upper()
            for ticker in raw_tickers
            if str(ticker).strip()
        )
    )


def _date_or_none(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _datetime_or_none(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def _rows(value: object) -> list[Mapping[str, object]]:
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, list | tuple):
        return [row for row in value if isinstance(row, Mapping)]
    return []


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _number_or_zero(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
