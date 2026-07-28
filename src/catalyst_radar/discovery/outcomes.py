"""Forward outcomes for discovery_row value-ledger labels (local bars only)."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.engine import Engine

from catalyst_radar.validation.value_ledger import load_value_ledger_entries_payload
from catalyst_radar.validation.value_outcomes import (
    load_value_outcomes_payload,
    value_outcome_update_payload,
)

OUTCOMES_SCHEMA = "discovery-outcomes-v1"


def build_discovery_outcomes_update(
    *,
    engine: Engine,
    execute: bool = False,
    limit: int = 50,
    outcome_available_at: datetime | None = None,
) -> dict[str, object]:
    """Preview/execute value-outcome updates for discovery_row ledger entries."""
    cutoff = outcome_available_at or datetime.now(tz=UTC)
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=UTC)
    else:
        cutoff = cutoff.astimezone(UTC)

    ledger = load_value_ledger_entries_payload(engine, limit=max(1, min(int(limit), 200)))
    raw = ledger.get("entries") if isinstance(ledger, Mapping) else []
    discovery_entries = [
        row
        for row in (raw or [])
        if isinstance(row, Mapping) and str(row.get("artifact_type") or "") == "discovery_row"
    ]

    results: list[dict[str, object]] = []
    computed = 0
    insufficient = 0
    missing = 0
    written = 0
    for row in discovery_entries:
        ledger_id = str(row.get("id") or "").strip()
        if not ledger_id:
            continue
        try:
            payload = value_outcome_update_payload(
                engine,
                value_ledger_entry_id=ledger_id,
                outcome_available_at=cutoff,
                execute=execute,
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "ledger_id": ledger_id,
                    "ticker": row.get("ticker"),
                    "status": "error",
                    "error": str(exc),
                }
            )
            missing += 1
            continue
        outcome = payload.get("outcome") if isinstance(payload.get("outcome"), Mapping) else {}
        status = str(outcome.get("status") or payload.get("mode") or "unknown")
        if status == "computed":
            computed += 1
        elif status == "insufficient_data":
            insufficient += 1
        else:
            missing += 1
        if execute and int(payload.get("db_writes_made") or 0) > 0:
            written += 1
        results.append(
            {
                "ledger_id": ledger_id,
                "ticker": row.get("ticker") or outcome.get("ticker"),
                "label": row.get("label"),
                "status": status,
                "trading_days_observed": outcome.get("trading_days_observed"),
                "return_5d": outcome.get("return_5d"),
                "return_10d": outcome.get("return_10d"),
                "return_20d": outcome.get("return_20d"),
                "spy_return_5d": outcome.get("spy_return_5d"),
                "mode": payload.get("mode"),
            }
        )

    return {
        "schema_version": OUTCOMES_SCHEMA,
        "mode": "executed" if execute else "preview",
        "outcome_available_at": cutoff.isoformat(),
        "discovery_ledger_count": len(discovery_entries),
        "result_count": len(results),
        "counts": {
            "computed": computed,
            "insufficient_data": insufficient,
            "missing_or_error": missing,
            "written": written,
        },
        "results": results,
        "external_calls_made": 0,
        "db_writes_made": written if execute else 0,
        "db_writes_required": len(discovery_entries) if execute else 0,
        "investment_advice": False,
        "next_action": (
            "Outcomes updated from local bars where horizon data exists."
            if execute
            else "Preview only. Re-run with --execute to write outcomes."
        ),
    }


def attach_outcomes_to_proof(
    proof: Mapping[str, Any],
    *,
    engine: Engine | None,
    limit: int = 50,
) -> dict[str, object]:
    """Enrich a discovery proof payload with latest outcomes for labeled rows."""
    out = dict(proof)
    if engine is None:
        out["outcomes"] = {
            "status": "no_db",
            "count": 0,
            "rows": [],
        }
        return out
    try:
        payload = load_value_outcomes_payload(engine, limit=max(1, min(int(limit), 200)))
    except Exception as exc:  # noqa: BLE001
        out["outcomes"] = {"status": "error", "error": str(exc), "count": 0, "rows": []}
        return out

    ledger_ids = {
        str(row.get("id") or "")
        for row in (proof.get("entries") or [])
        if isinstance(row, Mapping)
    }
    rows_out: list[dict[str, object]] = []
    raw_outcomes = payload.get("outcomes") if isinstance(payload.get("outcomes"), list) else []
    for row in raw_outcomes:
        if not isinstance(row, Mapping):
            continue
        lid = str(row.get("value_ledger_entry_id") or "")
        if ledger_ids and lid and lid not in ledger_ids:
            continue
        rows_out.append(dict(row))

    computed = sum(1 for r in rows_out if str(r.get("status")) == "computed")
    out["outcomes"] = {
        "status": "ready" if rows_out else "empty",
        "count": len(rows_out),
        "computed_count": computed,
        "rows": rows_out[:limit],
        "next_action": (
            "Outcomes available for labeled discoveries with enough local bars."
            if rows_out
            else "Run discovery-outcomes --execute after labels exist and bars advance."
        ),
    }
    summary = dict(out.get("summary") or {}) if isinstance(out.get("summary"), Mapping) else {}
    summary["outcome_count"] = len(rows_out)
    summary["outcome_computed_count"] = computed
    out["summary"] = summary
    return out
