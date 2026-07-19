"""Discovery proof surface: value-ledger history for discovery_row labels."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.engine import Engine

from catalyst_radar.validation.value_ledger import (
    CLAIMABLE_VALUE_LABELS,
    load_value_ledger_entries_payload,
)

PROOF_SCHEMA = "discovery-proof-v1"
DISCOVERY_LABELS = (
    "good-research",
    "useful",
    "noisy",
    "too-late",
    "false-positive",
    "too-early",
    "duplicate",
    "ignored",
)


def build_discovery_proof(
    *,
    engine: Engine | None,
    limit: int = 50,
    ticker: str | None = None,
) -> dict[str, object]:
    """Return recent discovery_row value-ledger labels for the Proof panel."""
    if engine is None:
        return {
            "schema_version": PROOF_SCHEMA,
            "status": "no_db",
            "headline": "No local database — labels cannot be stored yet.",
            "count": 0,
            "entries": [],
            "summary": {
                "total": 0,
                "by_label": {},
                "claimable_count": 0,
                "claimable_value_usd": 0.0,
                "unique_tickers": 0,
            },
            "label_choices": list(DISCOVERY_LABELS),
            "next_action": (
                "Point CATALYST_DATABASE_URL at the local DB, then label leads from "
                "the case panel."
            ),
            "investment_advice": False,
            "external_calls_made": 0,
            "db_writes_made": 0,
        }

    try:
        payload = load_value_ledger_entries_payload(
            engine,
            ticker=ticker,
            limit=max(1, min(int(limit), 200)),
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "schema_version": PROOF_SCHEMA,
            "status": "error",
            "error": str(exc),
            "headline": "Could not load discovery labels from the value ledger.",
            "count": 0,
            "entries": [],
            "summary": {
                "total": 0,
                "by_label": {},
                "claimable_count": 0,
                "claimable_value_usd": 0.0,
                "unique_tickers": 0,
            },
            "label_choices": list(DISCOVERY_LABELS),
            "next_action": "Check the local database, then retry.",
            "investment_advice": False,
            "external_calls_made": 0,
            "db_writes_made": 0,
        }

    raw_entries = payload.get("entries") if isinstance(payload, Mapping) else []
    rows: list[dict[str, object]] = []
    if isinstance(raw_entries, list):
        for item in raw_entries:
            if not isinstance(item, Mapping):
                continue
            if str(item.get("artifact_type") or "") != "discovery_row":
                continue
            rows.append(dict(item))

    by_label = Counter(str(row.get("label") or "unknown") for row in rows)
    claimable = [
        row
        for row in rows
        if str(row.get("label") or "") in CLAIMABLE_VALUE_LABELS
    ]
    claimable_value = 0.0
    for row in claimable:
        try:
            claimable_value += float(row.get("estimated_value_usd") or 0.0)
        except (TypeError, ValueError):
            continue
    tickers = {
        str(row.get("ticker") or "").upper()
        for row in rows
        if str(row.get("ticker") or "").strip()
    }
    summary = {
        "total": len(rows),
        "by_label": dict(sorted(by_label.items())),
        "claimable_count": len(claimable),
        "claimable_value_usd": round(claimable_value, 2),
        "unique_tickers": len(tickers),
    }
    if not rows:
        headline = "No discovery labels yet — rate a few leads from the case panel."
        next_action = (
            "Open a discovery lead, use the disposition buttons, and execute a label."
        )
        status = "empty"
    else:
        headline = (
            f"{len(rows)} discovery label(s) · "
            f"{summary['claimable_count']} claimable · "
            f"${summary['claimable_value_usd']:.0f} estimated attention value"
        )
        next_action = (
            "Review whether high-gap leads you labeled good-research still look useful."
        )
        status = "ready"

    return {
        "schema_version": PROOF_SCHEMA,
        "status": status,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "headline": headline,
        "count": len(rows),
        "entries": rows[: max(1, min(int(limit), 200))],
        "summary": summary,
        "label_choices": list(DISCOVERY_LABELS),
        "next_action": next_action,
        "investment_advice": False,
        "can_make_investment_decision": False,
        "decision_support_only": True,
        "external_calls_made": 0,
        "db_writes_made": 0,
    }
