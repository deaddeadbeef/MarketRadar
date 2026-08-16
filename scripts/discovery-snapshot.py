"""Lightweight zero-call snapshot for World Events desktop browsing.

The desktop client appends CLI-style flags to whatever snapshot command is
configured (e.g. --page world-events --ticker MU --scan-limit 50). Accept and
honor the useful ones; ignore the rest so the process never dies on unknown args.

Also supports local discovery commands (labels) via --command for the proof loop:
  discovery-snapshot.py --command "label FRO good-research --execute"
"""
from __future__ import annotations

import argparse
import json
import shlex
import sys
from datetime import UTC, datetime
from pathlib import Path

# Ensure repo src is importable when launched from radar-desktop.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from catalyst_radar.discovery.brief import (
    build_discovery_brief,
    classify_events_path,
    default_events_path,
    empty_world_events_brief,
)
from catalyst_radar.discovery.case_file import build_discovery_case_file
from catalyst_radar.discovery.proof import build_discovery_proof
from catalyst_radar.security.secrets import load_app_dotenv


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--page", default="world-events")
    parser.add_argument("--ticker")
    parser.add_argument("--events", type=Path)
    parser.add_argument("--available-at")
    parser.add_argument("--alert-status")
    parser.add_argument("--alert-route")
    parser.add_argument("--priced-in-status")
    parser.add_argument("--usefulness")
    parser.add_argument("--source-gap", action="append", default=[])
    parser.add_argument("--decision-gap", action="append", default=[])
    parser.add_argument("--stocks-only", action="store_true")
    parser.add_argument("--scan-limit", type=int, default=20)
    parser.add_argument("--scan-offset", type=int, default=0)
    parser.add_argument("--telemetry-limit", type=int, default=8)
    parser.add_argument("--database-url")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--fast", action="store_true")
    parser.add_argument(
        "--command",
        help="Optional discovery local command, e.g. 'label MU good-research --execute'.",
    )
    # Desktop may pass extra unknown flags as the client evolves.
    args, _unknown = parser.parse_known_args(argv)
    return args


def _local_engine(database_url: str | None = None):
    """Best-effort local DB for priced-in join and value-ledger labels."""
    try:
        from catalyst_radar.core.config import AppConfig
        from catalyst_radar.storage.db import create_schema, engine_from_url

        # Prefer .env.local (same as CLI) so desktop joins the live local DB.
        load_app_dotenv()
        config = AppConfig.from_env()
        url = (database_url or config.database_url or "").strip()
        if not url:
            return None
        engine = engine_from_url(url)
        create_schema(engine)
        return engine
    except Exception:
        return None


def _emit(payload: dict[str, object]) -> int:
    sys.stdout.write(json.dumps(payload, sort_keys=True, default=str))
    sys.stdout.write("\n")
    return 0


def _handle_command(
    *,
    command_text: str,
    events_path: Path | str,
    engine,
    ticker_hint: str | None,
) -> int:
    """Run a local discovery command and emit dashboard-command-result shaped JSON."""
    raw = str(command_text or "").strip()
    if not raw:
        return _emit(
            {
                "schema_version": "dashboard-command-result-v1",
                "status": "error",
                "message": "Empty discovery command.",
                "external_calls_made": 0,
                "db_writes_made": 0,
                "investment_advice": False,
            }
        )

    try:
        tokens = shlex.split(raw, posix=False)
    except ValueError as exc:
        return _emit(
            {
                "schema_version": "dashboard-command-result-v1",
                "status": "error",
                "message": f"Could not parse command: {exc}",
                "external_calls_made": 0,
                "db_writes_made": 0,
                "investment_advice": False,
            }
        )

    if not tokens:
        return _emit(
            {
                "schema_version": "dashboard-command-result-v1",
                "status": "error",
                "message": "Empty discovery command tokens.",
                "external_calls_made": 0,
                "db_writes_made": 0,
                "investment_advice": False,
            }
        )

    head = tokens[0].strip().lower()
    # Accept: label TICKER LABEL [--execute]
    #     or: discovery-label --ticker X --label Y [--execute]
    label = None
    ticker = (ticker_hint or "").strip().upper() or None
    event_id = None
    execute = False

    if head in {"outcomes", "discovery-outcomes", "discovery_outcomes"}:
        if engine is None:
            return _emit(
                {
                    "schema_version": "dashboard-command-result-v1",
                    "status": "error",
                    "message": "Local database unavailable; cannot update outcomes.",
                    "external_calls_made": 0,
                    "db_writes_made": 0,
                    "investment_advice": False,
                }
            )
        execute = any(tok.lower() in {"--execute", "-x"} for tok in tokens[1:])
        from catalyst_radar.discovery.outcomes import build_discovery_outcomes_update

        result = build_discovery_outcomes_update(engine=engine, execute=execute, limit=50)
        counts = result.get("counts") if isinstance(result.get("counts"), dict) else {}
        return _emit(
            {
                "schema_version": "dashboard-command-result-v1",
                "status": "ok",
                "message": (
                    f"Discovery outcomes {result.get('mode')}: "
                    f"computed={counts.get('computed')} "
                    f"insufficient={counts.get('insufficient_data')} "
                    f"writes={result.get('db_writes_made')}"
                ),
                "page": "world-events",
                "result": result,
                "external_calls_made": 0,
                "db_writes_made": int(result.get("db_writes_made") or 0),
                "investment_advice": False,
            }
        )

    if head in {"label", "discovery-label", "discovery_label"}:
        rest = tokens[1:]
        i = 0
        positional: list[str] = []
        while i < len(rest):
            tok = rest[i]
            low = tok.lower()
            if low in {"--execute", "-x"}:
                execute = True
                i += 1
                continue
            if low in {"--preview"}:
                execute = False
                i += 1
                continue
            if low in {"--ticker", "-t"} and i + 1 < len(rest):
                ticker = rest[i + 1].strip().upper()
                i += 2
                continue
            if low in {"--label", "-l"} and i + 1 < len(rest):
                label = rest[i + 1].strip()
                i += 2
                continue
            if low in {"--event-id", "--event"} and i + 1 < len(rest):
                event_id = rest[i + 1].strip()
                i += 2
                continue
            if low.startswith("--"):
                i += 1
                continue
            positional.append(tok)
            i += 1
        if label is None and len(positional) >= 2:
            ticker = positional[0].strip().upper()
            label = positional[1].strip()
        elif label is None and len(positional) == 1 and ticker:
            label = positional[0].strip()
        elif label is None and len(positional) == 1:
            # label good-research with ticker from filter
            label = positional[0].strip()
    else:
        return _emit(
            {
                "schema_version": "dashboard-command-result-v1",
                "status": "error",
                "message": (
                    "Unknown discovery command. Use: "
                    "label TICKER good-research|noisy|too-late|false-positive|useful [--execute] "
                    "OR outcomes [--execute]"
                ),
                "external_calls_made": 0,
                "db_writes_made": 0,
                "investment_advice": False,
            }
        )

    if not ticker or not label:
        return _emit(
            {
                "schema_version": "dashboard-command-result-v1",
                "status": "error",
                "message": "label requires ticker and label name.",
                "external_calls_made": 0,
                "db_writes_made": 0,
                "investment_advice": False,
            }
        )
    if engine is None:
        return _emit(
            {
                "schema_version": "dashboard-command-result-v1",
                "status": "error",
                "message": "Local database unavailable; cannot write labels.",
                "external_calls_made": 0,
                "db_writes_made": 0,
                "investment_advice": False,
            }
        )

    from catalyst_radar.discovery.label import build_discovery_label_payload

    try:
        result = build_discovery_label_payload(
            engine=engine,
            ticker=ticker,
            label=label,
            event_id=event_id,
            events_path=events_path,
            execute=execute,
        )
    except Exception as exc:  # noqa: BLE001
        return _emit(
            {
                "schema_version": "dashboard-command-result-v1",
                "status": "error",
                "message": f"Label failed: {exc}",
                "external_calls_made": 0,
                "db_writes_made": 0,
                "investment_advice": False,
            }
        )

    status = str(result.get("label_status") or "preview")
    message = (
        f"Labeled {ticker} as {label} ({status})."
        if status in {"written", "preview"}
        else str(result.get("next_action") or result.get("headline") or status)
    )
    return _emit(
        {
            "schema_version": "dashboard-command-result-v1",
            "status": "ok" if status in {"written", "preview"} else status,
            "message": message,
            "page": "world-events",
            "filters": {"ticker": ticker},
            "result": result,
            "external_calls_made": int(result.get("external_calls_made") or 0),
            "db_writes_made": int(result.get("db_writes_made") or 0),
            "investment_advice": False,
            "can_make_investment_decision": False,
            "decision_support_only": True,
        }
    )


def main(argv: list[str] | None = None) -> int:
    # Load operator env before resolving default events/database paths.
    load_app_dotenv()
    args = parse_args(argv)
    events_path = Path(args.events) if args.events else default_events_path()
    events_kind = classify_events_path(events_path)

    engine = _local_engine(args.database_url)
    focus = (args.ticker or "").strip().upper()

    if args.command:
        return _handle_command(
            command_text=args.command,
            events_path=events_path,
            engine=engine,
            ticker_hint=focus or None,
        )

    limit = max(1, min(int(args.scan_limit or 20), 50))

    try:
        from catalyst_radar.discovery.ux import (
            apply_novice_case_file,
            apply_novice_ux,
            company_name,
            _price_detail,
        )

        # Desktop must not serve data/sample as today's briefing.
        if events_kind != "local":
            brief = empty_world_events_brief(
                events_path=events_path,
                events_path_kind=events_kind,
            )
        else:
            brief = build_discovery_brief(
                events_path=events_path,
                engine=engine,
                limit=limit,
            )
            brief["events_path_kind"] = events_kind
        brief = apply_novice_ux(brief)
        if events_kind == "local":
            brief["status"] = "ready"
        discoveries = brief.get("discoveries") or []
        novice = brief.get("novice") if isinstance(brief.get("novice"), dict) else {}
        if not focus:
            focus = str(novice.get("focus_ticker") or "").strip().upper()
        if not focus and discoveries and isinstance(discoveries[0], dict):
            focus = str(discoveries[0].get("ticker") or "").strip().upper()
        if focus and events_kind == "local":
            case = build_discovery_case_file(
                ticker=focus,
                events_path=events_path,
                engine=engine,
            )
            discovery_row = case.get("discovery") if isinstance(case.get("discovery"), dict) else {}
            case["company_name"] = company_name(focus)
            case["price_detail"] = _price_detail(
                discovery_row.get("ret_5d_pct"),
                joined=str(discovery_row.get("join_status")) == "joined",
            )
            brief["case_file"] = apply_novice_case_file(case)
        proof = build_discovery_proof(engine=engine, limit=40)
        brief["proof"] = proof
        brief["events_path_kind"] = events_kind
    except Exception as exc:
        brief = {
            "schema_version": "discovery-brief-v1",
            "status": "error",
            "error": str(exc),
            "events": [],
            "discoveries": [],
            "events_path": str(events_path),
            "events_path_kind": events_kind,
            "headline": f"Discovery snapshot failed: {exc}",
            "external_calls_made": 0,
            "db_writes_made": 0,
            "investment_advice": False,
        }
        proof = build_discovery_proof(engine=None, limit=40)

    now = datetime.now(tz=UTC).isoformat()
    try:
        from catalyst_radar.core.config import AppConfig
        from catalyst_radar.deprecation import SCOPE_VERSION, product_scope_payload

        app_config = AppConfig.from_env()
        legacy_on = bool(app_config.enable_legacy_workbench)
        scope_payload = product_scope_payload()
    except Exception:
        legacy_on = False
        scope_payload = {}
        SCOPE_VERSION = "event-first-discovery-v1"

    proof_payload = proof if "proof" not in brief else brief.get("proof")
    goal = brief.get("goal_status") if isinstance(brief.get("goal_status"), dict) else {}
    proof_summary = (
        proof_payload.get("summary")
        if isinstance(proof_payload, dict) and isinstance(proof_payload.get("summary"), dict)
        else {}
    )
    if isinstance(goal, dict):
        goal = {
            **goal,
            "proof_label_count": int(proof_summary.get("total") or 0),
            "proof_claimable_count": int(proof_summary.get("claimable_count") or 0),
            "proof_ok": int(proof_summary.get("total") or 0) > 0,
        }
        brief["goal_status"] = goal

    payload = {
        "schema_version": "dashboard-cli-snapshot-v1",
        "snapshot_mode": "discovery_fast",
        "generated_at": now,
        "status": "discovery_ready",
        "first_blocker": None,
        "next_action": brief.get("next_action")
        or "Review World Events discovery queue as research-only leads.",
        "next_command": brief.get("next_command")
        or f"catalyst-radar discovery-brief --events {events_path} --json",
        "canonical_next_action": brief.get("canonical_next_action") or brief.get("next_action"),
        "canonical_next_command": brief.get("canonical_next_command")
        or brief.get("next_command"),
        "external_calls_made": 0,
        "events_path": str(events_path),
        "events_path_kind": events_kind,
        "event_discovery": brief,
        "discovery_proof": proof_payload,
        "product_ui": {
            "schema_version": "market-radar-product-ui-v1",
            "scope_version": SCOPE_VERSION,
            "enable_legacy_workbench": legacy_on,
            "active_pages": ["world-events", "help"],
            "docs": {
                "scope": "docs/PRODUCT_SCOPE.md",
                "deprecation": "docs/DEPRECATION.md",
            },
            "goal_status": goal,
        },
        "product_scope": {
            "scope_version": scope_payload.get("scope_version") if scope_payload else SCOPE_VERSION,
            "desktop_active": (scope_payload.get("desktop_pages") or {}).get("active")
            if scope_payload
            else ["help", "world-events"],
        },
        "candidates": {"count": 0, "rows": []},
        "alerts": {"count": 0, "rows": []},
        "themes": {"count": 0, "rows": []},
        "investment_advice": False,
        "can_make_investment_decision": False,
        "decision_support_only": True,
        "selected_page": args.page or "world-events",
    }
    # Print JSON only — no logging on stdout (desktop parses the whole stream).
    return _emit(payload)


if __name__ == "__main__":
    raise SystemExit(main())
