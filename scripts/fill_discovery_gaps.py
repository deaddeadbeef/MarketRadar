"""Fill discovery join gaps: bars for recent sessions + scan mapped tickers."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from catalyst_radar.security.secrets import load_app_dotenv


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-external-call", action="store_true")
    parser.add_argument("--capture-days", type=int, default=8)
    parser.add_argument("--events", type=Path, default=Path("data/local/world_events.json"))
    parser.add_argument("--as-of", type=date.fromisoformat)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def _cli(*args: str) -> subprocess.CompletedProcess[str]:
    python = ROOT / ".venv" / "Scripts" / "python.exe"
    cmd = [str(python if python.is_file() else "python"), "-m", "catalyst_radar.cli", *args]
    return subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


def _weekday_sessions(end: date, count: int) -> list[date]:
    """Approximate US trading sessions (weekdays only; ignores holidays)."""
    sessions: list[date] = []
    cursor = end
    while len(sessions) < max(1, count):
        if cursor.weekday() < 5:
            sessions.append(cursor)
        cursor -= timedelta(days=1)
        if (end - cursor).days > 60:
            break
    return list(reversed(sessions))


def _mapped_tickers(events_path: Path) -> list[str]:
    from catalyst_radar.discovery.brief import build_discovery_brief

    brief = build_discovery_brief(events_path=events_path, engine=None, limit=200)
    tickers: list[str] = []
    for row in brief.get("discoveries") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if ticker and ticker not in tickers:
            tickers.append(ticker)
    # Always include SPY for scan benchmarks.
    if "SPY" not in tickers:
        tickers.append("SPY")
    return tickers


def _parse_json_stdout(proc: subprocess.CompletedProcess[str]) -> dict[str, object]:
    text = (proc.stdout or "").strip()
    if not text:
        return {
            "status": "empty_stdout",
            "returncode": proc.returncode,
            "stderr": (proc.stderr or "")[-2000:],
        }
    # CLI may print non-JSON lines; take the last JSON object line.
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                payload.setdefault("returncode", proc.returncode)
                return payload
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {
            "status": "non_json",
            "returncode": proc.returncode,
            "stdout_tail": text[-2000:],
            "stderr": (proc.stderr or "")[-2000:],
        }
    if isinstance(payload, dict):
        payload.setdefault("returncode", proc.returncode)
        return payload
    return {"status": "unexpected_payload", "returncode": proc.returncode}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_app_dotenv()
    from catalyst_radar.core.config import AppConfig
    from catalyst_radar.discovery.brief import build_discovery_brief, default_events_path
    from catalyst_radar.storage.db import engine_from_url

    events_path = args.events if args.events.is_file() else default_events_path()
    config = AppConfig.from_env()
    tickers = _mapped_tickers(Path(events_path))
    # Prefer last weekday before "today" for live capture target.
    today = datetime.now(tz=UTC).date()
    target_as_of = args.as_of
    if target_as_of is None:
        sessions = _weekday_sessions(today - timedelta(days=1), 1)
        target_as_of = sessions[-1] if sessions else today - timedelta(days=1)

    plan: dict[str, object] = {
        "schema_version": "fill-discovery-gaps-plan-v1",
        "mode": "execute" if args.execute else "preview",
        "events_path": str(events_path),
        "database_url": config.database_url,
        "polygon_configured": bool(config.polygon_api_key_configured),
        "mapped_ticker_count": len(tickers),
        "mapped_tickers_sample": tickers[:20],
        "target_as_of": target_as_of.isoformat(),
        "capture_days": args.capture_days,
        "confirm_external_call": bool(args.confirm_external_call),
        "steps": [],
        "external_calls_planned": 0,
        "external_calls_made": 0,
        "db_writes_made": 0,
    }
    steps: list[dict[str, object]] = []

    # 1) Import local saved fixtures only when that as-of date still has thin bar coverage.
    from sqlalchemy import text

    engine = engine_from_url(config.database_url)
    existing_bar_counts: dict[str, int] = {}
    with engine.connect() as conn:
        for row in conn.execute(
            text("SELECT date, COUNT(*) AS n FROM daily_bars GROUP BY date")
        ):
            existing_bar_counts[str(row[0])[:10]] = int(row[1] or 0)

    local_fixtures = sorted(ROOT.glob("data/local/polygon-grouped-daily-*.json"))
    for fixture in local_fixtures:
        stamp = fixture.stem.replace("polygon-grouped-daily-", "")
        try:
            fixture_date = date.fromisoformat(stamp)
        except ValueError:
            continue
        have = existing_bar_counts.get(fixture_date.isoformat(), 0)
        cmd = [
            "market-bars",
            "saved-import",
            "--expected-as-of",
            fixture_date.isoformat(),
            "--fixture",
            str(fixture),
            "--json",
        ]
        step: dict[str, object] = {
            "action": "saved_import",
            "date": fixture_date.isoformat(),
            "fixture": str(fixture),
            "existing_bar_count": have,
            "command": "catalyst-radar " + " ".join(cmd),
        }
        # Full-market fixtures land ~10k+ rows; skip slow re-import when already loaded.
        if have >= 5000:
            step["result"] = {
                "status": "skipped_already_loaded",
                "existing_bar_count": have,
            }
            steps.append(step)
            continue
        if args.execute:
            proc = _cli(*cmd, "--execute")
            payload = _parse_json_stdout(proc)
            step["result"] = {
                "status": payload.get("status"),
                "executed": payload.get("executed"),
                "db_writes_made": payload.get("db_writes_made"),
                "returncode": payload.get("returncode"),
            }
            if payload.get("db_writes_made"):
                plan["db_writes_made"] = int(plan["db_writes_made"] or 0) + int(
                    payload.get("db_writes_made") or 0
                )
            if payload.get("status") == "imported":
                existing_bar_counts[fixture_date.isoformat()] = max(
                    have, int(payload.get("daily_bar_count") or 0)
                )
        else:
            step["result"] = {"status": "preview_only"}
        steps.append(step)

    # 2) Capture recent sessions via Polygon grouped-daily (explicit external calls).
    capture_sessions = _weekday_sessions(target_as_of, args.capture_days)
    for session in capture_sessions:
        out = ROOT / "data" / "local" / f"polygon-grouped-daily-{session.isoformat()}.json"
        capture_cmd = [
            "market-bars",
            "saved-capture",
            "--expected-as-of",
            session.isoformat(),
            "--out",
            str(out),
            "--json",
        ]
        import_cmd = [
            "market-bars",
            "saved-import",
            "--expected-as-of",
            session.isoformat(),
            "--fixture",
            str(out),
            "--json",
        ]
        step = {
            "action": "capture_and_import",
            "date": session.isoformat(),
            "out": str(out),
            "already_on_disk": out.is_file(),
            "capture_command": "catalyst-radar " + " ".join(capture_cmd),
            "import_command": "catalyst-radar " + " ".join(import_cmd),
        }
        have = existing_bar_counts.get(session.isoformat(), 0)
        step["existing_bar_count"] = have
        if have >= 5000:
            step["capture_result"] = {"status": "skipped_already_loaded"}
            steps.append(step)
            continue
        if out.is_file():
            plan["external_calls_planned"] = int(plan["external_calls_planned"] or 0)
            if args.execute:
                proc = _cli(*import_cmd, "--execute")
                payload = _parse_json_stdout(proc)
                step["import_result"] = {
                    "status": payload.get("status"),
                    "db_writes_made": payload.get("db_writes_made"),
                    "returncode": payload.get("returncode"),
                }
                if payload.get("db_writes_made"):
                    plan["db_writes_made"] = int(plan["db_writes_made"] or 0) + int(
                        payload.get("db_writes_made") or 0
                    )
                if payload.get("status") == "imported":
                    existing_bar_counts[session.isoformat()] = max(
                        have, int(payload.get("daily_bar_count") or 0)
                    )
            steps.append(step)
            continue

        plan["external_calls_planned"] = int(plan["external_calls_planned"] or 0) + 1
        # Preview first to read approval-guard expectation counts.
        preview_proc = _cli(*capture_cmd)
        preview = _parse_json_stdout(preview_proc)
        guard = preview.get("approval_guard") if isinstance(preview.get("approval_guard"), dict) else {}
        expect_active = guard.get("expected_active_security_count")
        expect_existing = guard.get("expected_existing_as_of_bar_count")
        expect_missing = guard.get("expected_missing_as_of_bar_count")
        step["approval_guard"] = {
            "expected_active_security_count": expect_active,
            "expected_existing_as_of_bar_count": expect_existing,
            "expected_missing_as_of_bar_count": expect_missing,
            "preview_status": preview.get("status"),
        }
        if args.execute and args.confirm_external_call:
            if None in (expect_active, expect_existing, expect_missing):
                step["capture_result"] = {
                    "status": "missing_approval_guard_counts",
                    "preview": {
                        "status": preview.get("status"),
                        "returncode": preview.get("returncode"),
                    },
                }
            else:
                proc = _cli(
                    *capture_cmd,
                    "--expect-active-count",
                    str(int(expect_active)),
                    "--expect-existing-count",
                    str(int(expect_existing)),
                    "--expect-missing-count",
                    str(int(expect_missing)),
                    "--confirm-external-call",
                )
                payload = _parse_json_stdout(proc)
                step["capture_result"] = {
                    "status": payload.get("status"),
                    "external_calls_made": payload.get("external_calls_made"),
                    "returncode": payload.get("returncode"),
                    "stderr_tail": (proc.stderr or "")[-500:],
                    "stdout_tail": (proc.stdout or "")[-500:],
                }
                if payload.get("external_calls_made"):
                    plan["external_calls_made"] = int(plan["external_calls_made"] or 0) + int(
                        payload.get("external_calls_made") or 0
                    )
                if out.is_file():
                    proc2 = _cli(*import_cmd, "--execute")
                    payload2 = _parse_json_stdout(proc2)
                    step["import_result"] = {
                        "status": payload2.get("status"),
                        "db_writes_made": payload2.get("db_writes_made"),
                        "returncode": payload2.get("returncode"),
                    }
                    if payload2.get("db_writes_made"):
                        plan["db_writes_made"] = int(plan["db_writes_made"] or 0) + int(
                            payload2.get("db_writes_made") or 0
                        )
        elif args.execute and not args.confirm_external_call:
            step["capture_result"] = {
                "status": "blocked_missing_confirm_external_call",
            }
        else:
            step["capture_result"] = {"status": "preview_only"}
        steps.append(step)

    # 3) Rescan mapped tickers on the latest date that actually has local bars.
    scan_as_of = target_as_of
    with engine.connect() as conn:
        latest_bar = conn.execute(text("SELECT MAX(date) FROM daily_bars")).scalar()
        if latest_bar is not None:
            if hasattr(latest_bar, "isoformat"):
                scan_as_of = (
                    latest_bar
                    if isinstance(latest_bar, date)
                    else date.fromisoformat(str(latest_bar)[:10])
                )
            else:
                scan_as_of = date.fromisoformat(str(latest_bar)[:10])
    available_at = datetime.now(tz=UTC).isoformat()
    scan_args = [
        "run-daily",
        "--as-of",
        scan_as_of.isoformat(),
        "--available-at",
        available_at,
        "--provider",
        "off",
        "--json",
    ]
    for ticker in tickers[:80]:
        scan_args.extend(["--ticker", ticker])
    step = {
        "action": "run_daily_mapped",
        "as_of": scan_as_of.isoformat(),
        "requested_as_of": target_as_of.isoformat(),
        "ticker_count": min(len(tickers), 80),
        "command": "catalyst-radar " + " ".join(scan_args),
    }
    if args.execute:
        proc = _cli(*scan_args)
        payload = _parse_json_stdout(proc)
        step["result"] = {
            "status": payload.get("status") or payload.get("run_status"),
            "reason": payload.get("reason"),
            "daily_result": payload.get("daily_result"),
            "returncode": payload.get("returncode"),
            "stderr_tail": (proc.stderr or "")[-800:],
            "stdout_tail": (proc.stdout or "")[-1200:],
            "keys": sorted(payload.keys())[:30] if isinstance(payload, dict) else [],
        }
        # Some run-daily paths print multi-line progress; accept exit 0 as success.
        if proc.returncode == 0:
            step["result"]["status"] = step["result"].get("status") or "completed"
    else:
        step["result"] = {"status": "preview_only"}
    steps.append(step)

    # 4) Discovery brief join report (always runs; zero external calls).
    brief = build_discovery_brief(
        events_path=events_path,
        engine=engine,
        limit=25,
    )
    plan["steps"] = steps
    plan["discovery_brief"] = {
        "headline": brief.get("headline"),
        "join_coverage": brief.get("join_coverage"),
        "freshness_status": brief.get("freshness_status"),
        "top": [
            {
                "ticker": row.get("ticker"),
                "join_status": row.get("join_status"),
                "priced_in_status": row.get("priced_in_status"),
                "reaction_score": row.get("reaction_score"),
                "ret_5d_pct": row.get("ret_5d_pct"),
                "quiet_tape": row.get("quiet_tape"),
                "usefulness": row.get("usefulness"),
            }
            for row in (brief.get("discoveries") or [])[:10]
            if isinstance(row, dict)
        ],
    }
    plan["status"] = "ready" if args.execute else "preview"
    plan["next_action"] = (
        "Review discovery-brief join coverage and World Events desktop."
        if args.execute
        else "Re-run with -Execute -ConfirmExternalCall to capture missing sessions and scan."
    )

    if args.json:
        print(json.dumps(plan, sort_keys=True, default=str))
    else:
        print(f"status={plan['status']} as_of={target_as_of} tickers={len(tickers)}")
        print(f"external_planned={plan['external_calls_planned']} made={plan['external_calls_made']}")
        cov = (plan.get("discovery_brief") or {}).get("join_coverage")  # type: ignore[union-attr]
        print(f"join_coverage={cov}")
        print(f"next={plan['next_action']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
