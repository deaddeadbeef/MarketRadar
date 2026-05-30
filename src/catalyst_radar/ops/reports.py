from __future__ import annotations

import html
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

REPORT_SCHEMA_VERSION = "ops-run-report-v1"


def build_ops_run_report_payload(
    *,
    result: Mapping[str, object],
    snapshot: Mapping[str, object],
    terminal_text: str,
) -> dict[str, object]:
    summary = _mapping(result.get("summary"))
    artifacts = _artifact_rows(result.get("artifacts"))
    next_action = _text(snapshot.get("next_action") or snapshot.get("canonical_next_action"))
    next_command = _text(snapshot.get("next_command") or snapshot.get("canonical_next_command"))
    external_calls = _int(snapshot.get("external_calls_made"), default=0)
    rows = _candidate_rows(snapshot)
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "run": {
            "run_id": _text(result.get("run_id")),
            "action": _text(result.get("action")),
            "page": _text(result.get("page")),
            "status": _text(result.get("status")),
            "started_at": _text(result.get("started_at")),
            "finished_at": _text(result.get("finished_at")),
            "elapsed_ms": _int(result.get("elapsed_ms"), default=0),
            "capture_mode": _text(result.get("capture_mode")),
            "renderer": _text(result.get("renderer")),
            "requested_renderer": _text(result.get("requested_renderer")),
        },
        "summary": {
            "status": _text(snapshot.get("status") or summary.get("dashboard_status")),
            "snapshot_mode": _text(snapshot.get("snapshot_mode") or summary.get("snapshot_mode")),
            "row_count": len(rows),
            "external_calls_made": external_calls,
            "provider_calls": external_calls,
            "first_blocker": _first_blocker(snapshot),
        },
        "next_steps": {
            "action": next_action,
            "command": next_command,
        },
        "boundary": {
            "provider_calls_made": external_calls,
            "mode": "read_only_artifact_write",
            "statement": (
                "Headless ops reports are generated from local JSON artifacts. "
                "Dashboard capture and report rendering make zero provider calls."
            ),
        },
        "renderer": {
            "name": _text(result.get("renderer")),
            "requested": _text(result.get("requested_renderer")),
            "command": result.get("command") if isinstance(result.get("command"), list) else [],
        },
        "artifacts": artifacts,
        "rows": rows,
        "terminal_preview": terminal_text.splitlines()[:32],
    }


def render_ops_run_report_html(payload: Mapping[str, object]) -> str:
    run = _mapping(payload.get("run"))
    summary = _mapping(payload.get("summary"))
    next_steps = _mapping(payload.get("next_steps"))
    boundary = _mapping(payload.get("boundary"))
    rows = _list_of_mappings(payload.get("rows"))
    artifacts = _list_of_mappings(payload.get("artifacts"))
    terminal_preview = [str(line) for line in _sequence(payload.get("terminal_preview"))[:24]]
    status = _text(summary.get("status"), fallback="unknown")
    run_id = _text(run.get("run_id"), fallback="unknown-run")
    generated_at = _text(payload.get("generated_at"))
    embedded_json = _json_for_script(payload)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MarketRadar Ops Report - {_escape(run_id)}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f7fb;
      --paper: #ffffff;
      --ink: #172033;
      --muted: #667085;
      --line: #d8dee9;
      --teal: #087e8b;
      --blue: #2454a6;
      --green: #1b7f4d;
      --amber: #a15c00;
      --red: #b42318;
      --code: #101828;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 15px/1.45 "Segoe UI", Arial, sans-serif;
    }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 32px 20px 48px; }}
    header {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 20px;
      align-items: start;
      padding: 28px;
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
    }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{ font-size: 30px; line-height: 1.15; letter-spacing: 0; }}
    h2 {{ font-size: 19px; margin-bottom: 14px; letter-spacing: 0; }}
    .subtitle {{ margin-top: 8px; color: var(--muted); max-width: 760px; }}
    .status {{
      min-width: 180px;
      padding: 14px 16px;
      border-radius: 8px;
      border: 1px solid var(--line);
      background: #fbfcff;
      text-align: right;
    }}
    .status b {{ display: block; font-size: 20px; color: {_status_color(status)}; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .metric {{
      padding: 18px;
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      min-height: 96px;
    }}
    .metric span {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }}
    .metric b {{ display: block; margin-top: 8px; font-size: 24px; line-height: 1.1; }}
    section {{
      margin-top: 16px;
      padding: 22px;
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
    }}
    .two-col {{
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(280px, .8fr);
      gap: 16px;
    }}
    .callout {{
      border-left: 4px solid var(--teal);
      padding: 14px 16px;
      background: #f0fbfc;
      border-radius: 0 8px 8px 0;
    }}
    .command {{
      margin-top: 12px;
      padding: 12px;
      background: #101828;
      color: #eef4ff;
      border-radius: 8px;
      overflow-wrap: anywhere;
      font-family: Consolas, "SFMono-Regular", monospace;
    }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; }}
    .artifact-list {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }}
    .artifact {{
      display: block;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      color: var(--ink);
      text-decoration: none;
      background: #fbfcff;
    }}
    .artifact strong {{ display: block; }}
    .artifact span {{ display: block; margin-top: 4px; color: var(--muted); font-size: 12px; }}
    .terminal {{
      margin: 0;
      padding: 16px;
      background: var(--code);
      color: #e6edf7;
      border-radius: 8px;
      overflow: auto;
      font: 13px/1.35 Consolas, "SFMono-Regular", monospace;
      max-height: 420px;
    }}
    .terminal-img {{
      width: 100%;
      margin-top: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #101828;
    }}
    .small {{ color: var(--muted); font-size: 13px; }}
    @media (max-width: 860px) {{
      header, .two-col, .grid, .artifact-list {{ grid-template-columns: 1fr; }}
      .status {{ text-align: left; }}
      main {{ padding: 18px 12px 36px; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>MarketRadar Ops Report</h1>
        <p class="subtitle">
          Headless dashboard evidence generated from JSON artifacts for aggregation
          and remote review.
        </p>
        <p class="small">Run {_escape(run_id)} generated {_escape(generated_at)}</p>
      </div>
      <div class="status">
        <span class="small">Dashboard status</span>
        <b>{_escape(status)}</b>
      </div>
    </header>

    <div class="grid">
      {_metric("Rows", summary.get("row_count"))}
      {_metric("Provider Calls", summary.get("external_calls_made"))}
      {_metric("Snapshot", summary.get("snapshot_mode"))}
      {_metric("Renderer", run.get("renderer"))}
    </div>

    <section class="two-col">
      <div>
        <h2>Next Safe Action</h2>
        <div class="callout">
          {_escape(_text(next_steps.get("action"), fallback="No action reported."))}
        </div>
        <div class="command">
          {_escape(_text(next_steps.get("command"), fallback="No command reported."))}
        </div>
      </div>
      <div>
        <h2>Boundary</h2>
        <p>{_escape(_text(boundary.get("statement")))}</p>
        <p class="small">
          Mode: {_escape(_text(boundary.get("mode")))}.
          Provider calls made: {_escape(_text(boundary.get("provider_calls_made")))}.
        </p>
      </div>
    </section>

    <section>
      <h2>Artifacts</h2>
      <div class="artifact-list">
        {_artifact_links(artifacts)}
      </div>
    </section>

    <section>
      <h2>Attention Queue</h2>
      {_rows_table(rows)}
    </section>

    <section>
      <h2>Terminal Frame</h2>
      <pre class="terminal">{_escape(chr(10).join(terminal_preview))}</pre>
      {_terminal_image(artifacts)}
    </section>
  </main>
  <script type="application/json" id="ops-report-data">{embedded_json}</script>
</body>
</html>
"""


def _candidate_rows(snapshot: Mapping[str, object]) -> list[dict[str, object]]:
    priced_in = _mapping(snapshot.get("priced_in_queue"))
    candidates = _mapping(snapshot.get("candidates"))
    raw_rows = (
        _sequence(priced_in.get("rows"))
        or _sequence(priced_in.get("items"))
        or _sequence(candidates.get("rows"))
        or _sequence(candidates.get("items"))
    )
    rows: list[dict[str, object]] = []
    for raw in raw_rows[:50]:
        row = _mapping(raw)
        rows.append(
            {
                "ticker": _text(row.get("ticker"), fallback="-"),
                "state": _text(row.get("state") or row.get("decision_status"), fallback="-"),
                "signal": _text(row.get("signal") or row.get("setup") or row.get("setup_type")),
                "next_action": _text(row.get("next_action") or row.get("operator_next")),
                "score": row.get("score") or row.get("final_score") or row.get("rank_score"),
            }
        )
    return rows


def _artifact_rows(value: object) -> list[dict[str, object]]:
    artifacts: list[dict[str, object]] = []
    for raw in _sequence(value):
        item = _mapping(raw)
        if not item:
            continue
        artifacts.append(
            {
                "name": _text(item.get("name")),
                "kind": _text(item.get("kind")),
                "path": _text(item.get("path")),
                "api_path": _text(item.get("api_path")),
                "size_bytes": _int(item.get("size_bytes"), default=0),
            }
        )
    return artifacts


def _rows_table(rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return '<p class="small">No queue rows were returned in this snapshot.</p>'
    body = "\n".join(
        "<tr>"
        f"<td>{_escape(row.get('ticker'))}</td>"
        f"<td>{_escape(row.get('state'))}</td>"
        f"<td>{_escape(row.get('signal'))}</td>"
        f"<td>{_escape(row.get('next_action'))}</td>"
        f"<td>{_escape(row.get('score'))}</td>"
        "</tr>"
        for row in rows
    )
    return (
        "<table><thead><tr><th>Ticker</th><th>State</th><th>Signal</th>"
        "<th>Next</th><th>Score</th></tr></thead><tbody>"
        f"{body}</tbody></table>"
    )


def _artifact_links(artifacts: Sequence[Mapping[str, object]]) -> str:
    if not artifacts:
        return '<p class="small">No artifacts were attached.</p>'
    return "\n".join(
        '<a class="artifact" href="'
        + _escape(_text(artifact.get("name")), quote=True)
        + '"><strong>'
        + _escape(artifact.get("name"))
        + "</strong><span>"
        + _escape(artifact.get("kind"))
        + " "
        + _escape(_text(artifact.get("size_bytes")))
        + " bytes</span></a>"
        for artifact in artifacts
    )


def _terminal_image(artifacts: Sequence[Mapping[str, object]]) -> str:
    if not any(_text(artifact.get("name")) == "terminal.png" for artifact in artifacts):
        return ""
    return '<img class="terminal-img" src="terminal.png" alt="Rendered terminal dashboard">'


def _metric(label: str, value: object) -> str:
    return (
        '<div class="metric"><span>'
        + _escape(label)
        + "</span><b>"
        + _escape(_text(value, fallback="-"))
        + "</b></div>"
    )


def _first_blocker(snapshot: Mapping[str, object]) -> str:
    for key in ("first_blocker", "blocker", "first_operator_blocker"):
        value = _text(snapshot.get(key))
        if value:
            return value
    readiness = _mapping(snapshot.get("readiness"))
    return _text(readiness.get("first_blocker") or readiness.get("status"))


def _json_for_script(payload: Mapping[str, object]) -> str:
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return html.escape(text.replace("</", "<\\/"), quote=False)


def _status_color(status: str) -> str:
    normalized = status.lower()
    if normalized in {"ready", "completed", "ok", "healthy"}:
        return "var(--green)"
    if normalized in {"setup_required", "blocked", "failed", "error"}:
        return "var(--red)"
    if normalized in {"warning", "degraded", "partial"}:
        return "var(--amber)"
    return "var(--blue)"


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: object) -> list[object]:
    if isinstance(value, list | tuple):
        return list(value)
    return []


def _list_of_mappings(value: object) -> list[Mapping[str, object]]:
    return [_mapping(item) for item in _sequence(value) if isinstance(item, Mapping)]


def _text(value: object, *, fallback: str = "") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _int(value: object, *, default: int) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _escape(value: object, *, quote: bool = False) -> str:
    return html.escape(_text(value), quote=quote)
