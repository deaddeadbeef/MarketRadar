from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from uuid import uuid4

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.tui import (
    MODERN_PAGES,
    PAGE_ALIASES,
    DashboardFilters,
    dashboard_filters_for_page,
    dashboard_json_default,
    dashboard_snapshot_payload,
    render_dashboard_tui,
)
from catalyst_radar.storage.db import create_schema, engine_from_url

RUN_SCHEMA_VERSION = "ops-run-v1"
SUPPORTED_ACTIONS = frozenset({"radar-dashboard"})
SUPPORTED_RENDERERS = frozenset({"auto", "rust", "python"})
ARTIFACT_NAMES = frozenset({"result.json", "snapshot.json", "terminal.txt", "terminal.png"})
_RUN_ID_RE = re.compile(r"^\d{8}T\d{6}Z-[a-f0-9]{8}$")


class OpsRunError(ValueError):
    pass


@dataclass(frozen=True)
class _RenderResult:
    renderer: str
    text: str
    command: list[str]
    error: str | None = None


def create_ops_run(
    *,
    action: str,
    page: str = "overview",
    renderer: str = "auto",
    frame_width: int = 140,
    frame_height: int = 42,
    copy_to_onedrive: bool = False,
    database_url: str | None = None,
) -> dict[str, object]:
    action = _validate_action(action)
    renderer = _validate_renderer(renderer)
    page = _normalize_page(page)
    frame_width = _bounded_int(frame_width, minimum=80, maximum=240, name="frame_width")
    frame_height = _bounded_int(frame_height, minimum=24, maximum=80, name="frame_height")

    started = _utc_now()
    run_id = _new_run_id(started)
    run_dir = _ops_run_root() / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    started_monotonic = monotonic()

    config = AppConfig.from_env()
    if database_url:
        config = replace(config, database_url=database_url)
    engine = engine_from_url(config.database_url)
    create_schema(engine)

    filters = dashboard_filters_for_page(DashboardFilters(), page)
    snapshot = dashboard_snapshot_payload(
        engine=engine,
        config=config,
        dotenv_loaded=True,
        filters=filters,
        fast_view=True,
    )
    snapshot["selected_page"] = page
    _write_json(run_dir / "snapshot.json", snapshot)

    render = _render_terminal_frame(
        snapshot=snapshot,
        page=page,
        renderer=renderer,
        frame_width=frame_width,
        frame_height=frame_height,
        database_url=config.database_url,
    )
    (run_dir / "terminal.txt").write_text(render.text, encoding="utf-8", newline="\n")
    _write_terminal_png(render.text, run_dir / "terminal.png", frame_width=frame_width)

    finished = _utc_now()
    status = "failed" if render.error and renderer == "rust" else "completed"
    result: dict[str, object] = {
        "schema_version": RUN_SCHEMA_VERSION,
        "run_id": run_id,
        "action": action,
        "page": page,
        "status": status,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_ms": int((monotonic() - started_monotonic) * 1000),
        "capture_mode": "headless-terminal-frame",
        "renderer": render.renderer,
        "requested_renderer": renderer,
        "command": render.command,
        "run_dir": str(run_dir.resolve()),
        "summary": _summary(snapshot),
        "artifacts": [],
        "onedrive": {"status": "not_requested"},
    }
    if render.error:
        result["renderer_error"] = render.error

    result_path = run_dir / "result.json"
    result["artifacts"] = _artifact_metadata(run_id, run_dir)
    _write_json(result_path, result)
    result["artifacts"] = _artifact_metadata(run_id, run_dir)
    if copy_to_onedrive:
        result["onedrive"] = _copy_to_onedrive(run_dir, run_id)
    _write_json(result_path, result)
    return result


def load_ops_run(run_id: str) -> dict[str, object]:
    run_dir = _resolve_run_dir(run_id)
    result_path = run_dir / "result.json"
    if not result_path.exists():
        raise OpsRunError(f"ops run not found: {run_id}")
    return json.loads(result_path.read_text(encoding="utf-8"))


def resolve_ops_artifact(run_id: str, artifact_name: str) -> Path:
    if Path(artifact_name).name != artifact_name or "/" in artifact_name or "\\" in artifact_name:
        raise OpsRunError(f"invalid artifact name: {artifact_name}")
    if artifact_name not in ARTIFACT_NAMES:
        raise OpsRunError(f"ops run artifact not found: {artifact_name}")
    run_dir = _resolve_run_dir(run_id)
    artifact_path = (run_dir / artifact_name).resolve()
    run_dir_resolved = run_dir.resolve()
    if not artifact_path.is_relative_to(run_dir_resolved):
        raise OpsRunError(f"invalid artifact path: {artifact_name}")
    if not artifact_path.exists():
        raise OpsRunError(f"ops run artifact not found: {artifact_name}")
    return artifact_path


def _validate_action(action: str) -> str:
    normalized = str(action or "").strip().lower()
    if normalized not in SUPPORTED_ACTIONS:
        raise OpsRunError(f"unsupported ops action: {action}")
    return normalized


def _validate_renderer(renderer: str) -> str:
    normalized = str(renderer or "").strip().lower()
    if normalized not in SUPPORTED_RENDERERS:
        raise OpsRunError(f"unsupported renderer: {renderer}")
    return normalized


def _normalize_page(page: str) -> str:
    text = str(page or "").strip().lower().replace(" ", "_").replace("-", "_")
    normalized = PAGE_ALIASES.get(text)
    allowed = {page_key for page_key, _, _ in MODERN_PAGES}
    if normalized not in allowed:
        raise OpsRunError(f"unsupported dashboard page: {page}")
    return normalized


def _bounded_int(value: int, *, minimum: int, maximum: int, name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise OpsRunError(f"{name} must be an integer") from exc
    if number < minimum or number > maximum:
        raise OpsRunError(f"{name} must be between {minimum} and {maximum}")
    return number


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _new_run_id(timestamp: datetime) -> str:
    return f"{timestamp.strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"


def _ops_run_root() -> Path:
    configured = os.environ.get("CATALYST_OPS_RUN_DIR")
    root = Path(configured) if configured else Path(".state") / "ops-runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _resolve_run_dir(run_id: str) -> Path:
    if not _RUN_ID_RE.match(str(run_id or "")):
        raise OpsRunError(f"invalid ops run id: {run_id}")
    run_dir = (_ops_run_root() / run_id).resolve()
    root = _ops_run_root().resolve()
    if not run_dir.is_relative_to(root):
        raise OpsRunError(f"invalid ops run id: {run_id}")
    if not run_dir.exists():
        raise OpsRunError(f"ops run not found: {run_id}")
    return run_dir


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, default=dashboard_json_default, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _render_terminal_frame(
    *,
    snapshot: dict[str, object],
    page: str,
    renderer: str,
    frame_width: int,
    frame_height: int,
    database_url: str,
) -> _RenderResult:
    if renderer in {"auto", "rust"}:
        try:
            return _render_with_rust(
                page=page,
                frame_width=frame_width,
                frame_height=frame_height,
                database_url=database_url,
            )
        except OpsRunError as exc:
            if renderer == "rust":
                return _RenderResult(
                    renderer="rust",
                    text=f"MarketRadar Rust TUI render failed\nerror={exc}\n",
                    command=[],
                    error=str(exc),
                )
            fallback = _render_with_python(snapshot=snapshot, page=page)
            return _RenderResult(
                renderer="python-fallback",
                text=fallback.text,
                command=fallback.command,
                error=str(exc),
            )
    return _render_with_python(snapshot=snapshot, page=page)


def _render_with_python(*, snapshot: dict[str, object], page: str) -> _RenderResult:
    return _RenderResult(
        renderer="python",
        text=render_dashboard_tui(snapshot, page=page),
        command=["catalyst-radar", "dashboard-snapshot", "--json", "--fast", "--page", page],
    )


def _render_with_rust(
    *,
    page: str,
    frame_width: int,
    frame_height: int,
    database_url: str,
) -> _RenderResult:
    repo_root = _repo_root()
    exe = repo_root / "target" / "release" / _rust_executable_name()
    snapshot_command = (
        f"& {_powershell_quote(str(Path(sys.executable).resolve()))} "
        "-m catalyst_radar.cli dashboard-snapshot --json --fast"
    )
    rust_args = [
        "--snapshot-command",
        snapshot_command,
        "--render-frame",
        "--page",
        page,
        "--database-url",
        database_url,
        "--frame-width",
        str(frame_width),
        "--frame-height",
        str(frame_height),
    ]
    if exe.exists():
        command = [str(exe), *rust_args]
    else:
        cargo = shutil.which("cargo")
        if not cargo:
            raise OpsRunError("Rust renderer unavailable: cargo was not found")
        command = [
            cargo,
            "run",
            "-p",
            "radar-tui",
            "--release",
            "--quiet",
            "--",
            *rust_args,
        ]

    env = os.environ.copy()
    env["CATALYST_DATABASE_URL"] = database_url
    src_path = str(repo_root / "src")
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        src_path if not existing_pythonpath else f"{src_path}{os.pathsep}{existing_pythonpath}"
    )
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=90,
        check=False,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise OpsRunError(f"Rust renderer failed with exit {completed.returncode}: {stderr}")
    return _RenderResult(renderer="rust", text=completed.stdout, command=command)


def _rust_executable_name() -> str:
    return "radar-tui.exe" if os.name == "nt" else "radar-tui"


def _repo_root() -> Path:
    candidates = [Path.cwd(), Path(__file__).resolve()]
    for candidate in candidates:
        current = candidate if candidate.is_dir() else candidate.parent
        for path in (current, *current.parents):
            if (path / "pyproject.toml").exists() and (path / "crates" / "radar-tui").exists():
                return path
    return Path.cwd()


def _powershell_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _write_terminal_png(text: str, output_path: Path, *, frame_width: int) -> None:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:
        raise OpsRunError("Pillow is required to write terminal.png artifacts") from exc

    lines = text.splitlines() or [""]
    font = _terminal_font(ImageFont)
    bbox = font.getbbox("M")
    char_width = max(8, bbox[2] - bbox[0])
    line_height = max(16, bbox[3] - bbox[1] + 6)
    padding = 18
    image_width = max(900, min(2200, frame_width * char_width + padding * 2))
    image_height = min(2600, max(520, len(lines) * line_height + padding * 2))
    image = Image.new("RGB", (image_width, image_height), color=(13, 18, 24))
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, image_width, image_height), fill=(13, 18, 24))
    y = padding
    for line in lines:
        draw.text((padding, y), line[: max(1, frame_width)], font=font, fill=(226, 232, 240))
        y += line_height
        if y > image_height - padding:
            break
    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path, format="PNG")


def _terminal_font(image_font_module):
    candidates = [
        Path(os.environ.get("SystemRoot", "C:\\Windows")) / "Fonts" / "consola.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"),
    ]
    for path in candidates:
        if path.exists():
            return image_font_module.truetype(str(path), size=16)
    try:
        return image_font_module.truetype("DejaVuSansMono.ttf", size=16)
    except OSError:
        return image_font_module.load_default()


def _summary(snapshot: dict[str, object]) -> dict[str, object]:
    priced_in = _mapping(snapshot.get("priced_in_queue"))
    candidates = _mapping(snapshot.get("candidates"))
    rows = priced_in.get("rows") or priced_in.get("items") or candidates.get("rows") or []
    return {
        "dashboard_status": snapshot.get("status"),
        "snapshot_mode": snapshot.get("snapshot_mode"),
        "next_action": snapshot.get("next_action") or snapshot.get("canonical_next_action"),
        "next_command": snapshot.get("next_command") or snapshot.get("canonical_next_command"),
        "row_count": len(rows) if isinstance(rows, list) else 0,
        "external_calls_made": int(snapshot.get("external_calls_made") or 0),
    }


def _mapping(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _artifact_metadata(run_id: str, run_dir: Path) -> list[dict[str, object]]:
    kinds = {
        "result.json": "ops-run-result",
        "snapshot.json": "dashboard-snapshot-json",
        "terminal.txt": "terminal-transcript",
        "terminal.png": "terminal-image",
    }
    artifacts: list[dict[str, object]] = []
    for name in ("result.json", "snapshot.json", "terminal.txt", "terminal.png"):
        path = run_dir / name
        artifacts.append(
            {
                "name": name,
                "kind": kinds[name],
                "path": str(path.resolve()),
                "api_path": f"/api/ops/runs/{run_id}/artifacts/{name}",
                "size_bytes": path.stat().st_size if path.exists() else 0,
            }
        )
    return artifacts


def _copy_to_onedrive(run_dir: Path, run_id: str) -> dict[str, object]:
    root = _onedrive_root()
    if root is None:
        return {"status": "unavailable", "reason": "OneDrive folder was not found"}
    destination = root / "MarketRadar" / "ops-runs" / run_id
    destination.mkdir(parents=True, exist_ok=True)
    for name in ARTIFACT_NAMES:
        source = run_dir / name
        if source.exists():
            shutil.copy2(source, destination / name)
    return {"status": "copied", "path": str(destination.resolve())}


def _onedrive_root() -> Path | None:
    candidates = [
        os.environ.get("OneDrive"),
        os.environ.get("ONEDRIVE"),
        os.environ.get("OneDriveCommercial"),
        str(Path.home() / "OneDrive"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() or candidate in {os.environ.get("OneDrive"), os.environ.get("ONEDRIVE")}:
            path.mkdir(parents=True, exist_ok=True)
            return path
    return None
