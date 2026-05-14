from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from pathlib import Path

APP_VERSION = "0.1.0"
SERVICE_NAME = "catalyst-radar"

_COMMIT_ENV_KEYS = (
    "CATALYST_BUILD_COMMIT",
    "GITHUB_SHA",
    "VERCEL_GIT_COMMIT_SHA",
    "RENDER_GIT_COMMIT",
)


def build_info(
    *,
    env: Mapping[str, str] | None = None,
    repo_root: Path | None = None,
) -> dict[str, str]:
    """Return non-secret build identity for API and dashboard operators."""
    source_env = env if env is not None else os.environ
    commit, source = _commit_from_env(source_env)
    if not commit:
        commit = _git_commit(repo_root or _default_repo_root())
        source = "git" if commit != "unknown" else "unknown"
    return {
        "service": SERVICE_NAME,
        "version": APP_VERSION,
        "commit": commit,
        "source": source,
    }


def _commit_from_env(env: Mapping[str, str]) -> tuple[str, str]:
    for key in _COMMIT_ENV_KEYS:
        value = _short_commit(env.get(key))
        if value:
            return value, key
    return "", "unknown"


def _short_commit(value: object) -> str:
    text = str(value or "").strip()
    return text[:12] if text else ""


def _git_commit(repo_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return _short_commit(result.stdout)


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


__all__ = ["APP_VERSION", "SERVICE_NAME", "build_info"]
