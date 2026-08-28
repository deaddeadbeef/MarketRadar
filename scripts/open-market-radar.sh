#!/usr/bin/env bash
# Launch the product desktop app on World Events with the discovery snapshot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DESKTOP="$REPO_ROOT/target/release/radar-desktop"
if [[ ! -x "$DESKTOP" ]]; then
  echo "Missing $DESKTOP. Build with: cargo build -p radar-desktop --release" >&2
  exit 1
fi

if [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
  PYTHON="$REPO_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON="$(command -v python)"
else
  echo "No python3 found. Install Python or create .venv." >&2
  exit 1
fi

SNAPSHOT="$REPO_ROOT/scripts/discovery-snapshot.py"
SNAPSHOT_COMMAND="PYTHONPATH='$REPO_ROOT/src' '$PYTHON' '$SNAPSHOT'"

# Prefer an existing graphical display; default to :3 on this box if unset.
if [[ -z "${DISPLAY:-}" ]]; then
  if [[ -S /tmp/.X11-unix/X3 ]]; then
    export DISPLAY=:3
  elif [[ -S /tmp/.X11-unix/X0 ]]; then
    export DISPLAY=:0
  fi
fi

# WebKitGTK is more reliable on VNC/Xvfb without GPU compositing.
export WEBKIT_DISABLE_COMPOSITING_MODE="${WEBKIT_DISABLE_COMPOSITING_MODE:-1}"
export WEBKIT_DISABLE_DMABUF_RENDERER="${WEBKIT_DISABLE_DMABUF_RENDERER:-1}"

if [[ -z "${DBUS_SESSION_BUS_ADDRESS:-}" ]] && command -v dbus-launch >/dev/null 2>&1; then
  # shellcheck disable=SC2046
  eval "$(dbus-launch --sh-syntax)"
fi

exec "$DESKTOP" --page world-events --snapshot-command "$SNAPSHOT_COMMAND" "$@"
