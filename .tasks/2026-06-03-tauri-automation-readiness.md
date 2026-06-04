## Tauri Automation Readiness

Objective: make the Tauri dashboard expose a machine-readable native Computer Use validation recipe while preserving the existing API and telemetry contracts.

Status:
- PR #1014 is open on `codex/tauri-dashboard-app`.
- Native Computer Use is blocked by `Computer Use native pipe path is unavailable`.
- Next repo-side step is to publish the native window title, executable path, command flow, and zero-call assertions in the dashboard automation manifest.
- Keep final native proof separate from app-side readiness evidence.
- Completed: Rust desktop config, FastAPI manifest, route tests, and README publish the same native recipe.
- In progress: desktop command-flow parity for `batch SOURCE`, `batch SOURCE all`, and `batch SOURCE execute N` so the Tauri command box opens Ops source plans and shows external PowerShell boundaries without provider/model/broker calls.
- In progress: desktop filter-validation parity so `source-gap` and `decision-gap` reject unsupported values before changing filters or refreshing snapshots.
- Validation passed: cargo tests for radar-desktop/radar-tui, release build, Ruff, targeted API route pytest, manifest probe, and git diff whitespace check.
- Remaining blocker: native Computer Use still fails before app discovery with Computer Use native pipe path is unavailable.
