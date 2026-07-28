"""Product scope and deprecation registry for MarketRadar.

Canonical narrative: docs/PRODUCT_SCOPE.md and docs/DEPRECATION.md.

This module is intentionally import-light so CLI and UI can consult it without
pulling the full dashboard graph.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

SCOPE_VERSION = "event-first-discovery-v1"
SCOPE_DATE = "2026-07-19"

# Opt-in flag for deprecated workbench CLI/UI. Default product path has this off.
LEGACY_WORKBENCH_ENV = "CATALYST_ENABLE_LEGACY_WORKBENCH"

# --- Python packages under catalyst_radar ---------------------------------

ACTIVE_PACKAGES: frozenset[str] = frozenset(
    {
        "discovery",
        "core",
        "storage",
        "security",
    }
)

SUPPORTING_PACKAGES: frozenset[str] = frozenset(
    {
        "scoring",
        "features",
        "pipeline",
        "market",
        "connectors",
        "events",
        "validation",
        "jobs",
        "ops",  # health/metrics only; remote ops still deprecated as product
    }
)

DEPRECATED_PACKAGES: frozenset[str] = frozenset(
    {
        "alerts",
        "brokers",
        "trading",
        "decision_cards",
        "ipo",
        "portfolio",
        "textint",
        "universe",
        "feedback",  # generic feedback desk; discovery labels use value_ledger
    }
)

# Agents: llm_provider is supporting; the rest of the package is deprecated product UX.
DEPRECATED_AGENT_MODULES: frozenset[str] = frozenset(
    {
        "paper_trading",
        "sdk_orchestrator",
        "review_service",
        "router",
        "tasks",
        "tools",
        "run_audit",
    }
)

# --- Desktop / TUI pages --------------------------------------------------

ACTIVE_DESKTOP_PAGES: frozenset[str] = frozenset(
    {
        "world-events",
        "help",
    }
)

# Shown only as secondary / legacy workbench surfaces.
DEPRECATED_DESKTOP_PAGES: frozenset[str] = frozenset(
    {
        "tutorial",
        "overview",
        "portfolio",
        "market-radar",
        "trade-planner",
        "risk-desk",
        "paper-trading",
        "backtest",
        "readiness",
        "run",
        "candidates",
        "review",
        "alerts",
        "ipo",
        "broker",
        "ops",
        "telemetry",
        "agent",
        "themes",
        "validation",
        "costs",
        "features",
        "journal",
    }
)

# --- CLI top-level commands -----------------------------------------------

ACTIVE_CLI_COMMANDS: frozenset[str] = frozenset(
    {
        "discovery-brief",
        "discovery-from-x",
        "discovery-ingest",
        "discovery-case",
        "discovery-label",
        "product-scope",
        "init-db",
    }
)

SUPPORTING_CLI_COMMANDS: frozenset[str] = frozenset(
    {
        "market-bars",
        "ingest-polygon",
        "scan",
        "run-daily",
        "value-ledger",
        "value-report",
        "value-outcome",
        "value-outcomes",
        "dashboard-snapshot",  # full snapshot still used by workbench; discovery-snapshot is preferred
    }
)

DEPRECATED_CLI_COMMANDS: frozenset[str] = frozenset(
    {
        "seed-dashboard-demo",
        "ingest-sec",
        "ingest-news",
        "ingest-earnings",
        "ingest-options",
        "schwab-market-sync",
        "events",
        "ipo-s1-analysis",
        "run-textint",
        "text-features",
        "build-packets",
        "build-decision-cards",
        "build-alerts",
        "alerts-list",
        "alert-digest",
        "send-alerts",
        "llm-budget-status",
        "run-llm-review",
        "candidate-packet",
        "decision-card",
        "validation-replay",
        "validation-report",
        "paper-decision",
        "agentic-paper-intent",
        "trading-platform-plan",
        "paper-update-outcomes",
        "useful-label",
        "build-universe",
        "assert-investable-readiness",
        "assert-shadow-ready",
        "assert-trial-ready",
        "shadow-mode",
        "agent-brief",
        "dashboard-tui",
        "dashboard-command",
        "priced-in-queue",
        "priced-in-source-batches",
        "priced-in-preflight",
        "priced-in-answer",
        "priced-in-audit",
        "candidate-detail",
    }
)

REMOVAL_PHASES: tuple[dict[str, Any], ...] = (
    {
        "id": "D1",
        "name": "Contract and labels",
        "status": "done",
        "summary": "Document scope, registry, UI/CLI labels; no deletions.",
    },
    {
        "id": "D2",
        "name": "Default UX lockdown",
        "status": "done",
        "summary": (
            "Legacy workbench behind CATALYST_ENABLE_LEGACY_WORKBENCH; "
            "default desktop/TUI nav is World Events + Help only."
        ),
    },
    {
        "id": "D3",
        "name": "CLI removal from default product",
        "status": "done",
        "summary": (
            "Deprecated CLI commands hard-blocked unless "
            f"{LEGACY_WORKBENCH_ENV} is truthy; warning still emitted when enabled."
        ),
    },
    {
        "id": "D4",
        "name": "Code quarantine",
        "status": "planned",
        "summary": "Move or isolate legacy packages; shrink dashboard.data.",
    },
    {
        "id": "D5",
        "name": "Delete",
        "status": "planned",
        "summary": "Remove deprecated packages, pages, routes, and tests.",
    },
)


def legacy_workbench_enabled() -> bool:
    """Return True when operators opt into deprecated workbench surfaces."""
    raw = os.environ.get(LEGACY_WORKBENCH_ENV, "")
    return str(raw).strip().casefold() in {"1", "true", "yes", "on"}


def package_status(package_name: str) -> str:
    name = str(package_name or "").strip().split(".")[0]
    if name in ACTIVE_PACKAGES:
        return "active"
    if name in SUPPORTING_PACKAGES:
        return "supporting"
    if name in DEPRECATED_PACKAGES:
        return "deprecated"
    if name == "agents":
        return "mixed"  # supporting llm_provider; rest deprecated
    if name == "api":
        return "mixed"
    if name == "dashboard":
        return "mixed"
    return "unclassified"


def desktop_page_status(page_key: str) -> str:
    key = str(page_key or "").strip().lower()
    if key in ACTIVE_DESKTOP_PAGES:
        return "active"
    if key in DEPRECATED_DESKTOP_PAGES:
        return "deprecated"
    return "unclassified"


def cli_command_status(command: str) -> str:
    name = str(command or "").strip().lower()
    if name in ACTIVE_CLI_COMMANDS:
        return "active"
    if name in SUPPORTING_CLI_COMMANDS:
        return "supporting"
    if name in DEPRECATED_CLI_COMMANDS:
        return "deprecated"
    return "unclassified"


def is_deprecated_desktop_page(page_key: str) -> bool:
    return desktop_page_status(page_key) == "deprecated"


def deprecate_page_label(label: str, page_key: str) -> str:
    """Prefix deprecated page labels for operator visibility."""
    if not is_deprecated_desktop_page(page_key):
        return label
    text = str(label or "").strip()
    if text.lower().startswith("legacy"):
        return text
    # Keep numeric shortcuts readable: "5 Alerts" -> "5 Legacy · Alerts"
    parts = text.split(" ", 1)
    if len(parts) == 2 and parts[0].isdigit():
        return f"{parts[0]} Legacy · {parts[1]}"
    return f"Legacy · {text}"


def product_scope_payload() -> dict[str, Any]:
    legacy_enabled = legacy_workbench_enabled()
    return {
        "schema_version": "market-radar-product-scope-v1",
        "scope_version": SCOPE_VERSION,
        "scope_date": SCOPE_DATE,
        "product": (
            "Event-first discovery: world events → ranked under-priced leads → "
            "case file → proof labels. Decision support only."
        ),
        "docs": {
            "scope": "docs/PRODUCT_SCOPE.md",
            "deprecation": "docs/DEPRECATION.md",
        },
        "packages": {
            "active": sorted(ACTIVE_PACKAGES),
            "supporting": sorted(SUPPORTING_PACKAGES),
            "deprecated": sorted(DEPRECATED_PACKAGES),
            "mixed": ["agents", "api", "dashboard"],
        },
        "desktop_pages": {
            "active": sorted(ACTIVE_DESKTOP_PAGES),
            "deprecated": sorted(DEPRECATED_DESKTOP_PAGES),
            "default_nav": sorted(ACTIVE_DESKTOP_PAGES),
        },
        "cli_commands": {
            "active": sorted(ACTIVE_CLI_COMMANDS),
            "supporting": sorted(SUPPORTING_CLI_COMMANDS),
            "deprecated": sorted(DEPRECATED_CLI_COMMANDS),
            "deprecated_default_reachable": legacy_enabled,
        },
        "legacy_workbench_enabled": legacy_enabled,
        "legacy_env": LEGACY_WORKBENCH_ENV,
        "removal_phases": list(REMOVAL_PHASES),
        "investment_advice": False,
        "decision_support_only": True,
    }


def warn_if_deprecated_cli(command: str) -> str | None:
    """Return a human warning string if command is deprecated, else None."""
    status = cli_command_status(command)
    if status != "deprecated":
        return None
    return (
        f"DEPRECATED: CLI command '{command}' is outside the event-first product "
        f"scope ({SCOPE_VERSION}). See docs/PRODUCT_SCOPE.md and docs/DEPRECATION.md. "
        "Prefer discovery-brief / discovery-from-x / discovery-case / discovery-label "
        "/ World Events UI."
    )


def block_deprecated_cli(command: str) -> str | None:
    """Return an error string if deprecated command is blocked (legacy env off)."""
    status = cli_command_status(command)
    if status != "deprecated":
        return None
    if legacy_workbench_enabled():
        return None
    return (
        f"BLOCKED: CLI command '{command}' is deprecated and not part of the default "
        f"event-first product ({SCOPE_VERSION}). Set {LEGACY_WORKBENCH_ENV}=1 to "
        "re-enable legacy workbench commands, or use discovery-brief / "
        "discovery-from-x / discovery-case / discovery-label. "
        "See docs/PRODUCT_SCOPE.md and docs/DEPRECATION.md."
    )


__all__ = [
    "ACTIVE_CLI_COMMANDS",
    "ACTIVE_DESKTOP_PAGES",
    "ACTIVE_PACKAGES",
    "DEPRECATED_AGENT_MODULES",
    "DEPRECATED_CLI_COMMANDS",
    "DEPRECATED_DESKTOP_PAGES",
    "DEPRECATED_PACKAGES",
    "LEGACY_WORKBENCH_ENV",
    "REMOVAL_PHASES",
    "SCOPE_DATE",
    "SCOPE_VERSION",
    "SUPPORTING_CLI_COMMANDS",
    "SUPPORTING_PACKAGES",
    "block_deprecated_cli",
    "cli_command_status",
    "deprecate_page_label",
    "desktop_page_status",
    "is_deprecated_desktop_page",
    "legacy_workbench_enabled",
    "package_status",
    "product_scope_payload",
    "warn_if_deprecated_cli",
]
