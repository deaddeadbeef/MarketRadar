from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

try:  # Imported lazily by tests and by the real execution path.
    from agents import function_tool
except ModuleNotFoundError:  # pragma: no cover - only for partially installed envs.
    function_tool = None


@dataclass(frozen=True)
class ReadOnlySnapshotTool:
    name: str
    description: str
    handler: Callable[..., dict[str, object]]


def build_market_radar_agent_tools(snapshot: Mapping[str, object]) -> list[object]:
    """Build the only tools allowed during a real MarketRadar agent run."""
    redacted_snapshot = dict(snapshot)

    def get_visible_scan_rows(limit: int = 10) -> dict[str, object]:
        rows = _rows(_mapping(redacted_snapshot.get("priced_in")).get("rows"))
        resolved_limit = max(1, min(50, int(limit or 10)))
        return {
            "rows": rows[:resolved_limit],
            "returned_count": min(len(rows), resolved_limit),
            "total_visible_count": len(rows),
            "external_calls_made": 0,
        }

    def get_candidate_detail(ticker: str) -> dict[str, object]:
        symbol = str(ticker or "").strip().upper()
        candidate_rows = _rows(_mapping(redacted_snapshot.get("candidates")).get("rows"))
        queue_rows = _rows(_mapping(redacted_snapshot.get("priced_in")).get("rows"))
        for row in (*candidate_rows, *queue_rows):
            if str(row.get("ticker") or "").strip().upper() == symbol:
                return {
                    "ticker": symbol,
                    "row": row,
                    "external_calls_made": 0,
                }
        return {
            "ticker": symbol,
            "row": {},
            "external_calls_made": 0,
        }

    def get_source_coverage() -> dict[str, object]:
        priced_in = _mapping(redacted_snapshot.get("priced_in"))
        return {
            "source_coverage": _mapping(priced_in.get("source_coverage")),
            "source_workflow": _mapping(priced_in.get("source_workflow")),
            "external_calls_made": 0,
        }

    def get_real_results_status() -> dict[str, object]:
        return {
            "real_results": _mapping(redacted_snapshot.get("real_results")),
            "external_calls_made": 0,
        }

    return [
        _tool(
            get_visible_scan_rows,
            name="get_visible_scan_rows",
            description="Return visible priced-in scan rows from the supplied snapshot only.",
        ),
        _tool(
            get_candidate_detail,
            name="get_candidate_detail",
            description="Return one candidate row from the supplied snapshot only.",
        ),
        _tool(
            get_source_coverage,
            name="get_source_coverage",
            description="Return source coverage from the supplied snapshot only.",
        ),
        _tool(
            get_real_results_status,
            name="get_real_results_status",
            description="Return the real-results gate from the supplied snapshot only.",
        ),
    ]


def _tool(
    handler: Callable[..., dict[str, object]],
    *,
    name: str,
    description: str,
) -> object:
    if function_tool is None:
        return ReadOnlySnapshotTool(name=name, description=description, handler=handler)
    return function_tool(
        name_override=name,
        description_override=description,
        use_docstring_info=False,
    )(handler)


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _rows(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list | tuple):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


__all__ = ["ReadOnlySnapshotTool", "build_market_radar_agent_tools"]
