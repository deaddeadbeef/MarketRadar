from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from catalyst_radar.pipeline.candidate_packet import (
    CandidatePacket,
    EvidenceItem,
    evidence_item_payload,
)

AGENT_EVIDENCE_PACKET_SCHEMA_VERSION = "agent-evidence-packet-v1"

_SOURCE_LINK_FIELDS = frozenset(
    {
        "claims",
        "bear_case",
        "supporting_points",
        "risks",
    }
)


def build_agent_evidence_packet(packet: CandidatePacket) -> Mapping[str, Any]:
    supporting = tuple(evidence_item_payload(item) for item in packet.supporting_evidence)
    disconfirming = tuple(evidence_item_payload(item) for item in packet.disconfirming_evidence)
    evidence_items = (*packet.supporting_evidence, *packet.disconfirming_evidence)

    return {
        "schema_version": AGENT_EVIDENCE_PACKET_SCHEMA_VERSION,
        "candidate_packet_id": packet.id,
        "ticker": packet.ticker,
        "as_of": packet.as_of.isoformat(),
        "available_at": packet.available_at.isoformat(),
        "state": packet.state.value,
        "final_score": packet.final_score,
        "supporting_evidence": list(supporting),
        "disconfirming_evidence": list(disconfirming),
        "conflicts": [dict(conflict) for conflict in packet.conflicts],
        "hard_blocks": list(packet.hard_blocks),
        "allowed_reference_ids": _allowed_reference_ids(evidence_items),
        "allowed_computed_feature_ids": _allowed_computed_feature_ids(evidence_items),
        "no_trade_execution": True,
    }


def source_faithfulness_violations(
    payload: Mapping[str, Any],
    evidence_packet: Mapping[str, Any],
) -> list[str]:
    allowed_reference_ids = _allowed_values(evidence_packet.get("allowed_reference_ids"))
    allowed_computed_feature_ids = _allowed_values(
        evidence_packet.get("allowed_computed_feature_ids")
    )
    violations: list[str] = []

    for path, item in _iter_source_link_items(payload):
        if not isinstance(item, Mapping):
            violations.append(
                f"{path} must be an object with source_id or computed_feature_id"
            )
            continue

        source_id = _optional_text(item.get("source_id"))
        computed_feature_id = _optional_text(item.get("computed_feature_id"))
        has_allowed_source = source_id in allowed_reference_ids if source_id else False
        has_allowed_feature = (
            computed_feature_id in allowed_computed_feature_ids
            if computed_feature_id
            else False
        )

        if not source_id and not computed_feature_id:
            violations.append(f"{path} must include source_id or computed_feature_id")
            continue
        if source_id and not has_allowed_source:
            violations.append(f"{path}.source_id is not in allowed_reference_ids: {source_id}")
        if computed_feature_id and not has_allowed_feature:
            violations.append(
                f"{path}.computed_feature_id is not in allowed_computed_feature_ids: "
                f"{computed_feature_id}"
            )

    return violations


def _allowed_reference_ids(items: Sequence[EvidenceItem]) -> list[str]:
    values: set[str] = set()
    for item in items:
        if item.source_id:
            values.add(item.source_id)
        if item.source_url:
            values.add(item.source_url)
    return sorted(values)


def _allowed_computed_feature_ids(items: Sequence[EvidenceItem]) -> list[str]:
    return sorted(
        item.computed_feature_id
        for item in items
        if item.computed_feature_id
    )


def _allowed_values(value: Any) -> set[str]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        return set()
    return {item for item in (_optional_text(item) for item in value) if item}


def _iter_source_link_items(value: Any, path: str = "") -> list[tuple[str, Any]]:
    items: list[tuple[str, Any]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            child_path = key_text if not path else f"{path}.{key_text}"
            if key_text in _SOURCE_LINK_FIELDS:
                if isinstance(child, Sequence) and not isinstance(child, str):
                    items.extend(
                        (f"{child_path}[{index}]", item)
                        for index, item in enumerate(child)
                    )
                else:
                    items.append((child_path, child))
                continue
            items.extend(_iter_source_link_items(child, child_path))
    elif isinstance(value, Sequence) and not isinstance(value, str):
        for index, child in enumerate(value):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            items.extend(_iter_source_link_items(child, child_path))
    return items


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "AGENT_EVIDENCE_PACKET_SCHEMA_VERSION",
    "build_agent_evidence_packet",
    "source_faithfulness_violations",
]
