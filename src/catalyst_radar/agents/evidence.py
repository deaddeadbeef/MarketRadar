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
        "unresolved_conflicts",
        "supporting_points",
        "risks",
    }
)


def build_agent_evidence_packet(packet: CandidatePacket) -> Mapping[str, Any]:
    supporting = tuple(
        _evidence_payload_with_ref(item, f"supporting_evidence[{index}]")
        for index, item in enumerate(packet.supporting_evidence)
    )
    disconfirming = tuple(
        _evidence_payload_with_ref(item, f"disconfirming_evidence[{index}]")
        for index, item in enumerate(packet.disconfirming_evidence)
    )
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
        "allowed_reference_ids": _allowed_reference_ids(
            evidence_items,
            packet.conflicts,
        ),
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

    for path, field_name, field_value in _iter_source_link_fields(payload):
        if not isinstance(field_value, Sequence) or isinstance(field_value, str):
            violations.append(f"{path} must be a list")
            continue

        for index, item in enumerate(field_value):
            item_path = f"{path}[{index}]"
            if field_name == "bear_case" and isinstance(item, str):
                continue
            if not isinstance(item, Mapping):
                violations.append(
                    f"{item_path} must be an object with source_id or computed_feature_id"
                )
                continue
            _append_item_violations(
                item,
                path=item_path,
                allowed_reference_ids=allowed_reference_ids,
                allowed_computed_feature_ids=allowed_computed_feature_ids,
                violations=violations,
            )

    return violations


def _evidence_payload_with_ref(item: EvidenceItem, ref: str) -> dict[str, Any]:
    payload = evidence_item_payload(item)
    payload["ref"] = ref
    return payload


def _append_item_violations(
    item: Mapping[str, Any],
    *,
    path: str,
    allowed_reference_ids: set[str],
    allowed_computed_feature_ids: set[str],
    violations: list[str],
) -> None:
    source_id = _strict_optional_string(item, "source_id", path, violations)
    computed_feature_id = _strict_optional_string(
        item,
        "computed_feature_id",
        path,
        violations,
    )
    has_invalid_source_field = any(
        violation.startswith(f"{path}.source_id must be a string")
        or violation.startswith(f"{path}.computed_feature_id must be a string")
        for violation in violations
    )
    if has_invalid_source_field:
        return

    if not source_id and not computed_feature_id:
        violations.append(f"{path} must include source_id or computed_feature_id")
        return
    if source_id and source_id not in allowed_reference_ids:
        violations.append(f"{path}.source_id is not in allowed_reference_ids: {source_id}")
    if computed_feature_id and computed_feature_id not in allowed_computed_feature_ids:
        violations.append(
            f"{path}.computed_feature_id is not in allowed_computed_feature_ids: "
            f"{computed_feature_id}"
        )


def _strict_optional_string(
    item: Mapping[str, Any],
    field_name: str,
    path: str,
    violations: list[str],
) -> str | None:
    if field_name not in item:
        return None
    value = item[field_name]
    if value is None:
        return None
    if not isinstance(value, str):
        violations.append(f"{path}.{field_name} must be a string")
        return None
    text = value.strip()
    return text or None


def _allowed_reference_ids(
    items: Sequence[EvidenceItem],
    conflicts: Sequence[Mapping[str, Any]],
) -> list[str]:
    values: set[str] = set()
    for item in items:
        if item.source_id:
            values.add(item.source_id)
        if item.source_url:
            values.add(item.source_url)
    for conflict in conflicts:
        source_id = conflict.get("source_id")
        if isinstance(source_id, str) and source_id.strip():
            values.add(source_id.strip())
        source_url = conflict.get("source_url")
        if isinstance(source_url, str) and source_url.strip():
            values.add(source_url.strip())
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
    return {item for item in value if isinstance(item, str) and item.strip()}


def _iter_source_link_fields(value: Any, path: str = "") -> list[tuple[str, str, Any]]:
    fields: list[tuple[str, str, Any]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            child_path = key_text if not path else f"{path}.{key_text}"
            if key_text in _SOURCE_LINK_FIELDS:
                fields.append((child_path, key_text, child))
                continue
            fields.extend(_iter_source_link_fields(child, child_path))
    elif isinstance(value, Sequence) and not isinstance(value, str):
        for index, child in enumerate(value):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            fields.extend(_iter_source_link_fields(child, child_path))
    return fields


__all__ = [
    "AGENT_EVIDENCE_PACKET_SCHEMA_VERSION",
    "build_agent_evidence_packet",
    "source_faithfulness_violations",
]
