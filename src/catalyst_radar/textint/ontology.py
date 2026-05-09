from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ThemeDefinition:
    theme_id: str
    terms: tuple[str, ...]
    sectors: tuple[str, ...]
    read_through: tuple[str, ...]


@dataclass(frozen=True)
class OntologyHit:
    theme_id: str
    score: float
    matched_terms: tuple[str, ...]
    sectors: tuple[str, ...]
    read_through: tuple[str, ...]

    @property
    def terms(self) -> tuple[str, ...]:
        return self.matched_terms


def load_theme_ontology(path: str | Path | None = None) -> dict[str, ThemeDefinition]:
    config_path = Path(path) if path is not None else _default_config_path()
    parsed = _parse_yaml_subset(config_path.read_text(encoding="utf-8"))
    themes = parsed.get("themes")
    if not isinstance(themes, Mapping) or not themes:
        msg = "theme ontology config must contain a non-empty themes mapping"
        raise ValueError(msg)

    ontology: dict[str, ThemeDefinition] = {}
    for theme_id in sorted(themes):
        raw_theme = themes[theme_id]
        if not isinstance(raw_theme, Mapping):
            msg = f"theme {theme_id} must be a mapping"
            raise ValueError(msg)
        ontology[str(theme_id)] = ThemeDefinition(
            theme_id=str(theme_id),
            terms=_string_list(raw_theme.get("terms"), f"{theme_id}.terms"),
            sectors=_string_list(raw_theme.get("sectors"), f"{theme_id}.sectors"),
            read_through=_string_list(
                raw_theme.get("read_through"),
                f"{theme_id}.read_through",
            ),
        )
    return ontology


def load_ontology(path: str | Path | None = None) -> dict[str, ThemeDefinition]:
    return load_theme_ontology(path)


def match_ontology(
    text: str,
    ontology: Mapping[str, ThemeDefinition] | None = None,
) -> tuple[OntologyHit, ...]:
    themes = ontology if ontology is not None else load_theme_ontology()
    normalized_text = str(text or "").casefold()
    hits: list[OntologyHit] = []
    for theme_id, theme in themes.items():
        matched_terms = tuple(term for term in theme.terms if _contains_term(normalized_text, term))
        if matched_terms:
            hits.append(
                OntologyHit(
                    theme_id=theme_id,
                    score=float(len(matched_terms)),
                    matched_terms=matched_terms,
                    sectors=theme.sectors,
                    read_through=theme.read_through,
                )
            )
    return tuple(sorted(hits, key=lambda hit: (-hit.score, hit.theme_id)))


def match_themes(
    text: str,
    ontology: Mapping[str, ThemeDefinition] | None = None,
) -> tuple[OntologyHit, ...]:
    return match_ontology(text, ontology)


def _default_config_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "themes.yaml"


def _contains_term(normalized_text: str, term: str) -> bool:
    normalized_term = term.casefold()
    if any(character.isalnum() for character in normalized_term):
        pattern = rf"(?<![a-z0-9]){re.escape(normalized_term)}(?![a-z0-9])"
        return re.search(pattern, normalized_text) is not None
    return normalized_term in normalized_text


def _parse_yaml_subset(text: str) -> dict[str, Any]:
    lines = _yaml_lines(text)
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any] | list[Any]]] = [(-1, root)]

    for index, (indent, content) in enumerate(lines):
        while indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if content.startswith("- "):
            if not isinstance(parent, list):
                msg = "YAML list item found outside a list"
                raise ValueError(msg)
            parent.append(_parse_scalar(content[2:].strip()))
            continue

        key, separator, raw_value = content.partition(":")
        if not separator:
            msg = f"YAML mapping line is missing ':' near {content!r}"
            raise ValueError(msg)
        if not isinstance(parent, dict):
            msg = "YAML mapping line found inside a scalar list"
            raise ValueError(msg)

        key = key.strip()
        value = raw_value.strip()
        if value:
            parent[key] = _parse_scalar(value)
            continue

        child: dict[str, Any] | list[Any]
        child = [] if _next_child_is_list(lines, index, indent) else {}
        parent[key] = child
        stack.append((indent, child))
    return root


def _yaml_lines(text: str) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent % 2 != 0:
            msg = "YAML subset only supports two-space indentation"
            raise ValueError(msg)
        lines.append((indent, line.strip()))
    return lines


def _next_child_is_list(lines: Sequence[tuple[int, str]], index: int, parent_indent: int) -> bool:
    for child_indent, child_content in lines[index + 1 :]:
        if child_indent <= parent_indent:
            return False
        if child_indent == parent_indent + 2:
            return child_content.startswith("- ")
    return False


def _parse_scalar(value: str) -> str | list[str]:
    if "," in value:
        return [part.strip() for part in value.split(",") if part.strip()]
    return value.strip()


def _string_list(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, str):
        items = (value,)
    elif isinstance(value, Sequence):
        items = tuple(str(item).strip() for item in value)
    else:
        msg = f"{field_name} must be a string or list"
        raise ValueError(msg)
    cleaned = tuple(item for item in items if item)
    if not cleaned:
        msg = f"{field_name} must not be empty"
        raise ValueError(msg)
    return cleaned
