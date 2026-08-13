"""Minimal YAML subset parser for theme maps. Keeps discovery off textint."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def parse_yaml_subset(text: str) -> dict[str, Any]:
    lines = _yaml_lines(text)
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any] | list[Any]]] = [(-1, root)]

    for index, (indent, content) in enumerate(lines):
        while indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if content.startswith("- "):
            if not isinstance(parent, list):
                raise ValueError("YAML list item found outside a list")
            parent.append(_parse_scalar(content[2:].strip()))
            continue

        key, separator, raw_value = content.partition(":")
        if not separator:
            raise ValueError(f"YAML mapping line is missing ':' near {content!r}")
        if not isinstance(parent, dict):
            raise ValueError("YAML mapping line found inside a scalar list")

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
            raise ValueError("YAML subset only supports two-space indentation")
        lines.append((indent, line.strip()))
    return lines


def _next_child_is_list(
    lines: Sequence[tuple[int, str]], index: int, parent_indent: int
) -> bool:
    for child_indent, child_content in lines[index + 1 :]:
        if child_indent <= parent_indent:
            return False
        if child_indent == parent_indent + 2:
            return child_content.startswith("- ")
    return False


def _parse_scalar(value: str) -> str | list[str]:
    text = value.strip()
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [_strip_quotes(part.strip()) for part in inner.split(",") if part.strip()]
    if "," in text:
        return [_strip_quotes(part.strip()) for part in text.split(",") if part.strip()]
    return _strip_quotes(text)


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value
