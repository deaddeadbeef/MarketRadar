from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Sequence

EMBEDDING_DIMENSIONS = 64
_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> tuple[str, ...]:
    return tuple(_TOKEN_PATTERN.findall(str(text or "").lower()))


def embed_text(text: str, *, dimensions: int = EMBEDDING_DIMENSIONS) -> tuple[float, ...]:
    if dimensions <= 0:
        msg = "dimensions must be positive"
        raise ValueError(msg)

    vector = [0.0] * dimensions
    for token in tokenize(text):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % dimensions
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[index] += sign

    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0.0:
        return tuple(0.0 for _ in range(dimensions))
    return tuple(round(value / norm, 6) for value in vector)


def text_embedding(text: str, *, dimensions: int = EMBEDDING_DIMENSIONS) -> tuple[float, ...]:
    return embed_text(text, dimensions=dimensions)


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        msg = "vectors must have the same dimensions"
        raise ValueError(msg)
    left_values = tuple(_finite_component(value) for value in left)
    right_values = tuple(_finite_component(value) for value in right)
    left_norm = math.sqrt(sum(value * value for value in left_values))
    right_norm = math.sqrt(sum(value * value for value in right_values))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    if left_values == right_values:
        return 1.0

    dot = sum(
        left_value * right_value
        for left_value, right_value in zip(left_values, right_values, strict=True)
    )
    score = dot / (left_norm * right_norm)
    return max(-1.0, min(1.0, round(score, 6)))


def _finite_component(value: float) -> float:
    result = float(value)
    if not math.isfinite(result):
        msg = "vectors must contain only finite values"
        raise ValueError(msg)
    return result
