from __future__ import annotations

import re

_POSITIVE_PHRASES = {
    "raises guidance": 0.75,
    "raised guidance": 0.75,
    "guidance raise": 0.55,
    "stronger demand": 0.45,
    "strong demand": 0.35,
    "beats expectations": 0.45,
    "margin expansion": 0.35,
    "accelerating growth": 0.35,
    "backlog growth": 0.30,
}

_NEGATIVE_PHRASES = {
    "cuts guidance": -0.80,
    "cut guidance": -0.80,
    "guidance cut": -0.60,
    "regulatory investigation": -0.55,
    "sec investigation": -0.55,
    "weaker demand": -0.45,
    "misses expectations": -0.45,
    "margin compression": -0.35,
    "supply constraint": -0.25,
}


def score_sentiment(text: str) -> float:
    normalized = _normalize(text)
    score = 0.0
    for phrase, weight in _POSITIVE_PHRASES.items():
        if _contains_phrase(normalized, phrase):
            score += weight
    for phrase, weight in _NEGATIVE_PHRASES.items():
        if _contains_phrase(normalized, phrase):
            score += weight
    return round(max(-1.0, min(1.0, score)), 6)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").casefold()).strip()


def _contains_phrase(text: str, phrase: str) -> bool:
    pattern = rf"(?<![a-z0-9]){re.escape(phrase.casefold())}(?![a-z0-9])"
    return re.search(pattern, text) is not None
