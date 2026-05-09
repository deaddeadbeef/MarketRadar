from __future__ import annotations

from collections.abc import Iterable, Mapping

from catalyst_radar.textint.embeddings import cosine_similarity, embed_text


def score_novelty(text: object, prior_snippets: Iterable[object]) -> float:
    prior_texts = tuple(
        prior_text
        for prior_text in (_snippet_text(snippet) for snippet in prior_snippets)
        if prior_text
    )
    if not prior_texts:
        return 100.0

    vector = embed_text(_snippet_text(text))
    max_similarity = max(
        cosine_similarity(vector, embed_text(prior_text))
        for prior_text in prior_texts
    )
    bounded_similarity = max(0.0, min(1.0, max_similarity))
    return round(100.0 * (1.0 - bounded_similarity), 6)


def novelty_score(text: object, prior_snippets: Iterable[object]) -> float:
    return score_novelty(text, prior_snippets)


def _snippet_text(snippet: object) -> str:
    if isinstance(snippet, str):
        return snippet
    text = getattr(snippet, "text", None)
    if text is not None:
        return str(text)
    if isinstance(snippet, Mapping):
        for key in ("text", "snippet_text", "body", "title"):
            value = snippet.get(key)
            if value:
                return str(value)
    return str(snippet if snippet is not None else "")
