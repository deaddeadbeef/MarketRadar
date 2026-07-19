"""DEPRECATED product surface: local text-intelligence pipeline.

Not required for the world-events discovery spine. See docs/PRODUCT_SCOPE.md.
"""

from catalyst_radar.textint.models import (
    EmbeddingVector,
    NoveltyResult,
    OntologyMatch,
    OntologyTheme,
    SentimentResult,
    TextFeature,
    TextSnippet,
)

__all__ = [
    "EmbeddingVector",
    "NoveltyResult",
    "OntologyMatch",
    "OntologyTheme",
    "SentimentResult",
    "TextFeature",
    "TextSnippet",
]
