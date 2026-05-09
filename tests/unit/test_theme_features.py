from __future__ import annotations

from datetime import UTC, datetime

from catalyst_radar.features.theme import (
    load_theme_peer_config,
    theme_for_security,
    theme_velocity_score,
)
from catalyst_radar.textint.models import TextFeature


def test_theme_config_maps_security_by_ticker_sector_and_industry() -> None:
    config = load_theme_peer_config("config/theme_peers.yaml")

    assert "ai_infrastructure_storage" in config.themes
    assert (
        theme_for_security(
            ticker="AAA",
            sector="Technology",
            industry="Software",
            metadata={},
            config=config,
        )
        == "ai_infrastructure_storage"
    )
    assert (
        theme_for_security(
            ticker="UNLISTED",
            sector="Industrials",
            industry="Construction",
            metadata={},
            config=config,
        )
        == "datacenter_power"
    )


def test_theme_metadata_takes_precedence_when_known() -> None:
    config = load_theme_peer_config("config/theme_peers.yaml")

    assert (
        theme_for_security(
            ticker="AAA",
            sector="Technology",
            industry="Software",
            metadata={"theme": "datacenter_power"},
            config=config,
        )
        == "datacenter_power"
    )


def test_theme_velocity_uses_matching_text_theme_evidence() -> None:
    feature = text_feature()

    score = theme_velocity_score(feature, "ai_infrastructure_storage")

    assert score > 60.0


def test_theme_velocity_is_neutral_without_matching_theme() -> None:
    feature = text_feature()

    assert theme_velocity_score(feature, "datacenter_power") == 0.0
    assert theme_velocity_score(None, "ai_infrastructure_storage") == 0.0


def text_feature(**overrides: object) -> TextFeature:
    values = {
        "id": "feature-1",
        "ticker": "AAA",
        "as_of": datetime(2026, 5, 8, 21, tzinfo=UTC),
        "feature_version": "textint-v1",
        "local_narrative_score": 70.0,
        "novelty_score": 100.0,
        "sentiment_score": 25.0,
        "source_quality_score": 85.0,
        "theme_match_score": 75.0,
        "conflict_penalty": 0.0,
        "selected_snippet_ids": ["snippet-1", "snippet-2"],
        "theme_hits": [
            {"theme_id": "ai_infrastructure_storage", "count": 2, "terms": ["NAND", "SSD"]}
        ],
        "source_ts": datetime(2026, 5, 8, 20, tzinfo=UTC),
        "available_at": datetime(2026, 5, 8, 20, 30, tzinfo=UTC),
        "payload": {"snippet_count": 2},
    }
    values.update(overrides)
    return TextFeature(**values)  # type: ignore[arg-type]
