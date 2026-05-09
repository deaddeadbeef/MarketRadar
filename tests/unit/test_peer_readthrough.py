from __future__ import annotations

from catalyst_radar.features.peers import peer_readthrough_score
from catalyst_radar.features.theme import load_theme_peer_config


def test_peer_readthrough_scores_configured_peer_from_matching_theme_evidence() -> None:
    config = load_theme_peer_config("config/theme_peers.yaml")

    score = peer_readthrough_score(
        "MSFT",
        [{"theme_id": "ai_infrastructure_storage", "count": 2, "terms": ["NAND", "SSD"]}],
        config,
    )

    assert score.ticker == "MSFT"
    assert score.theme_id == "ai_infrastructure_storage"
    assert score.score == 70.0
    assert score.peers == ("NVDA", "MU")


def test_peer_readthrough_does_not_score_primary_theme_ticker_as_its_own_peer() -> None:
    config = load_theme_peer_config("config/theme_peers.yaml")

    score = peer_readthrough_score(
        "AAA",
        [{"theme_id": "ai_infrastructure_storage", "count": 1, "terms": ["NAND"]}],
        config,
    )

    assert score.score == 0.0
    assert score.theme_id == ""


def test_peer_readthrough_is_neutral_without_matching_evidence() -> None:
    config = load_theme_peer_config("config/theme_peers.yaml")

    unknown = peer_readthrough_score(
        "ZZZ",
        [{"theme_id": "ai_infrastructure_storage", "count": 2, "terms": ["NAND"]}],
        config,
    )
    missing_theme = peer_readthrough_score("AAA", [], config)

    assert unknown.score == 0.0
    assert unknown.theme_id == ""
    assert missing_theme.score == 0.0
    assert missing_theme.theme_id == ""
