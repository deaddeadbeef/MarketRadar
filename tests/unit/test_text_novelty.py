from catalyst_radar.textint.novelty import score_novelty


def test_repeated_or_similar_text_lowers_novelty() -> None:
    prior = ["NAND SSD demand creates a storage bottleneck for AI inference."]

    repeated = score_novelty(
        "NAND SSD demand creates a storage bottleneck for AI inference.",
        prior,
    )
    similar = score_novelty("SSD demand creates another AI inference storage bottleneck.", prior)

    assert repeated == 0.0
    assert similar < 100.0


def test_different_theme_text_has_higher_novelty() -> None:
    prior = ["NAND SSD demand creates a storage bottleneck for AI inference."]

    similar = score_novelty("SSD demand creates another AI inference storage bottleneck.", prior)
    different = score_novelty("Switchgear and UPS demand rises as power density increases.", prior)

    assert different > similar
