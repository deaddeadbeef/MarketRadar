from catalyst_radar.textint.ontology import load_theme_ontology, match_ontology


def test_theme_config_loads_expected_subset() -> None:
    ontology = load_theme_ontology()

    storage = ontology["ai_infrastructure_storage"]
    assert storage.terms == (
        "NAND",
        "SSD",
        "datacenter storage",
        "inference storage",
        "storage bottleneck",
    )
    assert storage.sectors == ("Semiconductors", "Technology Hardware")


def test_matches_ai_storage_terms_case_insensitively() -> None:
    ontology = load_theme_ontology()

    hits = match_ontology(
        "NAND supply and SSD demand could worsen the storage bottleneck.",
        ontology,
    )

    assert hits[0].theme_id == "ai_infrastructure_storage"
    assert hits[0].matched_terms == ("NAND", "SSD", "storage bottleneck")
    assert hits[0].score == 3.0
