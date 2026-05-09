from catalyst_radar.textint.embeddings import cosine_similarity, embed_text


def test_hashing_embedding_is_deterministic() -> None:
    text = "NAND SSD storage bottleneck"

    assert embed_text(text) == embed_text(text)
    assert len(embed_text(text)) == 64


def test_identical_nonempty_text_cosine_is_one() -> None:
    vector = embed_text("NAND SSD storage bottleneck")

    assert cosine_similarity(vector, vector) == 1.0
