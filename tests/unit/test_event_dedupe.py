from catalyst_radar.events.dedupe import body_hash, canonicalize_url, dedupe_key


def test_canonicalize_url_removes_tracking_params() -> None:
    assert (
        canonicalize_url("https://Example.com/path?utm_source=x&id=123#section")
        == "https://example.com/path?id=123"
    )


def test_body_hash_is_stable_across_whitespace() -> None:
    assert body_hash("Guidance raised\n\nfor FY 2026") == body_hash(
        "Guidance raised for FY 2026"
    )


def test_dedupe_key_prefers_canonical_url() -> None:
    assert (
        dedupe_key(
            ticker="msft",
            provider="news",
            canonical_url="https://example.com/article",
            content_hash="abc",
        )
        == "MSFT:news:https://example.com/article"
    )
