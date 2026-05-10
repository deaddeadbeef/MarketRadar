from catalyst_radar.security.redaction import redact_text, redact_value


def test_redacts_secret_keys_recursively_without_mutating_input() -> None:
    payload = {
        "api_key": "abc123",
        "nested": [{"Authorization": "Bearer sk-test"}, {"safe": "value"}],
    }

    redacted = redact_value(payload)

    assert redacted == {
        "api_key": "<redacted>",
        "nested": [{"Authorization": "<redacted>"}, {"safe": "value"}],
    }
    assert payload["api_key"] == "abc123"


def test_redacts_database_urls_and_query_secrets_in_text() -> None:
    text = (
        "postgresql://user:pass@localhost:5432/db "
        "https://api.example.test/v1?apikey=abc&token=def&x=1"
    )

    redacted = redact_text(text)

    assert "pass" not in redacted
    assert "abc" not in redacted
    assert "def" not in redacted
    assert "postgresql://user:<redacted>@localhost:5432/db" in redacted
    assert "apikey=<redacted>" in redacted
    assert "token=<redacted>" in redacted


def test_redacts_known_secret_values_inside_error_text() -> None:
    redacted = redact_text(
        "request failed with OPENAI_API_KEY=sk-live-secret",
        known_secrets=("sk-live-secret",),
    )

    assert "sk-live-secret" not in redacted


def test_redacts_env_style_secret_assignments() -> None:
    redacted = redact_text(
        "OPENAI_API_KEY=sk-live-secret CATALYST_POLYGON_API_KEY='poly-secret'"
    )

    assert "sk-live-secret" not in redacted
    assert "poly-secret" not in redacted
    assert "OPENAI_API_KEY=<redacted>" in redacted
    assert "CATALYST_POLYGON_API_KEY='<redacted>'" in redacted


def test_redacts_secret_assignments_and_embedded_urls() -> None:
    text = (
        'bad apikey=secret-token token:"abc123" password = "pw" '
        'url="https://api.example.test/v1?apikey=url-secret&x=1"'
    )

    redacted = redact_text(text)

    assert "secret-token" not in redacted
    assert "abc123" not in redacted
    assert '"pw"' not in redacted
    assert "url-secret" not in redacted
    assert "apikey=<redacted>" in redacted
    assert 'token:"<redacted>"' in redacted


def test_redaction_preserves_token_usage_metrics() -> None:
    payload = {
        "input_tokens": 100,
        "cached_input_tokens": 10,
        "output_tokens": 25,
        "token_usage": {"input_tokens": 100},
        "access_token": "secret-token",
    }

    redacted = redact_value(payload)

    assert redacted["input_tokens"] == 100
    assert redacted["cached_input_tokens"] == 10
    assert redacted["output_tokens"] == 25
    assert redacted["token_usage"] == {"input_tokens": 100}
    assert redacted["access_token"] == "<redacted>"
