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
