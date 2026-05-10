import json

from catalyst_radar.security.redaction import (
    minimize_prompt_payload,
    redact_text,
    redact_value,
)


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


def test_redacts_serialized_secret_assignments() -> None:
    redacted = redact_text(
        """{"api_key": "abc123", 'Authorization': 'Bearer sk-test', """
        """password="abc,def"}"""
    )

    assert "abc123" not in redacted
    assert "Bearer sk-test" not in redacted
    assert "abc,def" not in redacted
    assert '"api_key": "<redacted>"' in redacted
    assert "'Authorization': '<redacted>'" in redacted
    assert 'password="<redacted>"' in redacted


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
        "github_token": "github-secret",
        "session_token": "session-secret",
        "aws_secret_access_key": "aws-secret",
    }

    redacted = redact_value(payload)

    assert redacted["input_tokens"] == 100
    assert redacted["cached_input_tokens"] == 10
    assert redacted["output_tokens"] == 25
    assert redacted["token_usage"] == {"input_tokens": 100}
    assert redacted["access_token"] == "<redacted>"
    assert redacted["github_token"] == "<redacted>"
    assert redacted["session_token"] == "<redacted>"
    assert redacted["aws_secret_access_key"] == "<redacted>"


def test_redacts_exact_structured_token_key_without_redacting_metrics() -> None:
    payload = {
        "token": "tok-secret",
        "total_tokens": 125,
        "token_count": 3,
    }

    redacted = redact_value(payload)

    assert redacted == {
        "token": "<redacted>",
        "total_tokens": 125,
        "token_count": 3,
    }


def test_minimize_prompt_payload_removes_account_sensitive_fields() -> None:
    payload = minimize_prompt_payload(
        {
            "candidate_packet": {
                "ticker": "MSFT",
                "payload": {
                    "portfolio_impact": {"portfolio_value": 100000, "cash": 5000},
                    "evidence": [
                        {
                            "source_id": "event-1",
                            "source_url": "https://x?apikey=secret",
                        }
                    ],
                },
            }
        }
    )

    text = json.dumps(payload)
    assert "portfolio_value" not in text
    assert "cash" not in text
    assert "secret" not in text
    assert "event-1" in text
    assert "source_url" in text
