from __future__ import annotations

from catalyst_radar.connectors.http import (
    FakeHttpTransport,
    HttpResponse,
    JsonHttpClient,
    redact_url,
)


def test_redact_url_hides_provider_tokens() -> None:
    url = "https://api.polygon.io/v2/aggs?apiKey=secret&token=abc&symbol=AAPL"

    assert redact_url(url) == (
        "https://api.polygon.io/v2/aggs?apiKey=REDACTED&token=REDACTED&symbol=AAPL"
    )


def test_json_client_uses_fake_transport() -> None:
    transport = FakeHttpTransport(
        {
            "https://example.test/data": HttpResponse(
                status_code=200,
                url="https://example.test/data",
                headers={"content-type": "application/json"},
                body=b'{"status":"OK","results":[{"T":"AAPL"}]}',
            )
        }
    )
    client = JsonHttpClient(transport=transport, timeout_seconds=3)

    payload = client.get_json("https://example.test/data")

    assert payload["status"] == "OK"
    assert transport.requests == ["https://example.test/data"]


def test_json_client_raises_redacted_error() -> None:
    transport = FakeHttpTransport(
        {
            "https://example.test/data?apiKey=secret": HttpResponse(
                status_code=429,
                url="https://example.test/data?apiKey=secret",
                headers={},
                body=b'{"error":"rate limited"}',
            )
        }
    )
    client = JsonHttpClient(transport=transport, timeout_seconds=3)

    try:
        client.get_json("https://example.test/data?apiKey=secret")
    except RuntimeError as exc:
        assert "apiKey=REDACTED" in str(exc)
        assert "secret" not in str(exc)
    else:
        raise AssertionError("expected HTTP error")


def test_json_client_rejects_invalid_json() -> None:
    transport = FakeHttpTransport(
        {
            "https://example.test/data": HttpResponse(
                status_code=200,
                url="https://example.test/data",
                headers={"content-type": "application/json"},
                body=b"not json",
            )
        }
    )
    client = JsonHttpClient(transport=transport, timeout_seconds=3)

    try:
        client.get_json("https://example.test/data")
    except RuntimeError as exc:
        assert "invalid JSON" in str(exc)
    else:
        raise AssertionError("expected invalid JSON error")

