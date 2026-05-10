from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SECRET_KEY_MARKERS = ("api_key", "apikey", "token", "password", "secret", "authorization")
REDACTED = "<redacted>"

_DATABASE_URL_USERINFO_PATTERN = re.compile(
    r"(?P<prefix>[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:)(?P<password>[^@\s/]+)(?P<suffix>@)"
)
_AUTHORIZATION_VALUE_PATTERN = re.compile(
    r"(?i)\b(authorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+"
)


def redact_value(value: Any, *, known_secrets: Sequence[str] = ()) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): REDACTED
            if _is_secret_key(str(key))
            else redact_value(item, known_secrets=known_secrets)
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [redact_value(item, known_secrets=known_secrets) for item in value]
    if isinstance(value, str):
        return redact_text(value, known_secrets=known_secrets)
    return value


def redact_text(text: str, *, known_secrets: Sequence[str] = ()) -> str:
    redacted = _redact_database_urls(text)
    redacted = _redact_query_params(redacted)
    redacted = _redact_authorization_values(redacted)
    for secret in known_secrets:
        if secret:
            redacted = redacted.replace(secret, REDACTED)
    return redacted


def redact_url(url: str, *, known_secrets: Sequence[str] = ()) -> str:
    return redact_text(url, known_secrets=known_secrets)


def _is_secret_key(key: str) -> bool:
    normalized = key.strip().lower().replace("-", "_")
    return any(marker in normalized for marker in SECRET_KEY_MARKERS)


def _redact_database_urls(text: str) -> str:
    return _DATABASE_URL_USERINFO_PATTERN.sub(r"\g<prefix><redacted>\g<suffix>", text)


def _redact_query_params(text: str) -> str:
    parts = text.split(" ")
    return " ".join(_redact_query_params_in_token(part) for part in parts)


def _redact_query_params_in_token(token: str) -> str:
    parsed = urlsplit(token)
    if not parsed.scheme or not parsed.netloc or not parsed.query:
        return token

    query = [
        (key, REDACTED if _is_secret_key(key) else value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    redacted_query = urlencode(query, doseq=True, safe="<>")
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, redacted_query, parsed.fragment)
    )


def _redact_authorization_values(text: str) -> str:
    return _AUTHORIZATION_VALUE_PATTERN.sub(r"\1<redacted>", text)
