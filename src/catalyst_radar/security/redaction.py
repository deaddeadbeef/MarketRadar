from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SECRET_EXACT_KEYS = {
    "api_key",
    "apikey",
    "api_token",
    "api-token",
    "access_token",
    "refresh_token",
    "auth_token",
    "bearer_token",
    "password",
    "secret",
    "authorization",
}
SECRET_SUFFIXES = ("_api_key", "_apikey", "_password", "_secret")
REDACTED = "<redacted>"

_DATABASE_URL_USERINFO_PATTERN = re.compile(
    r"(?P<prefix>[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:)(?P<password>[^@\s/]+)(?P<suffix>@)"
)
_AUTHORIZATION_VALUE_PATTERN = re.compile(
    r"(?i)\b(authorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+"
)
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"""(?ix)
    (?P<prefix>\b(?:
        api[_-]?key
        | api[_-]?token
        | access[_-]?token
        | refresh[_-]?token
        | auth[_-]?token
        | bearer[_-]?token
        | token
        | password
        | secret
        | [a-z0-9_]*_(?:api[_-]?key|api[_-]?token|token|password|secret)
    )\b\s*[:=]\s*)
    (?P<quote>["']?)
    (?P<value>[^\s"',;&}\]]+)
    (?P=quote)
    """
)
_URL_PATTERN = re.compile(r"""https?://[^\s"'<>]+""")


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
    redacted = _redact_secret_assignments(redacted)
    for secret in known_secrets:
        if secret:
            redacted = redacted.replace(secret, REDACTED)
    return redacted


def redact_url(url: str, *, known_secrets: Sequence[str] = ()) -> str:
    return redact_text(url, known_secrets=known_secrets)


def _is_secret_key(key: str) -> bool:
    normalized = key.strip().lower().replace("-", "_")
    return normalized in SECRET_EXACT_KEYS or any(
        normalized.endswith(suffix) for suffix in SECRET_SUFFIXES
    )


def _redact_database_urls(text: str) -> str:
    return _DATABASE_URL_USERINFO_PATTERN.sub(r"\g<prefix><redacted>\g<suffix>", text)


def _redact_query_params(text: str) -> str:
    return _URL_PATTERN.sub(lambda match: _redact_query_params_in_url(match.group(0)), text)


def _redact_query_params_in_url(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc or not parsed.query:
        return url

    query = [
        (key, REDACTED if _is_secret_query_key(key) else value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    redacted_query = urlencode(query, doseq=True, safe="<>")
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, redacted_query, parsed.fragment)
    )


def _redact_authorization_values(text: str) -> str:
    return _AUTHORIZATION_VALUE_PATTERN.sub(r"\1<redacted>", text)


def _is_secret_query_key(key: str) -> bool:
    normalized = key.strip().lower().replace("-", "_")
    return normalized == "token" or _is_secret_key(normalized)


def _redact_secret_assignments(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        prefix = match.group("prefix")
        quote_value = match.group("quote") or ""
        if quote_value:
            return f"{prefix}{quote_value}{REDACTED}{quote_value}"
        return f"{prefix}{REDACTED}"

    return _SECRET_ASSIGNMENT_PATTERN.sub(replace, text)
