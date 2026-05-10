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
    "token",
    "access_token",
    "refresh_token",
    "auth_token",
    "bearer_token",
    "id_token",
    "session_token",
    "client_secret",
    "secret_key",
    "secret_access_key",
    "password",
    "passwd",
    "secret",
    "authorization",
}
SECRET_SUFFIXES = (
    "_api_key",
    "_apikey",
    "_api_token",
    "_token",
    "_password",
    "_passwd",
    "_secret",
    "_client_secret",
    "_secret_key",
    "_secret_access_key",
)
NON_SECRET_EXACT_KEYS = {
    "cached_input_tokens",
    "cached_prompt_tokens",
    "completion_tokens",
    "estimated_tokens",
    "input_tokens",
    "max_input_tokens",
    "max_output_tokens",
    "max_tokens",
    "output_tokens",
    "prompt_tokens",
    "token_count",
    "token_estimate",
    "token_usage",
    "total_tokens",
}
ACCOUNT_SENSITIVE_KEYS = {
    "account_equity",
    "cash",
    "market_value",
    "notes",
    "portfolio_cash",
    "portfolio_value",
    "shares",
    "user_notes",
}
REDACTED = "<redacted>"

_DATABASE_URL_USERINFO_PATTERN = re.compile(
    r"(?P<prefix>[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:)(?P<password>[^@\s/]+)(?P<suffix>@)"
)
_AUTHORIZATION_VALUE_PATTERN = re.compile(
    r"(?i)\b(authorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+"
)
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"""(?ix)
    (?P<prefix>
        (?P<key_quote>["']?)
        \b(?:
            api[_-]?key
            | api[_-]?token
            | access[_-]?token
            | refresh[_-]?token
            | auth[_-]?token
            | bearer[_-]?token
            | id[_-]?token
            | session[_-]?token
            | token
            | password
            | passwd
            | secret(?:[_-]?access[_-]?key|[_-]?key)?
            | client[_-]?secret
            | authorization
            | [a-z0-9_]*_(?:
                api[_-]?key
                | api[_-]?token
                | token
                | password
                | passwd
                | secret(?:[_-]?access[_-]?key|[_-]?key)?
            )
        )\b
        (?P=key_quote)
        \s*[:=]\s*
    )
    (?:
        (?P<quote>["'])(?P<quoted_value>.*?)(?P=quote)
        |
        (?P<value>[^\s"',;&}\]]+)
    )
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


def minimize_prompt_payload(value: Mapping[str, Any]) -> Mapping[str, Any]:
    redacted = redact_value(value)
    if not isinstance(redacted, Mapping):
        msg = "prompt payload must be a mapping"
        raise TypeError(msg)
    minimized = _drop_keys(redacted, ACCOUNT_SENSITIVE_KEYS)
    if not isinstance(minimized, Mapping):
        msg = "prompt payload must remain a mapping"
        raise TypeError(msg)
    return minimized


def _is_secret_key(key: str) -> bool:
    normalized = key.strip().lower().replace("-", "_")
    if normalized in NON_SECRET_EXACT_KEYS:
        return False
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


def _drop_keys(value: Any, keys: set[str]) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _drop_keys(item, keys)
            for key, item in value.items()
            if _normalized_key(str(key)) not in keys
        }
    if isinstance(value, list | tuple):
        return [_drop_keys(item, keys) for item in value]
    return value


def _normalized_key(key: str) -> str:
    return key.strip().lower().replace("-", "_")
