from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

_SECRET_QUERY_KEYS = {"apikey", "api_key", "api-token", "api_token", "token"}


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    url: str
    headers: Mapping[str, str]
    body: bytes


class HttpTransport(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        raise NotImplementedError


class UrlLibHttpTransport:
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        request = Request(url, headers=dict(headers), method="GET")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return HttpResponse(
                    status_code=int(response.status),
                    url=url,
                    headers=dict(response.headers.items()),
                    body=response.read(),
                )
        except HTTPError as exc:
            return HttpResponse(
                status_code=int(exc.code),
                url=url,
                headers=dict(exc.headers.items()),
                body=exc.read(),
            )


class FakeHttpTransport:
    def __init__(self, responses: Mapping[str, HttpResponse]) -> None:
        self._responses = dict(responses)
        self.requests: list[str] = []

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        self.requests.append(url)
        if url not in self._responses:
            raise RuntimeError(f"missing fake HTTP response for {redact_url(url)}")
        return self._responses[url]


class JsonHttpClient:
    def __init__(self, transport: HttpTransport, timeout_seconds: float) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        self.transport = transport
        self.timeout_seconds = timeout_seconds

    def get_json(self, url: str, headers: Mapping[str, str] | None = None) -> Any:
        response = self.transport.get(
            url,
            headers=headers or {},
            timeout_seconds=self.timeout_seconds,
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f"HTTP {response.status_code} from {redact_url(response.url)}")
        try:
            return json.loads(response.body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid JSON from {redact_url(response.url)}") from exc


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        redacted = "REDACTED" if key.lower() in _SECRET_QUERY_KEYS else value
        query.append((key, redacted))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


__all__ = [
    "FakeHttpTransport",
    "HttpResponse",
    "HttpTransport",
    "JsonHttpClient",
    "UrlLibHttpTransport",
    "redact_url",
]
