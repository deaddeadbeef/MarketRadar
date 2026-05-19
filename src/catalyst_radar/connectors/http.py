from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from catalyst_radar.security.redaction import redact_text, redact_url


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

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
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

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
        timeout_seconds: float,
    ) -> HttpResponse:
        request = Request(url, headers=dict(headers), data=body, method="POST")
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


class HeaderInjectingTransport:
    def __init__(
        self,
        transport: HttpTransport,
        headers: Mapping[str, str],
    ) -> None:
        self.transport = transport
        self.headers = dict(headers)

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        return self.transport.get(
            url,
            headers={**self.headers, **dict(headers)},
            timeout_seconds=timeout_seconds,
        )

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
        timeout_seconds: float,
    ) -> HttpResponse:
        return self.transport.post(
            url,
            headers={**self.headers, **dict(headers)},
            body=body,
            timeout_seconds=timeout_seconds,
        )


class FakeHttpTransport:
    def __init__(self, responses: Mapping[str, HttpResponse]) -> None:
        self._responses = dict(responses)
        self.requests: list[str] = []
        self.post_requests: list[tuple[str, bytes]] = []

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

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
        timeout_seconds: float,
    ) -> HttpResponse:
        self.post_requests.append((url, body))
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
            raise RuntimeError(_http_error_message(response))
        try:
            return json.loads(response.body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid JSON from {redact_url(response.url)}") from exc

    def post_form_json(
        self,
        url: str,
        *,
        data: Mapping[str, str],
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        response = self.transport.post(
            url,
            headers={
                "content-type": "application/x-www-form-urlencoded",
                **dict(headers or {}),
            },
            body=urlencode(data).encode("utf-8"),
            timeout_seconds=self.timeout_seconds,
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(_http_error_message(response))
        try:
            return json.loads(response.body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid JSON from {redact_url(response.url)}") from exc


def _http_error_message(response: HttpResponse) -> str:
    detail = _http_error_detail(response.body)
    message = f"HTTP {response.status_code} from {redact_url(response.url)}"
    return f"{message}; detail={detail}" if detail else message


def _http_error_detail(body: bytes) -> str:
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return ""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        detail = text
    else:
        if isinstance(payload, Mapping):
            parts = [
                str(payload.get(key) or "").strip()
                for key in ("status", "message", "error")
                if str(payload.get(key) or "").strip()
            ]
            detail = ": ".join(parts) if parts else json.dumps(payload, sort_keys=True)
        else:
            detail = str(payload)
    detail = redact_text(detail)
    return detail[:300] + "..." if len(detail) > 300 else detail


__all__ = [
    "FakeHttpTransport",
    "HeaderInjectingTransport",
    "HttpResponse",
    "HttpTransport",
    "JsonHttpClient",
    "UrlLibHttpTransport",
    "redact_url",
]
