from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
}


def canonicalize_url(url: str | None) -> str | None:
    if url is None or not url.strip():
        return None

    parsed = urlsplit(url.strip())
    scheme = parsed.scheme.lower() or "https"
    hostname = (parsed.hostname or "").lower()
    netloc = hostname
    if parsed.port is not None and not _is_default_port(scheme, parsed.port):
        netloc = f"{netloc}:{parsed.port}"

    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not _is_tracking_param(key)
    ]
    query = urlencode(sorted(query_items), doseq=True)
    path = parsed.path or ""

    return urlunsplit((scheme, netloc, path, query, ""))


def body_hash(body: str | None) -> str:
    normalized = _normalize_content(body or "")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def dedupe_key(
    *,
    ticker: str,
    provider: str,
    canonical_url: str | None,
    content_hash: str,
) -> str:
    identity = canonical_url or content_hash
    return f"{ticker.upper()}:{provider}:{identity}"


def _normalize_content(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _is_tracking_param(key: str) -> bool:
    normalized = key.lower()
    return normalized.startswith("utm_") or normalized in _TRACKING_PARAMS


def _is_default_port(scheme: str, port: int) -> bool:
    return (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
