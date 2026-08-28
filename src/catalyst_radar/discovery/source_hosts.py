"""Classify world-event URLs by host. Wrapper provider (x) does not win."""

from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urlparse

# Strongest evidence first. Hunt posts wrap official pages as provider=x;
# the host is what the north-star "primary source" bar actually needs.
REGULATORY = "regulatory"
PRIMARY_SOURCE = "primary_source"
COMPANY_PRESS_RELEASE = "company_press_release"
REPUTABLE_NEWS = "reputable_news"
SOCIAL = "social"
UNKNOWN = "unknown"

_RANK = {
    REGULATORY: 40,
    PRIMARY_SOURCE: 40,
    COMPANY_PRESS_RELEASE: 30,
    REPUTABLE_NEWS: 20,
    SOCIAL: 10,
    UNKNOWN: 0,
    "promotional": 5,
    "aggregator": 15,
    "analyst_provider": 18,
}

_REPUTABLE_HOSTS = (
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "apnews.com",
    "nytimes.com",
    "cnbc.com",
)

_WIRE_HOSTS = (
    "businesswire.com",
    "prnewswire.com",
    "globenewswire.com",
    "accesswire.com",
)

_SOCIAL_HOSTS = (
    "x.com",
    "twitter.com",
    "t.co",
    "truthsocial.com",
    "facebook.com",
    "instagram.com",
    "reddit.com",
    "stocktwits.com",
    "youtube.com",
    "tiktok.com",
)

_IR_HOST_PREFIXES = (
    "investors.",
    "investor.",
    "ir.",
    "shareholder.",
    "shareholders.",
)


def classify_url(url: object) -> str:
    """Return a SourceCategory-compatible string for one URL."""
    host = _host(url)
    if not host:
        return UNKNOWN
    if _suffix_match(host, _SOCIAL_HOSTS):
        return SOCIAL
    if host.endswith(".gov") or host.endswith(".fed.us"):
        return REGULATORY
    if host.endswith("fed.org") or host.endswith("federalreserve.org"):
        return REGULATORY
    if _suffix_match(host, _WIRE_HOSTS):
        return COMPANY_PRESS_RELEASE
    if _is_company_ir(host):
        return PRIMARY_SOURCE
    if _suffix_match(host, _REPUTABLE_HOSTS):
        return REPUTABLE_NEWS
    return UNKNOWN


def best_source_category(
    urls: Iterable[object],
    *,
    declared: str | None = None,
) -> str:
    """Pick the strongest category among URL hosts and an optional declared value.

    Never downgrades a stronger declared label. Upgrades `social` when an
    official/IR/wire URL is sitting on the same event.
    """
    best = _norm(declared)
    best_rank = _RANK.get(best, 0)
    for url in urls:
        kind = classify_url(url)
        rank = _RANK.get(kind, 0)
        if rank > best_rank:
            best = kind
            best_rank = rank
    return best if best != UNKNOWN else SOCIAL


def is_primary_host(url: object) -> bool:
    return classify_url(url) in {REGULATORY, PRIMARY_SOURCE, COMPANY_PRESS_RELEASE}


def is_reputable_host(url: object) -> bool:
    return classify_url(url) == REPUTABLE_NEWS


def is_social_host(url: object) -> bool:
    return classify_url(url) == SOCIAL


def _host(url: object) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = (parsed.hostname or "").casefold()
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_company_ir(host: str) -> bool:
    if any(host.startswith(prefix) for prefix in _IR_HOST_PREFIXES):
        return True
    # investors.example.com already covered; example.com/investors is a path,
    # not a host. Host-only on purpose — path IR is too easy to spoof.
    return False


def _suffix_match(host: str, suffixes: Iterable[str]) -> bool:
    for suffix in suffixes:
        needle = suffix.casefold()
        if host == needle or host.endswith("." + needle):
            return True
    return False


def _norm(value: str | None) -> str:
    text = str(value or "").strip().casefold()
    if not text:
        return UNKNOWN
    return text
