from __future__ import annotations

import html
import re
from collections.abc import Mapping
from typing import Any

ANALYSIS_VERSION = "ipo-s1-analysis-v1"
IPO_REGISTRATION_FORMS = frozenset({"S-1", "S-1/A"})

_KNOWN_EXCHANGES = (
    "Nasdaq Global Select Market",
    "Nasdaq Global Market",
    "Nasdaq Capital Market",
    "New York Stock Exchange",
    "NYSE",
    "Nasdaq",
)

_RISK_FLAG_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "history_of_losses",
        (
            "history of losses",
            "net losses",
            "incurred losses",
            "may not achieve profitability",
            "may never achieve profitability",
        ),
    ),
    (
        "customer_concentration",
        (
            "customer concentration",
            "limited number of customers",
            "significant customer",
            "largest customer",
        ),
    ),
    ("going_concern_language", ("going concern",)),
    (
        "dual_class_or_controlled_company",
        (
            "dual class",
            "class b common stock",
            "voting control",
            "controlled company",
        ),
    ),
    ("emerging_growth_company", ("emerging growth company",)),
)

_SECTION_HEADINGS = (
    "prospectus summary",
    "risk factors",
    "use of proceeds",
    "dilution",
    "capitalization",
    "underwriting",
)


def is_ipo_registration_form(form_type: str) -> bool:
    return str(form_type or "").strip().upper() in IPO_REGISTRATION_FORMS


def strip_sec_html(document: str) -> str:
    text = str(document or "")
    text = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", text)
    text = re.sub(r"(?i)</?(p|div|br|tr|li|h[1-6]|table|section)\b[^>]*>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def analyze_s1_offering(
    document_text: str,
    *,
    company_name: str | None,
    ticker: str | None,
    form_type: str,
    source_url: str | None,
) -> dict[str, object]:
    text = strip_sec_html(document_text)
    proposed_ticker = _extract_ticker(text) or _optional_upper(ticker)
    exchange = _extract_exchange(text)
    shares_offered = _extract_shares_offered(text)
    price_low, price_high = _extract_price_range(text)
    midpoint = _midpoint(price_low, price_high)
    estimated_gross_proceeds = _extract_gross_proceeds(text)
    if estimated_gross_proceeds is None and shares_offered is not None and midpoint is not None:
        estimated_gross_proceeds = round(shares_offered * midpoint, 2)

    return {
        "analysis_version": ANALYSIS_VERSION,
        "company_name": _optional_text(company_name),
        "form_type": str(form_type or "").strip().upper(),
        "source_url": _optional_text(source_url),
        "proposed_ticker": proposed_ticker,
        "exchange": exchange,
        "shares_offered": shares_offered,
        "price_range_low": price_low,
        "price_range_high": price_high,
        "price_range_midpoint": midpoint,
        "estimated_gross_proceeds": estimated_gross_proceeds,
        "underwriters": _extract_underwriters(text),
        "use_of_proceeds_summary": _extract_use_of_proceeds(text),
        "risk_flags": _extract_risk_flags(text),
        "sections_found": _sections_found(text),
    }


def summarize_s1_analysis(analysis: Mapping[str, Any]) -> str:
    parts: list[str] = []
    ticker = _optional_text(analysis.get("proposed_ticker"))
    exchange = _optional_text(analysis.get("exchange"))
    if ticker and exchange:
        parts.append(f"proposed listing {ticker} on {exchange}")
    elif ticker:
        parts.append(f"proposed listing {ticker}")
    shares = analysis.get("shares_offered")
    low = analysis.get("price_range_low")
    high = analysis.get("price_range_high")
    if shares is not None:
        parts.append(f"{shares:,} shares offered")
    if low is not None and high is not None:
        parts.append(f"price range ${float(low):.2f}-${float(high):.2f}")
    proceeds = analysis.get("estimated_gross_proceeds")
    if proceeds is not None:
        parts.append(f"estimated gross proceeds ${float(proceeds):,.0f}")
    risk_flags = analysis.get("risk_flags")
    if isinstance(risk_flags, (list, tuple)) and risk_flags:
        parts.append("risk flags " + ", ".join(str(flag) for flag in risk_flags))
    return "; ".join(parts) if parts else "S-1 IPO registration statement detected"


def _extract_ticker(text: str) -> str | None:
    patterns = (
        r"under\s+the\s+(?:ticker\s+)?symbol\s+[\"']?([A-Z][A-Z0-9.-]{0,9})[\"']?",
        r"ticker\s+symbol\s+(?:will\s+be\s+)?[\"']?([A-Z][A-Z0-9.-]{0,9})[\"']?",
        r"trading\s+symbol\s+(?:will\s+be\s+)?[\"']?([A-Z][A-Z0-9.-]{0,9})[\"']?",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).upper().rstrip(".")
    return None


def _extract_exchange(text: str) -> str | None:
    for exchange in _KNOWN_EXCHANGES:
        if re.search(rf"\b{re.escape(exchange)}\b", text, flags=re.IGNORECASE):
            return exchange
    return None


def _extract_shares_offered(text: str) -> int | None:
    patterns = (
        r"(?:we\s+are\s+)?offering\s+(?:up\s+to\s+)?([\d,]+)\s+shares",
        r"([\d,]+)\s+shares\s+(?:of\s+[\w\s]+?\s+)?(?:are\s+being\s+)?offered",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1).replace(",", ""))
    return None


def _extract_price_range(text: str) -> tuple[float | None, float | None]:
    patterns = (
        r"between\s+\$?\s*([\d,.]+)\s+and\s+\$?\s*([\d,.]+)\s+per\s+share",
        r"price\s+range\s+of\s+\$?\s*([\d,.]+)\s+to\s+\$?\s*([\d,.]+)",
        r"expected\s+to\s+be\s+\$?\s*([\d,.]+)\s+to\s+\$?\s*([\d,.]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return _money_to_float(match.group(1)), _money_to_float(match.group(2))
    return None, None


def _extract_gross_proceeds(text: str) -> float | None:
    match = re.search(
        r"gross\s+proceeds[^$.]{0,120}\$?\s*([\d,.]+)\s*(million|billion)?",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    value = _money_to_float(match.group(1))
    if value is None:
        return None
    unit = str(match.group(2) or "").lower()
    if unit == "billion":
        return round(value * 1_000_000_000, 2)
    if unit == "million":
        return round(value * 1_000_000, 2)
    return value


def _extract_underwriters(text: str) -> list[str]:
    patterns = (
        r"([A-Z][A-Za-z0-9 &.,'-]+?)\s+are\s+acting\s+as\s+"
        r"(?:lead\s+)?(?:book-running\s+)?managers",
        r"representatives\s+of\s+the\s+underwriters\s+are\s+([A-Z][A-Za-z0-9 &.,'-]+?)\.",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        names = _split_names(match.group(1))
        if names:
            return names
    return []


def _extract_use_of_proceeds(text: str) -> str | None:
    match = re.search(
        r"use\s+of\s+proceeds\s+(.{20,600}?)(?:risk\s+factors|dilution|capitalization|underwriting|$)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return _sentence_summary(match.group(1), max_length=360)


def _extract_risk_flags(text: str) -> list[str]:
    lower = text.lower()
    flags: list[str] = []
    for flag, patterns in _RISK_FLAG_PATTERNS:
        if any(pattern in lower for pattern in patterns):
            flags.append(flag)
    return flags


def _sections_found(text: str) -> list[str]:
    lower = text.lower()
    return [heading for heading in _SECTION_HEADINGS if heading in lower]


def _split_names(value: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", value).strip(" ,.;")
    if not cleaned:
        return []
    parts = re.split(r"\s+and\s+|,\s+(?=[A-Z][A-Za-z]+(?:\s|$))", cleaned)
    names = [part.strip(" ,.;") for part in parts if part.strip(" ,.;")]
    return names[:8]


def _sentence_summary(value: str, *, max_length: int) -> str:
    cleaned = re.sub(r"\s+", " ", value).strip(" .")
    if len(cleaned) <= max_length:
        return cleaned
    truncated = cleaned[:max_length].rsplit(" ", maxsplit=1)[0].strip(" .,")
    return f"{truncated}..."


def _money_to_float(value: str) -> float | None:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _midpoint(low: float | None, high: float | None) -> float | None:
    if low is None or high is None:
        return None
    return round((low + high) / 2, 4)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_upper(value: object) -> str | None:
    text = _optional_text(value)
    return text.upper() if text else None
