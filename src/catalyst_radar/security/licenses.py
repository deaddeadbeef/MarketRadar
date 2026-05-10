from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProviderLicensePolicy:
    license_tag: str
    retention_policy: str
    raw_retention_days: int | None
    normalized_retention_days: int | None
    prompt_allowed: bool
    external_export_allowed: bool
    attribution_required: bool


POLICIES = {
    "sec-public": ProviderLicensePolicy(
        license_tag="sec-public",
        retention_policy="public-sec-retain",
        raw_retention_days=None,
        normalized_retention_days=None,
        prompt_allowed=True,
        external_export_allowed=True,
        attribution_required=True,
    ),
    "local-csv-fixture": ProviderLicensePolicy(
        license_tag="local-csv-fixture",
        retention_policy="local-fixture-retain",
        raw_retention_days=None,
        normalized_retention_days=None,
        prompt_allowed=True,
        external_export_allowed=False,
        attribution_required=False,
    ),
    "news-fixture": ProviderLicensePolicy(
        license_tag="news-fixture",
        retention_policy="fixture-retain",
        raw_retention_days=None,
        normalized_retention_days=None,
        prompt_allowed=True,
        external_export_allowed=False,
        attribution_required=False,
    ),
    "earnings-fixture": ProviderLicensePolicy(
        license_tag="earnings-fixture",
        retention_policy="fixture-retain",
        raw_retention_days=None,
        normalized_retention_days=None,
        prompt_allowed=True,
        external_export_allowed=False,
        attribution_required=False,
    ),
    "options-fixture": ProviderLicensePolicy(
        license_tag="options-fixture",
        retention_policy="local-fixture-retain",
        raw_retention_days=None,
        normalized_retention_days=None,
        prompt_allowed=True,
        external_export_allowed=False,
        attribution_required=False,
    ),
    "polygon-market-data": ProviderLicensePolicy(
        license_tag="polygon-market-data",
        retention_policy="retain-per-provider-license",
        raw_retention_days=365,
        normalized_retention_days=None,
        prompt_allowed=False,
        external_export_allowed=False,
        attribution_required=True,
    ),
}

PROVIDER_LICENSE_TAGS = {
    "csv": "local-csv-fixture",
    "dry-run": "local-csv-fixture",
    "fixture": "local-csv-fixture",
    "local": "local-csv-fixture",
    "sample": "local-csv-fixture",
    "sec": "sec-public",
    "sec_edgar": "sec-public",
    "news": "news-fixture",
    "news_fixture": "news-fixture",
    "earnings": "earnings-fixture",
    "earnings_fixture": "earnings-fixture",
    "options": "options-fixture",
    "options_fixture": "options-fixture",
    "polygon": "polygon-market-data",
}


class ProviderLicenseError(ValueError):
    pass


def policy_for_license(license_tag: str) -> ProviderLicensePolicy:
    try:
        return POLICIES[license_tag]
    except KeyError as exc:
        raise ValueError(f"unknown provider license tag: {license_tag}") from exc


def license_tag_for_provider(provider: str) -> str:
    key = str(provider).strip().lower()
    try:
        return PROVIDER_LICENSE_TAGS[key]
    except KeyError as exc:
        raise ProviderLicenseError(f"unknown provider license policy: {provider}") from exc


def provider_license_report(license_tags: Iterable[str]) -> dict[str, Any]:
    tags = tuple(dict.fromkeys(str(tag).strip() for tag in license_tags if str(tag).strip()))
    if not tags:
        return {
            "license_tags": [],
            "metadata_complete": False,
            "prompt_allowed": False,
            "external_export_allowed": False,
            "attribution_required": False,
            "policies": [],
        }
    policies = [policy_for_license(tag) for tag in tags]
    return {
        "license_tags": sorted(tags),
        "metadata_complete": True,
        "prompt_allowed": all(policy.prompt_allowed for policy in policies),
        "external_export_allowed": all(
            policy.external_export_allowed for policy in policies
        ),
        "attribution_required": any(policy.attribution_required for policy in policies),
        "policies": [
            {
                "license_tag": policy.license_tag,
                "retention_policy": policy.retention_policy,
                "raw_retention_days": policy.raw_retention_days,
                "normalized_retention_days": policy.normalized_retention_days,
                "prompt_allowed": policy.prompt_allowed,
                "external_export_allowed": policy.external_export_allowed,
                "attribution_required": policy.attribution_required,
            }
            for policy in policies
        ],
    }


def provider_license_report_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    direct = _direct_policy_report(payload)
    if direct is not None:
        return direct
    license_tags = sorted(_collect_license_tags(payload))
    if license_tags:
        return provider_license_report(license_tags)
    providers = sorted(_collect_providers(payload))
    if providers:
        return provider_license_report(license_tag_for_provider(provider) for provider in providers)
    return provider_license_report(())


def require_prompt_allowed(payload: Mapping[str, Any]) -> dict[str, Any]:
    report = provider_license_report_from_payload(payload)
    if not report["metadata_complete"]:
        raise ProviderLicenseError("provider license metadata missing")
    if not report["prompt_allowed"]:
        raise ProviderLicenseError(
            "provider license blocks prompt use: "
            + ",".join(report["license_tags"])
        )
    return report


def require_external_export_allowed(payload: Mapping[str, Any]) -> dict[str, Any]:
    report = provider_license_report_from_payload(payload)
    if not report["metadata_complete"]:
        raise ProviderLicenseError("provider license metadata missing")
    if not report["external_export_allowed"]:
        raise ProviderLicenseError(
            "provider license blocks external export: "
            + ",".join(report["license_tags"])
        )
    return report


def redact_restricted_external_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        report = _direct_policy_report(value)
        if report is not None and not report["external_export_allowed"]:
            return {
                "external_export_blocked": True,
                "license_tags": report["license_tags"],
                "attribution_required": report["attribution_required"],
            }
        return {
            str(key): redact_restricted_external_payload(child)
            for key, child in value.items()
        }
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return [redact_restricted_external_payload(child) for child in value]
    return value


def validate_raw_record_policy(
    license_tag: str,
    retention_policy: str,
) -> ProviderLicensePolicy:
    policy = policy_for_license(license_tag)
    if policy.retention_policy != retention_policy:
        msg = (
            f"retention_policy {retention_policy} does not match license "
            f"{license_tag}"
        )
        raise ValueError(msg)
    return policy


def _direct_policy_report(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    audit = payload.get("audit")
    if not isinstance(audit, Mapping):
        return None
    report = audit.get("provider_license_policy")
    if not isinstance(report, Mapping):
        return None
    raw_tags = report.get("license_tags")
    if not isinstance(raw_tags, Iterable) or isinstance(raw_tags, (str, bytes)):
        return None
    return provider_license_report(str(tag) for tag in raw_tags)


def _collect_license_tags(value: Any) -> set[str]:
    tags: set[str] = set()
    if isinstance(value, Mapping):
        raw_tag = value.get("license_tag")
        if isinstance(raw_tag, str) and raw_tag.strip():
            tags.add(raw_tag.strip())
        for child in value.values():
            tags.update(_collect_license_tags(child))
    elif isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        for child in value:
            tags.update(_collect_license_tags(child))
    return tags


def _collect_providers(value: Any) -> set[str]:
    providers: set[str] = set()
    if isinstance(value, Mapping):
        raw_provider = value.get("provider")
        if isinstance(raw_provider, str) and raw_provider.strip():
            providers.add(raw_provider.strip())
        for child in value.values():
            providers.update(_collect_providers(child))
    elif isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        for child in value:
            providers.update(_collect_providers(child))
    return providers


__all__ = [
    "POLICIES",
    "PROVIDER_LICENSE_TAGS",
    "ProviderLicenseError",
    "ProviderLicensePolicy",
    "license_tag_for_provider",
    "policy_for_license",
    "provider_license_report",
    "provider_license_report_from_payload",
    "redact_restricted_external_payload",
    "require_external_export_allowed",
    "require_prompt_allowed",
    "validate_raw_record_policy",
]
