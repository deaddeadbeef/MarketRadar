from __future__ import annotations

from dataclasses import dataclass


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


def policy_for_license(license_tag: str) -> ProviderLicensePolicy:
    try:
        return POLICIES[license_tag]
    except KeyError as exc:
        raise ValueError(f"unknown provider license tag: {license_tag}") from exc


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


__all__ = [
    "POLICIES",
    "ProviderLicensePolicy",
    "policy_for_license",
    "validate_raw_record_policy",
]
