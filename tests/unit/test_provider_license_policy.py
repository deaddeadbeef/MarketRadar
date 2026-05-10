from __future__ import annotations

import pytest

from catalyst_radar.security.licenses import (
    ProviderLicenseError,
    policy_for_license,
    provider_license_report_from_payload,
    redact_restricted_external_payload,
    require_external_export_allowed,
    require_prompt_allowed,
    validate_raw_record_policy,
)


def test_known_license_policy_defines_prompt_and_export_flags() -> None:
    policy = policy_for_license("polygon-market-data")

    assert policy.license_tag == "polygon-market-data"
    assert policy.retention_policy == "retain-per-provider-license"
    assert policy.prompt_allowed is False
    assert policy.external_export_allowed is False
    assert policy.attribution_required is True


def test_fixture_license_policy_allows_prompt_but_blocks_external_export() -> None:
    policy = policy_for_license("local-csv-fixture")

    assert policy.prompt_allowed is True
    assert policy.external_export_allowed is False
    assert policy.retention_policy == "local-fixture-retain"


def test_unknown_license_tag_fails_closed() -> None:
    with pytest.raises(ValueError, match="unknown provider license tag"):
        policy_for_license("unknown")


def test_mismatched_retention_policy_fails_closed() -> None:
    with pytest.raises(ValueError, match="does not match license"):
        validate_raw_record_policy("polygon-market-data", "local-fixture-retain")


def test_prompt_boundary_rejects_prompt_blocked_provider_payload() -> None:
    payload = {"audit": {"provider_license_policy": _report("polygon-market-data")}}

    with pytest.raises(ProviderLicenseError, match="blocks prompt use"):
        require_prompt_allowed(payload)


def test_export_boundary_rejects_export_blocked_provider_payload() -> None:
    payload = {"audit": {"provider_license_policy": _report("local-csv-fixture")}}

    with pytest.raises(ProviderLicenseError, match="blocks external export"):
        require_external_export_allowed(payload)


def test_export_redaction_replaces_restricted_nested_payload() -> None:
    payload = {
        "candidate_packet": {
            "id": "packet-msft",
            "payload": {
                "supporting_evidence": [{"summary": "restricted"}],
                "audit": {"provider_license_policy": _report("local-csv-fixture")},
            },
        }
    }

    redacted = redact_restricted_external_payload(payload)

    assert redacted == {
        "candidate_packet": {
            "id": "packet-msft",
            "payload": {
                "external_export_blocked": True,
                "license_tags": ["local-csv-fixture"],
                "attribution_required": False,
            },
        }
    }


def test_provider_license_report_infers_provider_and_fails_unknown() -> None:
    assert provider_license_report_from_payload({"provider": "sec"})["prompt_allowed"] is True
    with pytest.raises(ProviderLicenseError, match="unknown provider license policy"):
        provider_license_report_from_payload({"provider": "unknown-provider"})


def _report(license_tag: str) -> dict[str, object]:
    policy = policy_for_license(license_tag)
    return {
        "license_tags": [license_tag],
        "metadata_complete": True,
        "prompt_allowed": policy.prompt_allowed,
        "external_export_allowed": policy.external_export_allowed,
        "attribution_required": policy.attribution_required,
        "policies": [],
    }
