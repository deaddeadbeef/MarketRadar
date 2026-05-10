from __future__ import annotations

import pytest

from catalyst_radar.security.licenses import (
    policy_for_license,
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
    assert policy.retention_policy == "retain-local-fixture"


def test_unknown_license_tag_fails_closed() -> None:
    with pytest.raises(ValueError, match="unknown provider license tag"):
        policy_for_license("unknown")


def test_mismatched_retention_policy_fails_closed() -> None:
    with pytest.raises(ValueError, match="does not match license"):
        validate_raw_record_policy("polygon-market-data", "retain-local-fixture")
