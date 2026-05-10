from __future__ import annotations

import pytest
from fastapi import HTTPException

from catalyst_radar.security.access import (
    Role,
    parse_role,
    require_dashboard_role,
    require_role,
    role_allows,
)


def test_role_ordering_allows_analyst_to_read_and_write() -> None:
    assert role_allows("analyst", "viewer") is True
    assert role_allows("viewer", "analyst") is False
    assert role_allows("admin", "analyst") is True


def test_unknown_role_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown role"):
        parse_role("owner")


def test_missing_role_is_rejected() -> None:
    with pytest.raises(ValueError, match="role is required"):
        parse_role(None)


def test_disabled_api_auth_returns_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CATALYST_API_AUTH_MODE", raising=False)

    assert require_role(Role.VIEWER)(None) == Role.ADMIN


def test_header_auth_dependency_rejects_missing_and_insufficient_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CATALYST_API_AUTH_MODE", "header")

    with pytest.raises(HTTPException) as missing:
        require_role(Role.VIEWER)(None)
    assert missing.value.status_code == 401

    with pytest.raises(HTTPException) as forbidden:
        require_role(Role.ANALYST)("viewer")
    assert forbidden.value.status_code == 403

    assert require_role(Role.ANALYST)("analyst") == Role.ANALYST


def test_header_dashboard_auth_rejects_insufficient_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import streamlit as st

    errors: list[str] = []

    def stop() -> None:
        raise RuntimeError("streamlit stopped")

    monkeypatch.setenv("CATALYST_DASHBOARD_AUTH_MODE", "header")
    monkeypatch.setenv("CATALYST_DASHBOARD_ROLE", "viewer")
    monkeypatch.setattr(st, "error", lambda message: errors.append(str(message)))
    monkeypatch.setattr(st, "stop", stop)

    with pytest.raises(RuntimeError, match="streamlit stopped"):
        require_dashboard_role(Role.ANALYST)

    assert errors == ["Insufficient role"]
