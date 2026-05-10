from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import Annotated

from fastapi import Header, HTTPException

from catalyst_radar.core.config import AppConfig


class Role(StrEnum):
    VIEWER = "viewer"
    ANALYST = "analyst"
    ADMIN = "admin"


ROLE_RANK = {
    Role.VIEWER: 1,
    Role.ANALYST: 2,
    Role.ADMIN: 3,
}


def parse_role(value: Role | str | None) -> Role:
    if isinstance(value, Role):
        return value
    if value is None or str(value).strip() == "":
        msg = "role is required"
        raise ValueError(msg)
    text = str(value).strip().lower()
    try:
        return Role(text)
    except ValueError as exc:
        msg = f"unknown role: {value}"
        raise ValueError(msg) from exc


def role_allows(actual: Role | str, required: Role | str) -> bool:
    actual_role = parse_role(actual)
    required_role = parse_role(required)
    return ROLE_RANK[actual_role] >= ROLE_RANK[required_role]


def require_role(required: Role) -> Callable[[str | None], Role]:
    def dependency(x_catalyst_role: Annotated[str | None, Header()] = None) -> Role:
        mode = AppConfig.from_env().api_auth_mode
        if mode == "disabled":
            return Role.ADMIN
        if mode != "header":
            raise HTTPException(status_code=500, detail=f"unsupported auth mode: {mode}")
        try:
            role = parse_role(x_catalyst_role)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        if not role_allows(role, required):
            raise HTTPException(status_code=403, detail="insufficient role")
        return role

    return dependency


def require_dashboard_role(required: Role = Role.VIEWER) -> Role:
    config = AppConfig.from_env()
    if config.dashboard_auth_mode == "disabled":
        return Role.ADMIN
    if config.dashboard_auth_mode != "header":
        import streamlit as st

        st.error(f"Unsupported dashboard auth mode: {config.dashboard_auth_mode}")
        st.stop()
    try:
        role = parse_role(config.dashboard_role)
    except ValueError as exc:
        import streamlit as st

        st.error(str(exc))
        st.stop()
    if not role_allows(role, required):
        import streamlit as st

        st.error("Insufficient role")
        st.stop()
    return role


__all__ = [
    "ROLE_RANK",
    "Role",
    "parse_role",
    "require_dashboard_role",
    "require_role",
    "role_allows",
]
