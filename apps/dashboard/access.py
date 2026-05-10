from __future__ import annotations

from catalyst_radar.security.access import Role, require_dashboard_role


def require_viewer() -> Role:
    return require_dashboard_role(Role.VIEWER)


__all__ = ["require_viewer"]
