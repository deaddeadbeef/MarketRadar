from __future__ import annotations

from catalyst_radar.core.runtime import APP_VERSION, SERVICE_NAME, build_info


def test_build_info_prefers_explicit_env_commit_without_secrets() -> None:
    info = build_info(
        env={
            "CATALYST_BUILD_COMMIT": "abcdef1234567890",
            "OPENAI_API_KEY": "sk-secret-value",
            "CATALYST_POLYGON_API_KEY": "polygon-secret-value",
        },
    )

    assert info == {
        "service": SERVICE_NAME,
        "version": APP_VERSION,
        "commit": "abcdef123456",
        "source": "CATALYST_BUILD_COMMIT",
    }
    assert "sk-secret-value" not in str(info)
    assert "polygon-secret-value" not in str(info)
