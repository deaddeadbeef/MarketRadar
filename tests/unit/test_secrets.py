import pytest

from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.secrets import SecretValue, load_local_dotenv, required_secret


def test_secret_value_never_renders_plaintext() -> None:
    secret = SecretValue("sk-test-secret")

    assert secret.reveal() == "sk-test-secret"
    assert str(secret) == "<redacted>"
    assert repr(secret) == "SecretValue(<redacted>)"
    assert secret.masked() == "sk***et"


def test_required_secret_wraps_nonblank_env_value() -> None:
    secret = required_secret({"OPENAI_API_KEY": " sk-live "}, "OPENAI_API_KEY")

    assert secret.reveal() == "sk-live"


def test_required_secret_fails_closed_for_missing_value() -> None:
    with pytest.raises(ValueError, match="OPENAI_API_KEY is required"):
        required_secret({}, "OPENAI_API_KEY")


def test_local_dotenv_loader_refuses_production() -> None:
    with pytest.raises(ValueError, match="must not load .env.local in production"):
        load_local_dotenv(environment="production", dotenv_path=".env.local")


def test_app_config_sanitized_payload_masks_secrets() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_POLYGON_API_KEY": "polygon-secret",
            "CATALYST_DATABASE_URL": "postgresql://user:pass@db:5432/app",
        }
    )

    payload = config.sanitized()

    assert payload["polygon_api_key"] == "<redacted>"
    assert payload["database_url"] == "postgresql://user:<redacted>@db:5432/app"
