from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from catalyst_radar.brokers.models import (
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerToken,
    broker_connection_id,
    broker_token_id,
)
from catalyst_radar.brokers.tokens import TokenCipher
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.db import create_schema, engine_from_url


def test_token_cipher_encrypts_and_decrypts_without_plaintext_storage() -> None:
    cipher = TokenCipher("local-development-key")

    encrypted = cipher.encrypt("access-token")

    assert encrypted != "access-token"
    assert cipher.decrypt(encrypted) == "access-token"


def test_token_cipher_rejects_wrong_key() -> None:
    encrypted = TokenCipher("key-a").encrypt("refresh-token")

    with pytest.raises(ValueError, match="could not be decrypted"):
        TokenCipher("key-b").decrypt(encrypted)


def test_token_cipher_requires_key_and_token_values() -> None:
    with pytest.raises(ValueError, match="encryption key is required"):
        TokenCipher("")

    with pytest.raises(ValueError, match="token value must not be blank"):
        TokenCipher("key").encrypt("")


def test_broker_repository_stores_encrypted_tokens(tmp_path: Path) -> None:
    engine = engine_from_url(f"sqlite:///{(tmp_path / 'tokens.db').as_posix()}")
    create_schema(engine)
    repo = BrokerRepository(engine)
    now = datetime(2026, 5, 12, 14, tzinfo=UTC)
    connection_id = broker_connection_id()
    cipher = TokenCipher("local-development-key")
    repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.CONNECTED,
            created_at=now,
            updated_at=now,
            metadata={},
        )
    )

    repo.upsert_token(
        BrokerToken(
            id=broker_token_id(connection_id),
            connection_id=connection_id,
            access_token_encrypted=cipher.encrypt("plain-access-token"),
            refresh_token_encrypted=cipher.encrypt("plain-refresh-token"),
            access_token_expires_at=now,
            refresh_token_expires_at=now,
            created_at=now,
            updated_at=now,
        )
    )

    stored = repo.latest_token(connection_id)

    assert stored is not None
    assert stored.access_token_encrypted != "plain-access-token"
    assert stored.refresh_token_encrypted != "plain-refresh-token"
    assert cipher.decrypt(stored.access_token_encrypted) == "plain-access-token"
    assert stored.refresh_token_encrypted is not None
    assert cipher.decrypt(stored.refresh_token_encrypted) == "plain-refresh-token"
