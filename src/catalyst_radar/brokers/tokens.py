from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken


class TokenCipher:
    def __init__(self, key_material: str) -> None:
        key = str(key_material or "").strip()
        if not key:
            msg = "broker token encryption key is required"
            raise ValueError(msg)
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        self._fernet = Fernet(base64.urlsafe_b64encode(digest))

    def encrypt(self, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            msg = "token value must not be blank"
            raise ValueError(msg)
        return self._fernet.encrypt(text.encode("utf-8")).decode("ascii")

    def decrypt(self, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            msg = "encrypted token must not be blank"
            raise ValueError(msg)
        try:
            return self._fernet.decrypt(text.encode("ascii")).decode("utf-8")
        except InvalidToken as exc:
            msg = "encrypted token could not be decrypted"
            raise ValueError(msg) from exc
