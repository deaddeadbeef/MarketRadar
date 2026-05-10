from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class SecretValue:
    _value: str

    def __post_init__(self) -> None:
        if not str(self._value).strip():
            raise ValueError("secret value must not be blank")
        object.__setattr__(self, "_value", str(self._value).strip())

    def reveal(self) -> str:
        return self._value

    def masked(self) -> str:
        if len(self._value) <= 4:
            return "<redacted>"
        return f"{self._value[:2]}***{self._value[-2:]}"

    def __str__(self) -> str:
        return "<redacted>"

    def __repr__(self) -> str:
        return "SecretValue(<redacted>)"


def required_secret(source: Mapping[str, str], key: str) -> SecretValue:
    raw = source.get(key)
    if raw is None or raw.strip() == "":
        raise ValueError(f"{key} is required")
    return SecretValue(raw)


def optional_secret(source: Mapping[str, str], key: str) -> SecretValue | None:
    raw = source.get(key)
    return None if raw is None or raw.strip() == "" else SecretValue(raw)


def load_local_dotenv(*, environment: str, dotenv_path: str = ".env.local") -> bool:
    if environment.strip().lower() in {"production", "prod"}:
        raise ValueError("must not load .env.local in production")
    from dotenv import load_dotenv

    return bool(load_dotenv(dotenv_path, override=False))
