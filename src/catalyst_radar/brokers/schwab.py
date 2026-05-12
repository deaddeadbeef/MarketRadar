from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

from catalyst_radar.connectors.http import JsonHttpClient
from catalyst_radar.core.config import AppConfig

DEFAULT_SCHWAB_SCOPES = ("readonly",)


class SchwabConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class SchwabOAuthSettings:
    client_id: str
    client_secret: str
    redirect_uri: str
    auth_base_url: str

    @classmethod
    def from_config(cls, config: AppConfig) -> SchwabOAuthSettings:
        missing = []
        if not config.schwab_client_id:
            missing.append("SCHWAB_CLIENT_ID")
        if not config.schwab_client_secret:
            missing.append("SCHWAB_CLIENT_SECRET")
        if not config.schwab_redirect_uri:
            missing.append("SCHWAB_REDIRECT_URI")
        if missing:
            msg = f"missing Schwab OAuth settings: {', '.join(missing)}"
            raise SchwabConfigurationError(msg)
        return cls(
            client_id=str(config.schwab_client_id),
            client_secret=str(config.schwab_client_secret),
            redirect_uri=str(config.schwab_redirect_uri),
            auth_base_url=config.schwab_auth_base_url.rstrip("/"),
        )


class SchwabOAuthService:
    def __init__(
        self,
        settings: SchwabOAuthSettings,
        client: JsonHttpClient | None = None,
    ) -> None:
        self.settings = settings
        self.client = client

    def authorization_url(
        self,
        *,
        state: str,
        scopes: Sequence[str] = DEFAULT_SCHWAB_SCOPES,
    ) -> str:
        query = urlencode(
            {
                "response_type": "code",
                "client_id": self.settings.client_id,
                "redirect_uri": self.settings.redirect_uri,
                "scope": " ".join(scopes),
                "state": state,
            }
        )
        return f"{self.settings.auth_base_url}/authorize?{query}"

    def exchange_code(self, code: str) -> Mapping[str, Any]:
        if self.client is None:
            msg = "Schwab token exchange client is not configured"
            raise SchwabConfigurationError(msg)
        auth = base64.b64encode(
            f"{self.settings.client_id}:{self.settings.client_secret}".encode()
        ).decode("ascii")
        payload = self.client.post_form_json(
            f"{self.settings.auth_base_url}/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.settings.redirect_uri,
            },
            headers={"authorization": f"Basic {auth}"},
        )
        return _mapping(payload, "token response")

    def refresh_access_token(self, refresh_token: str) -> Mapping[str, Any]:
        if self.client is None:
            msg = "Schwab token refresh client is not configured"
            raise SchwabConfigurationError(msg)
        auth = base64.b64encode(
            f"{self.settings.client_id}:{self.settings.client_secret}".encode()
        ).decode("ascii")
        payload = self.client.post_form_json(
            f"{self.settings.auth_base_url}/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            headers={"authorization": f"Basic {auth}"},
        )
        return _mapping(payload, "refresh token response")


class SchwabClient:
    def __init__(
        self,
        *,
        client: JsonHttpClient,
        access_token: str,
        base_url: str = "https://api.schwabapi.com",
    ) -> None:
        token = str(access_token or "").strip()
        if not token:
            msg = "Schwab access token is required"
            raise SchwabConfigurationError(msg)
        self.client = client
        self.access_token = token
        self.base_url = base_url.rstrip("/")

    def get_account_numbers(self) -> list[Mapping[str, Any]]:
        payload = self.client.get_json(
            f"{self.base_url}/trader/v1/accounts/accountNumbers",
            headers=self._headers(),
        )
        return _records(payload)

    def get_accounts_with_positions(self) -> list[Mapping[str, Any]]:
        payload = self.client.get_json(
            f"{self.base_url}/trader/v1/accounts?fields=positions",
            headers=self._headers(),
        )
        return _records(payload)

    def get_open_orders(
        self,
        account_hash: str,
        *,
        now: datetime | None = None,
    ) -> list[Mapping[str, Any]]:
        timestamp = (now or datetime.now(UTC)).astimezone(UTC)
        from_time = (timestamp - timedelta(days=30)).isoformat().replace("+00:00", "Z")
        to_time = timestamp.isoformat().replace("+00:00", "Z")
        query = urlencode(
            {
                "fromEnteredTime": from_time,
                "toEnteredTime": to_time,
                "status": "WORKING",
            }
        )
        payload = self.client.get_json(
            f"{self.base_url}/trader/v1/accounts/{account_hash}/orders?{query}",
            headers=self._headers(),
        )
        return _records(payload)

    def get_quotes(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        normalized = sorted({str(symbol).strip().upper() for symbol in symbols if symbol})
        if not normalized:
            return {}
        query = urlencode({"symbols": ",".join(normalized), "indicative": "false"})
        payload = self.client.get_json(
            f"{self.base_url}/marketdata/v1/quotes?{query}",
            headers=self._headers(),
        )
        return _mapping(payload, "quotes response")

    def get_price_history(
        self,
        symbol: str,
        *,
        period_type: str = "day",
        period: int = 10,
        frequency_type: str = "minute",
        frequency: int = 5,
    ) -> Mapping[str, Any]:
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            msg = "Schwab price history symbol is required"
            raise SchwabConfigurationError(msg)
        query = urlencode(
            {
                "symbol": ticker,
                "periodType": period_type,
                "period": period,
                "frequencyType": frequency_type,
                "frequency": frequency,
                "needExtendedHoursData": "true",
                "needPreviousClose": "true",
            }
        )
        payload = self.client.get_json(
            f"{self.base_url}/marketdata/v1/pricehistory?{query}",
            headers=self._headers(),
        )
        return _mapping(payload, "price history response")

    def get_option_chain(self, symbol: str) -> Mapping[str, Any]:
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            msg = "Schwab option chain symbol is required"
            raise SchwabConfigurationError(msg)
        query = urlencode({"symbol": ticker, "contractType": "ALL", "strategy": "SINGLE"})
        payload = self.client.get_json(
            f"{self.base_url}/marketdata/v1/chains?{query}",
            headers=self._headers(),
        )
        return _mapping(payload, "option chain response")

    def _headers(self) -> Mapping[str, str]:
        return {"authorization": f"Bearer {self.access_token}"}


def _records(value: object) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [_mapping(item, "record") for item in value if isinstance(item, Mapping)]
    if isinstance(value, Mapping) and isinstance(value.get("accounts"), list):
        return [
            _mapping(item, "account")
            for item in value.get("accounts", [])
            if isinstance(item, Mapping)
        ]
    return []


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"Schwab {name} must be a JSON object"
        raise RuntimeError(msg)
    return value


__all__ = [
    "SchwabClient",
    "SchwabConfigurationError",
    "SchwabOAuthService",
    "SchwabOAuthSettings",
]
