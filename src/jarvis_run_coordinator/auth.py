from __future__ import annotations

import asyncio
import os

from arp_auth import AuthClient, AuthError
from arp_standard_server import ArpServerError, AuthSettings

DEFAULT_DEV_KEYCLOAK_ISSUER = "http://localhost:8080/realms/arp-dev"


def _has_auth_env() -> bool:
    return any(key.startswith("ARP_AUTH_") for key in os.environ)


def auth_settings_from_env_or_dev_secure() -> AuthSettings:
    if _has_auth_env():
        return AuthSettings.from_env()
    return AuthSettings(mode="required", issuer=DEFAULT_DEV_KEYCLOAK_ISSUER)


def auth_client_from_env_optional() -> AuthClient | None:
    client_id = (os.environ.get("ARP_AUTH_CLIENT_ID") or "").strip()
    client_secret = (os.environ.get("ARP_AUTH_CLIENT_SECRET") or "").strip()
    if not client_id and not client_secret:
        return None
    if not client_id or not client_secret:
        raise RuntimeError("ARP_AUTH_CLIENT_ID and ARP_AUTH_CLIENT_SECRET are required for outbound auth.")
    try:
        return AuthClient.from_env()
    except AuthError as exc:
        raise RuntimeError(f"Invalid ARP_AUTH_* token exchange config: {exc}") from exc


async def client_credentials_token(
    auth_client: AuthClient,
    *,
    audience: str | None,
    scope: str | None,
    service_label: str,
) -> str:
    try:
        token = await asyncio.to_thread(
            auth_client.client_credentials,
            audience=audience,
            scope=scope,
        )
    except Exception as exc:
        raise ArpServerError(
            code="token_request_failed",
            message=f"{service_label} token request failed",
            status_code=getattr(exc, "status_code", None) or 502,
            details=None,
        ) from exc
    return token.access_token
