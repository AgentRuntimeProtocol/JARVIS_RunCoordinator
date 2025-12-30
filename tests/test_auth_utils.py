import asyncio
import os
from types import SimpleNamespace
from typing import cast

import pytest
from arp_auth import AuthClient, AuthError
from arp_standard_server import ArpServerError

from jarvis_run_coordinator.auth import (
    DEFAULT_DEV_KEYCLOAK_ISSUER,
    auth_client_from_env_optional,
    auth_settings_from_env_or_dev_secure,
    client_credentials_token,
)
from jarvis_run_coordinator.utils import normalize_base_url


def _clear_auth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("ARP_AUTH_"):
            monkeypatch.delenv(key, raising=False)


def test_auth_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_auth_env(monkeypatch)
    settings = auth_settings_from_env_or_dev_secure()
    assert settings.mode == "required"
    assert settings.issuer == DEFAULT_DEV_KEYCLOAK_ISSUER

    monkeypatch.setenv("ARP_AUTH_PROFILE", "dev-insecure")
    settings = auth_settings_from_env_or_dev_secure()
    assert settings.mode == "disabled"


def test_auth_client_from_env_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_auth_env(monkeypatch)
    assert auth_client_from_env_optional() is None

    monkeypatch.setenv("ARP_AUTH_CLIENT_ID", "client")
    with pytest.raises(RuntimeError):
        auth_client_from_env_optional()

    monkeypatch.setenv("ARP_AUTH_CLIENT_SECRET", "secret")

    def fail_from_env():
        raise AuthError("boom")

    monkeypatch.setattr("arp_auth.AuthClient.from_env", fail_from_env)
    with pytest.raises(RuntimeError):
        auth_client_from_env_optional()


def test_client_credentials_token_success() -> None:
    class DummyAuthClient:
        def client_credentials(self, *, audience, scope):
            _ = audience
            _ = scope
            return SimpleNamespace(access_token="token-123")

    token = asyncio.run(
        client_credentials_token(
            cast(AuthClient, DummyAuthClient()),
            audience="aud",
            scope=None,
            service_label="Test",
        )
    )
    assert token == "token-123"


def test_client_credentials_token_error() -> None:
    class DummyError(Exception):
        status_code = 401

    class DummyAuthClient:
        def client_credentials(self, *, audience, scope):
            _ = audience
            _ = scope
            raise DummyError("boom")

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(
            client_credentials_token(
                cast(AuthClient, DummyAuthClient()),
                audience="aud",
                scope=None,
                service_label="Test",
            )
        )
    assert excinfo.value.status_code == 401


def test_normalize_base_url() -> None:
    assert normalize_base_url("http://example.com/") == "http://example.com"
    assert normalize_base_url("http://example.com/v1") == "http://example.com"
    assert normalize_base_url("http://example.com/v1/") == "http://example.com"
