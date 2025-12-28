from __future__ import annotations

from typing import Any

import httpx
from arp_auth import AuthClient
from arp_standard_server import ArpServerError

from ..auth import client_credentials_token
from ..utils import normalize_base_url


class ArtifactStoreClient:
    """HTTP client for the JARVIS Artifact Store service."""

    def __init__(
        self,
        *,
        base_url: str,
        auth_client: AuthClient | None = None,
        audience: str | None = None,
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._auth_client = auth_client
        self._audience = audience
        if client is None:
            self.base_url = normalize_base_url(base_url)
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout_seconds)
        else:
            self._client = client
            self.base_url = normalize_base_url(str(client.base_url))

    async def create_artifact(self, data: bytes, *, content_type: str | None = None) -> dict[str, Any]:
        headers = await self._auth_headers()
        if content_type:
            headers["Content-Type"] = content_type
        response = await self._request("POST", "/v1/artifacts", content=data, headers=headers)
        return self._json_or_error(response)

    async def get_metadata(self, artifact_id: str) -> dict[str, Any]:
        headers = await self._auth_headers()
        response = await self._request("GET", f"/v1/artifacts/{artifact_id}/metadata", headers=headers)
        if response.status_code == 404:
            raise ArpServerError(
                code="artifact_not_found",
                message=self._detail(response) or "Artifact not found.",
                status_code=404,
            )
        return self._json_or_error(response)

    async def _auth_headers(self) -> dict[str, str]:
        if self._auth_client is None:
            return {}
        token = await client_credentials_token(
            self._auth_client,
            audience=self._audience,
            scope=None,
            service_label="Artifact Store",
        )
        return {"Authorization": f"Bearer {token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
    ) -> httpx.Response:
        try:
            return await self._client.request(method, path, content=content, headers=headers)
        except Exception as exc:
            raise ArpServerError(
                code="artifact_store_unavailable",
                message="Artifact Store request failed",
                status_code=502,
                details={"artifact_store_url": self.base_url, "error": str(exc)},
            ) from exc

    def _json_or_error(self, response: httpx.Response) -> dict[str, Any]:
        if response.is_error:
            raise ArpServerError(
                code="artifact_store_error",
                message=self._detail(response) or "Artifact Store request failed.",
                status_code=response.status_code,
                details={"artifact_store_url": self.base_url},
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ArpServerError(
                code="artifact_store_error",
                message="Artifact Store response was not valid JSON.",
                status_code=502,
                details={"artifact_store_url": self.base_url},
            ) from exc
        return payload

    @staticmethod
    def _detail(response: httpx.Response) -> str | None:
        try:
            payload = response.json()
        except ValueError:
            return response.text
        detail = payload.get("detail") if isinstance(payload, dict) else None
        if isinstance(detail, str):
            return detail
        return None
