from __future__ import annotations

from typing import Any

import httpx
from arp_auth import AuthClient
from arp_standard_server import ArpServerError

from ..auth import client_credentials_token
from ..utils import normalize_base_url


class EventStreamClient:
    """HTTP client for the JARVIS Event Stream service."""

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

    async def append_events(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        response = await self._request("POST", "/v1/run-events", json={"events": events})
        if response.status_code == 409:
            raise ArpServerError(
                code="event_conflict",
                message=self._detail(response) or "Event already exists.",
                status_code=409,
            )
        if response.status_code == 422:
            raise ArpServerError(
                code="event_validation_failed",
                message=self._detail(response) or "Event validation failed.",
                status_code=422,
            )
        return self._json_or_error(response)

    async def stream_run_events(self, run_id: str) -> str:
        response = await self._request(
            "GET",
            f"/v1/runs/{run_id}/events",
            headers={"Accept": "application/x-ndjson"},
        )
        if response.is_error:
            raise ArpServerError(
                code="event_stream_error",
                message=self._detail(response) or "Event Stream request failed.",
                status_code=response.status_code,
                details={"event_stream_url": self.base_url},
            )
        return response.text

    async def stream_node_run_events(self, node_run_id: str) -> str:
        response = await self._request(
            "GET",
            f"/v1/node-runs/{node_run_id}/events",
            headers={"Accept": "application/x-ndjson"},
        )
        if response.is_error:
            raise ArpServerError(
                code="event_stream_error",
                message=self._detail(response) or "Event Stream request failed.",
                status_code=response.status_code,
                details={"event_stream_url": self.base_url},
            )
        return response.text

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        req_headers: dict[str, str] = {}
        if headers:
            req_headers.update(headers)
        if self._auth_client is not None:
            token = await client_credentials_token(
                self._auth_client,
                audience=self._audience,
                scope=None,
                service_label="Event Stream",
            )
            req_headers["Authorization"] = f"Bearer {token}"
        try:
            return await self._client.request(method, path, json=json, params=params, headers=req_headers)
        except Exception as exc:
            raise ArpServerError(
                code="event_stream_unavailable",
                message="Event Stream request failed",
                status_code=502,
                details={"event_stream_url": self.base_url, "error": str(exc)},
            ) from exc

    def _json_or_error(self, response: httpx.Response) -> dict[str, Any]:
        if response.is_error:
            raise ArpServerError(
                code="event_stream_error",
                message=self._detail(response) or "Event Stream request failed.",
                status_code=response.status_code,
                details={"event_stream_url": self.base_url},
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ArpServerError(
                code="event_stream_error",
                message="Event Stream response was not valid JSON.",
                status_code=502,
                details={"event_stream_url": self.base_url},
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
