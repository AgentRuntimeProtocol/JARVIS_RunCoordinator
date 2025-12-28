from __future__ import annotations

from typing import Any

import httpx
from arp_auth import AuthClient
from arp_standard_model import NodeRun, Run
from arp_standard_server import ArpServerError

from ..auth import client_credentials_token
from ..utils import normalize_base_url


class RunStoreClient:
    """HTTP client for the JARVIS Run Store service."""

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

    async def create_run(self, run: Run, *, idempotency_key: str | None = None) -> Run:
        payload: dict[str, Any] = {"run": run.model_dump(mode="json")}
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        response = await self._request("POST", "/v1/runs", json=payload)
        if response.status_code == 409:
            raise ArpServerError(
                code="run_already_exists",
                message=self._detail(response) or "Run already exists.",
                status_code=409,
            )
        data = self._json_or_error(response)
        return Run.model_validate(data["run"])

    async def get_run(self, run_id: str) -> Run | None:
        response = await self._request("GET", f"/v1/runs/{run_id}")
        if response.status_code == 404:
            return None
        data = self._json_or_error(response)
        return Run.model_validate(data["run"])

    async def update_run(self, run: Run) -> Run:
        response = await self._request("PUT", f"/v1/runs/{run.run_id}", json={"run": run.model_dump(mode="json")})
        if response.status_code == 404:
            raise ArpServerError(
                code="run_not_found",
                message=self._detail(response) or "Run not found.",
                status_code=404,
            )
        data = self._json_or_error(response)
        return Run.model_validate(data["run"])

    async def create_node_run(self, node_run: NodeRun, *, idempotency_key: str | None = None) -> NodeRun:
        payload: dict[str, Any] = {"node_run": node_run.model_dump(mode="json")}
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        response = await self._request("POST", "/v1/node-runs", json=payload)
        if response.status_code == 409:
            raise ArpServerError(
                code="node_run_already_exists",
                message=self._detail(response) or "NodeRun already exists.",
                status_code=409,
            )
        data = self._json_or_error(response)
        return NodeRun.model_validate(data["node_run"])

    async def get_node_run(self, node_run_id: str) -> NodeRun | None:
        response = await self._request("GET", f"/v1/node-runs/{node_run_id}")
        if response.status_code == 404:
            return None
        data = self._json_or_error(response)
        return NodeRun.model_validate(data["node_run"])

    async def update_node_run(self, node_run: NodeRun) -> NodeRun:
        response = await self._request(
            "PUT",
            f"/v1/node-runs/{node_run.node_run_id}",
            json={"node_run": node_run.model_dump(mode="json")},
        )
        if response.status_code == 404:
            raise ArpServerError(
                code="node_run_not_found",
                message=self._detail(response) or "NodeRun not found.",
                status_code=404,
            )
        data = self._json_or_error(response)
        return NodeRun.model_validate(data["node_run"])

    async def list_node_runs_for_run(self, run_id: str, *, limit: int = 500) -> list[NodeRun]:
        items: list[NodeRun] = []
        page_token: str | None = None
        while True:
            params: dict[str, Any] = {"limit": limit}
            if page_token:
                params["page_token"] = page_token
            response = await self._request("GET", f"/v1/runs/{run_id}/node-runs", params=params)
            data = self._json_or_error(response)
            items.extend(NodeRun.model_validate(item) for item in data.get("items", []))
            page_token = data.get("next_token")
            if not page_token:
                break
        return items

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        headers: dict[str, str] = {}
        if self._auth_client is not None:
            token = await client_credentials_token(
                self._auth_client,
                audience=self._audience,
                scope=None,
                service_label="Run Store",
            )
            headers["Authorization"] = f"Bearer {token}"
        try:
            return await self._client.request(method, path, json=json, params=params, headers=headers)
        except Exception as exc:
            raise ArpServerError(
                code="run_store_unavailable",
                message="Run Store request failed",
                status_code=502,
                details={"run_store_url": self.base_url, "error": str(exc)},
            ) from exc

    def _json_or_error(self, response: httpx.Response) -> dict[str, Any]:
        if response.is_error:
            raise ArpServerError(
                code="run_store_error",
                message=self._detail(response) or "Run Store request failed.",
                status_code=response.status_code,
                details={"run_store_url": self.base_url},
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ArpServerError(
                code="run_store_error",
                message="Run Store response was not valid JSON.",
                status_code=502,
                details={"run_store_url": self.base_url},
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
