from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from arp_auth import AuthClient
from arp_standard_client.composite_executor import CompositeExecutorClient
from arp_standard_client.errors import ArpApiError
from arp_standard_model import (
    CompositeBeginRequest,
    CompositeBeginResponse,
    CompositeExecutorBeginCompositeNodeRunRequest,
    CompositeExecutorCancelCompositeNodeRunParams,
    CompositeExecutorCancelCompositeNodeRunRequest,
    CompositeExecutorHealthRequest,
    CompositeExecutorVersionRequest,
    Health,
    VersionInfo,
)
from arp_standard_server import ArpServerError

from ..auth import client_credentials_token

class CompositeExecutorGatewayClient:
    """Outgoing Composite Executor client wrapper for the Run Coordinator."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        base_url: str,
        auth_client: AuthClient,
        audience: str | None = None,
        scope: str | None = None,
        client: CompositeExecutorClient | None = None,
        client_factory: Callable[[Any], CompositeExecutorClient] | None = None,
    ) -> None:
        self.base_url = base_url
        self._client = client or CompositeExecutorClient(base_url=base_url)
        self._auth_client = auth_client
        self._audience = audience
        self._scope = scope
        self._client_factory = client_factory or (lambda raw_client: CompositeExecutorClient(client=raw_client))

    # Core methods - outgoing Composite Executor calls
    async def begin_composite_node_run(self, body: CompositeBeginRequest) -> CompositeBeginResponse:
        return await self._call(
            "begin_composite_node_run",
            CompositeExecutorBeginCompositeNodeRunRequest(body=body),
        )

    async def cancel_composite_node_run(self, node_run_id: str) -> None:
        return await self._call(
            "cancel_composite_node_run",
            CompositeExecutorCancelCompositeNodeRunRequest(
                params=CompositeExecutorCancelCompositeNodeRunParams(node_run_id=node_run_id)
            ),
        )

    async def health(self) -> Health:
        return await self._call(
            "health",
            CompositeExecutorHealthRequest(),
        )

    async def version(self) -> VersionInfo:
        return await self._call(
            "version",
            CompositeExecutorVersionRequest(),
        )

    # Helpers (internal): implementation detail for the reference implementation.
    async def _call(self, method_name: str, request: Any) -> Any:
        client = await self._client_for()
        fn: Callable[[Any], Any] = getattr(client, method_name)
        try:
            return await asyncio.to_thread(fn, request)
        except ArpApiError as exc:
            raise ArpServerError(
                code=exc.code,
                message=exc.message,
                status_code=exc.status_code or 502,
                details=exc.details,
            ) from exc
        except Exception as exc:
            raise ArpServerError(
                code="composite_executor_unavailable",
                message="Composite Executor request failed",
                status_code=502,
                details={
                    "composite_executor_url": self.base_url,
                    "error": str(exc),
                },
            ) from exc

    async def _client_for(self) -> CompositeExecutorClient:
        bearer_token = await client_credentials_token(
            self._auth_client,
            audience=self._audience,
            scope=self._scope,
            service_label="Composite Executor",
        )
        raw_client = self._client.raw_client.with_headers({"Authorization": f"Bearer {bearer_token}"})
        return self._client_factory(raw_client)
