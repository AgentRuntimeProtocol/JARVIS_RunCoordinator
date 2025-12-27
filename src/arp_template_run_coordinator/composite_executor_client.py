from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, TypeVar

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

T = TypeVar("T")


class CompositeExecutorGatewayClient:
    """Outgoing Composite Executor client wrapper for the Run Coordinator template."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str | None = None,
        client: CompositeExecutorClient | None = None,
    ) -> None:
        self.base_url = base_url
        self._client = client or CompositeExecutorClient(base_url=base_url, bearer_token=bearer_token)

    # Core methods - outgoing Composite Executor calls
    async def begin_composite_node_run(self, body: CompositeBeginRequest) -> CompositeBeginResponse:
        return await self._call(
            self._client.begin_composite_node_run,
            CompositeExecutorBeginCompositeNodeRunRequest(body=body),
        )

    async def cancel_composite_node_run(self, node_run_id: str) -> None:
        return await self._call(
            self._client.cancel_composite_node_run,
            CompositeExecutorCancelCompositeNodeRunRequest(
                params=CompositeExecutorCancelCompositeNodeRunParams(node_run_id=node_run_id)
            ),
        )

    async def health(self) -> Health:
        return await self._call(
            self._client.health,
            CompositeExecutorHealthRequest(),
        )

    async def version(self) -> VersionInfo:
        return await self._call(
            self._client.version,
            CompositeExecutorVersionRequest(),
        )

    # Helpers (internal): implementation detail for the template.
    async def _call(self, fn: Callable[[Any], T], request: Any) -> T:
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
