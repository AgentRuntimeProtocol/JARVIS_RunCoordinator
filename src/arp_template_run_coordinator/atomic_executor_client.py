from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, TypeVar

from arp_standard_client.atomic_executor import AtomicExecutorClient
from arp_standard_client.errors import ArpApiError
from arp_standard_model import (
    AtomicExecuteRequest,
    AtomicExecuteResult,
    AtomicExecutorCancelAtomicNodeRunParams,
    AtomicExecutorCancelAtomicNodeRunRequest,
    AtomicExecutorExecuteAtomicNodeRunRequest,
    AtomicExecutorHealthRequest,
    AtomicExecutorVersionRequest,
    Health,
    VersionInfo,
)
from arp_standard_server import ArpServerError

T = TypeVar("T")


class AtomicExecutorGatewayClient:
    """Outgoing Atomic Executor client wrapper for the Run Coordinator template."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str | None = None,
        client: AtomicExecutorClient | None = None,
    ) -> None:
        self.base_url = base_url
        self._client = client or AtomicExecutorClient(base_url=base_url, bearer_token=bearer_token)

    # Core methods - outgoing Atomic Executor calls
    async def execute_atomic_node_run(self, body: AtomicExecuteRequest) -> AtomicExecuteResult:
        return await self._call(
            self._client.execute_atomic_node_run,
            AtomicExecutorExecuteAtomicNodeRunRequest(body=body),
        )

    async def cancel_atomic_node_run(self, node_run_id: str) -> None:
        return await self._call(
            self._client.cancel_atomic_node_run,
            AtomicExecutorCancelAtomicNodeRunRequest(
                params=AtomicExecutorCancelAtomicNodeRunParams(node_run_id=node_run_id)
            ),
        )

    async def health(self) -> Health:
        return await self._call(
            self._client.health,
            AtomicExecutorHealthRequest(),
        )

    async def version(self) -> VersionInfo:
        return await self._call(
            self._client.version,
            AtomicExecutorVersionRequest(),
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
                code="atomic_executor_unavailable",
                message="Atomic Executor request failed",
                status_code=502,
                details={
                    "atomic_executor_url": self.base_url,
                    "error": str(exc),
                },
            ) from exc
