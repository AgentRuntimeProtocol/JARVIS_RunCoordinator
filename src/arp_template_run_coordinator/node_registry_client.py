from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, TypeVar

from arp_standard_client.errors import ArpApiError
from arp_standard_client.node_registry import NodeRegistryClient
from arp_standard_model import (
    Health,
    NodeKind,
    NodeRegistryGetNodeTypeParams,
    NodeRegistryGetNodeTypeRequest,
    NodeRegistryHealthRequest,
    NodeRegistryListNodeTypesParams,
    NodeRegistryListNodeTypesRequest,
    NodeRegistryPublishNodeTypeRequest,
    NodeRegistryVersionRequest,
    NodeType,
    NodeTypePublishRequest,
    VersionInfo,
)
from arp_standard_server import ArpServerError

T = TypeVar("T")


class NodeRegistryGatewayClient:
    """Outgoing Node Registry client wrapper for the Run Coordinator template."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str | None = None,
        client: NodeRegistryClient | None = None,
    ) -> None:
        self.base_url = base_url
        self._client = client or NodeRegistryClient(base_url=base_url, bearer_token=bearer_token)

    # Core methods - outgoing Node Registry calls
    async def publish_node_type(self, node_type: NodeType) -> NodeType:
        return await self._call(
            self._client.publish_node_type,
            NodeRegistryPublishNodeTypeRequest(body=NodeTypePublishRequest(node_type=node_type)),
        )

    async def get_node_type(self, node_type_id: str, version: str | None = None) -> NodeType:
        return await self._call(
            self._client.get_node_type,
            NodeRegistryGetNodeTypeRequest(
                params=NodeRegistryGetNodeTypeParams(node_type_id=node_type_id, version=version)
            ),
        )

    async def list_node_types(self, q: str | None = None, kind: NodeKind | None = None) -> list[NodeType]:
        return await self._call(
            self._client.list_node_types,
            NodeRegistryListNodeTypesRequest(params=NodeRegistryListNodeTypesParams(q=q, kind=kind)),
        )

    async def health(self) -> Health:
        return await self._call(
            self._client.health,
            NodeRegistryHealthRequest(),
        )

    async def version(self) -> VersionInfo:
        return await self._call(
            self._client.version,
            NodeRegistryVersionRequest(),
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
                code="node_registry_unavailable",
                message="Node Registry request failed",
                status_code=502,
                details={
                    "node_registry_url": self.base_url,
                    "error": str(exc),
                },
            ) from exc
