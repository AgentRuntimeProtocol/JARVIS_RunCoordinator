from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from arp_auth import AuthClient
from arp_standard_client.errors import ArpApiError
from arp_standard_client.pdp import PdpClient
from arp_standard_model import (
    Health,
    PdpDecidePolicyRequest,
    PdpHealthRequest,
    PdpVersionRequest,
    PolicyDecision,
    PolicyDecisionRequest,
    VersionInfo,
)
from arp_standard_server import ArpServerError

from ..auth import client_credentials_token

class PdpGatewayClient:
    """Outgoing PDP client wrapper for the Run Coordinator."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        base_url: str,
        auth_client: AuthClient,
        audience: str | None = None,
        scope: str | None = None,
        client: PdpClient | None = None,
        client_factory: Callable[[Any], PdpClient] | None = None,
    ) -> None:
        self.base_url = base_url
        self._client = client or PdpClient(base_url=base_url)
        self._auth_client = auth_client
        self._audience = audience
        self._scope = scope
        self._client_factory = client_factory or (lambda raw_client: PdpClient(client=raw_client))

    # Core methods - outgoing PDP calls
    async def decide_policy(self, body: PolicyDecisionRequest) -> PolicyDecision:
        return await self._call(
            "decide_policy",
            PdpDecidePolicyRequest(body=body),
        )

    async def health(self) -> Health:
        return await self._call(
            "health",
            PdpHealthRequest(),
        )

    async def version(self) -> VersionInfo:
        return await self._call(
            "version",
            PdpVersionRequest(),
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
                code="pdp_unavailable",
                message="PDP request failed",
                status_code=502,
                details={
                    "pdp_url": self.base_url,
                    "error": str(exc),
                },
            ) from exc

    async def _client_for(self) -> PdpClient:
        bearer_token = await client_credentials_token(
            self._auth_client,
            audience=self._audience,
            scope=self._scope,
            service_label="PDP",
        )
        raw_client = self._client.raw_client.with_headers({"Authorization": f"Bearer {bearer_token}"})
        return self._client_factory(raw_client)
