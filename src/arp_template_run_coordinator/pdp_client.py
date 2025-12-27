from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, TypeVar

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

T = TypeVar("T")


class PdpGatewayClient:
    """Outgoing PDP client wrapper for the Run Coordinator template."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str | None = None,
        client: PdpClient | None = None,
    ) -> None:
        self.base_url = base_url
        self._client = client or PdpClient(base_url=base_url, bearer_token=bearer_token)

    # Core methods - outgoing PDP calls
    async def decide_policy(self, body: PolicyDecisionRequest) -> PolicyDecision:
        return await self._call(
            self._client.decide_policy,
            PdpDecidePolicyRequest(body=body),
        )

    async def health(self) -> Health:
        return await self._call(
            self._client.health,
            PdpHealthRequest(),
        )

    async def version(self) -> VersionInfo:
        return await self._call(
            self._client.version,
            PdpVersionRequest(),
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
                code="pdp_unavailable",
                message="PDP request failed",
                status_code=502,
                details={
                    "pdp_url": self.base_url,
                    "error": str(exc),
                },
            ) from exc
