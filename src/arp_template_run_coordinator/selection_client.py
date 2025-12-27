from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, TypeVar

from arp_standard_client.errors import ArpApiError
from arp_standard_client.selection import SelectionClient
from arp_standard_model import (
    CandidateSet,
    CandidateSetRequest,
    Health,
    SelectionGenerateCandidateSetRequest,
    SelectionHealthRequest,
    SelectionVersionRequest,
    VersionInfo,
)
from arp_standard_server import ArpServerError

T = TypeVar("T")


class SelectionGatewayClient:
    """Outgoing Selection Service client wrapper for the Run Coordinator template."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str | None = None,
        client: SelectionClient | None = None,
    ) -> None:
        self.base_url = base_url
        self._client = client or SelectionClient(base_url=base_url, bearer_token=bearer_token)

    # Core methods - outgoing Selection calls
    async def generate_candidate_set(self, body: CandidateSetRequest) -> CandidateSet:
        return await self._call(
            self._client.generate_candidate_set,
            SelectionGenerateCandidateSetRequest(body=body),
        )

    async def health(self) -> Health:
        return await self._call(
            self._client.health,
            SelectionHealthRequest(),
        )

    async def version(self) -> VersionInfo:
        return await self._call(
            self._client.version,
            SelectionVersionRequest(),
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
                code="selection_service_unavailable",
                message="Selection Service request failed",
                status_code=502,
                details={
                    "selection_service_url": self.base_url,
                    "error": str(exc),
                },
            ) from exc
