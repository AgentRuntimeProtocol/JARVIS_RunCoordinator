from __future__ import annotations

import os

from .auth import auth_client_from_env_optional, auth_settings_from_env_or_dev_secure
from .clients import (
    ArtifactStoreClient,
    AtomicExecutorGatewayClient,
    CompositeExecutorGatewayClient,
    EventStreamClient,
    NodeRegistryGatewayClient,
    PdpGatewayClient,
    RunStoreClient,
    SelectionGatewayClient,
)
from .coordinator import RunCoordinator
from .utils import normalize_base_url


def create_app():
    run_store_url = _require_url("JARVIS_RUN_STORE_URL")
    event_stream_url = _require_url("JARVIS_EVENT_STREAM_URL")
    artifact_store_url = _require_url("JARVIS_ARTIFACT_STORE_URL")

    if (auth_client := auth_client_from_env_optional()) is None:
        raise RuntimeError("ARP_AUTH_CLIENT_ID and ARP_AUTH_CLIENT_SECRET are required for outbound auth.")
    run_store = RunStoreClient(
        base_url=run_store_url,
        auth_client=auth_client,
        audience=_audience_from_env(
            "JARVIS_RUN_STORE_AUDIENCE",
            default="arp-jarvis-runstore",
        ),
    )
    event_stream = EventStreamClient(
        base_url=event_stream_url,
        auth_client=auth_client,
        audience=_audience_from_env(
            "JARVIS_EVENT_STREAM_AUDIENCE",
            default="arp-jarvis-eventstream",
        ),
    )
    artifact_store = ArtifactStoreClient(
        base_url=artifact_store_url,
        auth_client=auth_client,
        audience=_audience_from_env(
            "JARVIS_ARTIFACT_STORE_AUDIENCE",
            default="arp-jarvis-artifactstore",
        ),
    )

    atomic_url = (
        os.environ.get("JARVIS_ATOMIC_EXECUTOR_URL")
        or os.environ.get("ARP_ATOMIC_EXECUTOR_URL")
        or ""
    ).strip()
    composite_url = (
        os.environ.get("JARVIS_COMPOSITE_EXECUTOR_URL")
        or os.environ.get("ARP_COMPOSITE_EXECUTOR_URL")
        or ""
    ).strip()
    selection_url = (
        os.environ.get("JARVIS_SELECTION_URL")
        or os.environ.get("ARP_SELECTION_URL")
        or ""
    ).strip()
    node_registry_url = (
        os.environ.get("JARVIS_NODE_REGISTRY_URL")
        or os.environ.get("ARP_NODE_REGISTRY_URL")
        or ""
    ).strip()
    pdp_url = (os.environ.get("JARVIS_PDP_URL") or os.environ.get("ARP_PDP_URL") or "").strip()

    atomic_executor = (
        AtomicExecutorGatewayClient(
            base_url=atomic_url,
            auth_client=auth_client,
            audience=_audience_from_env(
                "JARVIS_ATOMIC_EXECUTOR_AUDIENCE",
                default="arp-jarvis-atomicexecutor",
            ),
        )
        if atomic_url
        else None
    )
    composite_executor = (
        CompositeExecutorGatewayClient(
            base_url=composite_url,
            auth_client=auth_client,
            audience=_audience_from_env(
                "JARVIS_COMPOSITE_EXECUTOR_AUDIENCE",
                default="arp-jarvis-compositeexecutor",
            ),
        )
        if composite_url
        else None
    )
    selection_service = (
        SelectionGatewayClient(
            base_url=selection_url,
            auth_client=auth_client,
            audience=_audience_from_env(
                "JARVIS_SELECTION_AUDIENCE",
                default="arp-jarvis-selection",
            ),
        )
        if selection_url
        else None
    )
    node_registry = (
        NodeRegistryGatewayClient(
            base_url=node_registry_url,
            auth_client=auth_client,
            audience=_audience_from_env(
                "JARVIS_NODE_REGISTRY_AUDIENCE",
                default="arp-jarvis-noderegistry",
            ),
        )
        if node_registry_url
        else None
    )
    pdp = (
        PdpGatewayClient(
            base_url=pdp_url,
            auth_client=auth_client,
            audience=_audience_from_env(
                "JARVIS_PDP_AUDIENCE",
                default="arp-jarvis-pdp",
            ),
        )
        if pdp_url
        else None
    )

    return RunCoordinator(
        atomic_executor=atomic_executor,
        composite_executor=composite_executor,
        selection_service=selection_service,
        node_registry=node_registry,
        pdp=pdp,
        run_store=run_store,
        event_stream=event_stream,
        artifact_store=artifact_store,
    ).create_app(
        title="JARVIS Run Coordinator",
        auth_settings=auth_settings_from_env_or_dev_secure(),
    )


app = create_app()


def _require_url(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} is required to start the Run Coordinator")
    return normalize_base_url(value)


def _audience_from_env(audience_var: str, *, default: str | None) -> str | None:
    value = (os.environ.get(audience_var) or "").strip()
    if value:
        return value
    return default
