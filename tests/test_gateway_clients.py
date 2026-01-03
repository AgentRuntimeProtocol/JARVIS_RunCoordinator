import asyncio
from datetime import datetime, timezone
from typing import Any, cast

import pytest
from arp_auth import AuthClient
from arp_standard_client.errors import ArpApiError
from arp_standard_model import (
    AtomicExecuteRequest,
    AtomicExecuteResult,
    Candidate,
    CandidateSet,
    CandidateSetRequest,
    CompositeBeginRequest,
    CompositeBeginResponse,
    EndpointLocator,
    Health,
    NodeKind,
    NodeRunState,
    NodeType,
    NodeTypeRef,
    PolicyDecision,
    PolicyDecisionOutcome,
    PolicyDecisionRequest,
    RunContext,
    Status,
    SubtaskSpec,
    VersionInfo,
)
from arp_standard_server import ArpServerError

import jarvis_run_coordinator.clients.atomic_executor_client as atomic_module
import jarvis_run_coordinator.clients.composite_executor_client as composite_module
import jarvis_run_coordinator.clients.node_registry_client as registry_module
import jarvis_run_coordinator.clients.pdp_client as pdp_module
import jarvis_run_coordinator.clients.selection_client as selection_module
from jarvis_run_coordinator.clients.atomic_executor_client import AtomicExecutorGatewayClient
from jarvis_run_coordinator.clients.composite_executor_client import CompositeExecutorGatewayClient
from jarvis_run_coordinator.clients.node_registry_client import NodeRegistryGatewayClient
from jarvis_run_coordinator.clients.pdp_client import PdpGatewayClient
from jarvis_run_coordinator.clients.selection_client import SelectionGatewayClient


class DummyRawClient:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

    def with_headers(self, headers: dict[str, str]):
        self.headers = headers
        return self


class DummyClient:
    def __init__(self, responses: dict[str, object]):
        self.raw_client = DummyRawClient()
        self._responses = responses

    def __getattr__(self, name: str):
        def handler(_request):
            value = self._responses[name]
            if isinstance(value, Exception):
                raise value
            return value

        return handler


class DummyAuthClient:
    pass


async def _fake_token(*_args, **_kwargs) -> str:
    return "token"


def _health() -> Health:
    return Health(status=Status.ok, time=datetime.now(timezone.utc))


def _version() -> VersionInfo:
    return VersionInfo(
        service_name="svc",
        service_version="0.1.0",
        supported_api_versions=["v1"],
    )


def test_atomic_executor_gateway_success(monkeypatch) -> None:
    responses = {
        "execute_atomic_node_run": AtomicExecuteResult(
            node_run_id="node-1",
            state=NodeRunState.succeeded,
        ),
        "cancel_atomic_node_run": None,
        "health": _health(),
        "version": _version(),
    }
    dummy = DummyClient(responses)
    monkeypatch.setattr(atomic_module, "client_credentials_token", _fake_token)

    gateway = AtomicExecutorGatewayClient(
        base_url="http://atomic",
        auth_client=cast(AuthClient, DummyAuthClient()),
        client=cast(Any, dummy),
        client_factory=cast(Any, lambda raw: dummy),
    )

    request = AtomicExecuteRequest(
        run_id="run-1",
        node_run_id="node-1",
        node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.7"),
        inputs={"ping": "pong"},
    )
    result = asyncio.run(gateway.execute_atomic_node_run(request))
    assert result.node_run_id == "node-1"
    assert asyncio.run(gateway.cancel_atomic_node_run("node-1")) is None
    assert asyncio.run(gateway.health()).status == Status.ok
    assert asyncio.run(gateway.version()).service_name == "svc"

    client = asyncio.run(gateway._client_for())
    assert client is dummy
    assert dummy.raw_client.headers["Authorization"] == "Bearer token"


def test_composite_executor_gateway_success(monkeypatch) -> None:
    responses = {
        "begin_composite_node_run": CompositeBeginResponse(accepted=True),
        "cancel_composite_node_run": None,
        "health": _health(),
        "version": _version(),
    }
    dummy = DummyClient(responses)
    monkeypatch.setattr(composite_module, "client_credentials_token", _fake_token)

    gateway = CompositeExecutorGatewayClient(
        base_url="http://composite",
        auth_client=cast(AuthClient, DummyAuthClient()),
        client=cast(Any, dummy),
        client_factory=cast(Any, lambda raw: dummy),
    )

    request = CompositeBeginRequest(
        run_id="run-1",
        node_run_id="node-1",
        node_type_ref=NodeTypeRef(node_type_id="composite.plan", version="0.1.0"),
        inputs={"goal": "do"},
        coordinator_endpoint=EndpointLocator.model_validate("http://coordinator"),
    )
    result = asyncio.run(gateway.begin_composite_node_run(request))
    assert result.accepted
    assert asyncio.run(gateway.cancel_composite_node_run("node-1")) is None
    assert asyncio.run(gateway.health()).status == Status.ok
    assert asyncio.run(gateway.version()).service_name == "svc"


def test_selection_gateway_success(monkeypatch) -> None:
    responses = {
        "generate_candidate_set": CandidateSet(
            candidate_set_id="set-1",
            subtask_id="subtask-1",
            candidates=[
                Candidate(
                    node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.7"),
                    score=0.5,
                )
            ],
        ),
        "health": _health(),
        "version": _version(),
    }
    dummy = DummyClient(responses)
    monkeypatch.setattr(selection_module, "client_credentials_token", _fake_token)

    gateway = SelectionGatewayClient(
        base_url="http://selection",
        auth_client=cast(AuthClient, DummyAuthClient()),
        client=cast(Any, dummy),
        client_factory=cast(Any, lambda raw: dummy),
    )

    request = CandidateSetRequest(
        subtask_spec=SubtaskSpec(goal="do thing", subtask_id="subtask-1"),
        run_context=RunContext.model_validate({}),
    )
    result = asyncio.run(gateway.generate_candidate_set(request))
    assert result.candidate_set_id == "set-1"
    assert asyncio.run(gateway.health()).status == Status.ok
    assert asyncio.run(gateway.version()).service_name == "svc"


def test_node_registry_gateway_success(monkeypatch) -> None:
    responses = {
        "publish_node_type": NodeType(
            node_type_id="jarvis.core.echo",
            version="0.3.7",
            kind=NodeKind.atomic,
        ),
        "get_node_type": NodeType(
            node_type_id="jarvis.core.echo",
            version="0.3.7",
            kind=NodeKind.atomic,
        ),
        "list_node_types": [],
        "health": _health(),
        "version": _version(),
    }
    dummy = DummyClient(responses)
    monkeypatch.setattr(registry_module, "client_credentials_token", _fake_token)

    gateway = NodeRegistryGatewayClient(
        base_url="http://registry",
        auth_client=cast(AuthClient, DummyAuthClient()),
        client=cast(Any, dummy),
        client_factory=cast(Any, lambda raw: dummy),
    )

    node_type = NodeType(
        node_type_id="jarvis.core.echo",
        version="0.3.7",
        kind=NodeKind.atomic,
    )
    assert asyncio.run(gateway.publish_node_type(node_type)).node_type_id == "jarvis.core.echo"
    assert asyncio.run(gateway.get_node_type("jarvis.core.echo", "0.3.7")).version == "0.3.7"
    assert asyncio.run(gateway.list_node_types()) == []
    assert asyncio.run(gateway.health()).status == Status.ok
    assert asyncio.run(gateway.version()).service_name == "svc"


def test_node_registry_gateway_skips_auth_when_disabled(monkeypatch) -> None:
    responses = {
        "get_node_type": NodeType(
            node_type_id="jarvis.composite.planner.general",
            version="0.3.7",
            kind=NodeKind.composite,
        ),
        "health": _health(),
        "version": _version(),
    }
    dummy = DummyClient(responses)

    def _should_not_be_called(*_args, **_kwargs) -> str:
        raise AssertionError("client_credentials_token should not be called when ARP_AUTH_MODE=disabled")

    monkeypatch.setenv("ARP_AUTH_MODE", "disabled")
    monkeypatch.setattr(registry_module, "client_credentials_token", _should_not_be_called)

    gateway = NodeRegistryGatewayClient(
        base_url="http://registry",
        auth_client=cast(AuthClient, DummyAuthClient()),
        client=cast(Any, dummy),
        client_factory=cast(Any, lambda raw: dummy),
    )

    client = asyncio.run(gateway._client_for())
    assert client is dummy
    assert "Authorization" not in dummy.raw_client.headers


def test_pdp_gateway_success(monkeypatch) -> None:
    responses = {
        "decide_policy": PolicyDecision(decision=PolicyDecisionOutcome.allow),
        "health": _health(),
        "version": _version(),
    }
    dummy = DummyClient(responses)
    monkeypatch.setattr(pdp_module, "client_credentials_token", _fake_token)

    gateway = PdpGatewayClient(
        base_url="http://pdp",
        auth_client=cast(AuthClient, DummyAuthClient()),
        client=cast(Any, dummy),
        client_factory=cast(Any, lambda raw: dummy),
    )

    request = PolicyDecisionRequest(action="run.start", run_id="run-1")
    decision = asyncio.run(gateway.decide_policy(request))
    assert decision.decision == PolicyDecisionOutcome.allow
    assert asyncio.run(gateway.health()).status == Status.ok
    assert asyncio.run(gateway.version()).service_name == "svc"


@pytest.mark.parametrize(
    "gateway_cls, module, error_code",
    [
        (AtomicExecutorGatewayClient, atomic_module, "atomic_executor_unavailable"),
        (CompositeExecutorGatewayClient, composite_module, "composite_executor_unavailable"),
        (SelectionGatewayClient, selection_module, "selection_service_unavailable"),
        (NodeRegistryGatewayClient, registry_module, "node_registry_unavailable"),
        (PdpGatewayClient, pdp_module, "pdp_unavailable"),
    ],
)
def test_gateway_generic_error(monkeypatch, gateway_cls, module, error_code) -> None:
    dummy = DummyClient({"health": RuntimeError("boom")})
    monkeypatch.setattr(module, "client_credentials_token", _fake_token)
    gateway = gateway_cls(
        base_url="http://svc",
        auth_client=cast(AuthClient, DummyAuthClient()),
        client=cast(Any, dummy),
        client_factory=cast(Any, lambda raw: dummy),
    )
    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(gateway.health())
    assert excinfo.value.code == error_code


@pytest.mark.parametrize(
    "gateway_cls, module",
    [
        (AtomicExecutorGatewayClient, atomic_module),
        (CompositeExecutorGatewayClient, composite_module),
        (SelectionGatewayClient, selection_module),
        (NodeRegistryGatewayClient, registry_module),
        (PdpGatewayClient, pdp_module),
    ],
)
def test_gateway_arp_api_error(monkeypatch, gateway_cls, module) -> None:
    dummy = DummyClient(
        {"health": ArpApiError("nope", "bad", status_code=418, details={"x": "y"})}
    )
    monkeypatch.setattr(module, "client_credentials_token", _fake_token)
    gateway = gateway_cls(
        base_url="http://svc",
        auth_client=cast(AuthClient, DummyAuthClient()),
        client=cast(Any, dummy),
        client_factory=cast(Any, lambda raw: dummy),
    )
    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(gateway.health())
    assert excinfo.value.code == "nope"
    assert excinfo.value.status_code == 418
