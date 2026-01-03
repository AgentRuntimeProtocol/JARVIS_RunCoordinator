import asyncio
from typing import cast

import httpx
import pytest
from arp_auth import AuthClient
from arp_standard_model import NodeRun, NodeRunState, NodeTypeRef, Run, RunState
from arp_standard_server import ArpServerError

from jarvis_run_coordinator.clients.artifact_store import ArtifactStoreClient
from jarvis_run_coordinator.clients.event_stream import EventStreamClient
from jarvis_run_coordinator.clients.run_store import RunStoreClient


def _run(run_id: str = "run-1") -> Run:
    return Run(run_id=run_id, root_node_run_id="root", state=RunState.running)


def _node_run(node_run_id: str = "node-1", run_id: str = "run-1") -> NodeRun:
    return NodeRun(
        node_run_id=node_run_id,
        run_id=run_id,
        node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.7"),
        state=NodeRunState.queued,
    )


def test_run_store_client_basic_flow() -> None:
    run = _run()
    node_run = _node_run()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/runs":
            return httpx.Response(200, json={"run": run.model_dump(mode="json")})
        if request.method == "GET" and path == "/v1/runs/run-1":
            return httpx.Response(200, json={"run": run.model_dump(mode="json")})
        if request.method == "PUT" and path == "/v1/runs/run-1":
            return httpx.Response(200, json={"run": run.model_dump(mode="json")})
        if request.method == "POST" and path == "/v1/node-runs":
            return httpx.Response(200, json={"node_run": node_run.model_dump(mode="json")})
        if request.method == "GET" and path == "/v1/node-runs/node-1":
            return httpx.Response(200, json={"node_run": node_run.model_dump(mode="json")})
        if request.method == "PUT" and path == "/v1/node-runs/node-1":
            return httpx.Response(200, json={"node_run": node_run.model_dump(mode="json")})
        if path == "/v1/runs/run-1/node-runs":
            if request.url.params.get("page_token"):
                return httpx.Response(200, json={"items": [], "next_token": None})
            return httpx.Response(
                200,
                json={
                    "items": [node_run.model_dump(mode="json")],
                    "next_token": "next",
                },
            )
        return httpx.Response(500, json={"detail": "unexpected"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://run-store"
    )
    store = RunStoreClient(base_url="http://run-store", client=client)

    created = asyncio.run(store.create_run(run, idempotency_key="run-1"))
    assert created.run_id == run.run_id
    fetched = asyncio.run(store.get_run(run.run_id))
    assert fetched is not None
    updated = asyncio.run(store.update_run(run))
    assert updated.run_id == run.run_id

    created_node = asyncio.run(store.create_node_run(node_run, idempotency_key="step-1"))
    assert created_node.node_run_id == node_run.node_run_id
    fetched_node = asyncio.run(store.get_node_run(node_run.node_run_id))
    assert fetched_node is not None
    updated_node = asyncio.run(store.update_node_run(node_run))
    assert updated_node.node_run_id == node_run.node_run_id

    listed = asyncio.run(store.list_node_runs_for_run(run.run_id))
    assert listed

    asyncio.run(client.aclose())


def test_run_store_client_errors() -> None:
    run = _run()
    node_run = _node_run(node_run_id="node-1")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/runs":
            return httpx.Response(409, json={"detail": "Run already exists"})
        if request.method == "GET" and request.url.path == "/v1/runs/missing":
            return httpx.Response(404, json={"detail": "not found"})
        if request.method == "PUT" and request.url.path == "/v1/runs/run-1":
            return httpx.Response(404, json={"detail": "not found"})
        if request.method == "POST" and request.url.path == "/v1/node-runs":
            return httpx.Response(409, json={"detail": "NodeRun already exists"})
        if request.method == "GET" and request.url.path == "/v1/node-runs/missing":
            return httpx.Response(404, json={"detail": "not found"})
        if request.method == "PUT" and request.url.path == "/v1/node-runs/node-1":
            return httpx.Response(404, json={"detail": "not found"})
        return httpx.Response(500, json={"detail": "unexpected"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://run-store"
    )
    store = RunStoreClient(base_url="http://run-store", client=client)

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(store.create_run(run))
    assert excinfo.value.code == "run_already_exists"

    assert asyncio.run(store.get_run("missing")) is None

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(store.update_run(run))
    assert excinfo.value.code == "run_not_found"

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(store.create_node_run(node_run))
    assert excinfo.value.code == "node_run_already_exists"

    assert asyncio.run(store.get_node_run("missing")) is None

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(store.update_node_run(node_run))
    assert excinfo.value.code == "node_run_not_found"

    asyncio.run(client.aclose())


def test_run_store_invalid_json_and_request_failure() -> None:
    client = httpx.AsyncClient(base_url="http://run-store")
    store = RunStoreClient(base_url="http://run-store", client=client)
    response = httpx.Response(200, content=b"not-json")

    with pytest.raises(ArpServerError):
        store._json_or_error(response)

    assert store._detail(response) == "not-json"

    asyncio.run(client.aclose())

    def fail_handler(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("boom")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(fail_handler), base_url="http://run-store"
    )
    store = RunStoreClient(base_url="http://run-store", client=client)
    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(store.create_run(_run()))
    assert excinfo.value.code == "run_store_unavailable"
    asyncio.run(client.aclose())


def test_run_store_auth_header_and_error_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_token(*_args, **_kwargs) -> str:
        return "token"

    monkeypatch.setattr(
        "jarvis_run_coordinator.clients.run_store.client_credentials_token", fake_token
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer token"
        return httpx.Response(500, json={"detail": "failed"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://run-store"
    )
    store = RunStoreClient(
        base_url="http://run-store",
        client=client,
        auth_client=cast(AuthClient, object()),
        audience="aud",
    )

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(store.create_run(_run()))
    assert excinfo.value.code == "run_store_error"

    assert store._detail(httpx.Response(200, json={"detail": 123})) is None
    asyncio.run(client.aclose())


def test_event_stream_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/run-events":
            return httpx.Response(200, json={"items": []})
        if request.method == "GET" and request.url.path.startswith("/v1/runs/"):
            return httpx.Response(200, text="{}\n")
        if request.method == "GET" and request.url.path.startswith("/v1/node-runs/"):
            return httpx.Response(200, text="{}\n")
        return httpx.Response(500, json={"detail": "unexpected"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://event-stream"
    )
    stream = EventStreamClient(base_url="http://event-stream", client=client)

    result = asyncio.run(stream.append_events([{"run_id": "run-1"}]))
    assert result["items"] == []
    assert asyncio.run(stream.stream_run_events("run-1")).strip() == "{}"
    assert asyncio.run(stream.stream_node_run_events("node-1")).strip() == "{}"

    asyncio.run(client.aclose())


def test_event_stream_conflicts_and_invalid_json() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(409, json={"detail": "conflict"})
        return httpx.Response(200, content=b"not-json")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://event-stream"
    )
    stream = EventStreamClient(base_url="http://event-stream", client=client)

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(stream.append_events([{"run_id": "run-1"}]))
    assert excinfo.value.code == "event_conflict"

    with pytest.raises(ArpServerError):
        stream._json_or_error(httpx.Response(200, content=b"not-json"))

    asyncio.run(client.aclose())


def test_event_stream_request_failure() -> None:
    def fail_handler(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("boom")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(fail_handler), base_url="http://event-stream"
    )
    stream = EventStreamClient(base_url="http://event-stream", client=client)
    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(stream.append_events([{"run_id": "run-1"}]))
    assert excinfo.value.code == "event_stream_unavailable"
    asyncio.run(client.aclose())


def test_event_stream_validation_and_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_token(*_args, **_kwargs) -> str:
        return "token"

    monkeypatch.setattr(
        "jarvis_run_coordinator.clients.event_stream.client_credentials_token", fake_token
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            assert request.headers.get("Authorization") == "Bearer token"
            return httpx.Response(422, json={"detail": "invalid"})
        if request.url.path.endswith("/events"):
            return httpx.Response(500, json={"detail": "error"})
        return httpx.Response(200, content=b"not-json")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://event-stream"
    )
    stream = EventStreamClient(
        base_url="http://event-stream",
        client=client,
        auth_client=cast(AuthClient, object()),
        audience="aud",
    )

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(stream.append_events([{"run_id": "run-1"}]))
    assert excinfo.value.code == "event_validation_failed"

    with pytest.raises(ArpServerError):
        asyncio.run(stream.stream_run_events("run-1"))

    with pytest.raises(ArpServerError):
        asyncio.run(stream.stream_node_run_events("node-1"))

    with pytest.raises(ArpServerError):
        stream._json_or_error(httpx.Response(500, json={"detail": "boom"}))

    assert stream._detail(httpx.Response(200, json={"detail": 123})) is None
    assert stream._detail(httpx.Response(200, content=b"not-json")) == "not-json"
    asyncio.run(client.aclose())


def test_artifact_store_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/artifacts":
            return httpx.Response(200, json={"artifact_id": "a1"})
        if request.method == "GET" and request.url.path.endswith("/metadata"):
            return httpx.Response(200, json={"artifact_id": "a1"})
        return httpx.Response(500, json={"detail": "unexpected"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://artifact-store"
    )
    store = ArtifactStoreClient(base_url="http://artifact-store", client=client)

    result = asyncio.run(store.create_artifact(b"data", content_type="text/plain"))
    assert result["artifact_id"] == "a1"
    metadata = asyncio.run(store.get_metadata("a1"))
    assert metadata["artifact_id"] == "a1"

    asyncio.run(client.aclose())


def test_artifact_store_not_found_and_invalid_json() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(404, json={"detail": "not found"})
        return httpx.Response(200, content=b"not-json")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://artifact-store"
    )
    store = ArtifactStoreClient(base_url="http://artifact-store", client=client)

    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(store.get_metadata("missing"))
    assert excinfo.value.code == "artifact_not_found"

    with pytest.raises(ArpServerError):
        store._json_or_error(httpx.Response(200, content=b"not-json"))
    with pytest.raises(ArpServerError):
        store._json_or_error(httpx.Response(500, json={"detail": "oops"}))

    assert store._detail(httpx.Response(200, json={"detail": 123})) is None
    assert store._detail(httpx.Response(200, content=b"not-json")) == "not-json"

    asyncio.run(client.aclose())


def test_artifact_store_request_failure() -> None:
    def fail_handler(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("boom")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(fail_handler), base_url="http://artifact-store"
    )
    store = ArtifactStoreClient(base_url="http://artifact-store", client=client)
    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(store.create_artifact(b"data"))
    assert excinfo.value.code == "artifact_store_unavailable"
    asyncio.run(client.aclose())


def test_artifact_store_auth_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_token(*_args, **_kwargs) -> str:
        return "token"

    monkeypatch.setattr(
        "jarvis_run_coordinator.clients.artifact_store.client_credentials_token",
        fake_token,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer token"
        return httpx.Response(200, json={"artifact_id": "a1"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://artifact-store"
    )
    store = ArtifactStoreClient(
        base_url="http://artifact-store",
        client=client,
        auth_client=cast(AuthClient, object()),
        audience="aud",
    )

    asyncio.run(store.create_artifact(b"data"))
    asyncio.run(client.aclose())
