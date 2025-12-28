import asyncio
import json
import os

import pytest

from arp_standard_model import (
    Error,
    NodeRun,
    NodeRunCompleteRequest,
    NodeRunCreateSpec,
    NodeRunTerminalState,
    NodeRunsCreateRequest,
    RunCoordinatorCompleteNodeRunParams,
    RunCoordinatorCompleteNodeRunRequest,
    RunCoordinatorCreateNodeRunsRequest,
    RunCoordinatorStartRunRequest,
    RunStartRequest,
    Run,
    NodeTypeRef,
)
from arp_standard_server import ArpServerError
from jarvis_run_coordinator.coordinator import RunCoordinator

os.environ.setdefault("JARVIS_RUN_COORDINATOR_AUTO_DISPATCH", "false")
os.environ.setdefault("JARVIS_POLICY_PROFILE", "dev-allow")


class InMemoryRunStore:
    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}
        self._node_runs: dict[str, NodeRun] = {}

    async def create_run(self, run: Run, *, idempotency_key: str | None = None):
        _ = idempotency_key
        if run.run_id in self._runs:
            raise ArpServerError(code="run_already_exists", message="Run already exists.", status_code=409)
        self._runs[run.run_id] = run
        return run

    async def get_run(self, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    async def update_run(self, run: Run) -> Run:
        if run.run_id not in self._runs:
            raise ArpServerError(code="run_not_found", message="Run not found.", status_code=404)
        self._runs[run.run_id] = run
        return run

    async def create_node_run(self, node_run: NodeRun, *, idempotency_key: str | None = None) -> NodeRun:
        _ = idempotency_key
        if node_run.node_run_id in self._node_runs:
            raise ArpServerError(
                code="node_run_already_exists",
                message="NodeRun already exists.",
                status_code=409,
            )
        self._node_runs[node_run.node_run_id] = node_run
        return node_run

    async def get_node_run(self, node_run_id: str) -> NodeRun | None:
        return self._node_runs.get(node_run_id)

    async def update_node_run(self, node_run: NodeRun) -> NodeRun:
        if node_run.node_run_id not in self._node_runs:
            raise ArpServerError(code="node_run_not_found", message="NodeRun not found.", status_code=404)
        self._node_runs[node_run.node_run_id] = node_run
        return node_run

    async def list_node_runs_for_run(self, run_id: str, *, limit: int = 500) -> list[NodeRun]:
        _ = limit
        return [node_run for node_run in self._node_runs.values() if node_run.run_id == run_id]


class InMemoryEventStream:
    def __init__(self) -> None:
        self._events: dict[str, list[dict[str, object]]] = {}
        self._next_seq: dict[str, int] = {}

    async def append_events(self, events):
        items = []
        for event in events:
            run_id = event["run_id"]
            seq = event.get("seq")
            if not isinstance(seq, int):
                seq = self._next_seq.get(run_id, 1)
                self._next_seq[run_id] = seq + 1
            event_payload = dict(event)
            event_payload["seq"] = seq
            self._events.setdefault(run_id, []).append(event_payload)
            items.append({"run_id": run_id, "seq": seq})
        return {"items": items, "next_seq_by_run": self._next_seq.copy()}

    async def stream_run_events(self, run_id):
        events = self._events.get(run_id, [])
        lines = [json.dumps(event, separators=(",", ":"), ensure_ascii=True) for event in events]
        return "\n".join(lines) + ("\n" if lines else "")


class DummyArtifactStore:
    async def create_artifact(self, data: bytes, *, content_type: str | None = None) -> dict[str, object]:
        _ = data
        _ = content_type
        return {}

    async def get_metadata(self, artifact_id: str) -> dict[str, object]:
        _ = artifact_id
        return {}


def test_create_node_runs_requires_run() -> None:
    coordinator = RunCoordinator(
        run_store=InMemoryRunStore(),
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id="missing_run",
            parent_node_run_id="root_node_run",
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.1"),
                    inputs={"ping": "pong"},
                )
            ],
        )
    )

    with pytest.raises(ArpServerError) as exc:
        asyncio.run(coordinator.create_node_runs(request))

    assert exc.value.code == "run_not_found"


def test_complete_node_run_persists_error_in_extensions() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_1",
            root_node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
            input={"prompt": "test"},
        )
    )
    run = asyncio.run(coordinator.start_run(start_request))

    create_request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id=run.run_id,
            parent_node_run_id=run.root_node_run_id,
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.1"),
                    inputs={"ping": "pong"},
                )
            ],
        )
    )
    response = asyncio.run(coordinator.create_node_runs(create_request))
    node_run_id = response.node_runs[0].node_run_id

    complete_request = RunCoordinatorCompleteNodeRunRequest(
        params=RunCoordinatorCompleteNodeRunParams(node_run_id=node_run_id),
        body=NodeRunCompleteRequest(
            state=NodeRunTerminalState.failed,
            error=Error(code="boom", message="failure"),
        ),
    )
    asyncio.run(coordinator.complete_node_run(complete_request))

    node_run = asyncio.run(run_store.get_node_run(node_run_id))
    assert node_run is not None
    assert node_run.extensions is not None
    assert node_run.extensions.model_dump()["completion_error"]["code"] == "boom"
