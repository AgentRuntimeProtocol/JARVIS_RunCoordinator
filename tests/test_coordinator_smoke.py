import asyncio
import json
import os

import pytest

from arp_standard_model import (
    Candidates,
    ConstraintEnvelope,
    BindingDecision,
    Error,
    EvaluationResult,
    EvaluationStatus,
    NodeRun,
    NodeRunCompleteRequest,
    NodeRunCreateSpec,
    NodeRunEvaluationReportRequest,
    NodeRunState,
    NodeRunTerminalState,
    NodeRunsCreateRequest,
    NodeKind,
    RecoveryAction,
    RecoveryActionType,
    RunCoordinatorCompleteNodeRunParams,
    RunCoordinatorCompleteNodeRunRequest,
    RunCoordinatorCancelRunParams,
    RunCoordinatorCancelRunRequest,
    RunCoordinatorCreateNodeRunsRequest,
    RunCoordinatorGetRunParams,
    RunCoordinatorGetRunRequest,
    RunCoordinatorHealthRequest,
    RunCoordinatorReportNodeRunEvaluationParams,
    RunCoordinatorReportNodeRunEvaluationRequest,
    RunCoordinatorStartRunRequest,
    RunCoordinatorStreamNodeRunEventsParams,
    RunCoordinatorStreamNodeRunEventsRequest,
    RunCoordinatorStreamRunEventsParams,
    RunCoordinatorStreamRunEventsRequest,
    RunCoordinatorVersionRequest,
    RunStartRequest,
    Run,
    NodeTypeRef,
    Structural,
    RunState,
    Status,
)
from arp_standard_server import ArpServerError
from jarvis_run_coordinator.coordinator import RunCoordinator

os.environ.setdefault("JARVIS_RUN_COORDINATOR_AUTO_DISPATCH", "false")
os.environ.setdefault("JARVIS_POLICY_PROFILE", "dev-allow")


class InMemoryRunStore:
    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}
        self._node_runs: dict[str, NodeRun] = {}
        self._run_idempotency: dict[str, str] = {}
        self._node_run_idempotency: dict[str, str] = {}

    async def create_run(self, run: Run, *, idempotency_key: str | None = None):
        if idempotency_key:
            if (existing_id := self._run_idempotency.get(idempotency_key)) is not None:
                existing = self._runs[existing_id]
                if existing.run_id != run.run_id:
                    raise ArpServerError(
                        code="run_already_exists",
                        message="Idempotency key already used for a different run_id.",
                        status_code=409,
                    )
                return existing
        if run.run_id in self._runs:
            raise ArpServerError(code="run_already_exists", message="Run already exists.", status_code=409)
        self._runs[run.run_id] = run
        if idempotency_key:
            self._run_idempotency[idempotency_key] = run.run_id
        return run

    async def get_run(self, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    async def update_run(self, run: Run) -> Run:
        if run.run_id not in self._runs:
            raise ArpServerError(code="run_not_found", message="Run not found.", status_code=404)
        self._runs[run.run_id] = run
        return run

    async def create_node_run(self, node_run: NodeRun, *, idempotency_key: str | None = None) -> NodeRun:
        if idempotency_key:
            if (existing_id := self._node_run_idempotency.get(idempotency_key)) is not None:
                existing = self._node_runs[existing_id]
                if existing.node_run_id != node_run.node_run_id:
                    raise ArpServerError(
                        code="node_run_already_exists",
                        message="Idempotency key already used for a different node_run_id.",
                        status_code=409,
                    )
                return existing
        if node_run.node_run_id in self._node_runs:
            raise ArpServerError(
                code="node_run_already_exists",
                message="NodeRun already exists.",
                status_code=409,
            )
        self._node_runs[node_run.node_run_id] = node_run
        if idempotency_key:
            self._node_run_idempotency[idempotency_key] = node_run.node_run_id
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
                    node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.5"),
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
                    node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.5"),
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


def test_start_run_persists_constraints_on_run_and_root() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    constraints = ConstraintEnvelope(structural=Structural(max_depth=1))
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_constraints",
            root_node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
            input={"prompt": "test"},
            constraints=constraints,
        )
    )
    run = asyncio.run(coordinator.start_run(start_request))
    stored_run = asyncio.run(run_store.get_run(run.run_id))
    assert stored_run is not None
    assert stored_run.extensions is not None
    assert stored_run.extensions.model_dump()["constraints"]["structural"]["max_depth"] == 1

    root_node = asyncio.run(run_store.get_node_run(run.root_node_run_id))
    assert root_node is not None
    assert root_node.extensions is not None
    assert root_node.extensions.model_dump()["constraints"]["structural"]["max_depth"] == 1


def test_create_node_runs_enforces_max_depth() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    constraints = ConstraintEnvelope(structural=Structural(max_depth=0))
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_depth",
            root_node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
            input={"prompt": "test"},
            constraints=constraints,
        )
    )
    run = asyncio.run(coordinator.start_run(start_request))
    create_request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id=run.run_id,
            parent_node_run_id=run.root_node_run_id,
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "pong"},
                )
            ],
        )
    )
    with pytest.raises(ArpServerError) as exc:
        asyncio.run(coordinator.create_node_runs(create_request))
    assert exc.value.code == "constraint_violation"


def test_create_node_runs_enforces_max_children() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    constraints = ConstraintEnvelope(structural=Structural(max_children_per_composite=1))
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_children",
            root_node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
            input={"prompt": "test"},
            constraints=constraints,
        )
    )
    run = asyncio.run(coordinator.start_run(start_request))
    create_request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id=run.run_id,
            parent_node_run_id=run.root_node_run_id,
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "one"},
                ),
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "two"},
                ),
            ],
        )
    )
    with pytest.raises(ArpServerError) as exc:
        asyncio.run(coordinator.create_node_runs(create_request))
    assert exc.value.code == "constraint_violation"


def test_create_node_runs_idempotent() -> None:
    run_store = InMemoryRunStore()
    event_stream = InMemoryEventStream()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=event_stream,
        artifact_store=DummyArtifactStore(),
    )
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_idem",
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
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "pong"},
                    idempotency_key="step-1",
                )
            ],
        )
    )
    response_first = asyncio.run(coordinator.create_node_runs(create_request))
    response_second = asyncio.run(coordinator.create_node_runs(create_request))
    assert response_first.node_runs[0].node_run_id == response_second.node_runs[0].node_run_id

    events = event_stream._events.get(run.run_id, [])
    composite_events = [event for event in events if event["type"] == "composite_decomposed"]
    assert len(composite_events) == 1


def test_create_node_runs_idempotency_conflict() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_idem_conflict",
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
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "pong"},
                    idempotency_key="step-1",
                )
            ],
        )
    )
    asyncio.run(coordinator.create_node_runs(create_request))

    conflict_request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id=run.run_id,
            parent_node_run_id=run.root_node_run_id,
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "different"},
                    idempotency_key="step-1",
                )
            ],
        )
    )
    with pytest.raises(ArpServerError) as exc:
        asyncio.run(coordinator.create_node_runs(conflict_request))
    assert exc.value.code == "idempotency_conflict"


def test_dispatch_enforces_candidate_allowlist() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    constraints = ConstraintEnvelope(
        candidates=Candidates(allowed_node_type_ids=["atomic.allowed"])
    )
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_candidates",
            root_node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
            input={"prompt": "test"},
            constraints=constraints,
        )
    )
    run = asyncio.run(coordinator.start_run(start_request))
    asyncio.run(coordinator._dispatch_node_run(run.root_node_run_id))
    root_node = asyncio.run(run_store.get_node_run(run.root_node_run_id))
    assert root_node is not None
    assert root_node.state == NodeRunState.failed


def test_create_node_runs_parent_mismatch() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    run_a = Run(run_id="run_a", root_node_run_id="root_a", state=RunState.running)
    run_b = Run(run_id="run_b", root_node_run_id="root_b", state=RunState.running)
    asyncio.run(run_store.create_run(run_a))
    asyncio.run(run_store.create_run(run_b))
    root = NodeRun(
        node_run_id="root_a",
        run_id="run_a",
        parent_node_run_id=None,
        node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
        state=NodeRunState.queued,
        kind=NodeKind.composite,
    )
    asyncio.run(run_store.create_node_run(root))

    request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id="run_b",
            parent_node_run_id="root_a",
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "pong"},
                )
            ],
        )
    )
    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(coordinator.create_node_runs(request))
    assert excinfo.value.code == "parent_node_run_mismatch"


def test_create_node_runs_parent_not_composite() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    run = Run(run_id="run_parent", root_node_run_id="parent", state=RunState.running)
    asyncio.run(run_store.create_run(run))
    parent = NodeRun(
        node_run_id="parent",
        run_id=run.run_id,
        parent_node_run_id=None,
        node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
        state=NodeRunState.queued,
        kind=NodeKind.atomic,
    )
    asyncio.run(run_store.create_node_run(parent))

    request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id=run.run_id,
            parent_node_run_id="parent",
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "pong"},
                )
            ],
        )
    )
    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(coordinator.create_node_runs(request))
    assert excinfo.value.code == "parent_node_run_invalid"


def test_create_node_runs_records_binding_decision() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_binding",
            root_node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
            input={"prompt": "test"},
        )
    )
    run = asyncio.run(coordinator.start_run(start_request))

    binding_decision = BindingDecision(
        subtask_id="subtask-1",
        chosen_node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
        candidate_set_id="set-1",
    )
    create_request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id=run.run_id,
            parent_node_run_id=run.root_node_run_id,
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "pong"},
                    binding_decision=binding_decision,
                )
            ],
        )
    )
    response = asyncio.run(coordinator.create_node_runs(create_request))
    node_run = response.node_runs[0]
    assert node_run.extensions is not None
    assert node_run.extensions.model_dump()["candidate_set_id"] == "set-1"


def test_create_node_runs_enforces_max_total_nodes() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    constraints = ConstraintEnvelope(structural=Structural(max_total_nodes_per_run=1))
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_total",
            root_node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
            input={"prompt": "test"},
            constraints=constraints,
        )
    )
    run = asyncio.run(coordinator.start_run(start_request))
    create_request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id=run.run_id,
            parent_node_run_id=run.root_node_run_id,
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "pong"},
                )
            ],
        )
    )
    with pytest.raises(ArpServerError) as excinfo:
        asyncio.run(coordinator.create_node_runs(create_request))
    assert excinfo.value.code == "constraint_violation"


def test_health_and_version() -> None:
    coordinator = RunCoordinator(
        run_store=InMemoryRunStore(),
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    health = asyncio.run(coordinator.health(RunCoordinatorHealthRequest()))
    version = asyncio.run(coordinator.version(RunCoordinatorVersionRequest()))
    assert health.status == Status.ok
    assert version.service_name == "arp-jarvis-run-coordinator"


def test_get_run_and_cancel_run() -> None:
    run_store = InMemoryRunStore()
    event_stream = InMemoryEventStream()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=event_stream,
        artifact_store=DummyArtifactStore(),
    )
    start_request = RunCoordinatorStartRunRequest(
        body=RunStartRequest(
            run_id="run_2",
            root_node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
            input={"prompt": "test"},
        )
    )
    run = asyncio.run(coordinator.start_run(start_request))

    fetched = asyncio.run(
        coordinator.get_run(
            RunCoordinatorGetRunRequest(
                params=RunCoordinatorGetRunParams(run_id=run.run_id)
            )
        )
    )
    assert fetched.run_id == run.run_id

    canceled = asyncio.run(
        coordinator.cancel_run(
            RunCoordinatorCancelRunRequest(
                params=RunCoordinatorCancelRunParams(run_id=run.run_id)
            )
        )
    )
    assert canceled.state == RunState.canceled
    assert event_stream._events[run.run_id][-1]["type"] == "run_completed"


def test_cancel_run_idempotent() -> None:
    run_store = InMemoryRunStore()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=InMemoryEventStream(),
        artifact_store=DummyArtifactStore(),
    )
    run = Run(run_id="run_done", root_node_run_id="root", state=RunState.canceled)
    asyncio.run(run_store.create_run(run))

    canceled = asyncio.run(
        coordinator.cancel_run(
            RunCoordinatorCancelRunRequest(
                params=RunCoordinatorCancelRunParams(run_id=run.run_id)
            )
        )
    )
    assert canceled.state == RunState.canceled


def test_stream_node_run_events_filters_descendants() -> None:
    run_store = InMemoryRunStore()
    event_stream = InMemoryEventStream()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=event_stream,
        artifact_store=DummyArtifactStore(),
    )
    run = Run(run_id="run_events", root_node_run_id="root", state=RunState.running)
    asyncio.run(run_store.create_run(run))
    root = NodeRun(
        node_run_id="root",
        run_id=run.run_id,
        parent_node_run_id=None,
        node_type_ref=NodeTypeRef(node_type_id="composite.echo", version="0.1.0"),
        state=NodeRunState.queued,
    )
    child = NodeRun(
        node_run_id="child",
        run_id=run.run_id,
        parent_node_run_id="root",
        node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
        state=NodeRunState.queued,
    )
    asyncio.run(run_store.create_node_run(root))
    asyncio.run(run_store.create_node_run(child))

    asyncio.run(
        event_stream.append_events(
            [
                {"run_id": run.run_id, "node_run_id": "root", "type": "event"},
                {"run_id": run.run_id, "node_run_id": "child", "type": "event"},
                {"run_id": "other", "node_run_id": "other", "type": "event"},
            ]
        )
    )

    response = asyncio.run(
        coordinator.stream_node_run_events(
            RunCoordinatorStreamNodeRunEventsRequest(
                params=RunCoordinatorStreamNodeRunEventsParams(node_run_id="root")
            )
        )
    )
    assert '"node_run_id":"root"' in response
    assert '"node_run_id":"child"' in response
    assert '"node_run_id":"other"' not in response


def test_report_node_run_evaluation_records() -> None:
    run_store = InMemoryRunStore()
    event_stream = InMemoryEventStream()
    coordinator = RunCoordinator(
        run_store=run_store,
        event_stream=event_stream,
        artifact_store=DummyArtifactStore(),
    )
    run = Run(run_id="run_eval", root_node_run_id="root", state=RunState.running)
    asyncio.run(run_store.create_run(run))
    node_run = NodeRun(
        node_run_id="node_eval",
        run_id=run.run_id,
        parent_node_run_id="root",
        node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
        state=NodeRunState.running,
    )
    asyncio.run(run_store.create_node_run(node_run))

    evaluation = EvaluationResult(status=EvaluationStatus.success)
    recovery = RecoveryAction(type=RecoveryActionType.retry)
    asyncio.run(
        coordinator.report_node_run_evaluation(
            RunCoordinatorReportNodeRunEvaluationRequest(
                params=RunCoordinatorReportNodeRunEvaluationParams(node_run_id="node_eval"),
                body=NodeRunEvaluationReportRequest(
                    evaluation_result=evaluation,
                    recovery_action=recovery,
                ),
            )
        )
    )

    updated = asyncio.run(run_store.get_node_run("node_eval"))
    assert updated is not None
    assert updated.evaluation_result is not None
    assert updated.recovery_actions
    assert event_stream._events[run.run_id][-1]["type"] == "recovery_applied"
