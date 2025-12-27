import asyncio

import pytest

from arp_standard_model import (
    Error,
    NodeRunCompleteRequest,
    NodeRunCreateSpec,
    NodeRunTerminalState,
    NodeRunsCreateRequest,
    RunCoordinatorCompleteNodeRunParams,
    RunCoordinatorCompleteNodeRunRequest,
    RunCoordinatorCreateNodeRunsRequest,
    RunCoordinatorStartRunRequest,
    RunStartRequest,
    NodeTypeRef,
)
from arp_standard_server import ArpServerError
from arp_template_run_coordinator.coordinator import RunCoordinator


def test_create_node_runs_requires_run() -> None:
    coordinator = RunCoordinator()
    request = RunCoordinatorCreateNodeRunsRequest(
        body=NodeRunsCreateRequest(
            run_id="missing_run",
            parent_node_run_id="root_node_run",
            node_runs=[
                NodeRunCreateSpec(
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
                    inputs={"ping": "pong"},
                )
            ],
        )
    )

    with pytest.raises(ArpServerError) as exc:
        asyncio.run(coordinator.create_node_runs(request))

    assert exc.value.code == "run_not_found"


def test_complete_node_run_persists_error_in_extensions() -> None:
    coordinator = RunCoordinator()
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
                    node_type_ref=NodeTypeRef(node_type_id="atomic.echo", version="0.1.0"),
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

    node_run = coordinator._node_runs[node_run_id]
    assert node_run.extensions is not None
    assert node_run.extensions.model_dump()["completion_error"]["code"] == "boom"
