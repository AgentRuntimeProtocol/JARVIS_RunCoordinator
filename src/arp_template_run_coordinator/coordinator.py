from __future__ import annotations

import json
import uuid

from arp_standard_model import (
    Health,
    Extensions,
    NodeRun,
    NodeRunCompleteRequest,
    NodeRunEvaluationReportRequest,
    NodeRunState,
    NodeRunsCreateRequest,
    NodeRunsCreateResponse,
    Run,
    RunCoordinatorCancelRunRequest,
    RunCoordinatorGetRunParams,
    RunCoordinatorGetRunRequest,
    RunCoordinatorStartRunRequest,
    RunCoordinatorStreamNodeRunEventsRequest,
    RunCoordinatorStreamRunEventsRequest,
    RunState,
    Status,
    VersionInfo,
)
from arp_standard_server import ArpServerError
from arp_standard_server.run_coordinator import BaseRunCoordinatorServer

from . import __version__
from .atomic_executor_client import AtomicExecutorGatewayClient
from .composite_executor_client import CompositeExecutorGatewayClient
from .node_registry_client import NodeRegistryGatewayClient
from .pdp_client import PdpGatewayClient
from .selection_client import SelectionGatewayClient
from .utils import now


class RunCoordinator(BaseRunCoordinatorServer):
    """Minimal in-memory Run Coordinator implementation."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        service_name: str = "arp-template-run-coordinator",
        service_version: str = __version__,
        atomic_executor: AtomicExecutorGatewayClient | None = None,
        composite_executor: CompositeExecutorGatewayClient | None = None,
        selection_service: SelectionGatewayClient | None = None,
        node_registry: NodeRegistryGatewayClient | None = None,
        pdp: PdpGatewayClient | None = None,
    ) -> None:
        """
        Not part of ARP spec; required to construct the coordinator.

        Args:
          - service_name: Name exposed by /v1/version.
          - service_version: Version exposed by /v1/version.
          - atomic_executor: Optional wrapper for Atomic Executor calls.
          - composite_executor: Optional wrapper for Composite Executor calls.
          - selection_service: Optional wrapper for Selection Service calls.
          - node_registry: Optional wrapper for Node Registry calls.
          - pdp: Optional wrapper for PDP calls.

        Potential modifications:
          - Replace in-memory stores with durable storage.
          - Add scheduler/queue integration for NodeRuns.
        """
        self._service_name = service_name
        self._service_version = service_version
        self._atomic_executor = atomic_executor
        self._composite_executor = composite_executor
        self._selection_service = selection_service
        self._node_registry = node_registry
        self._pdp = pdp
        self._runs: dict[str, Run] = {}
        self._node_runs: dict[str, NodeRun] = {}

    # Core methods - Run Coordinator API implementations
    async def health(self, request) -> Health:  # type: ignore[override]
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorHealthRequest (unused).
        """
        _ = request
        return Health(status=Status.ok, time=now())

    async def version(self, request) -> VersionInfo:  # type: ignore[override]
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorVersionRequest (unused).
        """
        _ = request
        return VersionInfo(
            service_name=self._service_name,
            service_version=self._service_version,
            supported_api_versions=["v1"],
        )

    async def create_node_runs(self, request) -> NodeRunsCreateResponse:  # type: ignore[override]
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorCreateNodeRunsRequest with NodeRunsCreateRequest body.
            The run_id must already exist.

        Potential modifications:
          - Enforce constraints and budgets before creating NodeRuns.
          - Persist NodeRuns and emit run events.
        """
        if request.body.run_id not in self._runs:
            raise ArpServerError(
                code="run_not_found",
                message=f"Run '{request.body.run_id}' not found",
                status_code=404,
            )
        parent_node_run = self._node_runs.get(request.body.parent_node_run_id)
        if parent_node_run is None:
            raise ArpServerError(
                code="parent_node_run_not_found",
                message=f"Parent NodeRun '{request.body.parent_node_run_id}' not found",
                status_code=404,
            )
        if parent_node_run.run_id != request.body.run_id:
            raise ArpServerError(
                code="parent_node_run_mismatch",
                message="Parent NodeRun does not belong to the requested run",
                status_code=409,
            )
        created: list[NodeRun] = []
        for spec in request.body.node_runs:
            node_run_id = f"node_run_{uuid.uuid4().hex}"
            node_run = NodeRun(
                node_run_id=node_run_id,
                run_id=request.body.run_id,
                parent_node_run_id=request.body.parent_node_run_id,
                node_type_ref=spec.node_type_ref,
                kind=None,
                state=NodeRunState.queued,
                created_at=now(),
                started_at=None,
                ended_at=None,
                inputs=spec.inputs,
                outputs=None,
                output_artifacts=None,
                executor_binding=None,
                evaluation_result=None,
                recovery_actions=None,
                extensions=spec.extensions,
            )
            self._node_runs[node_run_id] = node_run
            created.append(node_run)
        return NodeRunsCreateResponse(node_runs=created, extensions=request.body.extensions)

    async def get_node_run(self, request) -> NodeRun:  # type: ignore[override]
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorGetNodeRunRequest with node_run_id.
        """
        node_run = self._node_runs.get(request.params.node_run_id)
        if node_run is None:
            raise ArpServerError(
                code="node_run_not_found",
                message=f"NodeRun '{request.params.node_run_id}' not found",
                status_code=404,
            )
        return node_run

    async def report_node_run_evaluation(self, request) -> None:  # type: ignore[override]
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorReportNodeRunEvaluationRequest with evaluation data.

        Potential modifications:
          - Persist evaluation results for later analytics.
          - Trigger recovery logic based on evaluation outcomes.
        """
        node_run = self._node_runs.get(request.params.node_run_id)
        if node_run is None:
            raise ArpServerError(
                code="node_run_not_found",
                message=f"NodeRun '{request.params.node_run_id}' not found",
                status_code=404,
            )
        self._node_runs[node_run.node_run_id] = node_run.model_copy(
            update={"evaluation_result": request.body.evaluation_result}
        )
        return None

    async def complete_node_run(self, request) -> None:  # type: ignore[override]
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorCompleteNodeRunRequest with completion payload.

        Potential modifications:
          - Record error details and terminal state.
          - Emit completion events and update run state.
        """
        node_run = self._node_runs.get(request.params.node_run_id)
        if node_run is None:
            raise ArpServerError(
                code="node_run_not_found",
                message=f"NodeRun '{request.params.node_run_id}' not found",
                status_code=404,
            )
        body: NodeRunCompleteRequest = request.body
        terminal_state = NodeRunState(body.state)
        updated_extensions = node_run.extensions
        if body.error is not None:
            extensions_payload = {}
            if updated_extensions is not None:
                extensions_payload = updated_extensions.model_dump()
            extensions_payload["completion_error"] = body.error.model_dump()
            updated_extensions = Extensions(**extensions_payload)

        updated = node_run.model_copy(
            update={
                "state": terminal_state,
                "outputs": body.outputs,
                "output_artifacts": body.output_artifacts,
                "ended_at": now(),
                "extensions": updated_extensions,
            }
        )
        self._node_runs[node_run.node_run_id] = updated
        return None

    async def start_run(self, request: RunCoordinatorStartRunRequest) -> Run:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorStartRunRequest with RunStartRequestBody.

        Potential modifications:
          - Persist the run and schedule a root NodeRun assignment.
          - Enforce budgets and policy checks before accepting the run.
        """
        run_id = request.body.run_id or f"run_{uuid.uuid4().hex}"
        if run_id in self._runs:
            raise ArpServerError(
                code="run_already_exists",
                message=f"Run '{run_id}' already exists",
                status_code=409,
            )

        root_node_run_id = f"node_run_{uuid.uuid4().hex}"
        run = Run(
            run_id=run_id,
            state=RunState.running,
            root_node_run_id=root_node_run_id,
            run_context=request.body.run_context,
            started_at=now(),
            ended_at=None,
            extensions=request.body.extensions,
        )
        self._runs[run_id] = run

        root_node_run = NodeRun(
            node_run_id=root_node_run_id,
            run_id=run_id,
            parent_node_run_id=None,
            node_type_ref=request.body.root_node_type_ref,
            kind=None,
            state=NodeRunState.queued,
            created_at=now(),
            started_at=None,
            ended_at=None,
            inputs=request.body.input,
            outputs=None,
            output_artifacts=None,
            executor_binding=None,
            evaluation_result=None,
            recovery_actions=None,
            extensions=request.body.extensions,
        )
        self._node_runs[root_node_run_id] = root_node_run
        return run

    async def get_run(self, request: RunCoordinatorGetRunRequest) -> Run:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorGetRunRequest with run_id.
        """
        run = self._runs.get(request.params.run_id)
        if run is None:
            raise ArpServerError(
                code="run_not_found",
                message=f"Run '{request.params.run_id}' not found",
                status_code=404,
            )
        return run

    async def cancel_run(self, request: RunCoordinatorCancelRunRequest) -> Run:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorCancelRunRequest with run_id.

        Potential modifications:
          - Cascade cancellation to active NodeRuns and executors.
        """
        run = await self.get_run(
            RunCoordinatorGetRunRequest(params=RunCoordinatorGetRunParams(run_id=request.params.run_id))
        )
        if run.state in {RunState.succeeded, RunState.failed, RunState.canceled}:
            return run
        updated = run.model_copy(update={"state": RunState.canceled, "ended_at": now()})
        self._runs[run.run_id] = updated
        return updated

    async def stream_run_events(self, request: RunCoordinatorStreamRunEventsRequest) -> str:
        """
        Optional (spec): Run event streaming endpoint for the Run Coordinator.

        Args:
          - request: RunCoordinatorStreamRunEventsRequest with run_id.
        """
        payload = {
            "run_id": request.params.run_id,
            "seq": 0,
            "type": "run_started",
            "time": now().isoformat(),
            "data": {"message": "Template Run Coordinator does not stream real events yet."},
        }
        return json.dumps(payload) + "\n"

    async def stream_node_run_events(self, request: RunCoordinatorStreamNodeRunEventsRequest) -> str:
        """
        Optional (spec): NodeRun event streaming endpoint for the Run Coordinator.

        Args:
          - request: RunCoordinatorStreamNodeRunEventsRequest with node_run_id.
        """
        payload = {
            "node_run_id": request.params.node_run_id,
            "seq": 0,
            "type": "node_run_started",
            "time": now().isoformat(),
            "data": {"message": "Template Run Coordinator does not stream real events yet."},
        }
        return json.dumps(payload) + "\n"
