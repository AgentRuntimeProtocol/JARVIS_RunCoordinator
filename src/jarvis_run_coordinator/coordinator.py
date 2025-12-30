from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from enum import StrEnum
from arp_standard_model import (
    AtomicExecuteRequest,
    Budgets,
    Candidates,
    CompositeBeginRequest,
    ConstraintEnvelope,
    EndpointLocator,
    Error,
    EvaluationResult,
    EvaluationStatus,
    ExecutorBinding,
    Extensions,
    Gates,
    Health,
    NodeKind,
    NodeRun,
    NodeRunCompleteRequest,
    NodeRunEvaluationReportRequest,
    NodeRunState,
    NodeRunTerminalState,
    NodeRunCreateSpec,
    NodeRunsCreateResponse,
    PolicyDecision,
    PolicyDecisionOutcome,
    PolicyDecisionRequest,
    Run,
    RunCoordinatorCancelRunRequest,
    RunCoordinatorCompleteNodeRunParams,
    RunCoordinatorCompleteNodeRunRequest,
    RunCoordinatorCreateNodeRunsRequest,
    RunCoordinatorGetRunParams,
    RunCoordinatorGetRunRequest,
    RunCoordinatorGetNodeRunRequest,
    RunCoordinatorHealthRequest,
    RunCoordinatorReportNodeRunEvaluationParams,
    RunCoordinatorReportNodeRunEvaluationRequest,
    RunCoordinatorStartRunRequest,
    RunCoordinatorStreamNodeRunEventsRequest,
    RunCoordinatorStreamRunEventsRequest,
    RunCoordinatorVersionRequest,
    RunState,
    SideEffectClass,
    Status,
    Structural,
    VersionInfo,
)
from arp_standard_server import ArpServerError
from arp_standard_server.run_coordinator import BaseRunCoordinatorServer

from . import __version__
from .clients import (
    ArtifactStoreClientLike,
    AtomicExecutorGatewayClient,
    CompositeExecutorGatewayClient,
    EventStreamClientLike,
    NodeRegistryGatewayClient,
    PdpGatewayClient,
    RunStoreClientLike,
    SelectionGatewayClient,
)
from .utils import now


class RunEventType(StrEnum):
    run_started = "run_started"
    node_run_assigned = "node_run_assigned"
    composite_decomposed = "composite_decomposed"
    candidate_set_generated = "candidate_set_generated"
    subtask_mapped = "subtask_mapped"
    atomic_executed = "atomic_executed"
    node_evaluated = "node_evaluated"
    recovery_applied = "recovery_applied"
    policy_decided = "policy_decided"
    run_completed = "run_completed"


class RunCoordinator(BaseRunCoordinatorServer):
    """Run Coordinator implementation wired to internal JARVIS services."""

    # Core method - API surface and main extension points
    def __init__(
        self,
        *,
        service_name: str = "arp-jarvis-run-coordinator",
        service_version: str = __version__,
        atomic_executor: AtomicExecutorGatewayClient | None = None,
        composite_executor: CompositeExecutorGatewayClient | None = None,
        selection_service: SelectionGatewayClient | None = None,
        node_registry: NodeRegistryGatewayClient | None = None,
        pdp: PdpGatewayClient | None = None,
        run_store: RunStoreClientLike | None = None,
        event_stream: EventStreamClientLike | None = None,
        artifact_store: ArtifactStoreClientLike | None = None,
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
          - run_store: Required Run Store client (Run + NodeRun persistence).
          - event_stream: Required Event Stream client (RunEvent append + stream).
          - artifact_store: Required Artifact Store client (artifact metadata).

        Potential modifications:
          - Replace internal service clients with your own implementations.
          - Add scheduler/queue integration for NodeRuns.
        """
        self._service_name = service_name
        self._service_version = service_version
        self._atomic_executor = atomic_executor
        self._composite_executor = composite_executor
        self._selection_service = selection_service
        self._node_registry = node_registry
        self._pdp = pdp
        if run_store is None:
            raise RuntimeError("Run Store client is required for Run Coordinator")
        if event_stream is None:
            raise RuntimeError("Event Stream client is required for Run Coordinator")
        if artifact_store is None:
            raise RuntimeError("Artifact Store client is required for Run Coordinator")
        self._run_store = run_store
        self._event_stream = event_stream
        self._artifact_store = artifact_store
        self._auto_dispatch = _env_flag("JARVIS_RUN_COORDINATOR_AUTO_DISPATCH", default=True)

        # Used when dispatching composites: CE needs a stable coordinator callback URL.
        # In most deployments this is the service's externally reachable base URL.
        self._public_url = _normalize_url(
            os.environ.get("JARVIS_RUN_COORDINATOR_PUBLIC_URL")
            or os.environ.get("JARVIS_RUN_COORDINATOR_URL")
            or ""
        )

    # Core methods - Run Coordinator API implementations
    async def health(self, request: RunCoordinatorHealthRequest) -> Health:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorHealthRequest (unused).
        """
        _ = request
        return Health(status=Status.ok, time=now())

    async def version(self, request: RunCoordinatorVersionRequest) -> VersionInfo:
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

    async def create_node_runs(self, request: RunCoordinatorCreateNodeRunsRequest) -> NodeRunsCreateResponse:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorCreateNodeRunsRequest with NodeRunsCreateRequest body.
            The run_id must already exist.
        """
        body = request.body

        # Sanity check: the run must exist (no orphan node runs).
        run = await self._get_run_or_404(body.run_id)
        run_constraints = _constraints_from_extensions(run.extensions)

        # Parent node run must exist. Root is always created via start_run.
        parent_node_run = await self._get_node_run_or_404(
            body.parent_node_run_id,
            code="parent_node_run_not_found",
            message=f"Parent NodeRun '{body.parent_node_run_id}' not found",
        )

        # Parent node run must belong to the requested run.
        if parent_node_run.run_id != body.run_id:
            raise ArpServerError(
                code="parent_node_run_mismatch",
                message="Parent NodeRun does not belong to the requested run",
                status_code=409,
            )

        # Only composite node runs can create child node runs.
        parent_kind = await self._resolve_node_kind(parent_node_run)
        if parent_kind != NodeKind.composite:
            raise ArpServerError(
                code="parent_node_run_invalid",
                message="Parent NodeRun must be composite to create child NodeRuns",
                status_code=409,
            )

        # Load existing NodeRuns so we can enforce structural constraints.
        node_runs_for_run = await self._run_store.list_node_runs_for_run(body.run_id)
        existing_children = [item for item in node_runs_for_run if item.parent_node_run_id == body.parent_node_run_id]
        total_existing = len(node_runs_for_run)

        # Structural limits are enforced at the coordinator (mechanical, not prompt-based).
        parent_constraints = _constraints_from_extensions(parent_node_run.extensions)
        effective_parent_constraints = _merge_constraints(run_constraints, parent_constraints)
        structural = effective_parent_constraints.structural if effective_parent_constraints else None
        parent_depth = await self._node_run_depth(parent_node_run)

        created: list[NodeRun] = []
        # Build pending NodeRuns first; we persist only after constraints pass.
        pending: list[tuple[NodeRun, str | None, Extensions | None, str | None]] = []
        new_count = 0
        for spec in body.node_runs:
            node_run_id = (
                _idempotent_node_run_id(body.run_id, body.parent_node_run_id, spec.idempotency_key)
                if spec.idempotency_key
                else f"node_run_{uuid.uuid4().hex}"
            )
            # Idempotent retry: validate and reuse the existing NodeRun.
            if spec.idempotency_key and (existing := await self._run_store.get_node_run(node_run_id)) is not None:
                _assert_idempotent_match(
                    existing,
                    spec=spec,
                    run_id=body.run_id,
                    parent_node_run_id=body.parent_node_run_id,
                    expected_constraints=_merge_constraints(
                        run_constraints,
                        await self._node_type_constraints(spec.node_type_ref),
                        spec.constraints,
                    ),
                )
                created.append(existing)
                continue

            new_count += 1
            kind = await self._resolve_node_kind_for_ref(spec.node_type_ref)
            effective_constraints = _merge_constraints(
                run_constraints,
                await self._node_type_constraints(spec.node_type_ref),
                spec.constraints,
            )

            # Decomposition/mapping metadata (from CE + Selection):
            # - candidate_set_id: identifies the candidate set returned by Selection for a subtask
            # - binding_decision: identifies which candidate was chosen (and why)
            #
            # We persist these into `NodeRun.extensions` (spec-friendly) and also emit RunEvents for observability.
            binding_decision = spec.binding_decision
            candidate_set_id = spec.candidate_set_id
            if binding_decision is not None and binding_decision.candidate_set_id:
                candidate_set_id = candidate_set_id or binding_decision.candidate_set_id

            extensions = spec.extensions
            extensions_payload = extensions.model_dump() if extensions is not None else {}

            if binding_decision is not None:
                extensions_payload["binding_decision"] = binding_decision.model_dump(exclude_none=True)

            if candidate_set_id is not None:
                extensions_payload["candidate_set_id"] = candidate_set_id
            if spec.idempotency_key:
                extensions_payload["idempotency_key"] = spec.idempotency_key
            if effective_constraints is not None:
                extensions_payload["constraints"] = effective_constraints.model_dump(exclude_none=True)
            extensions = Extensions(**extensions_payload) if extensions_payload else spec.extensions

            # Create new node run
            node_run = NodeRun(
                node_run_id=node_run_id,
                run_id=body.run_id,
                parent_node_run_id=body.parent_node_run_id,
                node_type_ref=spec.node_type_ref,
                kind=kind,
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
                extensions=extensions,
            )
            pending.append((node_run, candidate_set_id, extensions, spec.idempotency_key))

        # Enforce structural constraints before writing new NodeRuns.
        if structural is not None:
            if structural.max_depth is not None and parent_depth + 1 > structural.max_depth:
                raise ArpServerError(
                    code="constraint_violation",
                    message="max_depth constraint violated for child NodeRuns",
                    status_code=409,
                )
            if structural.max_children_per_composite is not None:
                if len(existing_children) + new_count > structural.max_children_per_composite:
                    raise ArpServerError(
                        code="constraint_violation",
                        message="max_children_per_composite constraint violated",
                        status_code=409,
                    )
            if structural.max_total_nodes_per_run is not None:
                if total_existing + new_count > structural.max_total_nodes_per_run:
                    raise ArpServerError(
                        code="constraint_violation",
                        message="max_total_nodes_per_run constraint violated",
                        status_code=409,
                    )

        if new_count:
            # Record decomposition + bump parent decomposition rounds before child writes.
            await self._emit_event(
                run_id=body.run_id,
                node_run_id=body.parent_node_run_id,
                event_type=RunEventType.composite_decomposed,
                data={"node_run_count": new_count},
            )

            parent_extensions_payload = parent_node_run.extensions.model_dump() if parent_node_run.extensions else {}
            rounds = int(parent_extensions_payload.get("decomposition_rounds") or 0)
            if structural is not None and structural.max_decomposition_rounds_per_node is not None:
                if rounds + 1 > structural.max_decomposition_rounds_per_node:
                    raise ArpServerError(
                        code="constraint_violation",
                        message="max_decomposition_rounds_per_node constraint violated",
                        status_code=409,
                    )
            parent_extensions_payload["decomposition_rounds"] = rounds + 1
            updated_parent = parent_node_run.model_copy(
                update={"extensions": Extensions(**parent_extensions_payload)}
            )
            await self._run_store.update_node_run(updated_parent)

        # Persist and emit per-node events after constraints pass.
        for node_run, candidate_set_id, extensions, idempotency_key in pending:
            await self._run_store.create_node_run(node_run, idempotency_key=idempotency_key)
            created.append(node_run)
            await self._emit_event(
                run_id=body.run_id,
                node_run_id=node_run.node_run_id,
                event_type=RunEventType.node_run_assigned,
                data={"parent_node_run_id": body.parent_node_run_id},
            )

            binding_decision = None
            extensions_payload = extensions.model_dump() if extensions is not None else {}
            if "binding_decision" in extensions_payload:
                binding_decision = extensions_payload["binding_decision"]
                await self._emit_event(
                    run_id=body.run_id,
                    node_run_id=node_run.node_run_id,
                    event_type=RunEventType.subtask_mapped,
                    data=binding_decision,
                )

            if candidate_set_id is not None:
                await self._emit_event(
                    run_id=body.run_id,
                    node_run_id=node_run.node_run_id,
                    event_type=RunEventType.candidate_set_generated,
                    data={
                        "candidate_set_id": candidate_set_id,
                        "subtask_id": binding_decision.get("subtask_id") if isinstance(binding_decision, dict) else None,
                    },
                )

            # If enabled, dispatch newly created NodeRuns in-process immediately.
            # A future design can replace this with a durable queue/worker model.
            if self._auto_dispatch:
                asyncio.create_task(self._dispatch_node_run(node_run.node_run_id))
        return NodeRunsCreateResponse(node_runs=created, extensions=body.extensions)

    async def get_node_run(self, request: RunCoordinatorGetNodeRunRequest) -> NodeRun:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorGetNodeRunRequest with node_run_id.
        """
        return await self._get_node_run_or_404(request.params.node_run_id)

    async def report_node_run_evaluation(self, request: RunCoordinatorReportNodeRunEvaluationRequest) -> None:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorReportNodeRunEvaluationRequest with evaluation data.

        Potential modifications:
          - Persist evaluation results for later analytics.
          - Trigger recovery logic based on evaluation outcomes.
        """
        # Load current NodeRun state.
        node_run = await self._get_node_run_or_404(request.params.node_run_id)

        # Persist evaluation (+ optional recovery hint) onto the NodeRun record.
        updated = node_run.model_copy(
            update={
                "evaluation_result": request.body.evaluation_result,
                "recovery_actions": [request.body.recovery_action] if request.body.recovery_action else None,
            }
        )
        await self._run_store.update_node_run(updated)

        # Emit durable events for observability/auditing.
        await self._emit_event(
            run_id=updated.run_id,
            node_run_id=updated.node_run_id,
            event_type=RunEventType.node_evaluated,
            data=request.body.evaluation_result.model_dump(exclude_none=True),
        )
        if request.body.recovery_action is not None:
            await self._emit_event(
                run_id=updated.run_id,
                node_run_id=updated.node_run_id,
                event_type=RunEventType.recovery_applied,
                data=request.body.recovery_action.model_dump(exclude_none=True),
            )
        return None

    async def complete_node_run(self, request: RunCoordinatorCompleteNodeRunRequest) -> None:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorCompleteNodeRunRequest with completion payload.

        Potential modifications:
          - Record error details and terminal state.
          - Emit completion events and update run state.
        """
        # Load current NodeRun state.
        node_run = await self._get_node_run_or_404(request.params.node_run_id)
        body: NodeRunCompleteRequest = request.body
        terminal_state = NodeRunState(body.state)

        # Attach completion error details (if present) into extensions for post-mortem debugging.
        updated_extensions = node_run.extensions
        if body.error is not None:
            extensions_payload = {}
            if updated_extensions is not None:
                extensions_payload = updated_extensions.model_dump()
            extensions_payload["completion_error"] = body.error.model_dump()
            updated_extensions = Extensions(**extensions_payload)

        # Persist terminal state + outputs.
        updated = node_run.model_copy(
            update={
                "state": terminal_state,
                "outputs": body.outputs,
                "output_artifacts": body.output_artifacts,
                "ended_at": now(),
                "extensions": updated_extensions,
            }
        )
        await self._run_store.update_node_run(updated)

        # Emit completion events.
        if updated.kind == NodeKind.atomic:
            await self._emit_event(
                run_id=updated.run_id,
                node_run_id=updated.node_run_id,
                event_type=RunEventType.atomic_executed,
                data={"state": updated.state},
            )

        # If the root NodeRun completed, transition the Run to a terminal state.
        if (run := await self._run_store.get_run(updated.run_id)) is not None and updated.node_run_id == run.root_node_run_id:
            run_state = RunState.succeeded
            if updated.state == NodeRunState.failed:
                run_state = RunState.failed
            if updated.state == NodeRunState.canceled:
                run_state = RunState.canceled
            updated_run = run.model_copy(update={"state": run_state, "ended_at": now()})
            await self._run_store.update_run(updated_run)
            await self._emit_event(
                run_id=updated.run_id,
                node_run_id=updated.node_run_id,
                event_type=RunEventType.run_completed,
                data={"state": updated_run.state},
            )
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
        # Allocate IDs.
        run_id = request.body.run_id or f"run_{uuid.uuid4().hex}"
        root_node_run_id = f"node_run_{uuid.uuid4().hex}"

        run_extensions_payload = _extensions_payload(request.body.extensions)
        if request.body.constraints is not None:
            run_extensions_payload["constraints"] = request.body.constraints.model_dump(exclude_none=True)
        run_extensions = Extensions(**run_extensions_payload) if run_extensions_payload else request.body.extensions

        # Persist the Run record and emit the run_started event.
        run = Run(
            run_id=run_id,
            state=RunState.running,
            root_node_run_id=root_node_run_id,
            run_context=request.body.run_context,
            started_at=now(),
            ended_at=None,
            extensions=run_extensions,
        )
        await self._run_store.create_run(run)
        await self._emit_event(
            run_id=run_id,
            node_run_id=None,
            event_type=RunEventType.run_started,
            data={"root_node_run_id": root_node_run_id},
        )

        # Create the root NodeRun in queued state.
        kind = await self._resolve_node_kind_for_ref(request.body.root_node_type_ref)
        root_constraints = _merge_constraints(
            request.body.constraints,
            await self._node_type_constraints(request.body.root_node_type_ref),
        )
        root_extensions_payload = _extensions_payload(request.body.extensions)
        if root_constraints is not None:
            root_extensions_payload["constraints"] = root_constraints.model_dump(exclude_none=True)
        root_extensions = Extensions(**root_extensions_payload) if root_extensions_payload else request.body.extensions
        root_node_run = NodeRun(
            node_run_id=root_node_run_id,
            run_id=run_id,
            parent_node_run_id=None,
            node_type_ref=request.body.root_node_type_ref,
            kind=kind,
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
            extensions=root_extensions,
        )
        await self._run_store.create_node_run(root_node_run)
        await self._emit_event(
            run_id=run_id,
            node_run_id=root_node_run_id,
            event_type=RunEventType.node_run_assigned,
            data={"root": True},
        )

        # Enforce the run.start checkpoint.
        decision = await self._policy_decide(
            action="run.start",
            run=run,
            node_run=root_node_run,
        )
        if not self._policy_allows(decision):
            await self._fail_node_run(root_node_run_id, code="policy_denied", message="Run start denied by policy")
            updated_run = await self._run_store.get_run(run_id)
            return updated_run or run

        # Kick off dispatch if configured (in v0.3.x this is an in-process task).
        if self._auto_dispatch:
            asyncio.create_task(self._dispatch_node_run(root_node_run_id))
        return run

    async def get_run(self, request: RunCoordinatorGetRunRequest) -> Run:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorGetRunRequest with run_id.
        """
        return await self._get_run_or_404(request.params.run_id)

    async def cancel_run(self, request: RunCoordinatorCancelRunRequest) -> Run:
        """
        Mandatory: Required by the ARP Run Coordinator API.

        Args:
          - request: RunCoordinatorCancelRunRequest with run_id.

        Potential modifications:
          - Cascade cancellation to active NodeRuns and executors.
        """
        # Load the Run.
        run = await self.get_run(
            RunCoordinatorGetRunRequest(params=RunCoordinatorGetRunParams(run_id=request.params.run_id))
        )

        # If already terminal, treat cancel as idempotent.
        if run.state in {RunState.succeeded, RunState.failed, RunState.canceled}:
            return run

        # Persist terminal cancellation state and emit run_completed.
        updated = run.model_copy(update={"state": RunState.canceled, "ended_at": now()})
        await self._run_store.update_run(updated)
        await self._emit_event(
            run_id=updated.run_id,
            node_run_id=None,
            event_type=RunEventType.run_completed,
            data={"state": updated.state},
        )
        return updated

    async def stream_run_events(self, request: RunCoordinatorStreamRunEventsRequest) -> str:
        """
        Optional (spec): Run event streaming endpoint for the Run Coordinator.

        Args:
          - request: RunCoordinatorStreamRunEventsRequest with run_id.
        """
        return await self._event_stream.stream_run_events(request.params.run_id)

    async def stream_node_run_events(self, request: RunCoordinatorStreamNodeRunEventsRequest) -> str:
        """
        Optional (spec): NodeRun event streaming endpoint for the Run Coordinator.

        Args:
          - request: RunCoordinatorStreamNodeRunEventsRequest with node_run_id.
        """
        # Load root NodeRun and find all descendants within this run.
        node_run = await self._get_node_run_or_404(request.params.node_run_id)
        descendant_ids = await self._collect_descendant_ids(node_run)

        # Stream run-level NDJSON, filter by node_run_id ∈ descendants, and re-emit NDJSON.
        raw = await self._event_stream.stream_run_events(node_run.run_id)
        lines: list[str] = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if payload.get("node_run_id") in descendant_ids:
                lines.append(json.dumps(payload, separators=(",", ":"), ensure_ascii=True))
        return "\n".join(lines) + ("\n" if lines else "")

    async def _dispatch_node_run(self, node_run_id: str) -> None:
        """
        Dispatch a queued NodeRun to the correct executor (best-effort, in-process).

        The coordinator remains the system-of-record: it loads state from Run Store, enforces policy,
        transitions the NodeRun to running, then invokes the executor. Durable dispatch (queue/worker)
        can replace this later without changing the external coordinator contract.
        """
        # Only dispatch queued NodeRuns. Anything else is a no-op.
        if (node_run := await self._run_store.get_node_run(node_run_id)) is None or node_run.state != NodeRunState.queued:
            return

        # Run is optional here: if it cannot be loaded, policy falls back to deny-by-default.
        run = await self._run_store.get_run(node_run.run_id)

        # Resolve atomic vs composite. Prefer registry lookup; fall back to node_type_id prefix inference.
        if (kind := await self._resolve_node_kind(node_run)) is None:
            await self._fail_node_run(node_run_id, code="unknown_node_kind", message="Unable to resolve node kind")
            return
        if node_run.kind != kind:
            node_run = node_run.model_copy(update={"kind": kind})
            await self._run_store.update_node_run(node_run)

        effective_constraints = await self._effective_constraints(run, node_run)
        if effective_constraints is not None and effective_constraints.candidates is not None:
            candidates = effective_constraints.candidates
            node_type_id = node_run.node_type_ref.node_type_id
            if candidates.allowed_node_type_ids is not None:
                if node_type_id not in candidates.allowed_node_type_ids:
                    await self._fail_node_run(
                        node_run_id,
                        code="node_type_not_allowed",
                        message="NodeType is not in the allowed candidate set",
                    )
                    return
            if candidates.denied_node_type_ids is not None:
                if node_type_id in candidates.denied_node_type_ids:
                    await self._fail_node_run(
                        node_run_id,
                        code="node_type_denied",
                        message="NodeType is denied by candidate constraints",
                    )
                    return

        # Coordinator is the enforcement point; executors should not run work without an allow decision.
        decision = await self._policy_decide(action="node.run.execute", run=run, node_run=node_run)
        if not self._policy_allows(decision):
            await self._fail_node_run(node_run_id, code="policy_denied", message="NodeRun denied by policy")
            return

        if kind == NodeKind.atomic:
            if self._atomic_executor is None:
                await self._fail_node_run(
                    node_run_id,
                    code="atomic_executor_unconfigured",
                    message="Atomic Executor is not configured",
                )
                return
            node_run = await self._mark_node_run_running(
                node_run,
                executor_instance_id="arp-jarvis-atomic-executor",
                executor_base_url=self._atomic_executor.base_url,
            )
            try:
                result = await self._atomic_executor.execute_atomic_node_run(
                    AtomicExecuteRequest(
                        node_run_id=node_run.node_run_id,
                        run_id=node_run.run_id,
                        node_type_ref=node_run.node_type_ref,
                        inputs=node_run.inputs or {},
                    )
                )
            except ArpServerError as exc:
                await self._fail_node_run(node_run_id, code="atomic_executor_unavailable", message=str(exc))
                return

            # For atomic work, we treat executor completion as the evaluation input (simple v0.3.x posture).
            evaluation = EvaluationResult(
                status=EvaluationStatus.success
                if result.state == NodeRunState.succeeded
                else EvaluationStatus.fail,
                reason_code="atomic_completed",
            )
            await self.report_node_run_evaluation(
                RunCoordinatorReportNodeRunEvaluationRequest(
                    params=RunCoordinatorReportNodeRunEvaluationParams(node_run_id=node_run_id),
                    body=NodeRunEvaluationReportRequest(evaluation_result=evaluation),
                )
            )
            terminal_state = NodeRunTerminalState(result.state.value if hasattr(result.state, "value") else result.state)
            await self.complete_node_run(
                RunCoordinatorCompleteNodeRunRequest(
                    params=RunCoordinatorCompleteNodeRunParams(node_run_id=node_run_id),
                    body=NodeRunCompleteRequest(
                        state=terminal_state,
                        outputs=result.outputs,
                        output_artifacts=result.output_artifacts,
                        error=result.error,
                    ),
                )
            )
            return

        if kind == NodeKind.composite:
            if self._composite_executor is None:
                await self._fail_node_run(
                    node_run_id,
                    code="composite_executor_unconfigured",
                    message="Composite Executor is not configured",
                )
                return
            if not self._public_url:
                await self._fail_node_run(
                    node_run_id,
                    code="coordinator_url_unconfigured",
                    message="Coordinator URL is required to dispatch composites",
                )
                return
            node_run = await self._mark_node_run_running(
                node_run,
                executor_instance_id="arp-jarvis-composite-executor",
                executor_base_url=self._composite_executor.base_url,
            )
            try:
                response = await self._composite_executor.begin_composite_node_run(
                    CompositeBeginRequest(
                        run_id=node_run.run_id,
                        node_run_id=node_run.node_run_id,
                        node_type_ref=node_run.node_type_ref,
                        inputs=node_run.inputs or {},
                        run_context=run.run_context if run is not None else None,
                        constraints=effective_constraints,
                        coordinator_endpoint=EndpointLocator.model_validate(self._public_url),
                        assignment_token=None,
                        extensions=node_run.extensions,
                    )
                )
            except ArpServerError as exc:
                await self._fail_node_run(node_run_id, code="composite_executor_unavailable", message=str(exc))
                return
            if not response.accepted:
                await self._fail_node_run(
                    node_run_id,
                    code="composite_rejected",
                    message=response.message or "Composite assignment rejected",
                )
            return

    async def _get_run_or_404(
        self,
        run_id: str,
        *,
        code: str = "run_not_found",
        message: str | None = None,
    ) -> Run:
        """Fetch a Run from Run Store or raise a 404-shaped ArpServerError."""
        if (run := await self._run_store.get_run(run_id)) is None:
            raise ArpServerError(
                code=code,
                message=message or f"Run '{run_id}' not found",
                status_code=404,
            )
        return run

    async def _get_node_run_or_404(
        self,
        node_run_id: str,
        *,
        code: str = "node_run_not_found",
        message: str | None = None,
    ) -> NodeRun:
        """Fetch a NodeRun from Run Store or raise a 404-shaped ArpServerError."""
        if (node_run := await self._run_store.get_node_run(node_run_id)) is None:
            raise ArpServerError(
                code=code,
                message=message or f"NodeRun '{node_run_id}' not found",
                status_code=404,
            )
        return node_run

    async def _fail_node_run(self, node_run_id: str, *, code: str, message: str) -> None:
        """Mark a NodeRun as failed using a consistent error envelope."""
        await self.complete_node_run(
            RunCoordinatorCompleteNodeRunRequest(
                params=RunCoordinatorCompleteNodeRunParams(node_run_id=node_run_id),
                body=NodeRunCompleteRequest(
                    state=NodeRunTerminalState.failed,
                    error=Error(code=code, message=message),
                ),
            )
        )

    async def _mark_node_run_running(
        self,
        node_run: NodeRun,
        *,
        executor_instance_id: str,
        executor_base_url: str,
    ) -> NodeRun:
        """Transition a NodeRun to running and persist an ExecutorBinding."""
        binding = ExecutorBinding(
            executor_instance_id=executor_instance_id,
            endpoint=EndpointLocator.model_validate(executor_base_url),
        )
        updated = node_run.model_copy(
            update={
                "state": NodeRunState.running,
                "started_at": now(),
                "executor_binding": binding,
            }
        )
        await self._run_store.update_node_run(updated)
        return updated

    async def _emit_event(
        self,
        *,
        run_id: str,
        node_run_id: str | None,
        event_type: RunEventType,
        data: dict | None,
    ) -> None:
        """
        Append a single RunEvent payload to the Event Stream.

        Note: sequence numbers are assigned by the Event Stream service; coordinator provides the event type + data.
        """
        event_type_value = event_type.value if hasattr(event_type, "value") else event_type
        payload: dict[str, object] = {
            "run_id": run_id,
            "node_run_id": node_run_id,
            "type": event_type_value,
            "time": now().isoformat(),
        }
        if data is not None:
            payload["data"] = data
        await self._event_stream.append_events([payload])

    async def _resolve_node_kind(self, node_run: NodeRun) -> NodeKind | None:
        """Resolve NodeKind for a NodeRun (prefer stored kind; otherwise derive from the NodeTypeRef)."""
        if node_run.kind is not None:
            return node_run.kind
        return await self._resolve_node_kind_for_ref(node_run.node_type_ref)

    async def _resolve_node_kind_for_ref(self, node_type_ref) -> NodeKind | None:
        """
        Resolve NodeKind for a NodeTypeRef.

        Uses Node Registry if configured; falls back to `atomic.*`/`composite.*` prefix inference.
        """
        if self._node_registry is not None:
            try:
                node_type = await self._node_registry.get_node_type(node_type_ref.node_type_id, node_type_ref.version)
                return node_type.kind
            except ArpServerError:
                pass
        return _infer_kind(node_type_ref.node_type_id)

    async def _node_type_constraints(self, node_type_ref) -> ConstraintEnvelope | None:
        """Load NodeType.constraints from Node Registry if configured."""
        if self._node_registry is None:
            return None
        try:
            node_type = await self._node_registry.get_node_type(node_type_ref.node_type_id, node_type_ref.version)
        except ArpServerError:
            return None
        return node_type.constraints

    async def _effective_constraints(self, run: Run | None, node_run: NodeRun) -> ConstraintEnvelope | None:
        """
        Resolve effective constraints for a NodeRun.

        Prefer constraints already persisted in NodeRun.extensions; otherwise merge run + node_type constraints.
        """
        if (existing := _constraints_from_extensions(node_run.extensions)) is not None:
            return existing
        run_constraints = _constraints_from_extensions(run.extensions) if run is not None else None
        node_type_constraints = await self._node_type_constraints(node_run.node_type_ref)
        return _merge_constraints(run_constraints, node_type_constraints)

    async def _node_run_depth(self, node_run: NodeRun) -> int:
        """Return the depth of a NodeRun (root depth = 0)."""
        depth = 0
        current = node_run
        while current.parent_node_run_id:
            depth += 1
            current = await self._get_node_run_or_404(current.parent_node_run_id)
        return depth

    async def _policy_decide(self, *, action: str, run: Run | None, node_run: NodeRun | None) -> PolicyDecision:
        """
        Ask the PDP for a decision and emit a durable `policy_decided` event.

        If no PDP is configured (or it is unavailable), we fall back to deny-by-default except for dev-allow profiles.
        """
        if run is None:
            # If the Run cannot be loaded, we cannot build a meaningful policy request; fall back locally.
            return self._fallback_policy_decision()

        # Build the PDP request from the RunContext + (optional) NodeRun context.
        request = PolicyDecisionRequest(
            action=action,
            run_id=run.run_id,
            node_run_id=node_run.node_run_id if node_run is not None else None,
            node_type_ref=node_run.node_type_ref if node_run is not None else None,
            run_context=run.run_context,
        )

        # Call PDP when configured; otherwise fall back.
        if self._pdp is None:
            decision = self._fallback_policy_decision()
        else:
            try:
                decision = await self._pdp.decide_policy(request)
            except ArpServerError as exc:
                decision = PolicyDecision(
                    decision=PolicyDecisionOutcome.deny,
                    reason_code="pdp_unavailable",
                    message=str(exc),
                )

        # Emit the decision as an event so downstream consumers (and humans) can debug why work was allowed/denied.
        decision_value = decision.decision.value if hasattr(decision.decision, "value") else decision.decision
        data = {
            "action": action,
            "decision": decision_value,
            "reason_code": decision.reason_code,
            "message": decision.message,
            "approval_ref": decision.approval_ref.model_dump(exclude_none=True) if decision.approval_ref else None,
        }
        await self._emit_event(
            run_id=run.run_id,
            node_run_id=node_run.node_run_id if node_run is not None else None,
            event_type=RunEventType.policy_decided,
            data={k: v for k, v in data.items() if v is not None},
        )
        return decision

    def _policy_allows(self, decision: PolicyDecision) -> bool:
        """Return True only when the policy decision resolves to allow."""
        if isinstance(decision.decision, PolicyDecisionOutcome):
            return decision.decision == PolicyDecisionOutcome.allow
        return decision.decision == PolicyDecisionOutcome.allow or decision.decision == "allow"

    def _fallback_policy_decision(self) -> PolicyDecision:
        """Local policy fallback for when no PDP is configured."""
        profile = (os.environ.get("JARVIS_POLICY_PROFILE") or "").strip().lower()
        legacy_mode = (os.environ.get("ARP_POLICY_MODE") or "").strip().lower()
        if profile == "dev-allow" or legacy_mode == "allow_all":
            return PolicyDecision(decision=PolicyDecisionOutcome.allow, reason_code="policy_bypass")
        return PolicyDecision(
            decision=PolicyDecisionOutcome.deny,
            reason_code="policy_unconfigured",
            message="No PDP configured for policy enforcement",
        )

    async def _collect_descendant_ids(self, root: NodeRun) -> set[str]:
        """
        Return the root NodeRun ID plus all descendant NodeRun IDs in the same Run.

        Used by `stream_node_run_events` to implement "NodeRun + descendants" semantics by filtering the run-level
        NDJSON stream from Event Stream.
        """
        # Load all NodeRuns for the Run and build a parent -> children adjacency list.
        node_runs = await self._run_store.list_node_runs_for_run(root.run_id)
        children: dict[str, list[str]] = {}
        for node_run in node_runs:
            if node_run.parent_node_run_id:
                children.setdefault(node_run.parent_node_run_id, []).append(node_run.node_run_id)

        # Walk descendants from the root node_run_id.
        seen: set[str] = set()
        stack = [root.node_run_id]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(children.get(current, []))
        return seen


def _infer_kind(node_type_id: str) -> NodeKind | None:
    """Infer NodeKind from node_type_id prefix (`atomic.*` / `composite.*`) when registry lookup is unavailable."""
    lowered = node_type_id.lower()
    if lowered.startswith("atomic."):
        return NodeKind.atomic
    if lowered.startswith("composite."):
        return NodeKind.composite
    return None


def _normalize_url(value: str) -> str | None:
    """Normalize a base URL (strip whitespace, remove trailing slash, remove `/v1` suffix)."""
    url = value.strip()
    if not url:
        return None
    normalized = url.rstrip("/")
    if normalized.endswith("/v1"):
        normalized = normalized[:-3]
    return normalized


def _env_flag(name: str, *, default: bool) -> bool:
    """Parse a boolean-ish environment flag (empty uses default)."""
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _extensions_payload(extensions: Extensions | None) -> dict[str, object]:
    if extensions is None:
        return {}
    return extensions.model_dump()


def _constraints_from_extensions(extensions: Extensions | None) -> ConstraintEnvelope | None:
    if extensions is None:
        return None
    payload = extensions.model_dump()
    raw = payload.get("constraints")
    if raw is None:
        return None
    try:
        return ConstraintEnvelope.model_validate(raw)
    except Exception as exc:
        raise ArpServerError(
            code="constraint_invalid",
            message="Constraints payload is not valid",
            status_code=409,
        ) from exc


def _min_int(values: list[int | None]) -> int | None:
    items = [value for value in values if value is not None]
    return min(items) if items else None


def _merge_constraints(*constraints: ConstraintEnvelope | None) -> ConstraintEnvelope | None:
    items = [item for item in constraints if item is not None]
    if not items:
        return None

    budgets_items = [item.budgets for item in items if item.budgets is not None]
    budgets = None
    if budgets_items:
        budgets = Budgets(
            max_wall_time_ms=_min_int([item.max_wall_time_ms for item in budgets_items]),
            max_steps=_min_int([item.max_steps for item in budgets_items]),
            max_external_calls=_min_int([item.max_external_calls for item in budgets_items]),
            max_cost=min(
                [item.max_cost for item in budgets_items if item.max_cost is not None],
                default=None,
            ),
        )

    candidate_items = [item.candidates for item in items if item.candidates is not None]
    candidates = None
    if candidate_items:
        allowed_sets = [
            set(item.allowed_node_type_ids)
            for item in candidate_items
            if item.allowed_node_type_ids is not None
        ]
        denied_sets = [
            set(item.denied_node_type_ids)
            for item in candidate_items
            if item.denied_node_type_ids is not None
        ]
        allowed = None
        if allowed_sets:
            allowed = sorted(set.intersection(*allowed_sets)) if allowed_sets else None
        denied = sorted(set.union(*denied_sets)) if denied_sets else None
        candidates = Candidates(
            allowed_node_type_ids=allowed,
            denied_node_type_ids=denied,
            max_candidates_per_subtask=_min_int(
                [item.max_candidates_per_subtask for item in candidate_items]
            ),
        )

    structural_items = [item.structural for item in items if item.structural is not None]
    structural = None
    if structural_items:
        structural = Structural(
            max_depth=_min_int([item.max_depth for item in structural_items]),
            max_children_per_composite=_min_int(
                [item.max_children_per_composite for item in structural_items]
            ),
            max_total_nodes_per_run=_min_int(
                [item.max_total_nodes_per_run for item in structural_items]
            ),
            max_decomposition_rounds_per_node=_min_int(
                [item.max_decomposition_rounds_per_node for item in structural_items]
            ),
        )

    gate_items = [item.gates for item in items if item.gates is not None]
    gates = None
    if gate_items:
        require_values = [item.require_approval for item in gate_items if item.require_approval is not None]
        require_approval = any(require_values) if require_values else None
        side_effect_values = []
        for item in gate_items:
            if item.side_effect_class is not None:
                side_effect_values.append(item.side_effect_class)
        side_effect_class = _most_restrictive_side_effect(side_effect_values)
        gates = Gates(require_approval=require_approval, side_effect_class=side_effect_class)

    if budgets is None and candidates is None and structural is None and gates is None:
        return None
    return ConstraintEnvelope(
        budgets=budgets,
        candidates=candidates,
        structural=structural,
        gates=gates,
    )


def _most_restrictive_side_effect(values: list[SideEffectClass | str]) -> SideEffectClass | None:
    if not values:
        return None
    order = {
        "read": 0,
        "write": 1,
        "irreversible": 2,
        SideEffectClass.read: 0,
        SideEffectClass.write: 1,
        SideEffectClass.irreversible: 2,
    }
    normalized: list[SideEffectClass] = []
    for value in values:
        if isinstance(value, SideEffectClass):
            normalized.append(value)
        else:
            try:
                normalized.append(SideEffectClass(value))
            except ValueError:
                continue
    if not normalized:
        return None
    return max(normalized, key=lambda item: order[item])


def _idempotent_node_run_id(run_id: str, parent_node_run_id: str, idempotency_key: str) -> str:
    seed = f"{run_id}:{parent_node_run_id}:{idempotency_key}".encode("utf-8")
    digest = hashlib.sha256(seed).hexdigest()[:32]
    return f"node_run_{digest}"


def _assert_idempotent_match(
    existing: NodeRun,
    *,
    spec: NodeRunCreateSpec,
    run_id: str,
    parent_node_run_id: str,
    expected_constraints: ConstraintEnvelope | None,
) -> None:
    if existing.run_id != run_id or existing.parent_node_run_id != parent_node_run_id:
        raise ArpServerError(
            code="idempotency_conflict",
            message="Idempotency key already used for a different parent/run",
            status_code=409,
        )
    if existing.node_type_ref != spec.node_type_ref:
        raise ArpServerError(
            code="idempotency_conflict",
            message="Idempotency key already used for a different NodeTypeRef",
            status_code=409,
        )
    if existing.inputs != spec.inputs:
        raise ArpServerError(
            code="idempotency_conflict",
            message="Idempotency key already used for different inputs",
            status_code=409,
        )
    expected_extensions: dict[str, object] = {}
    if spec.binding_decision is not None:
        expected_extensions["binding_decision"] = spec.binding_decision.model_dump(exclude_none=True)
    if spec.candidate_set_id is not None:
        expected_extensions["candidate_set_id"] = spec.candidate_set_id
    if expected_constraints is not None:
        expected_extensions["constraints"] = expected_constraints.model_dump(exclude_none=True)
    existing_extensions = existing.extensions.model_dump() if existing.extensions else {}
    for key, value in expected_extensions.items():
        if existing_extensions.get(key) != value:
            raise ArpServerError(
                code="idempotency_conflict",
                message=f"Idempotency key already used with different {key}",
                status_code=409,
            )
