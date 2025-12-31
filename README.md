# JARVIS Run Coordinator

First-party OSS reference implementation of the **ARP Run Coordinator** service.

This implementation is **advanced**. The Run Coordinator is correctness-critical and stateful; it is more complex than other JARVIS services.

This reference implementation uses the ARP SDK packages plus `arp-auth` to call
internal services (Run Store, Event Stream, Artifact Store).

Implements: ARP Standard `spec/v1` Run Coordinator API (contract: `ARP_Standard/spec/v1/openapi/run-coordinator.openapi.yaml`).

## Requirements

- Python >= 3.11

## Install

```bash
python3 -m pip install -e .
```

## Local configuration (optional)

For local dev convenience, copy the example env file:

```bash
cp .env.example .env.local
```

`src/scripts/dev_server.sh` auto-loads `.env.local` (or `.env`).

## Run

- Run Coordinator listens on `http://127.0.0.1:8081` by default.
- Run Store, Event Stream, and Artifact Store must be running and configured.

```bash
python3 -m pip install -e .
python3 -m jarvis_run_coordinator
```

> [!TIP]
> Use `bash src/scripts/dev_server.sh --host ... --port ... --reload` for dev convenience.

## Using this repo

This is a first-party coordinator implementation meant to showcase the ARP lifecycle.

To build your own coordinator, fork this repository and replace the persistence and orchestration logic while keeping request/response semantics stable.

If all you need is to change NodeRun lifecycle behavior and storage, edit:
- `src/jarvis_run_coordinator/coordinator.py`

Outgoing client wrappers (coordinator -> other components):
- `src/jarvis_run_coordinator/clients/atomic_executor_client.py`
- `src/jarvis_run_coordinator/clients/composite_executor_client.py`
- `src/jarvis_run_coordinator/clients/selection_client.py` (reserved; not used by coordinator logic in v0.3.3)
- `src/jarvis_run_coordinator/clients/node_registry_client.py`
- `src/jarvis_run_coordinator/clients/pdp_client.py`

Internal service clients:
- `src/jarvis_run_coordinator/clients/run_store.py` (Run + NodeRun persistence)
- `src/jarvis_run_coordinator/clients/event_stream.py` (RunEvent append + NDJSON stream proxy)
- `src/jarvis_run_coordinator/clients/artifact_store.py` (artifact refs; executors upload blobs)

### Default behavior

- System-of-record: Runs + NodeRuns persist via Run Store (HTTP) and RunEvents append via Event Stream (NDJSON).
- Root creation: `start_run` creates the Run + root NodeRun, emits `run_started` + `node_run_assigned`, enforces `run.start`, then optionally dispatches.
- Composite ingestion: `create_node_runs` only allows composite parents, persists metadata (`candidate_set_id`, `binding_decision`) in `NodeRun.extensions`, and emits `composite_decomposed` / `candidate_set_generated` / `subtask_mapped` when present.
- Constraint enforcement (mechanical): structural limits (`max_depth`, `max_children_per_composite`, `max_total_nodes_per_run`, `max_decomposition_rounds_per_node`) are enforced during `create_node_runs`; candidate allow/deny lists are enforced at dispatch.
- Idempotent child creation: when `NodeRunCreateSpec.idempotency_key` is set, repeated calls return the same NodeRun (or 409 if the spec differs).
- Best-effort dispatch: when `JARVIS_RUN_COORDINATOR_AUTO_DISPATCH=true`, queued NodeRuns are dispatched in-process via `asyncio.create_task`.
- Policy posture: PDP is optional; when unconfigured, policy is deny-by-default unless `JARVIS_POLICY_PROFILE=dev-allow`.

### Extensions

The ARP `Run` / `NodeRun` models include an `extensions` field used for non-normative metadata.
This coordinator persists several spec-adjacent values into `extensions` so they are queryable later.

Written/consumed by the coordinator:
- `constraints` (on both `Run.extensions` and `NodeRun.extensions`): persisted effective `ConstraintEnvelope` used for later structural enforcement.
- `decomposition_rounds` (on `NodeRun.extensions` for composite nodes): increments when the coordinator accepts decomposition and is used for `max_decomposition_rounds_per_node`.
- `completion_error` (on `NodeRun.extensions`): copied from `complete_node_run` error payload for post-mortem debugging.

Written by callers (e.g., Composite Executor) and persisted by the coordinator on `NodeRun.extensions`:
- `candidate_set_id` (Selection linkage)
- `binding_decision` (chosen candidate + rationale)
- `idempotency_key` (mirrors create spec idempotency; used for deterministic child IDs)

Full cross-stack list: `https://github.com/AgentRuntimeProtocol/BusinessDocs/blob/main/Business_Docs/JARVIS/Extensions.md`.

### Notes on API surface

- In `spec/v1`, Run lifecycle endpoints (start/get/cancel) are defined on both Run Gateway (client-facing) and Run Coordinator (run authority).
- The coordinator focuses on the standardized NodeRun APIs. Execution hierarchy is derived from NodeRuns and is an internal coordinator concern.

## Implementation overview

Run start flow:
1) Run Gateway (or a client) calls `POST /v1/runs:start` on the coordinator.
2) Coordinator persists the `Run` and root `NodeRun`, emits `run_started` and `node_run_assigned`.
3) Coordinator enforces `run.start` via PDP (if configured), emits `policy_decided`, and denies by default when unconfigured.
4) If auto-dispatch is enabled, coordinator dispatches the root NodeRun based on NodeKind (atomic vs composite).

Composite decomposition flow:
1) Coordinator dispatches a composite NodeRun to Composite Executor (`begin_composite_node_run`).
2) Composite Executor decomposes the work and calls `create_node_runs` to create child NodeRuns.
3) Coordinator validates the composite parent relationship, persists child NodeRuns as queued, and (optionally) auto-dispatches them.

NDJSON streaming:
- `stream_run_events`: proxies Event Stream’s NDJSON for a Run.
- `stream_node_run_events`: filters the Run’s NDJSON to the requested NodeRun and its descendants.

## Quick health check

```bash
curl http://127.0.0.1:8081/v1/health
```

## Configuration

CLI flags:
- `--host` (default `127.0.0.1`)
- `--port` (default `8081`)
- `--reload` (dev only)

Environment variables:
Internal services (required):
- `JARVIS_RUN_STORE_URL`
- `JARVIS_EVENT_STREAM_URL`
- `JARVIS_ARTIFACT_STORE_URL`

Internal service audiences (optional):
- `JARVIS_RUN_STORE_AUDIENCE` (default `arp-jarvis-runstore`)
- `JARVIS_EVENT_STREAM_AUDIENCE` (default `arp-jarvis-eventstream`)
- `JARVIS_ARTIFACT_STORE_AUDIENCE` (default `arp-jarvis-artifactstore`)

Downstream services (optional; enable features when set):
- `JARVIS_ATOMIC_EXECUTOR_URL` (enables atomic dispatch)
- `JARVIS_COMPOSITE_EXECUTOR_URL` (enables composite dispatch)
- `JARVIS_NODE_REGISTRY_URL` (improves NodeKind resolution)
- `JARVIS_PDP_URL` (enables centralized policy)
- `JARVIS_SELECTION_URL` (reserved; coordinator does not call Selection in v0.3.3)

Downstream audiences (optional):
- `JARVIS_ATOMIC_EXECUTOR_AUDIENCE` (default `arp-jarvis-atomicexecutor`)
- `JARVIS_COMPOSITE_EXECUTOR_AUDIENCE` (default `arp-jarvis-compositeexecutor`)
- `JARVIS_NODE_REGISTRY_AUDIENCE` (default `arp-jarvis-noderegistry`)
- `JARVIS_PDP_AUDIENCE` (default `arp-jarvis-pdp`)
- `JARVIS_SELECTION_AUDIENCE` (default `arp-jarvis-selection`)

Coordinator behavior:
- `JARVIS_RUN_COORDINATOR_PUBLIC_URL` (required only to dispatch composites; passed to Composite Executor)
- `JARVIS_RUN_COORDINATOR_AUTO_DISPATCH` (default true; if false, NodeRuns remain queued in v0.3.3)

Policy posture:
- `JARVIS_POLICY_PROFILE=dev-allow` (explicitly allow for local dev; otherwise deny-by-default when PDP is unset)

## Validate conformance (`arp-conformance`)

Once the service is running, validate it against the ARP Standard:

```bash
python3 -m pip install arp-conformance
arp-conformance check run-coordinator --url http://127.0.0.1:8081 --tier smoke
arp-conformance check run-coordinator --url http://127.0.0.1:8081 --tier surface
```

## Helper scripts

- `src/scripts/dev_server.sh`: run the server (flags: `--host`, `--port`, `--reload`).
- `src/scripts/send_request.py`: create NodeRuns from a JSON file and fetch one back.

  ```bash
  python3 src/scripts/send_request.py --request src/scripts/request.json
  ```

## Authentication

Auth is enabled by default (JWT). To disable for local dev, set `ARP_AUTH_PROFILE=dev-insecure`.
Incoming JWT validation is configured via `ARP_AUTH_PROFILE` plus optional overrides (issuer/audience/JWKS).
Because this service requires outbound STS credentials (`ARP_AUTH_CLIENT_ID`/`ARP_AUTH_CLIENT_SECRET`), you will typically
have `ARP_AUTH_*` variables set; ensure you also set the inbound issuer/audience (or JWKS settings) for validation.

To enable local Keycloak defaults, set:
- `ARP_AUTH_PROFILE=dev-secure-keycloak`
- `ARP_AUTH_AUDIENCE=arp-run-coordinator`
- `ARP_AUTH_ISSUER=http://localhost:8080/realms/arp-dev`

Outbound service auth (Run Store / Event Stream / Artifact Store):
- `ARP_AUTH_CLIENT_ID`
- `ARP_AUTH_CLIENT_SECRET`
- `ARP_AUTH_TOKEN_ENDPOINT` (or `ARP_AUTH_ISSUER`)

## Upgrading

When upgrading to a new ARP Standard SDK release, bump pinned versions in `pyproject.toml` (`arp-standard-*==...`) and re-run conformance.
