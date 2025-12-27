# ARP Template Run Coordinator

Use this repo as a starting point for building an **ARP compliant Run Coordinator** service.

This template is **advanced**. The Run Coordinator is correctness-critical and stateful; a starter implementation will necessarily be more complex and less DX-friendly than other templates.

This minimal template implements the Run Coordinator API using only the SDK packages:
`arp-standard-server`, `arp-standard-model`, and `arp-standard-client`.

Implements: ARP Standard `spec/v1` Run Coordinator API (contract: `ARP_Standard/spec/v1/openapi/run-coordinator.openapi.yaml`).

## Requirements

- Python >= 3.10

## Install

```bash
python3 -m pip install -e .
```

## Local configuration (optional)

For local dev convenience, copy the template env file:

```bash
cp .env.example .env.local
```

`src/scripts/dev_server.sh` auto-loads `.env.local` (or `.env`).

## Run

- Run Coordinator listens on `http://127.0.0.1:8081` by default.

```bash
python3 -m pip install -e '.[run]'
python3 -m arp_template_run_coordinator
```

> [!TIP]
> Use `bash src/scripts/dev_server.sh --host ... --port ... --reload` for dev convenience.

## Using this repo

This template is the minimum viable standalone Run Coordinator implementation.

To build your own coordinator, fork this repository and replace the in-memory stores and orchestration logic while keeping request/response semantics stable.

If all you need is to change NodeRun lifecycle behavior and storage, edit:
- `src/arp_template_run_coordinator/coordinator.py`

Outgoing client wrappers (coordinator -> other components):
- `src/arp_template_run_coordinator/atomic_executor_client.py`
- `src/arp_template_run_coordinator/composite_executor_client.py`
- `src/arp_template_run_coordinator/selection_client.py`
- `src/arp_template_run_coordinator/node_registry_client.py`
- `src/arp_template_run_coordinator/pdp_client.py`

### Default behavior

- NodeRuns are stored in memory.
- `create_node_runs` creates NodeRuns with `state=queued`.
- `get_node_run` returns stored NodeRuns.
- `complete_node_run` marks a NodeRun as `succeeded` and stores outputs.
- `report_node_run_evaluation` stores `evaluation_result` on the NodeRun.

### Notes on API surface

- In `spec/v1`, Run lifecycle endpoints (start/get/cancel) are defined on both Run Gateway (client-facing) and Run Coordinator (run authority).
- The coordinator template focuses on the standardized NodeRun APIs. Execution hierarchy is derived from NodeRuns and is an internal coordinator concern.

## Quick health check

```bash
curl http://127.0.0.1:8081/v1/health
```

## Configuration

CLI flags:
- `--host` (default `127.0.0.1`)
- `--port` (default `8081`)
- `--reload` (dev only)

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

For out-of-the-box usability, this template defaults to auth-disabled unless you set `ARP_AUTH_MODE` or `ARP_AUTH_PROFILE`.

To enable JWT auth, set either:
- `ARP_AUTH_PROFILE=dev-secure-keycloak` + `ARP_AUTH_SERVICE_ID=<audience>`
- or `ARP_AUTH_MODE=required` with `ARP_AUTH_ISSUER` and `ARP_AUTH_AUDIENCE`

## Upgrading

When upgrading to a new ARP Standard SDK release, bump pinned versions in `pyproject.toml` (`arp-standard-*==...`) and re-run conformance.
