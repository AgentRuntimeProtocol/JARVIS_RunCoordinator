#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"

# Optional local dev config.
# NOTE: This sources a local file; only use it in trusted dev environments.
if [[ -f "${REPO_ROOT}/.env.local" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${REPO_ROOT}/.env.local"
  set +a
elif [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${REPO_ROOT}/.env"
  set +a
fi

HOST="127.0.0.1"
PORT="8081"
RELOAD=0

usage() {
  cat <<'EOF'
Usage: dev_server.sh [--host HOST] [--port PORT] [--reload]

Options:
  -H, --host        Host to bind (default: 127.0.0.1)
  -p, --port        Port to bind (default: 8081)
      --reload      Enable auto-reload (dev only)
  -h, --help        Show this help text
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -H|--host)
      HOST="${2:?Missing value for --host}"
      shift 2
      ;;
    -p|--port)
      PORT="${2:?Missing value for --port}"
      shift 2
      ;;
    --reload)
      RELOAD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

args=(--host "$HOST" --port "$PORT")
if [[ "$RELOAD" -eq 1 ]]; then
  args+=(--reload)
fi

exec python3 -m jarvis_run_coordinator "${args[@]}"
