from __future__ import annotations

import argparse
import importlib
from types import ModuleType


def _load_uvicorn() -> ModuleType:
    try:
        return importlib.import_module("uvicorn")
    except ImportError as exc:
        raise SystemExit(
            "uvicorn is not installed. Install with `pip install -e .[run]` "
            "or `pip install uvicorn`."
        ) from exc


def main() -> None:
    uvicorn = _load_uvicorn()

    parser = argparse.ArgumentParser(description="Start the ARP Template Run Coordinator server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload (dev only).")
    args = parser.parse_args()

    if args.reload:
        uvicorn.run("arp_template_run_coordinator.app:app", host=args.host, port=args.port, reload=True)
        return

    from .app import create_app

    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()

