from __future__ import annotations

import argparse
import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the JARVIS Run Coordinator server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload (dev only).")
    args = parser.parse_args()

    if args.reload:
        uvicorn.run("jarvis_run_coordinator.app:app", host=args.host, port=args.port, reload=True)
        return

    from .app import create_app

    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
