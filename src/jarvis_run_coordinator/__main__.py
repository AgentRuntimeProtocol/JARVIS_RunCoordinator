from __future__ import annotations

import argparse
import os

import uvicorn

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s %(levelname)s %(name)s: %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": LOG_LEVEL,
    },
    "loggers": {
        "uvicorn": {"handlers": ["console"], "level": LOG_LEVEL, "propagate": False},
        "uvicorn.error": {"level": LOG_LEVEL},
        "uvicorn.access": {"handlers": ["console"], "level": LOG_LEVEL, "propagate": False},
    },
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the JARVIS Run Coordinator server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload (dev only).")
    args = parser.parse_args()

    if args.reload:
        uvicorn.run(
            "jarvis_run_coordinator.app:app",
            host=args.host,
            port=args.port,
            reload=True,
            log_config=LOG_CONFIG,
        )
        return

    from .app import create_app

    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port, reload=False, log_config=LOG_CONFIG)


if __name__ == "__main__":
    main()
