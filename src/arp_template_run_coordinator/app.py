from __future__ import annotations

from .coordinator import RunCoordinator
from .utils import auth_settings_from_env_or_dev_insecure


def create_app():
    return RunCoordinator().create_app(
        title="ARP Template Run Coordinator",
        auth_settings=auth_settings_from_env_or_dev_insecure(),
    )


app = create_app()

