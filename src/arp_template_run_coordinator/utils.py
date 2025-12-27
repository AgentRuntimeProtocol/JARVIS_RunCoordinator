from __future__ import annotations

import os
from datetime import datetime, timezone

from arp_standard_server import AuthSettings


def now() -> datetime:
    return datetime.now(timezone.utc)


def auth_settings_from_env_or_dev_insecure() -> AuthSettings:
    if os.environ.get("ARP_AUTH_MODE") or os.environ.get("ARP_AUTH_PROFILE"):
        return AuthSettings.from_env()
    return AuthSettings(mode="disabled")

