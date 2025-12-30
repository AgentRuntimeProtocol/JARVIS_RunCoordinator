import importlib
import sys

import pytest


class DummyAuthClient:
    pass


def _load_modules(monkeypatch, tmp_path):
    monkeypatch.setenv("JARVIS_RUN_STORE_URL", "http://run-store")
    monkeypatch.setenv("JARVIS_EVENT_STREAM_URL", "http://event-stream")
    monkeypatch.setenv("JARVIS_ARTIFACT_STORE_URL", "http://artifact-store")
    monkeypatch.setenv("ARP_AUTH_MODE", "disabled")
    monkeypatch.setenv("ARP_AUTH_CLIENT_ID", "client")
    monkeypatch.setenv("ARP_AUTH_CLIENT_SECRET", "secret")
    monkeypatch.setattr("arp_auth.AuthClient.from_env", lambda: DummyAuthClient())

    import jarvis_run_coordinator.app as app_module
    import jarvis_run_coordinator.__main__ as main_module

    importlib.reload(app_module)
    importlib.reload(main_module)
    return app_module, main_module


def test_create_app_success(monkeypatch, tmp_path) -> None:
    app_module, _ = _load_modules(monkeypatch, tmp_path)
    app = app_module.create_app()
    assert app is not None


def test_create_app_requires_auth(monkeypatch, tmp_path) -> None:
    app_module, _ = _load_modules(monkeypatch, tmp_path)
    monkeypatch.delenv("ARP_AUTH_CLIENT_ID", raising=False)
    monkeypatch.delenv("ARP_AUTH_CLIENT_SECRET", raising=False)

    with pytest.raises(RuntimeError):
        app_module.create_app()


def test_require_url(monkeypatch, tmp_path) -> None:
    app_module, _ = _load_modules(monkeypatch, tmp_path)
    monkeypatch.delenv("MISSING_URL", raising=False)

    with pytest.raises(RuntimeError):
        app_module._require_url("MISSING_URL")

    monkeypatch.setenv("TEST_AUDIENCE", "audience")
    assert app_module._audience_from_env("TEST_AUDIENCE", default=None) == "audience"


def test_main_runs_uvicorn_reload(monkeypatch, tmp_path) -> None:
    _, main_module = _load_modules(monkeypatch, tmp_path)
    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(main_module.uvicorn, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["prog", "--reload"])

    main_module.main()

    args, kwargs = calls[0]
    assert args[0] == "jarvis_run_coordinator.app:app"
    assert kwargs["reload"] is True


def test_main_runs_uvicorn_no_reload(monkeypatch, tmp_path) -> None:
    _, main_module = _load_modules(monkeypatch, tmp_path)
    calls = []
    app = object()

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(main_module.uvicorn, "run", fake_run)
    monkeypatch.setattr("jarvis_run_coordinator.app.create_app", lambda: app)
    monkeypatch.setattr(sys, "argv", ["prog"])

    main_module.main()

    args, kwargs = calls[0]
    assert args[0] is app
    assert kwargs["reload"] is False
