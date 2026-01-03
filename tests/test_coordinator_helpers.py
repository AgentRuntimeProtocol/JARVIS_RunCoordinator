import os

import pytest
from arp_standard_model import (
    Budgets,
    Candidates,
    ConstraintEnvelope,
    Extensions,
    Gates,
    NodeKind,
    NodeRun,
    NodeRunCreateSpec,
    NodeRunState,
    NodeTypeRef,
    SideEffectClass,
    Structural,
)
from arp_standard_server import ArpServerError

from jarvis_run_coordinator.coordinator import (
    _assert_idempotent_match,
    _constraints_from_extensions,
    _env_flag,
    _extensions_payload,
    _idempotent_node_run_id,
    _infer_kind,
    _merge_constraints,
    _min_int,
    _most_restrictive_side_effect,
    _normalize_url,
    RunCoordinator,
)


class DummyRunStore:
    async def create_run(self, run, *, idempotency_key=None):
        return run

    async def get_run(self, run_id: str):
        return None

    async def update_run(self, run):
        return run

    async def create_node_run(self, node_run, *, idempotency_key=None):
        return node_run

    async def get_node_run(self, node_run_id: str):
        return None

    async def update_node_run(self, node_run):
        return node_run

    async def list_node_runs_for_run(self, run_id: str, *, limit: int = 500):
        _ = (run_id, limit)
        return []


class DummyEventStream:
    async def append_events(self, events):
        return {"items": events}

    async def stream_run_events(self, run_id: str):
        _ = run_id
        return ""


class DummyArtifactStore:
    async def create_artifact(self, data: bytes, *, content_type=None):
        _ = (data, content_type)
        return {}

    async def get_metadata(self, artifact_id: str):
        _ = artifact_id
        return {}


def test_infer_kind() -> None:
    assert _infer_kind("atomic.echo") == NodeKind.atomic
    assert _infer_kind("composite.plan") == NodeKind.composite
    assert _infer_kind("jarvis.core.echo") is None


def test_normalize_url() -> None:
    assert _normalize_url(" http://example.com/v1/ ") == "http://example.com"
    assert _normalize_url("") is None


def test_env_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_FLAG", raising=False)
    assert _env_flag("TEST_FLAG", default=True)
    monkeypatch.setenv("TEST_FLAG", "0")
    assert not _env_flag("TEST_FLAG", default=True)


def test_extensions_payload_and_constraints() -> None:
    assert _extensions_payload(None) == {}

    constraints = ConstraintEnvelope(budgets=Budgets(max_steps=3))
    extensions = Extensions.model_validate({"constraints": constraints.model_dump(exclude_none=True)})
    parsed = _constraints_from_extensions(extensions)
    assert parsed is not None
    assert parsed.budgets is not None
    assert parsed.budgets.max_steps == 3

    bad_extensions = Extensions.model_validate({"constraints": {"budgets": {"max_steps": "nope"}}})
    with pytest.raises(ArpServerError):
        _constraints_from_extensions(bad_extensions)


def test_merge_constraints() -> None:
    first = ConstraintEnvelope(
        budgets=Budgets(max_steps=5, max_external_calls=10),
        candidates=Candidates(
            allowed_node_type_ids=["a", "b"],
            denied_node_type_ids=["x"],
            max_candidates_per_subtask=5,
        ),
        structural=Structural(max_depth=5, max_children_per_composite=10),
        gates=Gates(require_approval=True, side_effect_class=SideEffectClass.write),
    )
    second = ConstraintEnvelope(
        budgets=Budgets(max_steps=2, max_external_calls=20),
        candidates=Candidates(
            allowed_node_type_ids=["b", "c"],
            denied_node_type_ids=["y"],
            max_candidates_per_subtask=2,
        ),
        structural=Structural(max_depth=3, max_children_per_composite=4),
        gates=Gates(require_approval=False, side_effect_class=SideEffectClass.irreversible),
    )

    merged = _merge_constraints(first, second)
    assert merged is not None
    assert merged.budgets is not None
    assert merged.budgets.max_steps == 2
    assert merged.budgets.max_external_calls == 10
    assert merged.candidates is not None
    assert merged.candidates.allowed_node_type_ids == ["b"]
    assert merged.candidates.denied_node_type_ids == ["x", "y"]
    assert merged.candidates.max_candidates_per_subtask == 2
    assert merged.structural is not None
    assert merged.structural.max_depth == 3
    assert merged.gates is not None
    assert merged.gates.require_approval is True
    assert merged.gates.side_effect_class == SideEffectClass.irreversible

    assert _merge_constraints() is None


def test_min_int_and_side_effect() -> None:
    assert _min_int([None, 3, 1]) == 1
    assert _min_int([None, None]) is None
    assert _most_restrictive_side_effect(["read", "write"]) == SideEffectClass.write
    assert _most_restrictive_side_effect(["invalid"]) is None


def test_idempotent_node_run_id() -> None:
    value = _idempotent_node_run_id("run-1", "parent-1", "key")
    assert value == _idempotent_node_run_id("run-1", "parent-1", "key")
    assert value != _idempotent_node_run_id("run-1", "parent-1", "key-2")


def test_assert_idempotent_match() -> None:
    spec = NodeRunCreateSpec(
        node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.5"),
        inputs={"ping": "pong"},
    )
    node_run = NodeRun(
        node_run_id="node-1",
        run_id="run-1",
        parent_node_run_id="parent-1",
        node_type_ref=spec.node_type_ref,
        state=NodeRunState.queued,
        inputs=spec.inputs,
        kind=None,
    )

    _assert_idempotent_match(
        node_run,
        spec=spec,
        run_id="run-1",
        parent_node_run_id="parent-1",
        expected_constraints=None,
    )

    with pytest.raises(ArpServerError):
        _assert_idempotent_match(
            node_run,
            spec=NodeRunCreateSpec(
                node_type_ref=NodeTypeRef(node_type_id="jarvis.core.echo", version="0.3.5"),
                inputs={"ping": "different"},
            ),
            run_id="run-1",
            parent_node_run_id="parent-1",
            expected_constraints=None,
        )


def test_run_coordinator_requires_clients() -> None:
    with pytest.raises(RuntimeError):
        RunCoordinator(run_store=None, event_stream=DummyEventStream(), artifact_store=DummyArtifactStore())
    with pytest.raises(RuntimeError):
        RunCoordinator(run_store=DummyRunStore(), event_stream=None, artifact_store=DummyArtifactStore())
    with pytest.raises(RuntimeError):
        RunCoordinator(run_store=DummyRunStore(), event_stream=DummyEventStream(), artifact_store=None)
