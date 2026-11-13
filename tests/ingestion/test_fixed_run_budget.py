import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from trr_backend.ingestion.fixed_run_budget import FixedRunBudget
from trr_backend.ingestion.fixed_validation import FixedManifest, FixedPost, _selection_setup
from trr_backend.ingestion.identity_policy import IdentityDeferred


def manifest():
    return FixedManifest(
        "fixture",
        "bravotv",
        (FixedPost("Abc123", "123_456", str(uuid4()), "https://www.instagram.com/p/Abc123/", "bravotv"),),
        max_source_requests=3,
        max_run_seconds=180,
    )


def consumed(budget):
    with sqlite3.connect(budget.path) as db:
        return db.execute("select consumed from budget where id=1").fetchone()[0]


def test_historical_manifest_hash_shape_is_preserved():
    legacy = replace(manifest(), max_source_requests=None, max_run_seconds=None)
    assert "max_source_requests" not in legacy.payload()
    assert "max_run_seconds" not in legacy.payload()
    assert FixedManifest.from_payload(legacy.payload()).digest == legacy.digest
    fixed = replace(legacy, posts=(replace(legacy.posts[0], catalog_post_id="00000000-0000-0000-0000-000000000001"),))
    assert fixed.digest == "b9d4f0903be77ab5dd3adfb6da035890b205c71b62af3c2f9faf06f794d5427f"


def test_unbounded_second_post_remains_unbounded():
    first = replace(manifest(), max_source_requests=None, max_run_seconds=None)
    second = replace(
        first.posts[0],
        shortcode="Other123",
        catalog_post_id=str(uuid4()),
        url="https://www.instagram.com/p/Other123/",
    )
    old = replace(first, posts=(*first.posts, second))
    context = {
        "run": {"execution_placement": "local", "intent": old.intent().payload(), "intent_hash": old.intent().digest},
        "target": {"stage": "comments", "target_key": second.shortcode, "operator_selection": _selection_setup(old)},
    }
    assert FixedRunBudget.from_context(context) is None


def test_bounded_profile_is_public_only():
    from trr_backend.ingestion.domain import SourceProfile

    with pytest.raises(ValueError):
        replace(manifest(), source_profile=SourceProfile.for_authenticated_relay())


@pytest.mark.parametrize("finalized", [False, True])
def test_actual_source_gate_transaction_protocol(tmp_path, monkeypatch, finalized):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from trr_backend.ingestion.identity_policy import SourceRequestGate

    gate = SourceRequestGate(
        cookies={},
        transaction=lambda: None,
        check_cancelled=lambda: None,
        deadline=time.monotonic() + 60,
        public_only=True,
        network_lane="local",
    )
    calls = []
    cur = SimpleNamespace(connection=object(), execute=lambda *args: calls.append("lane"))

    @contextmanager
    def locked(**kwargs):
        yield cur, {"blocked": False, "request_times": [], "failure_generation": 0}, datetime.now(UTC)

    monkeypatch.setattr(gate, "_locked", locked)

    def before(conn):
        calls.append("ordinary")

    if finalized:
        before.finalize_in_transaction = lambda *args: calls.append("finalize")
    budget = FixedRunBudget(tmp_path / "private", uuid4(), "hash", 1, time.time() + 180)
    gate.before_counted_request(budget.wrap(before))
    assert consumed(budget) == 1
    assert calls == (["finalize"] if finalized else ["ordinary", "lane"])
    with pytest.raises(IdentityDeferred, match="operator_request_limit"):
        gate.before_counted_request(budget.wrap(before))
    assert gate.request_count == 1


def test_unsafe_directory_blocks_source_admission(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "linked"
    link.symlink_to(outside)
    with pytest.raises(IdentityDeferred, match="operator_budget_unavailable"):
        FixedRunBudget(link, uuid4(), "hash", 1, time.time() + 180)


def test_worker_exhaustion_holds_target_and_resets_deadline(tmp_path, monkeypatch):
    from trr_backend.ingestion import worker as module
    from trr_backend.ingestion.domain import Stage
    from trr_backend.ingestion.reply_context import execution_deadline

    frozen = manifest()
    run_id = str(uuid4())
    monkeypatch.setenv("TRR_INSTAGRAM_LOCAL_WORKER_BUDGET_DIR", str(tmp_path / "private"))
    context = {
        "run": {
            "run_id": run_id,
            "intent": frozen.intent().payload(),
            "intent_hash": frozen.intent().digest,
            "execution_placement": "local",
            "created_at": datetime.now(UTC),
        },
        "target": {
            "stage": "comments",
            "target_key": "Abc123",
            "owner_started_at": None,
            "checkpoint": {},
            "operator_selection": _selection_setup(frozen),
        },
        "post": {"id": str(uuid4()), "comments_count": 1},
    }
    failures = []

    class Store:
        def acquire(self, **kwargs):
            return {}

        def checkpoint_failure(self, **kwargs):
            failures.append(kwargs["failure"])

    class Source:
        def __call__(self, **callbacks):
            for _ in range(4):
                callbacks["pre_source_request"]()

        def finish_failure(self, error):
            assert isinstance(error, IdentityDeferred)

    source = Source()
    worker = module.Worker(
        Store(),
        listing_source=None,
        detail_source=None,
        comment_source=lambda *args: source,
        media_loader=None,
        media_storage=None,
    )
    monkeypatch.setattr(worker, "context", lambda owned: context)
    monkeypatch.setattr(
        module.CommentActivity, "execute", lambda self, **kwargs: kwargs["fetch"](pre_source_request=lambda: None)
    )
    before = execution_deadline.get()
    result = worker.execute(
        {
            "run_id": run_id,
            "account": "bravotv",
            "generation": 1,
            "target_key": "Abc123",
            "owner_token": "fixture",
            "stage": Stage.COMMENTS,
        }
    )
    assert result["outcome"] == "unresolved"
    assert result["code"] == "operator_request_limit"
    assert failures[0].identity_blocked is True
    assert execution_deadline.get() == before


def test_public_worker_rejects_authenticated_intent_before_source(monkeypatch):
    from trr_backend.ingestion.domain import RunIntent, SourceProfile
    from trr_backend.ingestion.worker import Worker

    failures = []

    class Store:
        def acquire(self, **kwargs):
            return {}

        def checkpoint_failure(self, **kwargs):
            failures.append(kwargs["failure"])

    def forbidden(*args, **kwargs):
        raise AssertionError("Attempted credential-dependent source construction")

    worker = Worker(
        Store(),
        listing_source=forbidden,
        detail_source=forbidden,
        comment_source=forbidden,
        media_loader=None,
        media_storage=None,
    )
    intent = RunIntent("bravotv", source_profile=SourceProfile.for_mode("authenticated"))
    monkeypatch.setenv("TRR_INSTAGRAM_LOCAL_WORKER_PUBLIC_ONLY", "1")
    monkeypatch.setattr(
        worker,
        "context",
        lambda owned: {"run": {"intent": intent.payload()}, "target": {"owner_started_at": None, "checkpoint": {}}},
    )
    result = worker.execute(
        {
            "run_id": str(uuid4()),
            "account": "bravotv",
            "generation": 1,
            "target_key": "Abc123",
            "owner_token": "fixture",
            "stage": "post_details",
        }
    )
    assert result["code"] == "operator_public_only"
    assert failures[0].identity_blocked is True


@pytest.mark.parametrize(
    "changes",
    [
        {"max_source_requests": None},
        {"max_run_seconds": None},
        {"max_source_requests": True},
        {"max_source_requests": 21},
        {"max_run_seconds": 301},
        {"max_run_seconds": 20},
        {"posts": ()},
    ],
)
def test_invalid_bounds_fail_closed(changes):
    with pytest.raises(ValueError):
        replace(manifest(), **changes)


def test_budget_survives_recreation_and_never_refunds_failure(tmp_path):
    run = str(uuid4())
    budget = FixedRunBudget(tmp_path / "private", run, "hash", 2, 1100, clock=lambda: 1000)

    def failed_send():
        raise RuntimeError("fixture transport failed")

    with pytest.raises(RuntimeError):
        budget.wrap(failed_send)()
    restarted = FixedRunBudget(tmp_path / "private", run, "hash", 2, 1100, clock=lambda: 1000)
    restarted.consume()
    with pytest.raises(IdentityDeferred, match="operator_request_limit") as error:
        restarted.consume()
    assert error.value.failure.identity_blocked is True
    assert consumed(restarted) == 2


def test_concurrent_admission_never_exceeds_limit(tmp_path):
    budget = FixedRunBudget(tmp_path / "private", uuid4(), "hash", 3, time.time() + 180)

    def attempt(_):
        try:
            budget.consume()
            return 1
        except IdentityDeferred:
            return 0

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(attempt, range(10))) == 3
    assert consumed(budget) == 3


def test_deadline_and_binding_fail_without_extra_admission(tmp_path):
    now = [1000]
    run = uuid4()
    budget = FixedRunBudget(tmp_path / "private", run, "hash", 3, 1100, clock=lambda: now[0])
    now[0] = 1100
    with pytest.raises(IdentityDeferred, match="operator_time_limit"):
        budget.consume()
    assert consumed(budget) == 0
    with pytest.raises(IdentityDeferred, match="operator_budget_unavailable"):
        FixedRunBudget(tmp_path / "private", run, "different", 3, 1100, clock=lambda: 1000)


def test_both_transaction_paths_count_once_and_preserve_release(tmp_path):
    budget = FixedRunBudget(tmp_path / "private", uuid4(), "hash", 3, time.time() + 180)
    calls = []

    def outer():
        calls.append("outer")

    def transactional(conn):
        calls.append("transactional")

    def finalize(conn, sql, params):
        calls.append("finalize")

    def release(*args):
        calls.append("release")

    transactional.release_in_transaction = release
    outer.in_transaction = transactional
    outer.run_id = "fixture"
    outer.deadline = time.monotonic() + 5
    wrapped = budget.wrap(outer)
    wrapped.in_transaction(None)
    transactional.finalize_in_transaction = finalize
    wrapped = budget.wrap(outer)
    wrapped.in_transaction.finalize_in_transaction(None, "sql", ())
    wrapped.in_transaction.release_in_transaction(None, None)
    assert consumed(budget) == 2
    assert calls == ["transactional", "finalize", "release"]
    assert wrapped.deadline == outer.deadline
    assert wrapped.run_id == "fixture"


def test_persisted_selection_and_intent_must_match(tmp_path, monkeypatch):
    frozen = manifest()
    monkeypatch.setenv("TRR_INSTAGRAM_LOCAL_WORKER_BUDGET_DIR", str(tmp_path / "private"))
    context = {
        "run": {
            "run_id": str(uuid4()),
            "intent": frozen.intent().payload(),
            "intent_hash": frozen.intent().digest,
            "execution_placement": "local",
            "created_at": datetime.now(UTC),
        },
        "target": {"stage": "comments", "target_key": "Abc123", "operator_selection": _selection_setup(frozen)},
    }
    assert FixedRunBudget.from_context(context).limit == 3
    context["target"]["operator_selection"]["manifest"]["max_source_requests"] = 4
    with pytest.raises(IdentityDeferred, match="operator_budget_invalid"):
        FixedRunBudget.from_context(context)


def test_create_only_queues_without_inline_worker(tmp_path, monkeypatch, capsys):
    from scripts.ingestion import local_driver
    from scripts.ingestion import validate_fixed_posts as cli
    from trr_backend.db import connection
    from trr_backend.ingestion import composition, validation_gate

    frozen = manifest()
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(frozen.payload()))
    run_id = str(uuid4())

    def forbidden(*args, **kwargs):
        raise AssertionError("Create-only started inline work")

    monkeypatch.setattr(local_driver, "_project_dsn", lambda _: True)
    monkeypatch.setattr(connection, "resolve_database_url_candidate_details", lambda: [{"url": "fixture"}])
    monkeypatch.setattr(validation_gate, "ValidationWorkflowStore", lambda **kwargs: object())
    monkeypatch.setattr(cli, "start_run", lambda *args, **kwargs: {"run_id": run_id, "generation": 1})
    monkeypatch.setattr(cli, "run_tranche", forbidden)
    monkeypatch.setattr(composition, "build_worker", forbidden)
    monkeypatch.setattr(
        cli,
        "inspect_run",
        lambda *args: {
            "run": {"state": "queued", "generation": 1},
            "validation": {"status": "pending", "passed": False},
            "targets": [],
        },
    )
    assert (
        cli.main(
            [
                "start",
                "--manifest",
                str(path),
                "--state",
                str(tmp_path / "state.json"),
                "--budget-path",
                str(tmp_path / "account.sqlite3"),
                "--create-only",
                "--apply",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "created"


@pytest.mark.parametrize("field,value", [("id", "999_456"), ("code", "Other123"), ("owner", "other")])
def test_bounded_detail_rejects_changed_frozen_identity_before_observation(tmp_path, monkeypatch, field, value):
    from trr_backend.ingestion.domain import Conflict

    frozen = manifest()
    monkeypatch.setenv("TRR_INSTAGRAM_LOCAL_WORKER_BUDGET_DIR", str(tmp_path / "private"))
    context = {
        "run": {
            "run_id": str(uuid4()),
            "intent": frozen.intent().payload(),
            "intent_hash": frozen.intent().digest,
            "execution_placement": "local",
            "created_at": datetime.now(UTC),
        },
        "target": {"stage": "post_details", "target_key": "Abc123", "operator_selection": _selection_setup(frozen)},
    }
    budget = FixedRunBudget.from_context(context)
    captured = []
    observe = budget.callbacks({"source_observer": lambda *a: captured.append(a)})["source_observer"]
    payload = {"id": "123_456", "code": "Abc123", "user": {"pk": "456", "username": "bravotv"}}
    observe("detail", payload)
    assert len(captured) == 1
    if field == "owner":
        payload["user"]["username"] = value
    else:
        payload[field] = value
    with pytest.raises(Conflict, match="frozen catalog identity"):
        observe("detail", payload)
    assert len(captured) == 1
