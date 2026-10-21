"""Exact selection uses production SQL, Scheduler, Worker, and durable activities."""

import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from scripts.ingestion.validate_fixed_posts import main
from tests.ingestion import test_public_comment_transport as relay
from tests.ingestion.test_comments import database as comments_database
from trr_backend.ingestion.domain import Conflict
from trr_backend.ingestion.fixed_validation import (
    AccountBudget,
    FixedManifest,
    FixedPost,
    ValidationPause,
    inspect_run,
    run_tranche,
    start_run,
)
from trr_backend.ingestion.validation_gate import ValidationWorkflowStore
from trr_backend.ingestion.worker import Worker

base_database = comments_database
ROOT = Path(__file__).resolve().parents[2]
_CREATED_RUNS = []


@pytest.fixture(autouse=True)
def cleanup_test_owned_runs():
    yield
    while _CREATED_RUNS:
        store, run_id, account = _CREATED_RUNS.pop()
        with store.transaction() as conn, conn.cursor() as cur:
            cur.execute("select state,generation from social.instagram_ingestion_runs where run_id=%s", (run_id,))
            state, generation = cur.fetchone()
        if state in {"queued", "running"}:
            store.control(
                run_id,
                account,
                action="cancel",
                expected_generation=generation,
                expected_placement="local",
                idempotency_key=uuid4().hex,
            )


@pytest.fixture(scope="module")
def database(base_database):
    _, tx = base_database
    with tx() as conn, conn.cursor() as cur:
        for name in (
            "20261016070100_instagram_ingestion_workflows.sql",
            "20261016153330_instagram_ingestion_run_holds.sql",
            "20261016173915_instagram_ingestion_execution_placement.sql",
        ):
            cur.execute((ROOT / "supabase/migrations" / name).read_text())
    return ValidationWorkflowStore(tx, execution_placement="local"), tx


def selected(database):
    store, tx = database
    code = "fixed" + uuid4().hex
    post = FixedPost(code, "123_456", str(uuid4()), f"https://www.instagram.com/p/{code}/", "bravotv")
    manifest = FixedManifest("fixture-revision", "bravotv", (post,))
    raw = {"pk": "123", "code": code, "user": {"pk": "456", "username": "bravotv"}}
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_account_catalog_posts
            (id,source_id,source_account,raw_data) values(%s,%s,'bravotv',%s::jsonb)""",
            (post.catalog_post_id, code, json.dumps(raw)),
        )
        cur.execute(
            """insert into social.instagram_posts(shortcode,username,media_id,caption)
            values(%s,'bravotv','123','stale catalog caption')""",
            (code,),
        )
    run = start_run(store, manifest, workflow_version="fixed-tests", idempotency_key=uuid4().hex)
    _CREATED_RUNS.append((store, run["run_id"], manifest.account))
    return manifest, run


def test_budget_persists_across_tranches_and_instances(tmp_path):
    clock = [100000.0]
    path = tmp_path / "private" / "budget.sqlite"
    for _tranche in range(4):
        budget = AccountBudget(path, "bravotv", clock=lambda: clock[0])
        with budget.stream():
            for _ in range(75):
                budget.consume()
    latest = AccountBudget(path, "bravotv", clock=lambda: clock[0])
    with latest.stream():
        latest.consume()
    assert latest.inspect()["rolling_24h_requests"] == 301
    assert latest.inspect()["remaining_24h_requests"] is None
    assert len(latest.inspect()["request_events"]) == 301
    clock[0] += 86401
    with latest.stream():
        latest.consume()
    assert latest.inspect()["rolling_24h_requests"] == 1
    assert path.stat().st_mode & 0o777 == 0o600


def test_single_stream_and_elapsed_limit(tmp_path):
    clock = [100000.0]
    first = AccountBudget(tmp_path / "budget.sqlite", "bravotv", clock=lambda: clock[0])
    second = AccountBudget(first.path, "bravotv", clock=lambda: clock[0])
    with first.stream():
        with pytest.raises(Conflict, match="Another account"):
            with second.stream():
                pytest.fail("concurrent source stream accepted")
        clock[0] += 900
        with pytest.raises(ValidationPause, match="time_budget"):
            first.consume()
    with pytest.raises(Conflict, match="single active"):
        first.consume()
    assert first.inspect()["rolling_24h_requests"] == 0


def test_selection_setup_has_no_source_receipt_and_detects_drift(database):
    store, tx = database
    manifest, run = selected(database)
    status = inspect_run(store, manifest, run["run_id"])
    assert status["run"]["intent"]["force"] is True
    assert status["run"]["intent"]["scope"] == "recent_window"
    assert len(status["targets"]) == 2
    with tx() as conn, conn.cursor() as cur:
        for table in ("instagram_ingestion_receipts", "instagram_ingestion_evidence"):
            cur.execute(f"select count(*) from social.{table} where run_id=%s", (run["run_id"],))
            assert cur.fetchone()[0] == 0
        cur.execute(
            """insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts)
            values(%s,'comments','unlisted',12)""",
            (run["run_id"],),
        )
    with pytest.raises(Conflict, match="frozen exact"):
        inspect_run(store, manifest, run["run_id"])


@pytest.mark.parametrize("boundary", ["parent", "child"])
def test_production_scheduler_worker_interrupt_and_resume(database, tmp_path, monkeypatch, boundary):
    store, tx = database
    manifest, run = selected(database)
    post = manifest.posts[0]
    budget = AccountBudget(tmp_path / "budget.sqlite", manifest.account)
    calls = []

    def handler(request):
        if request.method == "GET":
            calls.append("bootstrap")
            return relay.bootstrap()
        _, variables = relay.body(request)
        parent = variables.get("parent_comment_id")
        calls.append("child" if parent else "parent")
        if parent:
            assert parent == "p"
            reply = relay.actual_parent_shape("r", 0)
            reply.pop("parent_comment_id")
            return httpx.Response(200, json=relay.page([reply], child=True))
        return httpx.Response(200, json=relay.page([relay.actual_parent_shape("p", 1)], count=2))

    def factory(workflow_store):
        comment_source, _ = relay.source(monkeypatch, handler, expected=2)

        def details(context, owned):
            def fetch(**callbacks):
                callbacks["on_request"]()
                payload = {
                    "pk": "123",
                    "code": post.shortcode,
                    "user": {"username": "bravotv", "pk": "456"},
                    "media_type": 1,
                    "taken_at": 1789500000,
                    "caption": {"text": "fresh native detail"},
                    "like_count": 0,
                    "comment_count": 2,
                    "display_url": "https://cdn.example/image.jpg",
                }
                callbacks["source_observer"]("details", payload)
                return payload

            return fetch

        return Worker(
            workflow_store,
            listing_source=lambda *_: pytest.fail("Operator setup invoked listing"),
            detail_source=details,
            comment_source=lambda *_: comment_source,
            media_loader=lambda *_: pytest.fail("Unlisted media scheduled"),
            media_storage=lambda *_: None,
            detail_cache=lambda *_: pytest.fail("Fresh validation invoked cache"),
        )

    interrupted = run_tranche(
        store, manifest, run["run_id"], budget=budget, worker_factory=factory, interrupt_after=boundary
    )
    assert interrupted["reason"] == "validation_interrupted_" + boundary
    assert interrupted["state"]["run"]["state"] == "cancelled"
    before_generation = interrupted["state"]["run"]["generation"]
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_comment_pages where run_id=%s", (run["run_id"],))
        before_pages = cur.fetchone()[0]
        assert before_pages == (1 if boundary == "parent" else 2)
        cur.execute("select caption from social.instagram_posts where shortcode=%s", (post.shortcode,))
        assert cur.fetchone()[0] == "fresh native detail"
    resumed = store.control(
        run["run_id"],
        manifest.account,
        action="resume",
        idempotency_key=uuid4().hex,
        expected_generation=before_generation,
        expected_placement="local",
    )
    assert resumed["generation"] == before_generation + 1
    result = run_tranche(store, manifest, run["run_id"], budget=budget, worker_factory=factory)
    assert result["reason"] == "completed"
    assert result["state"]["run"]["state"] == "completed"
    assert result["validation"]["passed"] is True
    assert all(row["verified"] for row in result["validation"]["targets"])
    assert all(target["outcome"] == "committed" for target in result["state"]["targets"])
    assert calls.count("parent") == 1  # Durable parent lane is never restarted.
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_comment_pages where run_id=%s", (run["run_id"],))
        assert cur.fetchone()[0] >= before_pages
        cur.execute(
            """select count(*) from social.instagram_comments
            where post_id=(select id from social.instagram_posts where shortcode=%s)""",
            (post.shortcode,),
        )
        assert cur.fetchone()[0] == 2


def test_catalog_owner_native_id_mismatch_fails_before_start(database):
    store, _ = database
    manifest, run = selected(database)
    store.control(
        run["run_id"],
        manifest.account,
        action="cancel",
        expected_generation=run["generation"],
        expected_placement="local",
        idempotency_key=uuid4().hex,
    )
    wrong = replace(manifest, posts=(replace(manifest.posts[0], native_id="999_456"),))
    with pytest.raises(Conflict, match="catalog setup"):
        start_run(store, wrong, workflow_version="fixed-tests", idempotency_key=uuid4().hex)


def test_cli_default_is_source_free_plan(tmp_path, capsys):
    code = "fixed"
    manifest = FixedManifest(
        "fixture-revision",
        "bravotv",
        (FixedPost(code, "123_456", str(uuid4()), f"https://www.instagram.com/p/{code}/", "bravotv"),),
    )
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest.payload()))
    assert (
        main(
            [
                "start",
                "--manifest",
                str(path),
                "--state",
                str(tmp_path / "state.json"),
                "--budget-path",
                str(tmp_path / "budget.sqlite"),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "plan"
    assert not (tmp_path / "state.json").exists()
    assert not (tmp_path / "budget.sqlite").exists()


def test_prior_request_count_does_not_pause_before_wire(database, tmp_path):
    store, _ = database
    manifest, run = selected(database)
    budget = AccountBudget(tmp_path / "budget.sqlite", manifest.account)
    for _ in range(4):
        with budget.stream():
            for _ in range(50):
                budget.consume("probe")
    sends = []

    def factory(workflow_store):
        def details(*_):
            def fetch(**callbacks):
                callbacks["on_request"]()
                sends.append("wire")
                raise RuntimeError("synthetic post-send interruption")

            return fetch

        return Worker(
            workflow_store,
            listing_source=lambda *_: None,
            detail_source=details,
            comment_source=lambda *_: None,
            media_loader=lambda *_: None,
            media_storage=lambda *_: None,
        )

    result = run_tranche(store, manifest, run["run_id"], budget=budget, worker_factory=factory)
    assert sends
    assert all(row["outcome"] == "unresolved" for row in result["state"]["targets"])
    assert result["budget"]["rolling_24h_requests"] > 200


def test_production_cooldown_survives_tranche_and_cancellation(database, tmp_path):
    from datetime import UTC, datetime, timedelta

    store, tx = database
    manifest, run = selected(database)
    cooldown = datetime.now(UTC) + timedelta(hours=4)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set next_attempt_at=%s
            where run_id=%s and stage='post_details'""",
            (cooldown, run["run_id"]),
        )

    def factory(workflow_store):
        def denied(*_):
            pytest.fail("Provider cooldown constructed a source")

        return Worker(
            workflow_store,
            listing_source=denied,
            detail_source=denied,
            comment_source=denied,
            media_loader=denied,
            media_storage=denied,
        )

    result = run_tranche(
        store,
        manifest,
        run["run_id"],
        budget=AccountBudget(tmp_path / "budget.sqlite", manifest.account),
        worker_factory=factory,
    )
    assert result["reason"] == "cooldown_or_wait"
    assert result["budget"]["rolling_24h_requests"] == 0
    detail = next(row for row in result["state"]["targets"] if row["stage"] == "post_details")
    assert detail["next_attempt_at"] == cooldown


@pytest.mark.parametrize(
    "changed",
    [{"pk": "999"}, {"code": "wrong"}, {"user": {"username": "wrong"}}, {"user": {"username": "bravotv", "pk": "999"}}],
)
def test_fresh_detail_identity_is_checked_before_observation(tmp_path, changed):
    from trr_backend.ingestion.fixed_validation import BoundedSource

    post = FixedPost("fixed", "123_456", str(uuid4()), "https://www.instagram.com/p/fixed/", "bravotv")
    observed = []
    payload = {"pk": "123", "code": "fixed", "user": {"username": "bravotv", "pk": "456"}, **changed}

    def fetch(**callbacks):
        callbacks["on_request"]()
        callbacks["source_observer"]("details", payload)
        return payload

    budget = AccountBudget(tmp_path / "budget.sqlite", "bravotv")
    with budget.stream(), pytest.raises(Conflict, match="Fresh native detail identity"):
        BoundedSource(fetch, budget=budget, post=post)(
            on_request=lambda: None, source_observer=lambda *args: observed.append(args)
        )
    assert observed == []


def test_lost_control_response_reuses_persisted_command(database, tmp_path):
    from types import SimpleNamespace

    from scripts.ingestion.validate_fixed_posts import _control

    store, _ = database
    manifest, run = selected(database)
    state = {"run_id": run["run_id"], "generation": run["generation"]}
    path = tmp_path / "state.json"
    lost = [True]

    def control(*args, **kwargs):
        result = store.control(*args, **kwargs)
        if lost.pop() if lost else False:
            raise RuntimeError("response lost after durable control")
        return result

    with pytest.raises(RuntimeError, match="response lost"):
        _control(SimpleNamespace(control=control), manifest, state, "cancel", path)
    persisted = json.loads(path.read_text())
    assert persisted["pending_control"]["generation"] == 1
    result = _control(SimpleNamespace(control=control), manifest, persisted, "cancel", path)
    assert result["generation"] == 2  # Retried command did not cancel twice.
    assert "pending_control" not in json.loads(path.read_text())


@pytest.mark.parametrize("obstacle", ["queued", "running", "surviving_lease", "uncertain_owner", "sending_dispatch"])
def test_other_operator_blocks_inspection_start_and_execution(database, tmp_path, obstacle):
    from datetime import UTC, datetime, timedelta

    store, tx = database
    manifest, own = selected(database)
    other = store.start(
        manifest.intent(),
        idempotency_key=uuid4().hex,
        engine="dbos",
        workflow_version="fixed-tests",
        initiated_by="other-operator",
    )
    _CREATED_RUNS.append((store, other["run_id"], manifest.account))
    token = str(uuid4())
    expiry = None if obstacle == "uncertain_owner" else datetime.now(UTC) + timedelta(hours=1)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set execution_placement='local',state=%s where run_id=%s",
            (obstacle if obstacle in {"queued", "running"} else "cancelled", other["run_id"]),
        )
        if obstacle not in {"queued", "running"}:
            cur.execute(
                """insert into social.instagram_ingestion_targets
                (run_id,stage,target_key,max_attempts,owner_token,owner_expires_at)
                values(%s,'comments','other-operator',12,%s,%s)""",
                (other["run_id"], token, expiry),
            )
        if obstacle == "sending_dispatch":
            cur.execute(
                "update social.instagram_ingestion_targets set owner_token=null,owner_expires_at=null where run_id=%s",
                (other["run_id"],),
            )
            cur.execute(
                """insert into social.instagram_ingestion_dispatches
                (owner_token,run_id,generation,stage,target_key,state)
                values(%s,%s,1,'comments','other-operator','sending')""",
                (token, other["run_id"]),
            )
    with pytest.raises(Conflict, match="Another Instagram"):
        inspect_run(store, manifest, own["run_id"])
    with pytest.raises(Conflict, match="Another Instagram"):
        start_run(store, manifest, workflow_version="fixed-tests", idempotency_key=uuid4().hex)
    with pytest.raises(Conflict, match="Another Instagram"):
        run_tranche(
            store,
            manifest,
            own["run_id"],
            budget=AccountBudget(tmp_path / "budget.sqlite", manifest.account),
            worker_factory=lambda *_: pytest.fail("Competing operator reached worker construction"),
        )
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select state,generation from social.instagram_ingestion_runs where run_id=%s", (other["run_id"],))
        assert cur.fetchone() == (obstacle if obstacle in {"queued", "running"} else "cancelled", 1)
        # Only test-owned injected obstacles are drained by this test teardown.
        cur.execute(
            "update social.instagram_ingestion_targets set owner_token=null,owner_expires_at=null where run_id=%s",
            (other["run_id"],),
        )
        cur.execute(
            "update social.instagram_ingestion_dispatches set state='expired' where run_id=%s", (other["run_id"],)
        )


def test_competitor_appearing_before_wire_fences_only_own_run(database, tmp_path):
    store, tx = database
    manifest, own = selected(database)
    competitors, sends = [], []

    def factory(workflow_store):
        def details(*_):
            def fetch(**callbacks):
                def initialize(conn, run_id):
                    with conn.cursor() as cur:
                        cur.execute(
                            "update social.instagram_ingestion_runs set execution_placement='local' where run_id=%s",
                            (run_id,),
                        )

                other = store.start(
                    manifest.intent(),
                    idempotency_key=uuid4().hex,
                    engine="dbos",
                    workflow_version="fixed-tests",
                    initiated_by="other-operator",
                    initialize=initialize,
                )
                competitors.append(other["run_id"])
                _CREATED_RUNS.append((store, other["run_id"], manifest.account))
                callbacks["on_request"]()
                sends.append("wire")
                pytest.fail("Competing operator reached source send")

            return fetch

        return Worker(
            workflow_store,
            listing_source=lambda *_: None,
            detail_source=details,
            comment_source=lambda *_: None,
            media_loader=lambda *_: None,
            media_storage=lambda *_: None,
        )

    budget = AccountBudget(tmp_path / "budget.sqlite", manifest.account)
    with pytest.raises(Conflict, match="Another Instagram"):
        run_tranche(store, manifest, own["run_id"], budget=budget, worker_factory=factory)
    assert sends == [] and budget.inspect()["rolling_24h_requests"] == 0
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select state from social.instagram_ingestion_runs where run_id=%s", (own["run_id"],))
        assert cur.fetchone()[0] == "cancelled"
        cur.execute("select state,generation from social.instagram_ingestion_runs where run_id=%s", (competitors[0],))
        assert cur.fetchone() == ("queued", 1)


def test_source_unavailable_workflow_completion_is_not_validation_acceptance(database, tmp_path, monkeypatch, capsys):
    from trr_backend.db import connection
    from trr_backend.ingestion import validation_gate
    from trr_backend.ingestion.comments import CommentFetchSummary

    store, _ = database
    manifest, run = selected(database)
    post = manifest.posts[0]
    budget = AccountBudget(tmp_path / "budget.sqlite", manifest.account)

    def factory(workflow_store):
        def details(*_):
            def fetch(**callbacks):
                callbacks["on_request"]()
                payload = {
                    "pk": "123",
                    "code": post.shortcode,
                    "user": {"username": "bravotv", "pk": "456"},
                    "media_type": 1,
                    "taken_at": 1789500000,
                    "caption": {"text": "fresh native detail"},
                    "like_count": 0,
                    "comment_count": 0,
                    "display_url": "https://cdn.example/image.jpg",
                }
                callbacks["source_observer"]("details", payload)
                return payload

            return fetch

        def comments(*_):
            def fetch(**callbacks):
                callbacks["pre_source_request"]()
                callbacks["source_observer"]("comments_api", {"comments_disabled": True})
                return CommentFetchSummary(expected_count=0, full_observation=True)

            return fetch

        return Worker(
            workflow_store,
            listing_source=lambda *_: pytest.fail("Listing was operator setup"),
            detail_source=details,
            comment_source=comments,
            media_loader=lambda *_: None,
            media_storage=lambda *_: None,
        )

    result = run_tranche(store, manifest, run["run_id"], budget=budget, worker_factory=factory)
    assert result["reason"] == "completed"
    assert result["state"]["run"]["state"] == "completed"
    assert result["validation"]["passed"] is False
    comment = next(row for row in result["validation"]["targets"] if row["stage"] == "comments")
    assert comment["outcome"] == "source_unavailable" and comment["verified"] is False
    detail = next(row for row in result["validation"]["targets"] if row["stage"] == "post_details")
    assert detail["verified"] is True

    manifest_path, state_path = tmp_path / "manifest.json", tmp_path / "state.json"
    manifest_path.write_text(json.dumps(manifest.payload()))
    state_path.write_text(
        json.dumps(
            {
                "run_id": run["run_id"],
                "generation": result["state"]["run"]["generation"],
                "manifest_hash": manifest.digest,
                "intent_hash": manifest.intent().digest,
            }
        )
    )
    monkeypatch.setattr(
        connection,
        "resolve_database_url_candidate_details",
        lambda: ({"url": "postgresql://fixture@db.vwxfvzutyufrkhfgoeaa.supabase.co/postgres"},),
    )
    monkeypatch.setattr(validation_gate, "ValidationWorkflowStore", lambda **_: store)
    exit_code = main(
        ["inspect", "--manifest", str(manifest_path), "--state", str(state_path), "--budget-path", str(budget.path)]
    )
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert output["status"] == "retrieval_not_passed" and output["workflow_state"] == "completed"
    assert output["validation"]["scope"] == "frozen_target_retrieval_only"
    assert output["validation"]["capability_matrix_acceptance"] == "not_evaluated"
    assert output["validation"]["passed"] is False
