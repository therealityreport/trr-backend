"""Temporary PostgreSQL only: run with --confcutdir=tests/ingestion."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from test_persistence import database as database_fixture

from api.schemas.v2.instagram_ingestion import (
    InstagramIngestionLaunchV2,
    InstagramReplayV2,
    InstagramRetryTargetsV2,
)
from trr_backend.ingestion.command_service import CommandService, ServiceUnavailableError
from trr_backend.ingestion.domain import Conflict, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.queries import Queries

database = database_fixture
NOW = datetime(2026, 10, 16, tzinfo=UTC)


def service(database, **kwargs):
    store, transaction = database
    options = {
        "engine": "dbos",
        "workflow_version": "test-v1",
        "enabled": True,
        "capacity": lambda account, payload: None,
        "clock": lambda: NOW,
    }
    options.update(kwargs)
    return CommandService(store, Queries(transaction), **options)


def launch(subject, payload=None, *, key=None, handle="@BravoTV"):
    return subject.launch(
        handle,
        InstagramIngestionLaunchV2.model_validate(payload or {}),
        idempotency_key=key or str(uuid4()),
        initiated_by="verified-admin",
    )


def persisted(database, run_id):
    _, transaction = database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("select intent from social.instagram_ingestion_runs where run_id=%s", (str(run_id),))
        return cur.fetchone()[0]


def test_capabilities_do_not_advertise_held_bravo_resume_tail(database):
    subject = service(database)
    assert "resume_tail" not in subject.capabilities("bravotv").supported_actions
    assert "resume_tail" in subject.capabilities("other_public_account").supported_actions


@pytest.mark.parametrize(
    "payload,scope,start,end,tasks",
    [
        ({"date_start": "2026-01-01T00:00:00Z"}, "full_history", None, None, ["post_details", "comments", "media"]),
        (
            {
                "backfill_scope": "bounded_window",
                "date_start": "2026-10-01T00:00:00Z",
                "date_end": "2026-10-02T00:00:00Z",
            },
            "bounded_window",
            "2026-10-01",
            "2026-10-02",
            ["post_details", "comments", "media"],
        ),
        (
            {"action": "sync_recent", "lookback_days": 7},
            "recent_window",
            "2026-10-09",
            "2026-10-16",
            ["post_details", "comments", "media"],
        ),
        ({"action": "sync_newer"}, "head_gap", "2026-10-14", "2026-10-16", ["post_details", "comments", "media"]),
        (
            {"action": "resume_tail", "selected_tasks": ["media"], "date_start": "2026-01-01T00:00:00Z"},
            "full_history",
            None,
            None,
            ["post_details", "comments", "media"],
        ),
    ],
)
def test_launch_freezes_action_semantics_and_initial_listing(database, payload, scope, start, end, tasks):
    subject = service(database, frontier=lambda account, source_scope: NOW - timedelta(days=2))
    result = launch(subject, payload)
    intent = persisted(database, result.run_id)
    assert intent["account"] == "bravotv" and intent["scope"] == scope
    assert intent["selected_tasks"] == tasks
    assert (intent["date_start"][:10] if intent["date_start"] else None) == start
    assert (intent["date_end"][:10] if intent["date_end"] else None) == end
    _, transaction = database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "select stage,target_key,max_attempts,checkpoint from social.instagram_ingestion_targets where run_id=%s",
            (str(result.run_id),),
        )
        assert cur.fetchall() == [("listing", "page:root", 12, {"cursor": None})]
        cur.execute(
            "select stage,discovery_complete from social.instagram_ingestion_manifests where run_id=%s",
            (str(result.run_id),),
        )
        manifests = cur.fetchall()
        assert len(manifests) == 4 and not any(complete for _, complete in manifests)


def test_time_derived_launch_is_idempotent_when_clock_and_frontier_move(database):
    subject = service(database, frontier=lambda *_: NOW - timedelta(days=2))
    key = str(uuid4())
    first = launch(subject, {"action": "sync_recent", "lookback_days": 3}, key=key)
    subject.clock = lambda: NOW + timedelta(days=1)
    second = launch(subject, {"action": "sync_recent", "lookback_days": 3}, key=key)
    assert first == second
    assert persisted(database, first.run_id)["date_end"].startswith("2026-10-16")
    with pytest.raises(Conflict):
        launch(subject, {"action": "sync_recent", "lookback_days": 4}, key=key)
    key = str(uuid4())
    first = launch(subject, {"action": "sync_newer"}, key=key)
    subject.frontier = lambda *_: NOW - timedelta(days=1)
    assert first == launch(subject, {"action": "sync_newer"}, key=key)


@pytest.mark.parametrize("listing,comments,expected_listing,expected_comments", [(0, 0, 1, 1), (50, 30, 50, 12)])
def test_budgets_and_capacity_validated_launch_options_are_frozen(
    database,
    listing,
    comments,
    expected_listing,
    expected_comments,
):
    validations = []

    def validate(account, payload):
        validations.append((account, payload.model_dump()))

    subject = service(database, capacity=validate, listing_max_attempts=listing, comments_max_attempts=comments)
    result = launch(
        subject,
        {
            "detail_worker_count": 12,
            "comments_worker_count": 24,
            "comments_enable_media_followups": False,
            "enable_cap4_canary": True,
            "operator_confirmation": "confirmed",
            "force_catalog_rediscovery": True,
        },
    )
    intent = persisted(database, result.run_id)
    assert intent["listing_max_attempts"] == expected_listing
    assert intent["comments_max_attempts"] == expected_comments
    assert intent["media_max_attempts"] == 3
    assert intent["detail_worker_count"] == 12 and intent["comments_worker_count"] == 24
    assert intent["comments_enable_media_followups"] is False
    assert intent["enable_cap4_canary"] and intent["force"]
    assert validations[0][0] == "bravotv"
    assert validations[0][1]["operator_confirmation"] == "confirmed"


def test_capacity_failure_and_unsupported_apply_never_create_runs(database):
    def reject(*args):
        raise Conflict("capacity denied")

    subject = service(database, capacity=reject)
    with pytest.raises(Conflict, match="capacity denied"):
        launch(subject)
    subject = service(database)
    with pytest.raises(Conflict, match="apply_run_id"):
        launch(subject, {"apply_run_id": str(uuid4())})
    subject.capacity = None
    assert not subject.capabilities("bravotv").enabled
    with pytest.raises(ServiceUnavailableError):
        launch(subject)


def test_missing_or_invalid_frontier_does_not_choose_an_arbitrary_window(database):
    with pytest.raises(ServiceUnavailableError):
        launch(service(database), {"action": "sync_newer"})
    with pytest.raises(Conflict, match="Initial backfill"):
        launch(service(database, frontier=lambda *_: None), {"action": "sync_newer"})
    with pytest.raises(Conflict, match="precede"):
        launch(service(database, frontier=lambda *_: NOW), {"action": "sync_newer"})


def test_disabled_routing_keeps_scoped_controls_and_selected_retry(database):
    subject = service(database)
    run = launch(subject)
    subject.store.discover(
        str(run.run_id),
        "bravotv",
        generation=run.generation,
        stage=Stage.COMMENTS,
        target_keys=["post1"],
        max_attempts=12,
        cursor_state={},
        page_id="comments:1",
        expected_cursor_state={},
        seal=True,
    )
    subject.enabled = False
    assert subject.progress("bravotv", run.run_id).run_id == run.run_id
    assert subject.recent("bravotv", source_scope="bravo", limit=100).runs
    cancelled = subject.control(
        "bravotv", run.run_id, action="cancel", idempotency_key=str(uuid4()), initiated_by="admin"
    )
    assert cancelled.state == "cancelled"
    with pytest.raises(LookupError):
        subject.control("unrelated", run.run_id, action="cancel", idempotency_key=str(uuid4()), initiated_by="admin")
    retried = subject.retry(
        "bravotv",
        run.run_id,
        InstagramRetryTargetsV2(stage="comments", target_keys=["post1"]),
        idempotency_key=str(uuid4()),
        initiated_by="admin",
    )
    assert retried.state == "queued" and retried.generation == cancelled.generation + 1
    assert not subject.capabilities("bravotv").enabled


def test_resume_requires_compatible_pinned_worker_but_cancel_remains_available(database):
    subject = service(database)
    run = launch(subject)
    subject.workflow_version = "new-version"
    subject.control("bravotv", run.run_id, action="cancel", idempotency_key=str(uuid4()), initiated_by="admin")
    with pytest.raises(Conflict, match="compatible worker"):
        subject.control("bravotv", run.run_id, action="resume", idempotency_key=str(uuid4()), initiated_by="admin")
    subject.worker_compatible = lambda row: row["workflow_version"] == "test-v1"
    assert (
        subject.control(
            "bravotv", run.run_id, action="resume", idempotency_key=str(uuid4()), initiated_by="admin"
        ).state
        == "queued"
    )


def test_diagnostics_use_read_only_transaction_and_bound_metadata_without_payloads(database):
    subject = service(database)
    run = launch(subject)
    store, transaction = database
    args = {
        "run_id": str(run.run_id),
        "account": "bravotv",
        "generation": 1,
        "stage": Stage.LISTING,
        "target_key": "page:root",
    }
    reserved = store.reserve(**args)
    args["owner_token"] = str(reserved["owner_token"])
    store.acquire(**args)
    for index in range(3):
        evidence = Evidence.capture(
            source_identity=f"instagram:listing:{index}",
            captured_at=NOW,
            parser_version="v1",
            payload={"caption": "source body must not appear"},
        )
        store.retain_evidence(**args, evidence=evidence)

    store.discover(
        str(run.run_id),
        "bravotv",
        generation=1,
        stage=Stage.DETAILS,
        target_keys=["post1", "post2"],
        max_attempts=3,
        cursor_state={},
        page_id="details:1",
        expected_cursor_state={},
        seal=True,
    )

    @contextmanager
    def read_only():
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("set transaction read only")
            yield conn

    subject.queries = Queries(read_only)
    diagnostic = subject.diagnostics("bravotv", run.run_id, limit=1)
    assert len(diagnostic.targets) == len(diagnostic.evidence) == 1
    assert diagnostic.evidence_truncated and diagnostic.targets_truncated
    listing = next(
        target for target in subject.diagnostics("bravotv", run.run_id, limit=100).targets if target.stage == "listing"
    )
    assert listing.attempts == 1
    assert diagnostic.evidence[0].pinned
    assert "source body" not in diagnostic.model_dump_json()
    assert "owner_token" not in diagnostic.model_dump_json()
    assert "checkpoint" not in diagnostic.model_dump_json()
    with pytest.raises(LookupError):
        subject.diagnostics("unrelated", run.run_id, limit=1)
    with pytest.raises(ValueError):
        subject.diagnostics("bravotv", run.run_id, limit=101)


def test_replay_requires_durable_adapter_and_validates_requested_run_identity(database):
    subject = service(database)
    run = launch(subject)
    payload = InstagramReplayV2(evidence_id=uuid4(), parser_version="v1")
    with pytest.raises(ServiceUnavailableError):
        subject.replay("bravotv", run.run_id, payload, idempotency_key="preview", initiated_by="admin")
    calls = []

    def replay(account, run_id, request, **kwargs):
        calls.append((account, run_id, request, kwargs))
        return {
            "mode": "dry_run",
            "evidence_id": request.evidence_id,
            "run_id": run_id,
            "stage": "listing",
            "target_key": "page:root",
            "captured_at": NOW,
            "content_hash": "hash",
            "capture_parser_version": "v1",
            "parser_version": "v1",
            "schema_version": 1,
            "normalized": {},
            "comparison": {"previous": None, "current": {}, "changed": True},
            "writes": 0,
            "source_requests": 0,
        }

    subject.replay_command = replay
    assert subject.replay("bravotv", run.run_id, payload, idempotency_key="preview", initiated_by="admin").writes == 0
    assert calls[0][3] == {"idempotency_key": "preview", "initiated_by": "admin"}
    with pytest.raises(LookupError):
        subject.replay("unrelated", run.run_id, payload, idempotency_key="preview", initiated_by="admin")
    with pytest.raises(LookupError):
        subject.replay("bravotv", uuid4(), payload, idempotency_key="preview", initiated_by="admin")
    assert len(calls) == 1
    original = subject.replay_command
    subject.replay_command = lambda *a, **kw: dict(original(*a, **kw), run_id=str(uuid4()))
    with pytest.raises(Conflict, match="requested run"):
        subject.replay("bravotv", run.run_id, payload, idempotency_key="preview", initiated_by="admin")

    subject.replay_command = lambda *a, **kw: dict(original(*a, **kw), mode="apply", receipt_id=str(uuid4()))
    with pytest.raises(Conflict, match="requested mode"):
        subject.replay("bravotv", run.run_id, payload, idempotency_key="preview", initiated_by="admin")
    subject.replay_command = lambda *a, **kw: dict(original(*a, **kw), parser_version="unexpected-parser")
    with pytest.raises(Conflict, match="parser"):
        subject.replay("bravotv", run.run_id, payload, idempotency_key="preview", initiated_by="admin")


@pytest.mark.parametrize("changed", ["missing_frontier", "denied_capacity", "disabled_routing"])
def test_accepted_launch_replay_precedes_mutable_prerequisites(database, changed):
    subject = service(database, frontier=lambda *_: NOW - timedelta(days=2))
    key = str(uuid4())
    first = launch(subject, {"action": "sync_newer"}, key=key)
    if changed == "missing_frontier":
        subject.frontier = lambda *_: None
    elif changed == "denied_capacity":

        def denied(*args):
            raise Conflict("capacity now denied")

        subject.capacity = denied
    else:
        subject.enabled = False
        subject.capacity = None
    assert launch(subject, {"action": "sync_newer"}, key=key) == first
    # A new key still sees the current launch prerequisites.
    with pytest.raises((Conflict, ServiceUnavailableError)):
        launch(subject, {"action": "sync_newer"})


def test_changed_request_conflicts_before_denied_capacity_is_called(database):
    subject = service(database)
    key = str(uuid4())
    launch(subject, {"action": "sync_recent", "lookback_days": 1}, key=key)
    subject.capacity = lambda *_: pytest.fail("Mutable capacity must not precede durable request identity")
    with pytest.raises(Conflict, match="different command"):
        launch(subject, {"action": "sync_recent", "lookback_days": 2}, key=key)


def test_concurrent_service_launch_keeps_final_locked_acceptance(database):
    subject = service(database)
    key = str(uuid4())
    with ThreadPoolExecutor(max_workers=4) as pool:
        commands = list(pool.map(lambda _: launch(subject, key=key), range(8)))
    assert all(command == commands[0] for command in commands)
    _, transaction = database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from social.instagram_ingestion_outbox where run_id=%s", (str(commands[0].run_id),)
        )
        assert cur.fetchone() == (1,)
        cur.execute(
            "select count(*) from social.instagram_ingestion_targets where run_id=%s", (str(commands[0].run_id),)
        )
        assert cur.fetchone() == (1,)
