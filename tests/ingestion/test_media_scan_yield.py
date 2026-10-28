"""Media continuation yields to comments using real isolated SQL and dispatch."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event
from typing import Any
from uuid import uuid4

import pytest

from scripts.ingestion import local_worker
from tests.ingestion import test_comments as comments
from tests.ingestion import test_media_snapshot as snapshots
from tests.ingestion import test_pipeline as pipeline
from trr_backend.ingestion.domain import Stage
from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader
from trr_backend.ingestion.workflows import Scheduler

base_database = comments.database
pipeline_database = pipeline.pipeline_database
NOW = datetime.now(UTC)


def query(tx, sql, args=()):
    return comments.query(tx, sql, args)


def fixture(database, *, pending_comment=True) -> Any:
    """Eight strict inventory failures before the ninth ready media target."""
    owned, tx, store, item, ordered, _ = snapshots._seed_media_readiness_page(database)
    key = "Available" + uuid4().hex
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_manifests set discovery_complete=true,sealed_at=clock_timestamp() "
            "where run_id=%s",
            (owned["run_id"],),
        )
        if pending_comment:
            cur.execute(
                "insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts) "
                "values(%s,'comments',%s,12)",
                (owned["run_id"], key),
            )
    return store, tx, store.seed(item), key, ordered


def preferred(part) -> dict[str, Any]:
    return dict(part, prefer_media=True)


def cursor(tx, part):
    return query(
        tx,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
        (part["run_id"],),
    )[0]["cursor_state"]


def restart_scan(tx, part):
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_manifests set cursor_state=jsonb_set(cursor_state,"
            "'{media_readiness_after}','\"\"'::jsonb) where run_id=%s and stage='media'",
            (part["run_id"],),
        )


def test_ready_comment_is_journaled_after_one_scan_then_ready_media_wins(pipeline_database, monkeypatch):
    store, tx, part, key, ordered = fixture(pipeline_database)
    checks = []
    original = MediaSnapshotLoader._completion

    def prove(*args, **kwargs):
        checks.append(args[3])
        return original(*args, **kwargs)

    monkeypatch.setattr(MediaSnapshotLoader, "_completion", staticmethod(prove))
    item = preferred(part)
    expected = {"kind": "target", "stage": "comments", "target_key": key, "media_scan_pending": True}
    assert store.decision(item, 0, NOW) == expected
    assert checks == [entry[0]["target_key"] for entry in ordered[:8]]
    state = cursor(tx, part)
    assert state["media_readiness_after"] == ordered[7][0]["target_key"]
    assert state["preserved"] == "value" and "media_readiness_cycle" not in state
    assert store.decision(item, 0, NOW) == expected and len(checks) == 8
    assert (
        query(
            tx,
            "select decision from social.instagram_ingestion_decisions where execution_id=%s and ordinal=0",
            (part["execution_id"],),
        )[0]["decision"]
        == expected
    )
    assert store.decision(item, 1, NOW) == {
        "kind": "target",
        "stage": "media",
        "target_key": ordered[-1][0]["target_key"],
    }
    assert checks[-1] == ordered[-1][0]["target_key"] and len(checks) == 9
    assert cursor(tx, part)["media_readiness_after"] == ordered[-1][0]["target_key"]


def test_scan_wait_does_not_promote_unproved_generic_media_target(pipeline_database):
    store, _, part, _, _ = fixture(pipeline_database, pending_comment=False)
    assert store.decision(preferred(part), 0, NOW) == {"kind": "wait", "seconds": 0.01}


def test_exhausted_scan_keeps_ordinary_comment_result_unmarked(pipeline_database):
    store, tx, part, key, ordered = fixture(pipeline_database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_comments set is_missing=true where post_id=%s", (ordered[-1][1]["post_id"],)
        )
    assert store.decision(preferred(part), 0, NOW)["media_scan_pending"] is True
    assert store.decision(preferred(part), 1, NOW) == {"kind": "target", "stage": "comments", "target_key": key}


def test_actual_scheduler_preserves_marker_and_bound_token_on_submission_and_replay(pipeline_database):
    store, tx, part, key, _ = fixture(pipeline_database)
    admissions, submitted = [], []

    def dispatch(reservation):
        submitted.append(reservation["owner_token"])
        args = {k: reservation[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")}
        store.acquire(**args, stage=Stage.COMMENTS)
        return "fixture-child"

    scheduler = Scheduler(
        store,
        admit=lambda item: admissions.append(item) or True,
        dispatch=dispatch,
        reconcile=lambda _: pytest.fail("reserved target must not reconcile"),
        clock=lambda: NOW,
    )
    assert scheduler.tick(preferred(part), 0) == {"ready": True, "media_scan_pending": True}
    recorded = store.decision(preferred(part), 0, NOW)
    assert recorded["media_scan_pending"] is True and recorded["owner_token"] == submitted[0]
    assert recorded["target_key"] == key and len(admissions) == 1
    assert scheduler.tick(preferred(part), 0) == {"sleep": 1.0}
    assert len(admissions) == len(submitted) == 1
    row = query(
        tx,
        "select attempts,owner_token from social.instagram_ingestion_targets "
        "where run_id=%s and stage='comments' and target_key=%s",
        (part["run_id"], key),
    )[0]
    assert row["attempts"] == 1 and str(row["owner_token"]) == submitted[0]


@pytest.mark.parametrize("remote", ["unknown", "accepted", "absent"])
def test_comment_reconcile_preserves_exact_token_and_only_marks_actual_dispatch(pipeline_database, remote):
    store, tx, part, key, _ = fixture(pipeline_database)
    first = store.decision(preferred(part), 0, NOW)
    reservation = store.reserve_decision(part, first, ordinal=0)
    assert reservation is not None
    assert store.transition(part, reservation["owner_token"], "sending")
    restart_scan(tx, part)
    second = store.decision(preferred(part), 1, NOW)
    assert second == {
        "kind": "reconcile",
        "stage": "comments",
        "target_key": key,
        "owner_token": reservation["owner_token"],
        "media_scan_pending": True,
    }
    submitted = []

    def dispatch(row):
        submitted.append(row["owner_token"])
        args = {k: row[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")}
        store.acquire(**args, stage=Stage.COMMENTS)

    scheduler = Scheduler(
        store,
        admit=lambda _: pytest.fail("existing token must not be admitted twice"),
        dispatch=dispatch,
        reconcile=lambda _: remote,
        clock=lambda: NOW,
    )
    expected = {"ready": True, "media_scan_pending": True} if remote == "absent" else {"sleep": 1.0}
    assert scheduler.tick(preferred(part), 1) == expected
    assert submitted == ([reservation["owner_token"]] if remote == "absent" else [])
    assert query(
        tx,
        "select attempts from social.instagram_ingestion_targets "
        "where run_id=%s and stage='comments' and target_key=%s",
        (part["run_id"], key),
    )[0]["attempts"] == (1 if remote == "absent" else 0)


def single_ordinal(monkeypatch):
    # A test-only one-ordinal partition exercises actual continuation receipts;
    # production bounds remain50 and are unchanged by this implementation.
    monkeypatch.setattr(local_worker, "PARTITION_SIZE", 1)
    monkeypatch.setattr("trr_backend.ingestion.workflow_store.PARTITION_SIZE", 1)


def next_partition(tx, part):
    return query(
        tx,
        "select p.*,r.account from social.instagram_ingestion_partitions p "
        "join social.instagram_ingestion_runs r using(run_id) where p.execution_id=("
        "select next_execution_id from social.instagram_ingestion_partitions where execution_id=%s)",
        (part["execution_id"],),
    )[0]


def test_real_partial_partition_keeps_scan_due_then_runs_ready_media_beside_comment(pipeline_database, monkeypatch):
    single_ordinal(monkeypatch)
    store, tx, part, key, ordered = fixture(pipeline_database)
    abort, entered, release = Event(), Event(), Event()
    occupancy, admissions, requests = [], [], []

    def execute(reservation, **kwargs) -> dict[str, Any]:
        args = {k: reservation[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")}
        args["stage"] = Stage(reservation["stage"])
        store.acquire(**args)
        requests.append(reservation["stage"])
        occupancy.append(len(dispatch.active) + int(dispatch.media_active))
        if reservation["stage"] == "comments":
            entered.set()
            assert release.wait(10)
        else:
            assert len(dispatch.active) == 1 and dispatch.media_active
        store.commit_target(**args, observed_at=NOW, write_canonical=lambda _: {"id": reservation["target_key"]})
        return {"outcome": "committed"}

    dispatch = local_worker.LocalDispatch(execute, abort)
    dispatch.bind(part, 6)
    fairness = dispatch.media_fairness[(part["run_id"], part["generation"])]
    fairness["due"] = True
    scheduler = Scheduler(
        store,
        admit=lambda item: admissions.append(item["stage"]) or True,
        dispatch=dispatch,
        reconcile=lambda _: "unknown",
        clock=lambda: NOW,
    )
    try:
        assert (
            local_worker.run_partition(
                part,
                store=store,
                scheduler=scheduler,
                dispatch=dispatch,
                abort=abort,
                effective_comments_limit=lambda _: 6,
            )
            == "continued"
        )
        assert entered.wait(3) and dispatch.comments_submitted == 1
        assert fairness["due"] is True and len(dispatch.active) == 1
        first = query(
            tx,
            "select decision from social.instagram_ingestion_decisions where execution_id=%s",
            (part["execution_id"],),
        )[0]["decision"]
        assert first["target_key"] == key and first["media_scan_pending"] is True and first["owner_token"]
        following = next_partition(tx, part)
        assert (
            local_worker.run_partition(
                following,
                store=store,
                scheduler=scheduler,
                dispatch=dispatch,
                abort=abort,
                effective_comments_limit=lambda _: 6,
            )
            == "continued"
        )
        assert fairness["due"] is False and len(dispatch.active) == 1
        assert admissions == ["comments", "media"] and requests == ["comments", "media"]
        assert max(occupancy) <= 6 and occupancy[-1] == 2
        media = query(
            tx,
            "select outcome from social.instagram_ingestion_targets "
            "where run_id=%s and stage='media' and target_key=%s",
            (part["run_id"], ordered[-1][0]["target_key"]),
        )[0]
        assert media["outcome"] == "committed"
    finally:
        release.set()
        dispatch.close(store)


@pytest.mark.parametrize("denial", ["false", "error"])
def test_pending_scan_comment_admission_denial_retains_due_without_reserving(pipeline_database, monkeypatch, denial):
    single_ordinal(monkeypatch)
    store, tx, part, key, ordered = fixture(pipeline_database)
    abort = Event()
    dispatch = local_worker.LocalDispatch(lambda *a, **k: pytest.fail("denied work executed"), abort)
    dispatch.bind(part, 6)
    dispatch.media_fairness[(part["run_id"], part["generation"])]["due"] = True
    waits, admitted = [], []
    dispatch.wait = waits.append  # pyright: ignore[reportAttributeAccessIssue]

    def admit(item):
        admitted.append(item["stage"])
        if denial == "error":
            raise RuntimeError("isolated admission failure")
        return False

    scheduler = Scheduler(store, admit=admit, dispatch=dispatch, reconcile=lambda _: "unknown", clock=lambda: NOW)
    try:
        assert (
            local_worker.run_partition(
                part,
                store=store,
                scheduler=scheduler,
                dispatch=dispatch,
                abort=abort,
                effective_comments_limit=lambda _: 6,
            )
            == "continued"
        )
        assert waits == ([1.0] if denial == "error" else [5.0]) and admitted == ["comments"]
        assert dispatch.media_fairness[(part["run_id"], part["generation"])]["due"] is True
        assert not dispatch.active and not dispatch.submitted
        row = query(
            tx,
            "select attempts,owner_token from social.instagram_ingestion_targets "
            "where run_id=%s and stage='comments' and target_key=%s",
            (part["run_id"], key),
        )[0]
        assert row["attempts"] == 0 and row["owner_token"] is None
        recorded = query(
            tx,
            "select decision from social.instagram_ingestion_decisions where execution_id=%s and ordinal=0",
            (part["execution_id"],),
        )[0]["decision"]
        assert recorded["media_scan_pending"] is True
        if denial == "error":
            assert recorded["control_failure"] == "ADMISSION_UNAVAILABLE"
        following = next_partition(tx, part)
        assert store.decision(preferred(following), 0, NOW) == {
            "kind": "target",
            "stage": "media",
            "target_key": ordered[-1][0]["target_key"],
        }
    finally:
        dispatch.close(store)


def test_completed_scan_comment_submission_consumes_due_turn(pipeline_database, monkeypatch):
    single_ordinal(monkeypatch)
    store, tx, part, _, ordered = fixture(pipeline_database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_comments set is_missing=true where post_id=%s", (ordered[-1][1]["post_id"],)
        )
        cur.execute(
            "update social.instagram_ingestion_manifests set cursor_state=jsonb_set(cursor_state,"
            "'{media_readiness_after}',to_jsonb(%s::text)) where run_id=%s and stage='media'",
            (ordered[7][0]["target_key"], part["run_id"]),
        )
    abort, release = Event(), Event()

    def execute(*args, **kwargs) -> dict[str, Any]:
        assert release.wait(10)
        return {"outcome": "committed"}

    dispatch = local_worker.LocalDispatch(execute, abort)
    dispatch.bind(part, 6)
    fairness = dispatch.media_fairness[(part["run_id"], part["generation"])]
    fairness["due"] = True
    scheduler = Scheduler(
        store, admit=lambda _: True, dispatch=dispatch, reconcile=lambda _: "unknown", clock=lambda: NOW
    )
    try:
        assert (
            local_worker.run_partition(
                part,
                store=store,
                scheduler=scheduler,
                dispatch=dispatch,
                abort=abort,
                effective_comments_limit=lambda _: 6,
            )
            == "continued"
        )
        assert dispatch.comments_submitted == 1 and fairness["due"] is False
        recorded = query(
            tx,
            "select decision from social.instagram_ingestion_decisions where execution_id=%s and ordinal=0",
            (part["execution_id"],),
        )[0]["decision"]
        assert "media_scan_pending" not in recorded
    finally:
        release.set()
        dispatch.close(store)


def test_pending_scan_concurrent_ordinal_dispatches_one_owner_and_scans_once(pipeline_database, monkeypatch):
    store, tx, part, key, _ = fixture(pipeline_database)
    checks, submitted = [], []
    original = MediaSnapshotLoader._completion

    def prove(*args, **kwargs):
        checks.append(args[3])
        return original(*args, **kwargs)

    def dispatch(reservation):
        args = {k: reservation[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")}
        store.acquire(**args, stage=Stage.COMMENTS)
        submitted.append(reservation["owner_token"])

    monkeypatch.setattr(MediaSnapshotLoader, "_completion", staticmethod(prove))
    scheduler = Scheduler(
        store, admit=lambda _: True, dispatch=dispatch, reconcile=lambda _: "unknown", clock=lambda: NOW
    )
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: scheduler.tick(preferred(part), 0), range(8)))
    assert len(checks) == 8 and len(submitted) == 1
    assert sum(result == {"ready": True, "media_scan_pending": True} for result in results) == 1
    saved = query(
        tx,
        "select decision from social.instagram_ingestion_decisions where execution_id=%s and ordinal=0",
        (part["execution_id"],),
    )
    assert len(saved) == 1 and saved[0]["decision"]["media_scan_pending"] is True
    assert saved[0]["decision"]["owner_token"] == submitted[0]
    target = query(
        tx,
        "select attempts,owner_token from social.instagram_ingestion_targets "
        "where run_id=%s and stage='comments' and target_key=%s",
        (part["run_id"], key),
    )[0]
    assert target["attempts"] == 1 and str(target["owner_token"]) == submitted[0]
