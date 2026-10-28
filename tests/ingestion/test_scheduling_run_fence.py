"""Readiness/run fencing on disposable PostgreSQL only; no provider requests."""

import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Timer
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_comments as comments
from tests.ingestion import test_media_snapshot as snapshots
from tests.ingestion import test_pipeline as pipeline
from tests.ingestion import test_profile_inventory as inventory
from tests.ingestion import test_profiles as profiles
from trr_backend.ingestion.comments import CommentActivity, parse_comment_evidence
from trr_backend.ingestion.domain import Conflict, LostOwnership, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.workflow_store import WorkflowStore

base_database = comments.database
pipeline_database = pipeline.pipeline_database
profiles_db = profiles.profiles_db
NOW = datetime.now(UTC)


def query(tx, statement, args=()):
    return comments.query(tx, statement, args)


def bounded(tx):
    @contextmanager
    def transaction():
        with tx() as conn:
            with conn.cursor() as cur:
                cur.execute("set local lock_timeout='150ms'")
            yield conn

    return transaction


def ready(database, *, cls=WorkflowStore):
    """One proved media post, plus an acquired independent comment writer."""
    owned, _, _, _ = snapshots.seed(database, selected_comments=True)
    _, tx = database
    store = cls(tx)
    key = "FenceWriter" + uuid4().hex
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_manifests set discovery_complete=true,sealed_at=clock_timestamp() "
            "where run_id=%s",
            (owned["run_id"],),
        )
        cur.execute(
            "insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts) "
            "values(%s,'comments',%s,12)",
            (owned["run_id"], key),
        )
        cur.execute(
            "insert into social.instagram_posts(shortcode,username) values(%s,'fixture_account') returning id", (key,)
        )
        post_id = str(cur.fetchone()[0])
    writer = {
        "run_id": owned["run_id"],
        "account": owned["account"],
        "generation": 1,
        "stage": Stage.COMMENTS,
        "target_key": key,
    }
    reserve = store.reserve(**writer)
    assert reserve is not None
    writer["owner_token"] = str(reserve["owner_token"])
    store.acquire(**writer)
    retained = Evidence.capture(
        source_identity=f"instagram:comments:{key}:comments_api",
        captured_at=NOW,
        parser_version=comments.COMMENT_PARSER_VERSION,
        payload=comments.NATIVE["rest"],
    )
    eid = store.retain_evidence(**writer, evidence=retained)
    page = parse_comment_evidence(retained, pinned=True)
    payload = {
        "post_id": post_id,
        "evidence": retained,
        "evidence_id": eid,
        "page": page,
        "expected_checkpoint": {},
        "checkpoint": {"fence_page": key},
        "metadata_writes_enabled": True,
        "assignment": None,
    }
    item = dict(owned, engine="dbos", workflow_version="fixture", prefer_media=True)
    part = store.seed(item)
    return store, tx, dict(part, prefer_media=True), (writer, payload), owned["target_key"]  # pyright: ignore[reportArgumentType, reportCallIssue]


def hold_proof(monkeypatch):
    held, release = Event(), Event()
    original = MediaSnapshotLoader._completion

    def completion(*args, **kwargs):
        held.set()
        assert release.wait(10), "test did not release readiness proof"
        return original(*args, **kwargs)

    monkeypatch.setattr(MediaSnapshotLoader, "_completion", staticmethod(completion))
    return held, release


def shared_probe(tx, item):
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        run = Store(tx)._execution_run(cur, item["run_id"], item["account"], shared_fence=True)
        assert run["generation"] == item["generation"]
        cur.execute("select count(*) from social.instagram_ingestion_comment_pages where run_id=%s", (item["run_id"],))
        return cur.fetchone()["count"]


def assert_page(tx, page):
    owned, payload = page
    saved = query(
        tx,
        "select checkpoint,canonical_identity from social.instagram_ingestion_comment_pages "
        "where run_id=%s and target_key=%s",
        (owned["run_id"], owned["target_key"]),
    )
    assert len(saved) == 1 and saved[0]["checkpoint"] == payload["checkpoint"]
    rows = query(
        tx,
        "select c.comment_id,p.comment_id as parent from social.instagram_comments c "
        "left join social.instagram_comments p on p.id=c.parent_comment_id "
        "where c.post_id=%s order by c.comment_id",
        (payload["post_id"],),
    )
    expected = {}

    def visit(comment):
        expected[comment.comment_id] = comment.parent_comment_id
        for child in comment.replies:
            visit(child)

    for comment in payload["page"].comments:
        visit(comment)
    assert {row["comment_id"]: row["parent"] for row in rows} == expected
    assert set(saved[0]["canonical_identity"]["comment_ids"]) == set(expected)
    target = query(
        tx,
        "select checkpoint,attempts,owner_token from social.instagram_ingestion_targets "
        "where run_id=%s and stage='comments' and target_key=%s",
        (owned["run_id"], owned["target_key"]),
    )[0]
    assert target["checkpoint"] == payload["checkpoint"] and target["attempts"] == 1
    assert str(target["owner_token"]) == owned["owner_token"]


def test_actual_comment_page_and_shared_audit_fence_overlap_readiness(pipeline_database, monkeypatch):
    store, tx, item, page, media_key = ready(pipeline_database)
    held, release = hold_proof(monkeypatch)
    with ThreadPoolExecutor(max_workers=3) as pool:
        decision = pool.submit(store.decision, item, 0, NOW)
        try:
            assert held.wait(5)
            writing = pool.submit(CommentActivity(Store(bounded(tx)))._persist_page, page[0], **page[1])
            writing.result(timeout=3)
            assert pool.submit(shared_probe, bounded(tx), item).result(timeout=3) == 1
            assert not decision.done()
            assert_page(tx, page)
        finally:
            release.set()
        assert decision.result(timeout=3) == {"kind": "target", "stage": "media", "target_key": media_key}


@pytest.mark.parametrize("change", ["cancel", "generation"])
def test_exclusive_control_waits_then_fences_old_generation(pipeline_database, monkeypatch, change):
    store, tx, item, page, _ = ready(pipeline_database)
    held, release = hold_proof(monkeypatch)

    def control(transaction):
        if change == "cancel":
            return Store(transaction).control(
                item["run_id"], item["account"], action="cancel", idempotency_key=str(uuid4())
            )
        with transaction() as conn, conn.cursor() as cur:
            cur.execute(
                "update social.instagram_ingestion_runs set generation=generation+1 where run_id=%s", (item["run_id"],)
            )

    with ThreadPoolExecutor(max_workers=1) as pool:
        decision = pool.submit(store.decision, item, 0, NOW)
        try:
            assert held.wait(5)
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                control(bounded(tx))
            # Other entrypoints retain the exclusive default, not just control.
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                WorkflowStore(bounded(tx)).eligible_media(item, NOW)
        finally:
            release.set()
        chosen = decision.result(timeout=3)
    control(tx)
    before = query(
        tx,
        "select checkpoint,owner_token,attempts from social.instagram_ingestion_targets "
        "where run_id=%s order by stage,target_key",
        (item["run_id"],),
    )
    with pytest.raises(LostOwnership):
        store.decision(item, 1, NOW)
    with pytest.raises(LostOwnership):
        store.reserve_decision(item, chosen, ordinal=0)
    with pytest.raises(LostOwnership):
        CommentActivity(Store(tx))._persist_page(page[0], **page[1])
    assert before == query(
        tx,
        "select checkpoint,owner_token,attempts from social.instagram_ingestion_targets "
        "where run_id=%s order by stage,target_key",
        (item["run_id"],),
    )
    assert not query(tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (item["run_id"],))


def test_same_ordinal_proves_and_journals_once(pipeline_database, monkeypatch):
    store, tx, item, _, media_key = ready(pipeline_database)
    held, release = hold_proof(monkeypatch)
    proofs = []
    original = MediaSnapshotLoader._details

    def details(*args, **kwargs):
        proofs.append(args[2])
        return original(*args, **kwargs)

    monkeypatch.setattr(MediaSnapshotLoader, "_details", staticmethod(details))
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = [pool.submit(store.decision, item, 0, NOW) for _ in range(8)]
        try:
            assert held.wait(5)
        finally:
            release.set()
        assert [result.result(timeout=5) for result in results] == [
            {"kind": "target", "stage": "media", "target_key": media_key}
        ] * 8
    assert proofs == [media_key]
    assert (
        len(
            query(
                tx, "select * from social.instagram_ingestion_decisions where execution_id=%s", (item["execution_id"],)
            )
        )
        == 1
    )


def test_different_partitions_serialize_media_cursor_and_resume_after_eight(pipeline_database, monkeypatch):
    owned, tx, store, item, ordered, _ = snapshots._seed_media_readiness_page(pipeline_database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_manifests set discovery_complete=true,sealed_at=clock_timestamp() "
            "where run_id=%s",
            (owned["run_id"],),
        )
        cur.execute(
            "insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts,blocked_reason) "
            "values(%s,'comments','Blocked',12,'source_gap')",
            (owned["run_id"],),
        )
    part = dict(store.seed(item), prefer_media=True)  # pyright: ignore[reportArgumentType, reportCallIssue]
    with tx() as conn, conn.cursor() as cur:
        other_id = store._insert(cur, part, 1)
    other = dict(part, execution_id=other_id)
    held, release = hold_proof(monkeypatch)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(store.decision, part, 0, NOW)
        try:
            assert held.wait(5)
            # Distinct partitions do not share their partition row lock. This
            # timeout must therefore be on the media manifest, before any proof.
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                WorkflowStore(bounded(tx)).decision(other, 0, NOW)
        finally:
            release.set()
        assert first.result(timeout=5) == {"kind": "wait", "seconds": 0.01}
    assert store.decision(other, 0, NOW) == {
        "kind": "target",
        "stage": "media",
        "target_key": ordered[-1][0]["target_key"],
    }
    cursor = query(
        tx,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
        (owned["run_id"],),
    )[0]["cursor_state"]
    assert cursor["preserved"] == "value" and cursor["media_readiness_after"] == ordered[-1][0]["target_key"]
    assert "media_readiness_cycle" not in cursor


def test_profile_inventory_decision_has_no_upgrade_and_rolls_back_atomically(profiles_db, monkeypatch):
    from trr_backend.ingestion import profile_inventory

    code = "Profiles" + uuid4().hex
    run = inventory.start(profiles_db, (code,))
    inventory.settle(
        profiles_db, run, code, [inventory.page([inventory.comment("1", "101", "reader")], expected=1)], expected=1
    )
    _, tx = profiles_db
    store = WorkflowStore(tx)
    item = store.seed(run)
    original_advance = profile_inventory.advance

    def crash(*args, **kwargs):
        original_advance(*args, **kwargs)
        raise RuntimeError("injected after profile inventory mutation")

    monkeypatch.setattr(profile_inventory, "advance", crash)
    with pytest.raises(RuntimeError, match="injected"):
        store.decision(item, 0, NOW)
    assert inventory.profiles(profiles_db, run) == []
    assert not query(
        tx,
        "select * from social.instagram_ingestion_decisions where execution_id=%s",
        (item["execution_id"],),  # pyright: ignore[reportArgumentType]
    )
    assert (
        query(
            tx,
            "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
            (run["run_id"],),
        )[0]["cursor_state"]
        == {}
    )
    monkeypatch.setattr(profile_inventory, "advance", original_advance)
    held, release = Event(), Event()
    original_state = profile_inventory._state

    def state(*args, **kwargs):
        result = original_state(*args, **kwargs)
        held.set()
        assert release.wait(10)
        return result

    monkeypatch.setattr(profile_inventory, "_state", state)
    with ThreadPoolExecutor(max_workers=2) as pool:
        deciding = pool.submit(store.decision, item, 0, NOW)
        try:
            assert held.wait(5)
            # If advance upgraded run SHARE to UPDATE, this probe would fail.
            pool.submit(shared_probe, bounded(tx), item).result(timeout=3)
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                Store(bounded(tx)).control(run["run_id"], run["account"], action="cancel", idempotency_key=str(uuid4()))
        finally:
            release.set()
        assert deciding.result(timeout=3) == {"kind": "wait", "seconds": 0.01}
    assert [row["target_key"] for row in inventory.profiles(profiles_db, run)] == ["profile:id:101"]
    assert (
        len(
            query(
                tx,
                "select * from social.instagram_ingestion_decisions where execution_id=%s",
                (item["execution_id"],),  # pyright: ignore[reportArgumentType]
            )
        )
        == 1
    )


class OldExclusiveDecision(WorkflowStore):
    """Exact old fence mode; all proof, journal and writer work is identical."""

    def _current(self, cur, item, *, shared_fence=False):
        return super()._current(cur, item, shared_fence=False)


def test_five_paired_controlled_contention_trials(pipeline_database, monkeypatch):
    trials = []
    original = MediaSnapshotLoader._completion
    for pair in range(5):
        for mode in ("old", "new") if pair % 2 == 0 else ("new", "old"):
            store, tx, item, page, media_key = ready(
                pipeline_database, cls=OldExclusiveDecision if mode == "old" else WorkflowStore
            )
            held, release, entering = Event(), Event(), Event()
            timing = {}

            def completion(*args, _held=held, _release=release, **kwargs):
                _held.set()
                assert _release.wait(10)
                return original(*args, **kwargs)

            monkeypatch.setattr(MediaSnapshotLoader, "_completion", staticmethod(completion))

            class TimedWriter(Store):
                def _execution_run(self, *args, _entering=entering, _timing=timing, **kwargs):
                    start = time.perf_counter()
                    _entering.set()
                    result = super()._execution_run(*args, **kwargs)
                    _timing["run_fence_wait_seconds"] = time.perf_counter() - start
                    return result

            def write(_tx=tx, _page=page, _timing=timing, _writer=TimedWriter):
                start = time.perf_counter()
                CommentActivity(_writer(_tx))._persist_page(_page[0], **_page[1])
                _timing["writer_elapsed_seconds"] = time.perf_counter() - start

            with ThreadPoolExecutor(max_workers=2) as pool:
                start = time.perf_counter()
                deciding = pool.submit(store.decision, item, 0, NOW)
                timer = None
                try:
                    assert held.wait(5)
                    writer = pool.submit(write)
                    assert entering.wait(5)
                    timer = Timer(0.25, release.set)
                    timer.start()
                    if mode == "new":
                        writer.result(timeout=0.2)
                        assert not release.is_set()
                    writer.result(timeout=5)
                    assert deciding.result(timeout=5) == {"kind": "target", "stage": "media", "target_key": media_key}
                finally:
                    release.set()
                    if timer:
                        timer.cancel()
                        timer.join()
                timing["scenario_elapsed_seconds"] = time.perf_counter() - start
            assert_page(tx, page)
            trials.append(dict(pair=pair, mode=mode, **timing))
    summary = {}
    for mode in ("old", "new"):
        rows = [row for row in trials if row["mode"] == mode]
        summary[mode] = {
            field: {
                "median": statistics.median(row[field] for row in rows),
                "variance": statistics.variance(row[field] for row in rows),
                "minimum": min(row[field] for row in rows),
                "maximum": max(row[field] for row in rows),
            }
            for field in ("run_fence_wait_seconds", "writer_elapsed_seconds", "scenario_elapsed_seconds")
        }
    assert summary["old"]["run_fence_wait_seconds"]["minimum"] >= 0.20
    assert summary["new"]["run_fence_wait_seconds"]["maximum"] < 0.20
    result = {
        "controlled_proof_hold_seconds": 0.25,
        "pairs": 5,
        "alternate_order": True,
        "trials": trials,
        "summary": summary,
        "qualification": "synthetic lock-contention evidence only; no production throughput claim",
    }
    output = os.environ.get("TRR_SCHEDULING_FENCE_TIMING_OUTPUT")
    if output:
        Path(output).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


@pytest.mark.parametrize("change", ["engine", "workflow_version", "placement"])
def test_shared_decision_preserves_engine_version_and_placement_fences(pipeline_database, change):
    store, tx, item, _, _ = ready(pipeline_database)
    if change == "placement":
        store = WorkflowStore(tx, execution_placement="local")
    else:
        item = dict(item, **{change: "different"})
    with pytest.raises(LostOwnership if change == "placement" else Conflict):
        store.decision(item, 0, NOW)
    assert not query(
        tx, "select * from social.instagram_ingestion_decisions where execution_id=%s", (item["execution_id"],)
    )


def test_failed_readiness_rolls_back_cursor_and_decision(pipeline_database, monkeypatch):
    owned, tx, store, item, _, _ = snapshots._seed_media_readiness_page(pipeline_database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_manifests set discovery_complete=true,sealed_at=clock_timestamp() "
            "where run_id=%s",
            (owned["run_id"],),
        )
    part = dict(store.seed(item), prefer_media=True)  # pyright: ignore[reportArgumentType, reportCallIssue]
    before = query(
        tx,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
        (owned["run_id"],),
    )
    original = MediaSnapshotLoader._completion
    calls = []

    def completion(*args, **kwargs):
        calls.append(args[3])
        if len(calls) == 2:
            raise RuntimeError("injected second readiness proof failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(MediaSnapshotLoader, "_completion", staticmethod(completion))
    with pytest.raises(RuntimeError, match="second readiness"):
        store.decision(part, 0, NOW)
    assert len(calls) == 2
    assert before == query(
        tx,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
        (owned["run_id"],),
    )
    assert not query(
        tx, "select * from social.instagram_ingestion_decisions where execution_id=%s", (part["execution_id"],)
    )
