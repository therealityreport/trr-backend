"""Preparation keeps partition ordinals and uncertain audit commits recoverable."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.ingestion import local_worker
from tests.ingestion import test_comment_recovery as recovery_fixture
from tests.ingestion import test_workflows as workflow_fixture
from trr_backend.ingestion.domain import fingerprint, stable_json

base_database = workflow_fixture.base_database
database = workflow_fixture.database
comment_database = recovery_fixture.database


@pytest.mark.parametrize("status", ["retained_audit_completed", "bounded_audit_deferred", "productive_budget_extended"])
def test_preparation_does_not_consume_a_partition_ordinal(database, monkeypatch, status):
    from trr_backend.ingestion import workflow_store

    monkeypatch.setattr(local_worker, "PARTITION_SIZE", 2)
    monkeypatch.setattr(workflow_store, "PARTITION_SIZE", 2)
    _, item = workflow_fixture.run_fixture(database)
    store = database[0]
    dispatch = SimpleNamespace(
        active={},
        latest=None,
        comments_submitted=0,
        limit=1,
        bind=lambda *_: None,
        drain=lambda *_a, **_k: None,
        wait=lambda *_: None,
    )
    outcomes = iter([{"status": status}, None, None])

    def tick(item, ordinal):
        store.decision(item, ordinal, datetime.now(UTC))
        return {"sleep": 0.001}

    scheduler = SimpleNamespace(tick=tick, clock=lambda: datetime.now(UTC))
    result = local_worker.run_partition(
        item,
        store=store,
        scheduler=scheduler,
        dispatch=dispatch,
        abort=local_worker.DiagnosticAbort(),
        before_recovery=lambda _: next(outcomes),
    )
    assert result == "continued"
    with database[1]() as conn, conn.cursor() as cur:
        cur.execute(
            "select ordinal from social.instagram_ingestion_decisions where execution_id=%s order by ordinal",
            (item["execution_id"],),
        )
        assert cur.fetchall() == [(0,), (1,)]


def test_lost_audit_acknowledgment_reuses_successful_durable_summary(comment_database, monkeypatch):
    from trr_backend.ingestion import comment_recovery, retained_comment_recovery
    from trr_backend.ingestion.bounded_recovery import window_key
    from trr_backend.ingestion.persistence import Store

    store, tx, args, _ = recovery_fixture.completed(comment_database, monkeypatch)
    binding = recovery_fixture.january_binding(tx, args)
    key = window_key(args["run_id"], 1)
    window = {
        "window_id": key,
        "generation": 1,
        "priority": "comments_first",
        "duration_seconds": 3600,
        "source_revision": "fixture",
        "deadline_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
    }
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            (
                Path(__file__).resolve().parents[2]
                / "supabase/migrations/20261016173915_instagram_ingestion_execution_placement.sql"
            ).read_text()
        )
        cur.execute(
            "update social.instagram_ingestion_runs set execution_placement='local' where run_id=%s", (args["run_id"],)
        )
        cur.execute(
            """insert into social.instagram_ingestion_commands
            (account,idempotency_key,command_hash,run_id,result) values('bravotv',%s,%s,%s,%s::jsonb)""",
            (key, fingerprint(window), args["run_id"], stable_json(window)),
        )
    for name, value in {
        "TRR_INSTAGRAM_RECOVERY_WINDOW_ID": key,
        "TRR_INSTAGRAM_LOCAL_WORKER_RUN_ID": args["run_id"],
        "TRR_INSTAGRAM_LOCAL_WORKER_GENERATION": "1",
        "TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION": "fixture",
    }.items():
        monkeypatch.setenv(name, value)
    local = Store(tx, execution_placement="local")
    calls = []

    def lost_ack(store, *, deadline_monotonic, **request):
        calls.append(request)
        result = retained_comment_recovery.audit_and_replay_retained(store, **request)
        assert result["stream_complete"] is True
        return {"stream_complete": False, "reason": "acknowledgment_lost"}

    monkeypatch.setattr(retained_comment_recovery, "bounded_retained_audit", lost_ack)
    assert (
        comment_recovery.enqueue_next_recovery(local, **binding, blocked_stage=True)["status"]
        == "bounded_audit_deferred"
    )
    assert (
        comment_recovery.enqueue_next_recovery(local, **binding, blocked_stage=True)["status"]
        == "public_recovery_queued"
    )
    assert len(calls) == 1
