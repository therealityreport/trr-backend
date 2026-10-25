"""Common root admission on fixture PostgreSQL; no configured services or sources."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from threading import Event
from types import SimpleNamespace

import pytest
from psycopg2.extras import RealDictCursor

from scripts.ingestion import local_worker
from tests.ingestion import test_comment_recovery as existing
from tests.ingestion import test_comments as fixtures
from trr_backend.ingestion import comment_recovery as recovery
from trr_backend.ingestion.domain import Conflict, stable_json
from trr_backend.ingestion.persistence import Store

# The existing fixture registers its loopback-only PostgreSQL and reaps it.
database = fixtures.database


def saved(tx, run_id):
    result = {}
    for table in ("targets", "commands", "receipts", "evidence", "comment_pages"):
        result[table] = fixtures.query(
            tx,
            f"select to_jsonb(t) as row from social.instagram_ingestion_{table} t "
            "where run_id=%s order by to_jsonb(t)::text",
            (run_id,),
        )
    return result


def roots(tx, run_id, number=4):
    """Pending and consumed restarts remain outstanding during every pause."""
    with tx() as conn, conn.cursor() as cur:
        for index in range(number):
            cp = (
                {"public_lane_restarts": {"__top__": {"retained": True}}}
                if index == 0
                else {"public_recovery": {"command_key": "prior-" + str(index), "pass": 1}}
            )
            cur.execute(
                """insert into social.instagram_ingestion_targets
                (run_id,stage,target_key,max_attempts,attempts,checkpoint,next_attempt_at,blocked_reason)
                values(%s,'comments',%s,12,%s,%s::jsonb,
                    case when %s then clock_timestamp()+interval '1 hour' end,%s)""",
                (
                    run_id,
                    "outstanding-" + str(index),
                    12 if index == 3 else 1,
                    stable_json(cp),
                    index == 1,
                    "source_payload_invalid" if index == 2 else None,
                ),
            )


def additional_settled(tx, args, key):
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_targets
            (run_id,stage,target_key,max_attempts,attempts,outcome,checkpoint,observed_at,committed_at)
            select run_id,stage,%s,max_attempts,attempts,outcome,checkpoint,observed_at,committed_at
            from social.instagram_ingestion_targets where run_id=%s and stage='comments' and target_key=%s""",
            (key, args["run_id"], args["target_key"]),
        )


def for_target(tx, args, key=None):
    key = key or args["target_key"]
    cp = fixtures.query(
        tx,
        "select checkpoint from social.instagram_ingestion_targets "
        "where run_id=%s and stage='comments' and target_key=%s",
        (args["run_id"], key),
    )[0]["checkpoint"]
    return dict(existing.command(tx, args), target_key=key, expected_checkpoint_hash=recovery.fingerprint(cp))


def test_pending_consumed_cooldown_blocked_and_exhausted_roots_deny_without_mutation(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    roots(tx, args["run_id"])
    request = for_target(tx, args)
    before = saved(tx, args["run_id"])
    with pytest.raises(recovery.RecoveryAdmissionDeferred, match="recovery_root_capacity_full"):
        recovery.recover_public_target(store, **request)
    assert saved(tx, args["run_id"]) == before


def test_capacity_is_released_when_one_root_settles(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    roots(tx, args["run_id"])
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set outcome='committed',"
            "observed_at=clock_timestamp(),committed_at=clock_timestamp() "
            "where run_id=%s and target_key='outstanding-1'",
            (args["run_id"],),
        )
    result = recovery.recover_public_target(store, **for_target(tx, args))
    assert result["pass"] == 1
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        recovery._check_root_capacity(cur, args["run_id"], args["target_key"])


def test_existing_counted_target_does_not_consume_a_second_slot(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    request = for_target(tx, args)
    recovery.recover_public_target(store, **request)
    # First-page consumption removes pending restart, but keeps the command.
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set checkpoint=checkpoint-'public_lane_restarts' "
            "where run_id=%s and target_key=%s",
            (args["run_id"], args["target_key"]),
        )
    roots(tx, args["run_id"], 3)
    assert recovery.recover_public_target(store, **for_target(tx, args))["pass"] == 2


def test_accepted_replay_precedes_capacity_gate_cancellation_and_isolation(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    request = for_target(tx, args)
    accepted = recovery.recover_public_target(store, **request)
    roots(tx, args["run_id"])
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set state='cancelled',generation=2 where run_id=%s",
            (args["run_id"],),
        )
    before = saved(tx, args["run_id"])
    with tx() as holder, holder.cursor(cursor_factory=RealDictCursor) as cur:
        recovery._claim_root_admission(cur, args["run_id"], args["account"])
        cur.execute(
            "select pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"instagram-command:{args['account']}:{request['idempotency_key']}",),
        )
        with tx() as conn, conn.cursor() as replay_cur:
            replay_cur.execute("set transaction isolation level repeatable read")
            replay_cur.execute("set local statement_timeout='2s'")
            nested = Store(transaction=lambda: nullcontext(conn))
            assert recovery.recover_public_target(nested, **request) == accepted
    assert saved(tx, args["run_id"]) == before


def test_command_committed_between_journal_read_and_gate_claim_replays(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    request = for_target(tx, args)
    lookup = recovery._replayed_recovery
    accepted = []
    racing = False

    def commit_between_lookup_and_claim(*values):
        nonlocal racing
        previous = lookup(*values)
        if not racing:
            racing = True
            # A separate transaction commits after the first empty snapshot.
            accepted.append(recovery.recover_public_target(store, **request))
        return previous

    monkeypatch.setattr(recovery, "_replayed_recovery", commit_between_lookup_and_claim)
    assert recovery.recover_public_target(store, **request) == accepted[0]
    journals = fixtures.query(
        tx,
        "select result from social.instagram_ingestion_commands where account=%s and idempotency_key=%s",
        (args["account"], request["idempotency_key"]),
    )
    assert journals == [{"result": accepted[0]}]


def test_rollback_releases_gate_and_preserves_preimage(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    request = for_target(tx, args)
    before = saved(tx, args["run_id"])
    with pytest.raises(RuntimeError, match="after helper before outer commit"):
        with tx() as conn:
            recovery.recover_public_target(Store(transaction=lambda: nullcontext(conn)), **request)
            raise RuntimeError("after helper before outer commit")
    assert saved(tx, args["run_id"]) == before
    assert recovery.recover_public_target(store, **request)["pass"] == 1


def test_repeatable_read_new_admission_fails_closed(database, monkeypatch):
    _, tx, args, _ = existing.completed(database, monkeypatch)
    request = for_target(tx, args)
    before = saved(tx, args["run_id"])
    with pytest.raises(Conflict, match="read-committed"):
        with tx() as conn, conn.cursor() as cur:
            cur.execute("set transaction isolation level repeatable read")
            recovery.recover_public_target(Store(transaction=lambda: nullcontext(conn)), **request)
    assert saved(tx, args["run_id"]) == before


def test_simultaneous_new_roots_serialize_three_to_four(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    additional_settled(tx, args, "second-settled")
    roots(tx, args["run_id"], 3)
    requests = [for_target(tx, args), for_target(tx, args, "second-settled")]
    held, rejected = Event(), Event()
    check = recovery._check_root_capacity

    def hold_gate(cur, run_id, key):
        check(cur, run_id, key)
        held.set()
        assert rejected.wait(5), "contender did not return while gate held"

    monkeypatch.setattr(recovery, "_check_root_capacity", hold_gate)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(recovery.recover_public_target, store, **requests[0])
        assert held.wait(5)
        try:
            with pytest.raises(recovery.RecoveryAdmissionDeferred, match="recovery_admission_busy"):
                recovery.recover_public_target(store, **requests[1])
        finally:
            rejected.set()
        assert first.result(timeout=5)["pass"] == 1
    monkeypatch.setattr(recovery, "_check_root_capacity", check)
    with pytest.raises(recovery.RecoveryAdmissionDeferred, match="recovery_root_capacity_full"):
        recovery.recover_public_target(store, **requests[1])
    assert (
        len(
            fixtures.query(
                tx,
                "select target_key from social.instagram_ingestion_targets where run_id=%s and outcome='unresolved'",
                (args["run_id"],),
            )
        )
        == 4
    )


@pytest.mark.parametrize("same_command", [False, True])
def test_wrapper_prelocks_and_normal_helper_never_deadlock(database, monkeypatch, same_command):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    wrapped_request, normal_request = for_target(tx, args), for_target(tx, args)
    if same_command:
        normal_request = dict(wrapped_request)
    prelocked, gate_owned = Event(), Event()
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into social.instagram_ingestion_manifests(run_id,stage,discovery_complete,sealed_at) "
            "values(%s,'media',true,clock_timestamp())",
            (args["run_id"],),
        )
        cur.execute(
            "insert into social.instagram_ingestion_targets"
            "(run_id,stage,target_key,max_attempts,outcome,observed_at,committed_at) "
            "values(%s,'media',%s,3,'committed',clock_timestamp(),clock_timestamp())",
            (args["run_id"], args["target_key"]),
        )

    class NormalStore(Store):
        def _execution_run(self, *values, **options):
            result = super()._execution_run(*values, **options)
            gate_owned.set()  # Root gate is already held; target lock comes next.
            return result

    def wrapped():
        with pytest.raises(recovery.RecoveryAdmissionDeferred, match="recovery_admission_busy"):
            with tx() as conn, conn.cursor() as cur:
                cur.execute("set local statement_timeout='3s'")
                cur.execute(
                    "select run_id from social.instagram_ingestion_runs where run_id=%s for share", (args["run_id"],)
                )
                for stage in ("comments", "media"):
                    cur.execute(
                        "select target_key from social.instagram_ingestion_targets "
                        "where run_id=%s and stage=%s and target_key=%s for update",
                        (args["run_id"], stage, args["target_key"]),
                    )
                cur.execute(
                    "select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:post:" + args["target_key"],)
                )
                cur.execute("select id from social.instagram_posts where id=%s for update", (args["post_id"],))
                prelocked.set()
                assert gate_owned.wait(5)
                recovery.recover_public_target(Store(transaction=lambda: nullcontext(conn)), **wrapped_request)
        return "deferred_and_rolled_back"

    with ThreadPoolExecutor(max_workers=2) as pool:
        wrapper = pool.submit(wrapped)
        assert prelocked.wait(5)
        normal = pool.submit(recovery.recover_public_target, NormalStore(tx), **normal_request)
        assert wrapper.result(timeout=7) == "deferred_and_rolled_back"
        assert normal.result(timeout=5)["pass"] == 1
    journals = fixtures.query(
        tx,
        "select idempotency_key from social.instagram_ingestion_commands where run_id=%s and idempotency_key=any(%s)",
        (args["run_id"], [wrapped_request["idempotency_key"], normal_request["idempotency_key"]]),
    )
    assert journals == [{"idempotency_key": normal_request["idempotency_key"]}]


def test_child_root_recovery_cannot_bypass_common_capacity(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set checkpoint=jsonb_set("
            "checkpoint,'{comment_pages,lanes,p}','{\"exhausted\":true}'::jsonb) "
            "where run_id=%s and stage='comments' and target_key=%s",
            (args["run_id"], args["target_key"]),
        )
    request = dict(for_target(tx, args), lanes=("p",))
    roots(tx, args["run_id"])
    before = saved(tx, args["run_id"])
    with pytest.raises(recovery.RecoveryAdmissionDeferred, match="recovery_root_capacity_full"):
        recovery.recover_public_target(store, **request)
    assert saved(tx, args["run_id"]) == before


def test_wrapper_admission_winner_defers_normal_helper_before_target_wait(database, monkeypatch):
    _, tx, args, _ = existing.completed(database, monkeypatch)
    wrapped_request, normal_request = for_target(tx, args), for_target(tx, args)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("set local statement_timeout='3s'")
        cur.execute("select run_id from social.instagram_ingestion_runs where run_id=%s for share", (args["run_id"],))
        cur.execute(
            "select target_key from social.instagram_ingestion_targets "
            "where run_id=%s and stage='comments' and target_key=%s for update",
            (args["run_id"], args["target_key"]),
        )
        cur.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:post:" + args["target_key"],))
        cur.execute("select id from social.instagram_posts where id=%s for update", (args["post_id"],))
        accepted = recovery.recover_public_target(Store(transaction=lambda: nullcontext(conn)), **wrapped_request)
        # The wrapped transaction has not committed and still owns its row/gate.
        with ThreadPoolExecutor(max_workers=1) as pool:
            contender = pool.submit(recovery.recover_public_target, Store(tx), **normal_request)
            with pytest.raises(recovery.RecoveryAdmissionDeferred, match="recovery_admission_busy"):
                contender.result(timeout=2)
    assert accepted["pass"] == 1
    assert recovery.recover_public_target(Store(tx), **wrapped_request) == accepted


def test_run_gate_does_not_block_a_different_run(database, monkeypatch):
    _, tx, first, _ = existing.completed(database, monkeypatch)
    store, _, other, _ = existing.completed(database, monkeypatch)
    request = for_target(tx, other)
    with tx() as holder, holder.cursor(cursor_factory=RealDictCursor) as cur:
        recovery._claim_root_admission(cur, first["run_id"], first["account"])
        assert recovery.recover_public_target(store, **request)["pass"] == 1


def test_blocked_finalizer_returns_typed_nonterminal_wait_at_capacity(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    binding = existing.january_binding(tx, args)
    roots(tx, args["run_id"])
    # Make every root blocked/exhausted, the exact reachable blocked-stop path.
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set attempts=max_attempts "
            "where run_id=%s and outcome='unresolved'",
            (args["run_id"],),
        )
    # First invocation retains the normal real offline audit; second attempts a root.
    assert recovery.enqueue_next_recovery(store, **binding, blocked_stage=True)["status"] == "retained_audit_completed"
    before = saved(tx, args["run_id"])
    assert recovery.enqueue_next_recovery(store, **binding, blocked_stage=True) == {
        "status": "recovery_admission_wait",
        "reason": "recovery_root_capacity_full",
    }
    assert saved(tx, args["run_id"]) == before


def test_nonadmission_conflicts_are_not_converted_to_wait(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    binding = existing.january_binding(tx, args)
    assert recovery.enqueue_next_recovery(store, **binding)["status"] == "retained_audit_completed"
    monkeypatch.setattr(
        recovery, "recover_public_target", lambda *a, **k: (_ for _ in ()).throw(Conflict("not admission"))
    )
    with pytest.raises(Conflict, match="not admission"):
        recovery.enqueue_next_recovery(store, **binding)


def test_normal_finalizer_admission_contention_is_nonterminal(database, monkeypatch):
    store, tx, args, _ = existing.completed(database, monkeypatch)
    binding = existing.january_binding(tx, args)
    assert recovery.enqueue_next_recovery(store, **binding)["status"] == "retained_audit_completed"
    before = saved(tx, args["run_id"])
    with tx() as holder, holder.cursor(cursor_factory=RealDictCursor) as cur:
        recovery._claim_root_admission(cur, args["run_id"], args["account"])
        assert recovery.enqueue_next_recovery(store, **binding) == {
            "status": "recovery_admission_wait",
            "reason": "recovery_admission_busy",
        }
    assert saved(tx, args["run_id"]) == before


@pytest.mark.parametrize("partition_size", [1, 2])
def test_real_local_callback_waits_abortably_and_cannot_finalize(monkeypatch, partition_size):
    """Execute main's real callback and run_partition; mock only process setup."""
    from scripts.ingestion import local_driver
    from trr_backend.db import pg
    from trr_backend.ingestion import runtime, workflow_store, workflows

    calls = []

    class Dispatch:
        active = {}
        latest = None
        comments_submitted = 0
        limit = 6

        def bind(self, *args):
            pass

        def drain(self, *args, **kwargs):
            calls.append("drain")

        def wait(self, seconds):
            calls.append(("wait", seconds))
            abort.set()

    class FakeStore:
        def complete_partition(self, *args, **kwargs):
            pytest.fail("capacity deferral must not complete or block the run")

    class FakeScheduler:
        def clock(self):
            return None

        def tick(self, *args):
            return {"stop": "blocked"}

    abort = local_worker.DiagnosticAbort()
    dispatch, store, scheduler = Dispatch(), FakeStore(), FakeScheduler()
    guard = SimpleNamespace(start=lambda: None, close=lambda: None)
    monkeypatch.setattr(local_worker, "DiagnosticAbort", lambda: abort)
    monkeypatch.setattr(local_worker, "AbortExitGuard", lambda *a: guard)
    monkeypatch.setattr(local_worker, "start_abort_reporter", lambda *a: None)
    monkeypatch.setattr(local_worker, "PARTITION_SIZE", partition_size)
    monkeypatch.setattr(runtime, "coordinator_enabled", lambda: True)
    monkeypatch.setattr(local_driver, "validate_configuration", lambda *a: None)
    monkeypatch.setattr(local_driver, "CoordinatorLease", lambda *a: None)
    monkeypatch.setattr(workflow_store, "WorkflowStore", lambda **k: store)
    monkeypatch.setattr(local_worker, "LocalDispatch", lambda *a: dispatch)
    monkeypatch.setattr(workflows, "Scheduler", lambda *a, **k: scheduler)
    monkeypatch.setattr(pg, "close_pool", lambda: None)
    monkeypatch.setenv("TRR_INSTAGRAM_DBOS_DATABASE_URL", "postgresql://fixture@127.0.0.1:1/postgres")
    monkeypatch.setenv("TRR_INSTAGRAM_DBOS_RUNTIME_ROLE", "fixture")
    monkeypatch.setenv("TRR_INSTAGRAM_JANUARY_RECOVERY_RUN_ID", "bound-run")
    monkeypatch.setenv("TRR_INSTAGRAM_JANUARY_RECOVERY_INTENT_HASH", "bound-intent")
    monkeypatch.setenv("TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION", "fixture")
    monkeypatch.setattr(
        recovery,
        "enqueue_next_recovery",
        lambda *a, **k: {"status": "recovery_admission_wait", "reason": "recovery_root_capacity_full"},
    )

    def execute(**options):
        with pytest.raises(local_worker.Interrupted):
            local_worker.run_partition(
                {"run_id": "bound-run", "account": "bravotv", "generation": 1},
                store=store,
                scheduler=scheduler,
                dispatch=dispatch,
                abort=abort,
                before_finalize=options["before_finalize"],
                on_completed=lambda *a: pytest.fail("must not certify a deferred run"),
            )
        return {"status": "stopped"}

    monkeypatch.setattr(local_worker, "run_worker", execute)
    assert local_worker.main() == 0
    assert ("wait", local_worker.WAIT_RECHECK_SECONDS) in calls and abort.is_set()
