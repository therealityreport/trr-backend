"""Durable execution policy against disposable, guarded PostgreSQL only."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_persistence
from trr_backend.ingestion.domain import Conflict, LostOwnership, RunIntent, Stage
from trr_backend.ingestion.execution_policy import ExecutionPolicy, effective_comments_limit_from_run
from trr_backend.ingestion.persistence import Store

base_database = test_persistence.database
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def database(base_database):
    _, transaction = base_database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            (ROOT / "supabase/migrations/20261016173915_instagram_ingestion_execution_placement.sql").read_text()
        )
        cur.execute((ROOT / "supabase/migrations/20261023140000_instagram_execution_policy.sql").read_text())
        cur.execute((ROOT / "supabase/migrations/20261023180000_instagram_execution_policy_six_eight.sql").read_text())
    return ExecutionPolicy(transaction), Store(transaction, execution_placement="local"), transaction


def seed(database, *, original=1, scope="bounded_window"):
    policy, store, transaction = database
    intent = RunIntent(
        account="bravotv",
        scope=scope,
        selected_tasks=(Stage.COMMENTS,),
        comments_worker_count=original,
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
    )
    run = store.start(
        intent, idempotency_key=str(uuid4()), engine="dbos", workflow_version="policy-test", initiated_by="fixture"
    )
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set execution_placement='local' where run_id=%s", (run["run_id"],)
        )
    item = {
        "run_id": run["run_id"],
        "account": intent.account,
        "generation": 1,
        "intent_hash": intent.digest,
        "execution_placement": "local",
        "engine": "dbos",
        "workflow_version": "policy-test",
    }
    setter = {
        "run_id": run["run_id"],
        "account": intent.account,
        "expected_generation": 1,
        "expected_intent_hash": intent.digest,
        "expected_revision": 0,
        "comments_parallelism": 2,
        "idempotency_key": str(uuid4()),
        "initiated_by": "fixture-operator",
    }
    return policy, item, setter


def records(database, run_id) -> Any:
    with database[2]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.instagram_ingestion_runs where run_id=%s", (run_id,))
        run = dict(cur.fetchone())
        cur.execute("select * from social.instagram_ingestion_execution_policies where run_id=%s", (run_id,))
        row = cur.fetchone()
    return run, dict(row) if row else None


def business_snapshot(database, run_id):
    result = {}
    with database[2]() as conn, conn.cursor() as cur:
        for table in (
            "instagram_ingestion_runs",
            "instagram_ingestion_outbox",
            "instagram_ingestion_manifests",
            "instagram_ingestion_targets",
            "instagram_ingestion_receipts",
            "instagram_ingestion_evidence",
        ):
            cur.execute(
                f"select to_jsonb(t) from social.{table} t where run_id=%s order by to_jsonb(t)::text", (run_id,)
            )
            result[table] = cur.fetchall()
        cur.execute("select to_jsonb(t) from social.scrape_runs t where id=%s", (run_id,))
        result["scrape_runs"] = cur.fetchall()
    return result


def test_policy_raises_from_one_to_two_to_four_without_changing_business_state(database):
    policy, item, setter = seed(database)
    store = database[1]
    store.discover(
        item["run_id"],
        item["account"],
        generation=1,
        stage=Stage.COMMENTS,
        target_keys=["fixture-post"],
        max_attempts=12,
        cursor_state={},
        page_id="page",
        expected_cursor_state={},
        seal=True,
    )
    store.reserve(item["run_id"], item["account"], generation=1, stage=Stage.COMMENTS, target_key="fixture-post")
    before = business_snapshot(database, item["run_id"])
    first = policy.set_limits(**setter)
    assert first["operation"] == "set_execution_limits"
    assert (
        first["previous_revision"],
        first["revision"],
        first["previous_comments_parallelism"],
        first["comments_parallelism"],
    ) == (0, 1, 1, 2)
    assert policy.effective_comments_limit(item) == 2
    second = policy.set_limits(
        **dict(setter, expected_revision=1, comments_parallelism=4, idempotency_key=str(uuid4()))
    )
    assert (
        second["previous_revision"],
        second["revision"],
        second["previous_comments_parallelism"],
        second["comments_parallelism"],
    ) == (1, 2, 2, 4)
    assert policy.effective_comments_limit(item) == 4
    assert business_snapshot(database, item["run_id"]) == before
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute(
            "select result from social.instagram_ingestion_commands where run_id=%s order by created_at",
            (item["run_id"],),
        )
        audit = [row[0] for row in cur.fetchall()]
    assert len(audit) == 3  # Original launch plus two policy commands only.
    assert [entry["revision"] for entry in audit if entry.get("operation") == "set_execution_limits"] == [1, 2]


def test_six_eight_policy_preserves_intent_and_revision_fences(database):
    policy, item, setter = seed(database)
    before = business_snapshot(database, item["run_id"])
    for revision, limit in enumerate((4, 6, 8)):
        command = dict(setter, expected_revision=revision, comments_parallelism=limit, idempotency_key=str(uuid4()))
        result = policy.set_limits(**command)
        assert result["revision"] == revision + 1 and result["comments_parallelism"] == limit
        assert policy.set_limits(**command) == result
        run, row = records(database, item["run_id"])
        assert policy.effective_comments_limit(item) == effective_comments_limit_from_run(run, row) == limit
        assert business_snapshot(database, item["run_id"]) == before
        with pytest.raises(Conflict, match="revision changed"):
            policy.set_limits(**dict(command, idempotency_key=str(uuid4())))
        with pytest.raises(LostOwnership):
            policy.effective_comments_limit(dict(item, generation=2))


@pytest.mark.parametrize("limit", [0, 3, 5, 7, 9, True])
def test_expanded_limits_reject_unsupported_values_in_python_and_sql(database, limit):
    policy, item, setter = seed(database)
    with pytest.raises(ValueError, match="parallelism"):
        policy.set_limits(**dict(setter, comments_parallelism=limit))
    assert records(database, item["run_id"])[1] is None
    policy.set_limits(**setter)
    run, row = records(database, item["run_id"])
    with pytest.raises(Conflict, match="malformed"):
        effective_comments_limit_from_run(run, dict(row, comments_parallelism=limit))  # pyright: ignore[reportArgumentType, reportCallIssue]
    if type(limit) is int:
        with pytest.raises(psycopg2.errors.CheckViolation):
            with database[2]() as conn, conn.cursor() as cur:
                cur.execute(
                    "update social.instagram_ingestion_execution_policies set comments_parallelism=%s where run_id=%s",
                    (limit, item["run_id"]),
                )
    assert policy.effective_comments_limit(item) == 2


def test_idempotent_replays_and_command_namespace_conflicts(database):
    policy, item, setter = seed(database)
    accepted = policy.set_limits(**setter)
    assert policy.set_limits(**setter) == accepted
    later = policy.set_limits(**dict(setter, expected_revision=1, comments_parallelism=4, idempotency_key=str(uuid4())))
    assert policy.set_limits(**setter) == accepted
    assert policy.effective_comments_limit(item) == 4
    assert later["revision"] == records(database, item["run_id"])[1]["revision"] == 2
    with pytest.raises(Conflict):
        policy.set_limits(**dict(setter, comments_parallelism=4))
    # Existing launch/control/replay keys cannot be repurposed by this operation.
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute(
            "select idempotency_key from social.instagram_ingestion_commands "
            "where run_id=%s and not result ? 'operation'",
            (item["run_id"],),
        )
        launch_key = cur.fetchone()[0]
    with pytest.raises(Conflict):
        policy.set_limits(**dict(setter, idempotency_key=launch_key))


def test_same_command_parallel_replays_apply_once(database):
    policy, item, setter = seed(database)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: policy.set_limits(**setter), range(8)))
    assert all(result == results[0] for result in results)
    assert records(database, item["run_id"])[1]["revision"] == 1
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from social.instagram_ingestion_commands where account=%s and idempotency_key=%s",
            (item["account"], setter["idempotency_key"]),
        )
        assert cur.fetchone()[0] == 1


def test_revision_cas_has_only_one_winner(database):
    policy, item, setter = seed(database)
    policy.set_limits(**setter)

    def attempt(value):
        try:
            return policy.set_limits(
                **dict(setter, expected_revision=1, comments_parallelism=value, idempotency_key=str(uuid4()))
            )
        except Conflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, (1, 4)))
    assert sum(result is not None for result in results) == 1
    assert records(database, item["run_id"])[1]["revision"] == 2
    with pytest.raises(Conflict):
        policy.set_limits(**dict(setter, expected_revision=0, idempotency_key=str(uuid4())))


@pytest.mark.parametrize(
    "field,value", [("expected_generation", 2), ("expected_intent_hash", "0" * 64), ("account", "other")]
)
def test_wrong_setter_fences_leave_no_policy_or_command(database, field, value):
    policy, item, setter = seed(database)
    before = business_snapshot(database, item["run_id"])
    with pytest.raises((LostOwnership, LookupError)):
        policy.set_limits(**dict(setter, **{field: value}))
    assert records(database, item["run_id"])[1] is None
    assert business_snapshot(database, item["run_id"]) == before
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from social.instagram_ingestion_commands where account=%s and idempotency_key=%s",
            (setter["account"], setter["idempotency_key"]),
        )
        assert cur.fetchone()[0] == 0


@pytest.mark.parametrize(
    "field,value", [("state", "completed"), ("state", "blocked"), ("execution_placement", "modal")]
)
def test_inactive_or_nonlocal_run_cannot_be_changed(database, field, value):
    policy, item, setter = seed(database)
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute(f"update social.instagram_ingestion_runs set {field}=%s where run_id=%s", (value, item["run_id"]))
    with pytest.raises(LostOwnership):
        policy.set_limits(**setter)
    assert records(database, item["run_id"])[1] is None


@pytest.mark.parametrize("original", [None, 1, 3, 24])
def test_admission_preserves_original_but_workers_stay_serial_without_current_policy(database, original):
    policy, item, setter = seed(database, original=original)
    run, _ = records(database, item["run_id"])
    assert effective_comments_limit_from_run(run, None) == original
    assert policy.effective_comments_limit(item) == 1
    policy.set_limits(**setter)
    assert policy.effective_comments_limit(item) == 2
    run, current = records(database, item["run_id"])
    for changes in (
        {"generation": 2},
        {"account": "other"},
        {"intent_hash": "0" * 64},
        {"execution_placement": "modal"},
        {"run_id": str(uuid4())},
    ):
        stale = dict(current, **changes, comments_parallelism=3)  # pyright: ignore[reportArgumentType, reportCallIssue]
        assert effective_comments_limit_from_run(run, stale) == original
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_ingestion_runs set generation=2 where run_id=%s", (item["run_id"],))
    run, row = records(database, item["run_id"])
    assert effective_comments_limit_from_run(run, row) == original
    assert policy.effective_comments_limit(dict(item, generation=2)) == 1
    with pytest.raises(LostOwnership):
        policy.effective_comments_limit(item)


@pytest.mark.parametrize("original", [3, 24])
def test_nonlocal_workers_remain_serial_even_with_retained_local_policy(database, original):
    policy, item, setter = seed(database, original=original)
    policy.set_limits(**setter)
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set execution_placement='modal' where run_id=%s", (item["run_id"],)
        )
    run, row = records(database, item["run_id"])
    assert effective_comments_limit_from_run(run, row) == original
    assert policy.effective_comments_limit(dict(item, execution_placement="modal")) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation", 2),
        ("intent_hash", "0" * 64),
        ("execution_placement", "modal"),
        ("engine", "temporal"),
        ("workflow_version", "other"),
    ],
)
def test_effective_limit_callback_fences_current_item(database, field, value):
    policy, item, _ = seed(database)
    with pytest.raises((Conflict, LostOwnership)):
        policy.effective_comments_limit(dict(item, **{field: value}))


def test_same_snapshot_helper_and_nonlocal_fallback(database):
    policy, item, setter = seed(database)
    policy.set_limits(**setter)
    run, row = records(database, item["run_id"])
    before = deepcopy((run, row))
    assert effective_comments_limit_from_run(run, row) == 2
    assert (run, row) == before
    assert effective_comments_limit_from_run(dict(run, execution_placement="modal"), row) == 1
    # Historical run snapshots with no placement retain Modal semantics.
    historical = dict(run)
    del historical["execution_placement"]
    assert effective_comments_limit_from_run(historical, row) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"comments_parallelism": 3},
        {"comments_parallelism": True},
        {"revision": 0},
        {"revision": True},
        {"generation": True},
        {"intent_hash": None},
    ],
)
def test_malformed_current_policy_fails_closed(database, changes):
    policy, item, setter = seed(database)
    policy.set_limits(**setter)
    run, row = records(database, item["run_id"])
    with pytest.raises(Conflict):
        effective_comments_limit_from_run(run, dict(row, **changes))  # pyright: ignore[reportArgumentType, reportCallIssue]
    with pytest.raises(Conflict):
        effective_comments_limit_from_run(run, {})


@pytest.mark.parametrize(
    "field,value",
    [
        ("comments_parallelism", 3),
        ("comments_parallelism", True),
        ("expected_revision", -1),
        ("expected_revision", True),
        ("expected_generation", True),
        ("expected_placement", "modal"),
    ],
)
def test_setter_rejects_unsupported_or_malformed_limits(database, field, value):
    policy, item, setter = seed(database)
    with pytest.raises(ValueError):
        policy.set_limits(**dict(setter, **{field: value}))
    assert records(database, item["run_id"])[1] is None


def test_full_history_gate_and_mutated_original_intent_fail_closed(database):
    policy, item, setter = seed(database, scope="full_history")
    with pytest.raises(Conflict):
        policy.set_limits(**setter)
    with pytest.raises(Conflict):
        policy.effective_comments_limit(item)
    assert records(database, item["run_id"])[1] is None
    policy, item, setter = seed(database)
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set intent=jsonb_set(intent,'{comments_worker_count}','4') "
            "where run_id=%s",
            (item["run_id"],),
        )
    with pytest.raises(Conflict):
        policy.set_limits(**setter)
    with pytest.raises(Conflict):
        policy.effective_comments_limit(item)


def test_stale_policy_can_be_replaced_only_with_current_fence_and_revision(database):
    policy, item, setter = seed(database)
    first = policy.set_limits(**setter)
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_ingestion_runs set generation=2 where run_id=%s", (item["run_id"],))
    assert policy.effective_comments_limit(dict(item, generation=2)) == 1
    with pytest.raises(Conflict):
        policy.set_limits(**dict(setter, expected_generation=2, expected_revision=0, idempotency_key=str(uuid4())))
    changed = policy.set_limits(
        **dict(
            setter,
            expected_generation=2,
            expected_revision=first["revision"],
            comments_parallelism=4,
            idempotency_key=str(uuid4()),
        )
    )
    assert changed["revision"] == 2 and changed["generation"] == 2
    assert policy.effective_comments_limit(dict(item, generation=2)) == 4


def test_policy_and_command_rollback_together(database):
    policy, item, setter = seed(database)
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute("""create function social.reject_policy_command() returns trigger language plpgsql as $$
            begin if new.result->>'operation'='set_execution_limits' then
                raise exception 'fixture command failure'; end if;
            return new; end $$;
            create trigger reject_policy_command before insert on social.instagram_ingestion_commands
            for each row execute function social.reject_policy_command();""")
    try:
        with pytest.raises(psycopg2.Error):
            policy.set_limits(**setter)
        assert records(database, item["run_id"])[1] is None
    finally:
        with database[2]() as conn, conn.cursor() as cur:
            cur.execute(
                "drop trigger reject_policy_command on social.instagram_ingestion_commands; "
                "drop function social.reject_policy_command()"
            )


def test_migration_enforces_bounds_and_private_role_grants(database):
    policy, item, setter = seed(database)
    policy.set_limits(**setter)
    with pytest.raises(psycopg2.errors.CheckViolation):
        with database[2]() as conn, conn.cursor() as cur:
            cur.execute(
                "update social.instagram_ingestion_execution_policies set comments_parallelism=3 where run_id=%s",
                (item["run_id"],),
            )
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute("""select relrowsecurity,exists(select 1 from aclexplode(relacl) a where a.grantee=0)
            from pg_class where oid='social.instagram_ingestion_execution_policies'::regclass""")
        assert cur.fetchone() == (True, False)
        for role in ("anon", "authenticated"):
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                cur.execute(
                    "select has_table_privilege(%s,'social.instagram_ingestion_execution_policies',%s)",
                    (role, privilege),
                )
                assert cur.fetchone()[0] is False
        cur.execute(
            "select has_table_privilege('service_role','social.instagram_ingestion_execution_policies',"
            "'SELECT,INSERT,UPDATE,DELETE')"
        )
        assert cur.fetchone()[0] is True
