"""Durable cancelled-run holds; isolated PostgreSQL and no provider operations."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg2.errors import CheckViolation

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from tests.ingestion.test_persistence import database as database_fixture
from tests.ingestion.test_persistence import launch, target
from trr_backend.ingestion.command_service import CommandService
from trr_backend.ingestion.composition import CanaryCommandService, CanaryPolicy, CanaryStore
from trr_backend.ingestion.domain import Conflict, RunIntent, SourceProfile, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.queries import Queries

database = database_fixture
REASON = "public_source_profile_replacement"
ACTOR = "fixture-operator"


def sql(database, statement, args=()) -> Any:
    with database[1]() as conn, conn.cursor() as cur:
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def cancelled(database):
    store, _ = database
    run = launch(store)
    result = store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    return result


def hold(database, run, **kwargs):
    return database[0].hold_cancelled_run(
        run["run_id"],
        "bravotv",
        kwargs.pop("expected_generation", run["generation"]),
        kwargs.pop("reason", REASON),
        kwargs.pop("idempotency_key", str(uuid4())),
        kwargs.pop("initiated_by", ACTOR),
        **kwargs,
    )


def history(database, run_id):
    result = {}
    for table, key in (
        ("instagram_ingestion_runs", "run_id"),
        ("scrape_runs", "id"),
        ("instagram_ingestion_manifests", "run_id"),
        ("instagram_ingestion_targets", "run_id"),
        ("instagram_ingestion_discovery_pages", "run_id"),
        ("instagram_ingestion_evidence", "run_id"),
        ("instagram_ingestion_receipts", "run_id"),
        ("instagram_ingestion_outbox", "run_id"),
        ("test_canonical", "run_id"),
    ):
        result[table] = sql(
            database, f"select to_jsonb(t) from social.{table} t where {key}=%s order by to_jsonb(t)::text", (run_id,)
        )
    return result


def test_hold_preserves_all_history_and_has_one_concurrent_idempotent_operator_receipt(database):
    store, _ = database
    run = launch(store)
    owned = target(store, run)
    token = str(store.reserve(**owned)["owner_token"])
    store.acquire(**owned, owner_token=token)
    observed = datetime(2026, 1, 2, tzinfo=UTC)
    evidence = store.retain_evidence(
        **owned,
        owner_token=token,
        evidence=Evidence.capture(
            source_identity="instagram:fixture",
            captured_at=observed,
            parser_version="fixture-v1",
            payload={"shortcode": "post1", "caption": "Retained original"},
        ),
    )

    def write(conn):
        with conn.cursor() as cur:
            cur.execute("insert into social.test_canonical(run_id,writes) values(%s,1)", (run["run_id"],))
        return {"post_id": "preserved-fixture"}

    store.commit_target(**owned, owner_token=token, observed_at=observed, evidence_id=evidence, write_canonical=write)
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=Stage.LISTING,
        target_keys=["page:next"],
        max_attempts=12,
        cursor_state={"cursor": "original-next"},
        page_id="original-page",
        expected_cursor_state={},
    )
    sql(
        database,
        """update social.instagram_ingestion_targets set attempts=2,requests=7,
        checkpoint='{"cursor":"original-next"}',next_attempt_at=clock_timestamp()+interval '1 hour'
        where run_id=%s and stage='listing'""",
        (run["run_id"],),
    )
    run = store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    before = history(database, run["run_id"])
    command_key = str(uuid4())
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: hold(database, run, idempotency_key=command_key), range(4)))
    assert all(result == results[0] for result in results)
    after = history(database, run["run_id"])
    assert after["instagram_ingestion_runs"][0][0].pop("execution_hold_reason") == REASON
    assert before["instagram_ingestion_runs"][0][0].pop("execution_hold_reason") is None
    assert after == before
    receipt = sql(
        database,
        "select result from social.instagram_ingestion_commands where account='bravotv' and idempotency_key=%s",
        (command_key,),
    )
    assert receipt == [(results[0],)]
    assert results[0]["initiated_by"] == ACTOR and results[0]["generation"] == run["generation"]
    assert hold(database, run, idempotency_key=command_key) == results[0]
    for mismatch in ({"reason": "another_reason"}, {"initiated_by": "another_operator"}, {"expected_generation": 99}):
        with pytest.raises(Conflict, match="Idempotency"):
            hold(database, run, idempotency_key=command_key, **mismatch)


@pytest.mark.parametrize("state", ["queued", "running", "blocked", "failed", "completed"])
def test_hold_requires_cancelled_state(database, state):
    run = cancelled(database)
    sql(database, "update social.instagram_ingestion_runs set state=%s where run_id=%s", (state, run["run_id"]))
    with pytest.raises(Conflict, match="exact cancelled"):
        hold(database, run)
    assert sql(
        database, "select execution_hold_reason from social.instagram_ingestion_runs where run_id=%s", (run["run_id"],)
    ) == [(None,)]


def test_hold_requires_exact_generation_and_account_and_does_not_overwrite_another_hold(database):
    run = cancelled(database)
    with pytest.raises(Conflict, match="exact cancelled"):
        hold(database, run, expected_generation=run["generation"] - 1)
    with pytest.raises(LookupError):
        database[0].hold_cancelled_run(run["run_id"], "another_account", run["generation"], REASON, str(uuid4()), ACTOR)
    hold(database, run)
    with pytest.raises(Conflict, match="different execution hold"):
        hold(database, run, reason="another_reason")


@pytest.mark.parametrize("owner", ["live", "lease_without_token", "token_without_lease"])
def test_hold_refuses_active_or_uncertain_owners(database, owner):
    store, _ = database
    original = launch(store)
    target(store, original)
    run = store.control(original["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    sql(
        database,
        """update social.instagram_ingestion_targets set owner_token=%s,
        owner_expires_at=case when %s then clock_timestamp()+interval '1 hour' else null end where run_id=%s""",
        (None if owner == "lease_without_token" else str(uuid4()), owner != "token_without_lease", run["run_id"]),
    )
    before = history(database, run["run_id"])
    with pytest.raises(Conflict, match="owner"):
        hold(database, run)
    assert history(database, run["run_id"]) == before


@pytest.mark.parametrize("reason", ["", " ", "unsafe text", "a" * 81, "Uppercase", "a\n"])
def test_hold_reason_is_bounded_and_fixed_in_command_and_database(database, reason):
    run = cancelled(database)
    with pytest.raises(ValueError, match="reason"):
        hold(database, run, reason=reason)
    with pytest.raises(CheckViolation):
        sql(
            database,
            "update social.instagram_ingestion_runs set execution_hold_reason=%s where run_id=%s",
            (reason, run["run_id"]),
        )


@pytest.mark.parametrize("action", ["resume", "retry_targets"])
def test_held_control_and_old_sql_cannot_resume_and_failed_transaction_preserves_history(database, action):
    store, _ = database
    original = launch(store)
    target(store, original)
    run = store.control(original["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    hold(database, run)
    before = history(database, run["run_id"])
    selection = {"stage": "post_details", "target_keys": ["post1"]} if action == "retry_targets" else None
    with pytest.raises(Conflict, match="held"):
        store.control(run["run_id"], "bravotv", action=action, idempotency_key=str(uuid4()), selection=selection)
    with pytest.raises(CheckViolation):
        with database[1]() as conn, conn.cursor() as cur:
            cur.execute("update social.test_canonical set writes=0 where run_id=%s", (run["run_id"],))
            cur.execute(
                "update social.instagram_ingestion_runs set state='queued',generation=generation+1 where run_id=%s",
                (run["run_id"],),
            )
    assert history(database, run["run_id"]) == before


def test_held_progress_is_typed_cancelled_incomplete_and_has_no_executable_actions(database):
    run = cancelled(database)
    hold(database, run)
    queries = Queries(database[1])
    internal = queries.progress(run["run_id"], "bravotv")
    # Progress reports the hold reason truthfully (47249752) and offers no actions.
    assert internal["execution_hold_reason"] == REASON and internal["allowed_actions"] == []
    service = CommandService(database[0], queries, engine="dbos", workflow_version="test-v1", enabled=False)
    progress = service.progress("bravotv", UUID(run["run_id"]))
    assert progress.state == "cancelled" and not progress.complete and progress.allowed_actions == []


def test_hold_frees_only_one_active_slot_without_resetting_total_or_allowing_old_resume(database):
    _, tx = database
    policy = CanaryPolicy(frozenset({"bravotv"}), 2, "hold-canary-" + str(uuid4()))
    store = CanaryStore(policy, tx)
    intent = RunIntent(
        account="bravotv",
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2027, 1, 1, tzinfo=UTC),
    )
    old = store.start(
        intent,
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version=policy.workflow_version,
        initiated_by=ACTOR,
    )
    old = store.control(old["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    service = CanaryCommandService(
        store,
        Queries(tx),
        engine="dbos",
        workflow_version=policy.workflow_version,
        enabled=True,
        capacity=lambda *_: None,
        source_profile=SourceProfile.for_mode("public"),
    )
    request = InstagramIngestionLaunchV2(
        backfill_scope="bounded_window",
        date_start=intent.date_start,
        date_end=intent.date_end,
    )
    with pytest.raises(Conflict, match="one ingestion"):
        service.launch("bravotv", request, idempotency_key=str(uuid4()), initiated_by=ACTOR)
    store.hold_cancelled_run(old["run_id"], "bravotv", old["generation"], REASON, str(uuid4()), ACTOR)

    def start(_):
        try:
            return service.launch("bravotv", request, idempotency_key=str(uuid4()), initiated_by=ACTOR)
        except Conflict:
            return None

    with ThreadPoolExecutor(max_workers=6) as pool:
        accepted = [result for result in pool.map(start, range(6)) if result is not None]
    assert len(accepted) == 1
    current = accepted[0]
    stored = sql(
        database, "select intent from social.instagram_ingestion_runs where run_id=%s", (str(current.run_id),)
    )[0][0]
    assert stored["source_profile"]["access_mode"] == "public"
    with pytest.raises(Conflict, match="held"):
        store.control(old["run_id"], "bravotv", action="resume", idempotency_key=str(uuid4()))
    store.control(str(current.run_id), "bravotv", action="cancel", idempotency_key=str(uuid4()))
    with pytest.raises(Conflict, match="total run budget"):
        service.launch("bravotv", request, idempotency_key=str(uuid4()), initiated_by=ACTOR)
    loose = CanaryStore(CanaryPolicy(policy.accounts, 3, policy.workflow_version), tx)
    with pytest.raises(Conflict, match="one ingestion"):
        loose.start(
            intent,
            idempotency_key=str(uuid4()),
            engine="dbos",
            workflow_version=policy.workflow_version,
            initiated_by=ACTOR,
        )
    assert sql(
        database,
        "select count(*) from social.instagram_ingestion_runs where workflow_version=%s",
        (policy.workflow_version,),
    ) == [(2,)]
