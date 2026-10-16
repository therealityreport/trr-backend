"""Version adoption uses disposable PostgreSQL; no configured services or sources."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from scripts.ingestion import adopt_profile_version as cli
from scripts.ingestion.local_driver import LocalRequest
from tests.ingestion.test_local_execution import database as local_database_fixture
from tests.ingestion.test_local_execution import place, sql
from tests.ingestion.test_persistence import database as base_database_fixture
from trr_backend.ingestion.domain import Conflict, LostOwnership, RunIntent, SourceProfile, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.version_adoption import ADOPTED_WORKFLOW_VERSION, adopt_profile_version

base_database = base_database_fixture
database = local_database_fixture


@pytest.fixture(autouse=True)
def clean(database):
    sql(database, "truncate social.instagram_ingestion_runs,social.scrape_jobs cascade")


def candidate(database, *, profile="public", history=False):
    store = database[0]
    intent = RunIntent(
        account="bravotv",
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2027, 1, 1, tzinfo=UTC),
        source_profile=SourceProfile.for_mode(profile),
    )
    run = store.start(
        intent,
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="instagram-ingestion-v1",
        initiated_by="offline-test",
    )
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=Stage.MEDIA,
        target_keys=["post0", "post1"] if history else ["post1"],
        max_attempts=3,
        cursor_state={"retained_cursor": "page2"},
        expected_cursor_state={},
        page_id="retained-page",
        seal=False,
    )
    if history:
        owned = {
            "run_id": run["run_id"],
            "account": "bravotv",
            "generation": 1,
            "stage": Stage.MEDIA,
            "target_key": "post0",
        }
        owned["owner_token"] = str(store.reserve(**owned)["owner_token"])
        store.acquire(**owned)
        observed_at = datetime(2026, 8, 1, tzinfo=UTC)
        evidence = Evidence.capture(
            source_identity="instagram:post0",
            captured_at=observed_at,
            parser_version="retained-v1",
            payload={"shortcode": "post0"},
        )
        evidence_id = store.retain_evidence(**owned, evidence=evidence)

        def canonical(conn):
            with conn.cursor() as cur:
                cur.execute("insert into social.test_canonical values(%s,1)", (run["run_id"],))
            return {"post_id": "post0", "observed_at": observed_at.isoformat()}

        store.commit_target(**owned, evidence_id=evidence_id, observed_at=observed_at, write_canonical=canonical)
    cancelled = store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    request = LocalRequest(run["run_id"], intent.digest, cancelled["generation"], "modal")
    if profile == "public":
        place(database, request)
    else:
        sql(database, "update social.instagram_ingestion_runs set execution_placement='local'")
    sql(
        database,
        """update social.instagram_ingestion_targets set attempts=2,requests=4,
        next_attempt_at=clock_timestamp()+interval '5 minutes',checkpoint='{"retained":true}'
        where run_id=%s""",
        (request.run_id,),
    )
    return LocalRequest(request.run_id, request.intent_hash, request.generation, "local")


def adopt(database, request, **overrides):
    arguments = {
        "run_id": request.run_id,
        "account": "bravotv",
        "expected_generation": request.generation,
        "expected_intent_hash": request.intent_hash,
        "idempotency_key": str(uuid4()),
    }
    arguments.update(overrides)
    return adopt_profile_version(database[0], **arguments)


def snapshot(database):
    tables = (
        "runs",
        "manifests",
        "targets",
        "outbox",
        "discovery_pages",
        "receipts",
        "evidence",
        "partitions",
        "decisions",
        "dispatches",
    )
    return {
        table: sql(database, f"select row_to_json(t) from social.instagram_ingestion_{table} t") for table in tables
    }


def test_atomic_adoption_changes_only_version_and_one_idempotent_journal(database):
    request = candidate(database)
    before = snapshot(database)
    commands = sql(database, "select count(*) from social.instagram_ingestion_commands")[0][0]
    key = str(uuid4())
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda _: adopt(database, request, idempotency_key=key), range(3)))
    assert results == [results[0]] * 3
    assert results[0]["workflow_version"] == ADOPTED_WORKFLOW_VERSION
    assert results[0]["state"] == "cancelled"
    after = snapshot(database)
    assert after["runs"][0][0].pop("workflow_version") == ADOPTED_WORKFLOW_VERSION
    assert before["runs"][0][0].pop("workflow_version") == "instagram-ingestion-v1"
    assert after == before
    assert sql(database, "select count(*) from social.instagram_ingestion_commands")[0][0] == commands + 1
    for override in ({"expected_generation": request.generation + 1}, {"expected_intent_hash": "0" * 64}):
        with pytest.raises(Conflict, match="Idempotency"):
            adopt(database, request, idempotency_key=key, **override)
    with pytest.raises(Conflict):
        adopt(database, request)  # v2 cannot be adopted under another key.


@pytest.mark.parametrize(
    "statement",
    [
        "update social.instagram_ingestion_runs set state='running'",
        "update social.instagram_ingestion_runs set state='completed'",
        "update social.instagram_ingestion_runs set execution_placement='modal'",
        "update social.instagram_ingestion_runs set execution_hold_reason='operator_hold'",
        "update social.instagram_ingestion_runs set engine='temporal'",
        "update social.instagram_ingestion_runs set workflow_version='other-version'",
        "update social.instagram_ingestion_runs set intent_hash=repeat('0',64)",
        "update social.instagram_ingestion_runs set intent=jsonb_set(intent,'{force}','true')",
        "delete from social.instagram_ingestion_targets where stage='media'; "
        "delete from social.instagram_ingestion_discovery_pages where stage='media'; "
        "delete from social.instagram_ingestion_manifests where stage='media'",
        "update social.instagram_ingestion_manifests set discovery_complete=true,sealed_at=clock_timestamp() "
        "where stage='media'",
    ],
)
def test_adoption_fails_closed_without_changing_history(database, statement):
    request = candidate(database)
    sql(database, statement)
    before = snapshot(database)
    with pytest.raises(Conflict):
        adopt(database, request)
    assert snapshot(database) == before


def test_exact_generation_hash_account_and_public_profile_required(database):
    request = candidate(database)
    for override in ({"expected_generation": 1}, {"expected_intent_hash": "0" * 64}, {"account": "other"}):
        with pytest.raises((Conflict, LookupError)):
            adopt(database, request, **override)
    sql(database, "truncate social.instagram_ingestion_runs cascade")
    request = candidate(database, profile="authenticated")
    with pytest.raises(Conflict):
        adopt(database, request)


@pytest.mark.parametrize(
    "ownership", ["live", "unknown_expiry", "sending", "accepted", "current_generation", "recent_cancellation"]
)
def test_active_and_uncertain_ownership_prevents_adoption(database, ownership):
    request = candidate(database)
    token = str(uuid4())
    if ownership in {"live", "unknown_expiry"}:
        sql(
            database,
            """update social.instagram_ingestion_targets set owner_token=%s,
            owner_expires_at=case when %s then clock_timestamp()+interval '30 seconds' else null end""",
            (token, ownership == "live"),
        )
    else:
        sql(
            database,
            """insert into social.instagram_ingestion_dispatches
            (owner_token,run_id,generation,stage,target_key,state) values(%s,%s,%s,'media','post1',%s)""",
            (
                token,
                request.run_id,
                request.generation if ownership == "current_generation" else 1,
                "accepted" if ownership != "sending" else "sending",
            ),
        )
        if ownership in {"current_generation", "recent_cancellation"}:
            sql(database, "update social.instagram_ingestion_dispatches set updated_at=now()-interval '601 seconds'")
        if ownership == "current_generation":
            sql(database, "update social.instagram_ingestion_runs set updated_at=now()-interval '601 seconds'")
    before = snapshot(database)
    with pytest.raises(Conflict, match="owner"):
        adopt(database, request)
    assert snapshot(database) == before


def drained_dispatch(database, request):
    sql(
        database,
        """insert into social.instagram_ingestion_dispatches
        (owner_token,run_id,generation,stage,target_key,state,updated_at)
        values(%s,%s,1,'media','post1','accepted',now()-interval '601 seconds')""",
        (str(uuid4()), request.run_id),
    )
    sql(database, "update social.instagram_ingestion_runs set updated_at=now()-interval '601 seconds'")


def test_safely_drained_old_dispatch_expires_with_adoption(database):
    request = candidate(database)
    drained_dispatch(database, request)
    adopt(database, request)
    assert sql(database, "select state from social.instagram_ingestion_dispatches") == [("expired",)]
    assert sql(database, "select generation,intent_hash,state from social.instagram_ingestion_runs") == [
        (request.generation, request.intent_hash, "cancelled")
    ]


def test_adoption_and_dispatch_expiry_rollback_together(database, monkeypatch):
    request = candidate(database)
    drained_dispatch(database, request)
    before = snapshot(database)
    original = Store._expire_drained_dispatches

    def fail_after_expiry(cur, run, generation):
        original(cur, run, generation)
        raise RuntimeError("injected transaction failure")

    monkeypatch.setattr(Store, "_expire_drained_dispatches", staticmethod(fail_after_expiry))
    with pytest.raises(RuntimeError):
        adopt(database, request)
    assert snapshot(database) == before


class Lease:
    active = False

    def __enter__(self):
        self.active = True
        return self

    def __exit__(self, *_):
        self.active = False


def test_cli_apply_holds_lease_for_validation_adoption_and_idempotent_replay(database, monkeypatch):
    request = candidate(database)
    lease, abort = Lease(), threading.Event()
    original = cli.validate_run

    def validate(*args, **kwargs):
        assert lease.active
        assert kwargs["expected_workflow_version"] == "instagram-ingestion-v1"
        return original(*args, **kwargs)

    monkeypatch.setattr(cli, "validate_run", validate)
    key = str(uuid4())
    result = cli.run_adoption(request, idempotency_key=key, store=database[0], lease=lease, abort=abort)
    assert not lease.active
    monkeypatch.setattr(cli, "validate_run", lambda *_a, **_k: pytest.fail("revalidated v1 after adoption"))
    assert cli.run_adoption(request, idempotency_key=key, store=database[0], lease=lease, abort=abort) == result


def test_cli_lease_loss_before_commit_rolls_back_adoption(database, monkeypatch):
    request = candidate(database)
    abort = threading.Event()
    original = Store._expire_drained_dispatches

    def lose_lease(cur, run, generation):
        original(cur, run, generation)
        abort.set()

    monkeypatch.setattr(Store, "_expire_drained_dispatches", staticmethod(lose_lease))
    before = snapshot(database)
    with pytest.raises(LostOwnership):
        cli.run_adoption(request, idempotency_key=str(uuid4()), store=database[0], lease=Lease(), abort=abort)
    assert snapshot(database) == before


def test_cli_busy_lease_opens_no_business_transaction():
    @contextmanager
    def busy():
        raise Conflict("Coordinator singleton is busy")
        yield

    @contextmanager
    def forbidden_transaction():
        pytest.fail("Business database opened before coordinator lease")
        yield

    request = LocalRequest(str(uuid4()), "c" * 64, 4, "local")
    with pytest.raises(Conflict):
        cli.run_adoption(
            request, idempotency_key="test", store=Store(forbidden_transaction), lease=busy(), abort=threading.Event()
        )


def test_default_cli_plan_has_no_configuration_database_or_lease_calls(monkeypatch, capsys):
    for name in ("validate_configuration", "CoordinatorLease", "Store", "run_adoption"):
        monkeypatch.setattr(cli, name, lambda *_a, **_k: pytest.fail("dry plan performed work"))
    assert (
        cli.main(
            [
                "--run-id",
                str(uuid4()),
                "--intent-hash",
                "c" * 64,
                "--generation",
                "4",
                "--idempotency-key",
                "offline-plan",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "plan" and result["resume"] is False


def test_cli_invalid_input_never_echoes_rejected_values(capsys):
    assert cli.main(["--run-id", "secret://not-an-argument"]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"status": "failed", "failure_class": "ValueError"}
    assert "secret" not in captured.out + captured.err


def test_adoption_preserves_nonempty_observations_receipts_and_canonical_history(database):
    request = candidate(database, history=True)
    before = snapshot(database)
    canonical_before = sql(database, "select * from social.test_canonical where run_id=%s", (request.run_id,))
    assert before["evidence"] and before["receipts"] and canonical_before
    assert datetime.fromisoformat(before["receipts"][0][0]["observed_at"]) == datetime(2026, 8, 1, tzinfo=UTC)
    adopt(database, request)
    after = snapshot(database)
    before["runs"][0][0].pop("workflow_version")
    after["runs"][0][0].pop("workflow_version")
    assert after == before
    assert sql(database, "select * from social.test_canonical where run_id=%s", (request.run_id,)) == canonical_before
