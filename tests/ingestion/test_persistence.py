"""Real isolated PostgreSQL through the non-live guard; no configured TRR services."""

import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest

from trr_backend.ingestion.domain import Conflict, Failure, LostOwnership, ReplayUnavailable, RunIntent, Stage
from trr_backend.ingestion.evidence import MAX_PAYLOAD_BYTES, Evidence
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.queries import Queries


@pytest.fixture(scope="module")
def database(tmp_path_factory, register_isolated_postgres):
    root = tmp_path_factory.mktemp("ingestion")
    binaries = Path("/opt/homebrew/opt/postgresql@17/bin")
    if not (binaries / "initdb").exists():
        pytest.skip("isolated PostgreSQL 17 binaries required")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "pg"
    subprocess.run(
        [
            str(binaries / "initdb"),
            "-D",
            str(data),
            "-A",
            "trust",
            "-U",
            "ingestion_test",
            "--no-locale",
            "--encoding=UTF8",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            str(binaries / "pg_ctl"),
            "-D",
            str(data),
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-p {port} -h 127.0.0.1 -k ''",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    dsn = f"postgresql://ingestion_test@127.0.0.1:{port}/postgres"

    @contextmanager
    def transaction():
        conn = psycopg2.connect(dsn)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    with register_isolated_postgres(host="127.0.0.1", port=port, dbname="postgres", user="ingestion_test"):
        try:
            with transaction() as conn, conn.cursor() as cur:
                cur.execute("""create role anon; create role authenticated; create role service_role;
                    create schema social;
                    create table social.scrape_runs(id uuid primary key,source_scope text,status text,
                        initiated_by text,config jsonb,cancelled_at timestamptz);
                    create table social.test_canonical(run_id uuid primary key, writes integer not null);""")
                cur.execute(
                    (
                        Path(__file__).resolve().parents[2]
                        / "supabase/migrations/20261016070000_instagram_ingestion_foundation.sql"
                    ).read_text()
                )
                cur.execute(
                    (
                        Path(__file__).resolve().parents[2]
                        / "supabase/migrations/20261016153330_instagram_ingestion_run_holds.sql"
                    ).read_text()
                )
            yield Store(transaction), transaction
        finally:
            subprocess.run(
                [str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
            )


def launch(store, key=None):
    return store.start(
        RunIntent(account="bravotv"),
        idempotency_key=key or str(uuid4()),
        engine="dbos",
        workflow_version="test-v1",
        initiated_by="fixture",
    )


def target(store, run):
    store.discover(
        run["run_id"],
        "bravotv",
        generation=run["generation"],
        stage=Stage.DETAILS,
        target_keys=["post1"],
        max_attempts=3,
        cursor_state={},
        page_id="page1",
        expected_cursor_state={},
        seal=True,
    )
    return {
        "run_id": run["run_id"],
        "account": "bravotv",
        "generation": run["generation"],
        "stage": Stage.DETAILS,
        "target_key": "post1",
    }


def test_atomic_idempotent_launch_and_payload_conflict(database):
    store, tx = database
    key = str(uuid4())
    with ThreadPoolExecutor(max_workers=8) as pool:
        runs = list(pool.map(lambda _: launch(store, key), range(16)))
    assert len({r["run_id"] for r in runs}) == 1
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_outbox where run_id=%s", (runs[0]["run_id"],))
        assert cur.fetchone()[0] == 1
    with pytest.raises(Conflict):
        store.start(
            RunIntent(account="bravotv", selected_tasks=(Stage.MEDIA,)),
            idempotency_key=key,
            engine="dbos",
            workflow_version="test-v1",
            initiated_by="fixture",
        )


def test_duplicate_worker_and_receipt_do_not_repeat_canonical_write(database):
    store, tx = database
    args = target(store, launch(store))
    reservation = store.reserve(**args)
    args["owner_token"] = str(reservation["owner_token"])
    store.acquire(**args)
    with pytest.raises(LostOwnership):
        store.acquire(**args)

    def write(conn):
        with conn.cursor() as cur:
            cur.execute("insert into social.test_canonical values(%s,1)", (args["run_id"],))
        return {"post": "post1"}

    first = store.commit_target(**args, observed_at=datetime.now(UTC), write_canonical=write)
    second = store.commit_target(**args, observed_at=datetime.now(UTC), write_canonical=write)
    assert first["receipt_id"] == second["receipt_id"]
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select writes from social.test_canonical where run_id=%s", (args["run_id"],))
        assert cur.fetchone()[0] == 1


def test_cancellation_fences_worker_resume_preserves_three_attempt_budget(database):
    store, tx = database
    run = launch(store)
    args = target(store, run)
    for attempt in range(1, 4):
        reservation = store.reserve(**args)
        token = str(reservation["owner_token"])
        acquired = store.acquire(**args, owner_token=token)
        assert acquired["attempts"] == attempt
        store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
        with pytest.raises(LostOwnership):
            store.commit_target(
                **args,
                owner_token=token,
                observed_at=datetime.now(UTC),
                write_canonical=lambda conn: pytest.fail("Stale worker invoked canonical writer"),
            )
        resumed = store.control(run["run_id"], "bravotv", action="resume", idempotency_key=str(uuid4()))
        assert resumed["workflow_version"] == run["workflow_version"]
        args["generation"] = resumed["generation"]
    assert store.reserve(**args) is None


def test_failed_canonical_write_rolls_back_and_can_retry_same_owner(database):
    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    store.acquire(**args)

    def crash(conn):
        with conn.cursor() as cur:
            cur.execute("insert into social.test_canonical values(%s,1)", (args["run_id"],))
        raise RuntimeError("simulated failure before receipt")

    with pytest.raises(RuntimeError):
        store.commit_target(**args, observed_at=datetime.now(UTC), write_canonical=crash)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.test_canonical where run_id=%s", (args["run_id"],))
        assert cur.fetchone()[0] == 0
        cur.execute("select outcome from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],))
        assert cur.fetchone()[0] == "unresolved"


@pytest.mark.parametrize(
    "code,blocked,expected",
    [
        ("identity_policy_unavailable", False, "identity_policy_unavailable"),
        ("credential_snapshot_unavailable", True, "credential_snapshot_unavailable"),
        ("https://private.example/?token=secret", False, "activity_failed"),
        ("unsafe private error text", True, "activity_failed"),
    ],
)
def test_operational_failure_diagnostics_roundtrip_preserves_policy_and_fence(database, code, blocked, expected):
    store, tx = database
    run = launch(store)
    owned = target(store, run)
    owned["owner_token"] = str(store.reserve(**owned)["owner_token"])
    store.acquire(**owned)
    retry_at = datetime.now(UTC) + timedelta(minutes=1)
    failure = Failure(code, retry_at=retry_at, identity_blocked=blocked)
    store.checkpoint_failure(**owned, failure=failure, checkpoint={"cursor": "kept"}, requests=0)
    diagnostic = Queries(tx).diagnostics(run["run_id"], "bravotv")["targets"][0]
    assert diagnostic["last_error_code"] == expected
    assert datetime.fromisoformat(diagnostic["retry_at"]) == retry_at
    assert diagnostic["blocked_reason"] == (expected if blocked else None)
    assert (diagnostic["attempts"], diagnostic["requests"], diagnostic["max_attempts"]) == (1, 0, 3)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select checkpoint from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],))
        assert cur.fetchone()[0] == {"cursor": "kept", "last_error_code": expected}
    with pytest.raises(LostOwnership):
        store.checkpoint_failure(**owned, failure=Failure("stale_worker"), checkpoint={}, requests=0)
    store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    resumed = store.control(run["run_id"], "bravotv", action="resume", idempotency_key=str(uuid4()))
    assert resumed["generation"] == 3
    after = Queries(tx).diagnostics(run["run_id"], "bravotv")["targets"][0]
    assert (after["attempts"], after["requests"], after["max_attempts"], after["last_error_code"]) == (
        1,
        0,
        3,
        expected,
    )


def test_discovery_replay_after_commit_is_idempotent_and_cannot_rewind(database):
    store, tx = database
    run = launch(store)
    args = {
        "run_id": run["run_id"],
        "account": "bravotv",
        "generation": 1,
        "stage": Stage.DETAILS,
        "max_attempts": 3,
        "target_keys": ["a"],
        "page_id": "first",
        "expected_cursor_state": {},
        "cursor_state": {"cursor": "next"},
    }
    store.discover(**args)
    final = dict(
        args,
        page_id="last",
        target_keys=["b"],
        expected_cursor_state={"cursor": "next"},
        cursor_state={"cursor": None},
        seal=True,
    )
    store.discover(**final)
    store.discover(**final)  # worker died after commit, before engine acknowledgement
    store.discover(**args)  # old delivery cannot move the cursor backwards
    with pytest.raises(Conflict):
        store.discover(**dict(args, cursor_state={"cursor": "different"}))
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='post_details'",
            (run["run_id"],),
        )
        assert cur.fetchone()[0] == {"cursor": None}
        cur.execute("select count(*) from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],))
        assert cur.fetchone()[0] == 2
    other = launch(store)
    store.discover(**dict(args, run_id=other["run_id"]))
    with pytest.raises(Conflict, match="cursor changed"):
        store.discover(**dict(args, run_id=other["run_id"], page_id="out-of-order"))


def insert_evidence(tx, run_id, evidence, *, target_key="post1", session_timezone="UTC"):
    evidence_id = str(uuid4())
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select set_config('TimeZone',%s,true)", (session_timezone,))
        cur.execute(
            """insert into social.instagram_ingestion_evidence
            (evidence_id,run_id,stage,target_key,source_identity,captured_at,expires_at,
            content_hash,parser_version,schema_version,payload)
            values(%s,%s,'post_details',%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                evidence_id,
                run_id,
                target_key,
                evidence.source_identity,
                evidence.captured_at,
                evidence.expires_at,
                evidence.content_hash,
                evidence.parser_version,
                evidence.schema_version,
                evidence.payload_json,
            ),
        )
    return evidence_id


@pytest.mark.parametrize("capture", [datetime(2026, 10, 16, tzinfo=UTC), datetime(2026, 3, 1, tzinfo=UTC)])
def test_evidence_retention_is_elapsed_utc_across_both_dst_transitions(database, capture):
    store, tx = database
    run = launch(store)
    target(store, run)
    evidence = Evidence.capture(
        source_identity="instagram:post:post1",
        captured_at=capture,
        parser_version="fixture-v1",
        payload={"pk": "post1"},
    )
    insert_evidence(tx, run["run_id"], evidence, session_timezone="America/New_York")


def test_capture_and_database_share_exact_utf8_size_limit(database):
    store, tx = database
    run = launch(store)
    target(store, run)
    # This expands beyond 4 MiB in JSONB::text, but the canonical text fits.
    evidence = Evidence.capture(
        source_identity="instagram:page:1",
        captured_at=datetime.now(UTC),
        parser_version="fixture-v1",
        payload={"items": [{"id": 1}] * 400000},
    )
    assert len(evidence.payload_json.encode()) < MAX_PAYLOAD_BYTES
    evidence_id = insert_evidence(tx, run["run_id"], evidence)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select octet_length(payload),payload from social.instagram_ingestion_evidence where evidence_id=%s",
            (evidence_id,),
        )
        size, payload = cur.fetchone()
        assert size == len(evidence.payload_json.encode())
        assert payload == evidence.payload_json


@pytest.mark.parametrize("different_run", [False, True])
def test_receipt_rejects_mixed_up_evidence_before_canonical_write(database, different_run):
    store, tx = database
    run = launch(store)
    args = target(store, run)
    other = launch(store) if different_run else run
    if different_run:
        target(store, other)
        evidence_target = "post1"
    else:
        evidence_target = "post2"
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts)
                values(%s,'post_details','post2',3)""",
                (run["run_id"],),
            )
    evidence = Evidence.capture(
        source_identity="instagram:post:" + evidence_target,
        captured_at=datetime.now(UTC),
        parser_version="v1",
        payload={"pk": evidence_target},
    )
    evidence_id = insert_evidence(tx, other["run_id"], evidence, target_key=evidence_target)
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    store.acquire(**args)
    with pytest.raises(Conflict, match="Evidence does not belong"):
        store.commit_target(
            **args,
            observed_at=datetime.now(UTC),
            evidence_id=evidence_id,
            write_canonical=lambda conn: pytest.fail("Mixed evidence reached canonical writer"),
        )
    # The database also rejects callers that bypass the service validation.
    with pytest.raises(psycopg2.errors.ForeignKeyViolation), tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_receipts
            (receipt_id,run_id,stage,target_key,owner_token,owner_generation,evidence_id,observed_at,outcome,canonical_identity)
            values(%s,%s,'post_details','post1',%s,1,%s,clock_timestamp(),'committed','{}')""",
            (str(uuid4()), run["run_id"], args["owner_token"], evidence_id),
        )


def test_retained_evidence_is_fenced_pinned_and_expired_without_losing_provenance(database):
    store, tx = database
    run = launch(store)
    args = target(store, run)
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    store.acquire(**args)
    evidence = Evidence.capture(
        source_identity="instagram:post:post1",
        captured_at=datetime(2025, 1, 1, tzinfo=UTC),
        parser_version="v1",
        payload={"pk": "post1", "cookies": {"secret": "forbidden"}},
    )
    evidence_id = store.retain_evidence(**args, evidence=evidence)
    queries = Queries(tx)
    assert queries.evidence(evidence_id, "bravotv")["payload"] == {"pk": "post1"}
    assert queries.evidence(evidence_id, "bravotv")["pinned"]
    with pytest.raises(LookupError):
        queries.evidence(evidence_id, "unrelated")
    store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    with pytest.raises(LostOwnership):
        store.retain_evidence(**args, evidence=evidence)
    store.expire_evidence()
    assert queries.evidence(evidence_id, "bravotv")["payload"] == {"pk": "post1"}
    # Model the final application completion checkpoint; retention is separate
    # from queries and keeps compact provenance even after discarding payload.
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_ingestion_runs set state='completed' where run_id=%s", (run["run_id"],))
    with pytest.raises(ReplayUnavailable):
        queries.evidence(evidence_id, "bravotv")
    store.expire_evidence()
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select payload,content_hash,captured_at from social.instagram_ingestion_evidence where evidence_id=%s",
            (evidence_id,),
        )
        payload, digest, captured = cur.fetchone()
        assert payload is None and digest == evidence.content_hash and captured == evidence.captured_at
