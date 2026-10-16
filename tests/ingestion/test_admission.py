"""Capacity SQL against disposable PostgreSQL; probe is always injected."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from tests.ingestion import test_persistence
from trr_backend.ingestion.admission import Admission
from trr_backend.ingestion.command_service import ServiceUnavailableError
from trr_backend.ingestion.domain import Conflict, RunIntent, Stage, stable_json

base_database = test_persistence.database
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def database(base_database):
    store, transaction = base_database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute((ROOT / "supabase/migrations/20261016070100_instagram_ingestion_workflows.sql").read_text())
        cur.execute("""create table social.scrape_jobs(id uuid primary key,run_id uuid,platform text,
            config jsonb not null default '{}',metadata jsonb not null default '{}',job_type text,
            status text,worker_id text,claimed_at timestamptz,completed_at timestamptz,
            heartbeat_at timestamptz,started_at timestamptz,created_at timestamptz default clock_timestamp());
            create table social.account_auth_cooldown(platform text,account_handle text,cooldown_until timestamptz);
        """)
        cur.execute((ROOT / "supabase/migrations/20260911121000_instagram_detail_request_lanes.sql").read_text())
    return store, transaction


@pytest.fixture(autouse=True)
def clear_isolated_state(database):
    with database[1]() as conn, conn.cursor() as cur:
        cur.execute("""truncate social.scrape_jobs,social.instagram_ingestion_runs,
            social.account_auth_cooldown,social.instagram_detail_request_lanes cascade""")


def sql(database, statement, params=()):
    with database[1]() as conn, conn.cursor() as cur:
        cur.execute(statement, params)
        return cur.fetchall() if cur.description else None


def candidate(database, stage="post_details", *, workers=12, comments_workers=24, account="bravotv", tasks=None):
    intent = RunIntent(
        account=account,
        detail_worker_count=workers,
        comments_worker_count=comments_workers,
        **({"selected_tasks": tasks} if tasks else {}),
    )
    run = database[0].start(
        intent, idempotency_key=str(uuid4()), engine="dbos", workflow_version="v1", initiated_by="test"
    )
    return dict(run, account=account, stage=stage, target_key="candidate")


def legacy(database, stage, *, count=1, platform="instagram", status="running", metadata=None, run_id=None):
    for _ in range(count):
        sql(
            database,
            """insert into social.scrape_jobs(id,run_id,platform,config,status,metadata)
            values(%s,%s,%s,%s::jsonb,%s,%s::jsonb)""",
            (
                str(uuid4()),
                run_id,
                platform,
                stable_json({"stage": stage, "account": "bravotv"}),
                status,
                stable_json(metadata or {}),
            ),
        )


def reserve(database, item, *, count=1, state=None):
    store, _ = database
    stage = Stage(item["stage"])
    keys = [uuid4().hex for _ in range(count)]
    store.discover(
        item["run_id"],
        item["account"],
        generation=1,
        stage=stage,
        target_keys=keys,
        max_attempts=3 if stage == Stage.DETAILS else 12,
        cursor_state={},
        page_id=uuid4().hex,
        expected_cursor_state={},
    )
    for key in keys:
        lease = store.reserve(item["run_id"], item["account"], generation=1, stage=stage, target_key=key)
        if state:
            sql(
                database,
                """insert into social.instagram_ingestion_dispatches
                (owner_token,run_id,generation,stage,target_key,state) values(%s,%s,1,%s,%s,%s)""",
                (str(lease["owner_token"]), item["run_id"], stage.value, key, state),
            )


def admission(database, *, env=None, identity_key=None, probe_available=True):
    probes = []
    active = []

    @contextmanager
    def read_only():
        with database[1]() as conn:
            with conn.cursor() as cur:
                cur.execute("set transaction read only")
            active.append(conn)
            try:
                yield conn
            finally:
                active.pop()

    def probe(**kwargs):
        assert not active, "Fresh-session probe ran inside capacity read transaction"
        probes.append(kwargs)
        return {"available": probe_available}

    return Admission(
        read_connection=read_only, fresh_session_probe=probe, environ=env or {}, identity_key=identity_key
    ), probes


def test_default_caps_and_readonly_launch_preserve_requested_counts(database):
    subject, probes = admission(database)
    payload = InstagramIngestionLaunchV2(detail_worker_count=12, comments_worker_count=24)
    subject.validate_launch("@BravoTV", payload)
    assert payload.detail_worker_count == 12 and payload.comments_worker_count == 24
    assert subject.combined_cap == 10 and subject.catalog_cap == 4
    assert subject.stage_caps == {
        "posts": 8,
        "shared_account_discovery": 3,
        "shared_account_posts": 5,
        "comments": 4,
        "media_mirror": 6,
    }
    assert subject.physical_caps == {"posts": 8, "comments": 4, "media": 1}
    assert probes == [{"requested_sessions": 1, "max_probe_sessions": 15}]
    assert sql(database, "select count(*) from social.instagram_ingestion_runs") == [(0,)]


def test_requested_detail_count_clamps_at_existing_catalog_cap(database):
    item = candidate(database)
    subject, _ = admission(database)
    assert subject.admit(item)
    reserve(database, item, count=4)
    assert not subject.admit(item)
    assert sql(database, "select intent->>'detail_worker_count' from social.instagram_ingestion_runs") == [("12",)]


def test_combined_budget_counts_legacy_and_excludes_media(database):
    legacy(database, "shared_account_posts", count=3)
    legacy(database, "posts", count=3)
    legacy(database, "comments_scrapling", count=4)
    subject, _ = admission(database)
    assert not subject.admit(candidate(database, "listing"))
    assert subject.admit(candidate(database, "media"))
    sql(database, "delete from social.scrape_jobs where id=(select id from social.scrape_jobs limit 1)")
    assert subject.admit(candidate(database, "listing"))


@pytest.mark.parametrize("status", ["pending", "completed"])
def test_dispatched_unclaimed_and_nonterminal_legacy_calls_retain_capacity(database, status):
    legacy(
        database,
        "comments",
        status=status,
        metadata={"dispatch": {"remote_invocation_id": "pending-call", "remote_invocation_status": "unknown"}},
    )
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "1"})
    item = candidate(database, "listing")
    assert not subject.admit(item)
    sql(
        database,
        "update social.scrape_jobs set status='completed',metadata=%s::jsonb",
        (stable_json({"dispatch": {"remote_invocation_id": "pending-call", "remote_invocation_status": "completed"}}),),
    )
    assert subject.admit(item)


def test_legacy_lease_metadata_expiry_and_malformed_read_fail_closed(database):
    until = datetime.now(UTC) + timedelta(minutes=10)
    legacy(
        database,
        "shared_account_posts",
        status="pending",
        metadata={"dispatch": {"dispatch_reservation_id": "unclaimed", "lease_expires_at": until.isoformat()}},
    )
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "1"})
    item = candidate(database, "listing")
    assert not subject.admit(item)
    sql(
        database,
        "update social.scrape_jobs set metadata=%s::jsonb",
        (
            stable_json(
                {
                    "dispatch": {
                        "dispatch_reservation_id": "unclaimed",
                        "lease_expires_at": (until - timedelta(hours=1)).isoformat(),
                    }
                }
            ),
        ),
    )
    assert subject.admit(item)
    sql(
        database,
        "update social.scrape_jobs set metadata=%s::jsonb",
        (stable_json({"dispatch": {"dispatch_reservation_id": "unclaimed", "lease_expires_at": "invalid"}}),),
    )
    assert not subject.admit(item)
    with pytest.raises(ServiceUnavailableError):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())


def test_new_unclaimed_leases_and_ambiguous_send_hold_capacity_without_double_count(database):
    item = candidate(database, workers=2)
    reserve(database, item, count=1, state="sending")
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "2"})
    assert subject.admit(candidate(database, "listing"))  # same owner counted once
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "1"})
    assert not subject.admit(candidate(database, "listing"))
    sql(database, "update social.instagram_ingestion_targets set owner_expires_at=clock_timestamp()-interval '1 hour'")
    assert not subject.admit(candidate(database, "listing"))  # unresolved remote send remains conservative
    sql(database, "update social.instagram_ingestion_dispatches set state='expired'")
    assert subject.admit(candidate(database, "listing"))


def test_global_stage_and_existing_physical_function_caps_include_other_platforms(database):
    legacy(database, "shared_account_posts", count=5, platform="tiktok")
    subject, _ = admission(database)
    assert not subject.admit(candidate(database))
    assert subject.admit(candidate(database, "listing"))
    legacy(database, "posts", count=3, platform="youtube")
    assert not subject.admit(candidate(database, "listing"))
    assert subject.admit(candidate(database, "comments"))


def test_frozen_requested_comments_count_is_a_per_run_dispatch_ceiling(database):
    item = candidate(database, "comments", comments_workers=1)
    reserve(database, item)
    subject, _ = admission(database)
    assert not subject.admit(item)
    assert subject.admit(candidate(database, "comments", comments_workers=2))


def test_account_cooldown_and_exact_identity_block_are_scoped(database):
    subject, _ = admission(database)
    item = candidate(database)
    sql(
        database,
        "insert into social.account_auth_cooldown values('instagram','unrelated',clock_timestamp()+interval '1 hour')",
    )
    sql(database, "insert into social.instagram_detail_request_lanes(identity_key,blocked) values('unrelated',true)")
    assert subject.admit(item)
    exact, _ = admission(database, identity_key="unrelated")
    assert not exact.admit(item)
    own, _ = admission(database, identity_key="own-identity")
    assert own.admit(item)
    sql(
        database,
        "insert into social.account_auth_cooldown values('instagram','bravotv',clock_timestamp()+interval '1 hour')",
    )
    assert not subject.admit(item)
    with pytest.raises(Conflict):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())


def test_probe_failure_and_bad_candidate_are_closed_without_mutating_state(database):
    item = candidate(database)
    subject, probes = admission(database, probe_available=False)
    assert not subject.admit(item) and probes
    with pytest.raises(ServiceUnavailableError):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())
    subject, probes = admission(database)
    for update in ({"stage": "unknown"}, {"account": "unrelated"}, {"generation": 99}, {"workflow_version": "future"}):
        assert not subject.admit(dict(item, **update))
    only_media = candidate(database, "comments", tasks=(Stage.MEDIA,))
    assert not subject.admit(only_media)
    assert not probes
    assert sql(database, "select count(*) from social.instagram_ingestion_targets") == [(0,)]


def test_env_budget_fallback_and_lowered_physical_caps_are_honored(database):
    env = {"SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET": "3", "TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT": "1"}
    subject, _ = admission(database, env=env)
    assert subject.combined_cap == 3
    legacy(database, "shared_account_posts")
    assert not subject.admit(candidate(database, "listing"))
    env["SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET"] = "2"
    assert admission(database, env=env)[0].combined_cap == 2


def test_media_uses_existing_single_physical_function_without_consuming_combined_budget(database):
    legacy(database, "comment_media_mirror")
    subject, _ = admission(database)
    assert not subject.admit(candidate(database, "media"))
    assert subject.admit(candidate(database, "listing"))


def test_read_connection_and_probe_exceptions_are_fail_closed(database):
    item = candidate(database)

    @contextmanager
    def unavailable():
        raise RuntimeError("Unavailable read connection")
        yield  # pragma: no cover - context manager protocol only

    subject = Admission(
        read_connection=unavailable, fresh_session_probe=lambda **_: pytest.fail("Probe after failed read"), environ={}
    )
    assert not subject.admit(item)
    with pytest.raises(ServiceUnavailableError):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())
    subject, _ = admission(database)

    def failed_probe(**kwargs):
        raise RuntimeError("Unavailable fresh session")

    subject.fresh_session_probe = failed_probe
    assert not subject.admit(item)
    with pytest.raises(ServiceUnavailableError):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())


_HISTORICAL_DISPATCHES = [
    {"remote_invocation_id": "historical", "remote_invocation_status": "running"},
    {"remote_invocation_id": "historical", "remote_invocation_status": "unknown"},
    {"remote_invocation_id": "historical", "remote_invocation_status": "pending"},
    {"dispatch_reservation_id": "historical", "remote_invocation_status": "unknown"},
]


def set_legacy_activity(database, *, hours_ago, completed=True):
    then = datetime.now(UTC) - timedelta(hours=hours_ago)
    sql(
        database,
        """update social.scrape_jobs set completed_at=%s,heartbeat_at=%s,
        started_at=%s,claimed_at=%s,created_at=%s""",
        (then if completed else None, then, then, then, then),
    )


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
@pytest.mark.parametrize("dispatch", _HISTORICAL_DISPATCHES)
def test_old_terminal_dispatch_metadata_does_not_hold_capacity_forever(database, status, dispatch):
    metadata = {"dispatch": dispatch}
    legacy(database, "comments", status=status, metadata=metadata)
    set_legacy_activity(database, hours_ago=25)
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "1"})
    assert subject._snapshot("bravotv")["counts"] == []
    subject.validate_launch("bravotv", InstagramIngestionLaunchV2())
    assert sql(database, "select status,metadata from social.scrape_jobs") == [(status, metadata)]


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
@pytest.mark.parametrize("dispatch", [_HISTORICAL_DISPATCHES[1], _HISTORICAL_DISPATCHES[3]])
def test_recent_terminal_remote_or_reserved_ambiguity_still_consumes_capacity(database, status, dispatch):
    legacy(database, "comments", status=status, metadata={"dispatch": dispatch})
    set_legacy_activity(database, hours_ago=1)
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "1"})
    with pytest.raises(ServiceUnavailableError, match="capacity"):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())


@pytest.mark.parametrize("with_remote_id", [False, True])
def test_explicit_future_lease_on_old_terminal_job_still_holds_capacity(database, with_remote_id):
    dispatch = {
        "remote_invocation_status": "unknown",
        "dispatch_reservation_id": "reserved",
        "lease_expires_at": (datetime.now(UTC) + timedelta(minutes=10)).isoformat(),
    }
    if with_remote_id:
        dispatch["remote_invocation_id"] = "accepted"
    legacy(database, "comments", status="cancelled", metadata={"dispatch": dispatch})
    set_legacy_activity(database, hours_ago=25)
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "1"})
    with pytest.raises(ServiceUnavailableError, match="capacity"):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())


def test_old_terminal_missing_completed_at_uses_established_timestamp_fallback(database):
    legacy(database, "comments", status="cancelled", metadata={"dispatch": _HISTORICAL_DISPATCHES[1]})
    set_legacy_activity(database, hours_ago=25, completed=False)
    subject, _ = admission(database)
    assert subject._snapshot("bravotv")["counts"] == []


def test_terminal_ambiguity_with_all_timestamps_missing_fails_closed(database):
    legacy(database, "comments", status="cancelled", metadata={"dispatch": _HISTORICAL_DISPATCHES[3]})
    sql(
        database,
        """update social.scrape_jobs set completed_at=null,heartbeat_at=null,
        started_at=null,claimed_at=null,created_at=null""",
    )
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "1"})
    with pytest.raises(ServiceUnavailableError, match="capacity"):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())


@pytest.mark.parametrize("status", ["pending", "running", "claimed", "processing"])
def test_old_nonterminal_remote_ambiguity_still_holds_capacity(database, status):
    legacy(database, "comments", status=status, metadata={"dispatch": _HISTORICAL_DISPATCHES[1]})
    set_legacy_activity(database, hours_ago=25)
    subject, _ = admission(database, env={"SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET": "1"})
    with pytest.raises(ServiceUnavailableError, match="capacity"):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())
