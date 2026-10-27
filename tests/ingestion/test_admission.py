"""Capacity SQL against disposable PostgreSQL; probe is always injected."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from tests.ingestion import test_persistence
from trr_backend.ingestion.admission import Admission
from trr_backend.ingestion.command_service import ServiceUnavailableError
from trr_backend.ingestion.domain import Conflict, RunIntent, SourceProfile, Stage, stable_json

base_database = test_persistence.database
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def database(base_database):
    store, transaction = base_database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute((ROOT / "supabase/migrations/20261016070100_instagram_ingestion_workflows.sql").read_text())
        cur.execute(
            (ROOT / "supabase/migrations/20261016173915_instagram_ingestion_execution_placement.sql").read_text()
        )
        cur.execute((ROOT / "supabase/migrations/20261023140000_instagram_execution_policy.sql").read_text())
        cur.execute("""create table social.scrape_jobs(id uuid primary key,run_id uuid,platform text,
            config jsonb not null default '{}',metadata jsonb not null default '{}',job_type text,
            status text,worker_id text,claimed_at timestamptz,completed_at timestamptz,
            heartbeat_at timestamptz,started_at timestamptz,created_at timestamptz default clock_timestamp());
            create table social.account_auth_cooldown(platform text,account_handle text,cooldown_until timestamptz,
            blocker_kind text default 'auth');
        """)
        cur.execute((ROOT / "supabase/migrations/20261023141000_instagram_admission_candidates.sql").read_text())
        cur.execute((ROOT / "supabase/migrations/20260911121000_instagram_detail_request_lanes.sql").read_text())
        cur.execute((ROOT / "supabase/migrations/20261022120000_instagram_server_retry_after.sql").read_text())
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
        scope="recent_window",
        date_start=datetime(2026, 9, 1, tzinfo=UTC),
        date_end=datetime(2026, 10, 1, tzinfo=UTC),
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


@pytest.mark.parametrize("stage", ["posts_scrapling", "tiktok_posts_scrapling", "threads_posts_scrapling"])
@pytest.mark.parametrize("status", ["running", "completed"])
def test_scrapling_posts_count_against_posts_capacity_without_blocking_comments(database, stage, status):
    legacy(
        database,
        stage,
        platform="tiktok",
        status=status,
        metadata={"dispatch": {"remote_invocation_id": "draining-call", "remote_invocation_status": "running"}},
    )
    subject, _ = admission(database, env={"TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT": "1"})
    snapshot = subject._snapshot("bravotv")
    assert snapshot["counts"][0]["stage"] == "posts"
    assert snapshot["counts"][0]["total"] == 1
    assert not subject.admit(candidate(database, "listing"))
    assert subject.admit(candidate(database, "comments"))


def test_unrecognized_legacy_stage_still_blocks_admission(database):
    legacy(database, "unrecognized_provider_stage", platform="tiktok")
    subject, _ = admission(database)
    assert not subject.admit(candidate(database, "comments"))


def test_local_account_cooldown_is_ignored_but_exact_identity_block_and_server_wait_apply(database):
    subject, _ = admission(database, identity_key="selected-identity")
    item = candidate(database)
    sql(
        database,
        "insert into social.account_auth_cooldown(platform,account_handle,cooldown_until) "
        "values('instagram','unrelated',clock_timestamp()+interval '1 hour')",
    )
    sql(database, "insert into social.instagram_detail_request_lanes(identity_key,blocked) values('unrelated',true)")
    assert subject.admit(item)
    exact, _ = admission(database, identity_key="unrelated")
    assert not exact.admit(item)
    own, _ = admission(database, identity_key="own-identity")
    assert own.admit(item)
    sql(
        database,
        "insert into social.account_auth_cooldown(platform,account_handle,cooldown_until) "
        "values('instagram','bravotv',clock_timestamp()+interval '1 hour')",
    )
    assert subject.admit(item)
    sql(
        database,
        "insert into social.instagram_detail_request_lanes(identity_key,server_retry_after_until) "
        "values(%s,clock_timestamp()+interval '1 hour')",
        (subject.identity_key,),
    )
    assert not subject.admit(item)
    with pytest.raises(Conflict):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())


def test_only_selected_account_explicit_checkpoint_blocks_admission(database):
    subject, _ = admission(database)
    item = candidate(database)
    sql(
        database,
        "insert into social.account_auth_cooldown(platform,account_handle,blocker_kind) "
        "values('instagram','unrelated','checkpoint')",
    )
    assert subject.admit(item)
    sql(
        database,
        "insert into social.account_auth_cooldown(platform,account_handle,blocker_kind) "
        "values('instagram','bravotv','checkpoint')",
    )
    assert not subject.admit(item)


@pytest.mark.parametrize("server_seconds", [30, -30])
def test_legacy_pace_reservation_cannot_extend_actual_server_wait(database, monkeypatch, server_seconds):
    from uuid import uuid4

    from psycopg2.extras import RealDictCursor

    from trr_backend.db import pg
    from trr_backend.socials.instagram.comments_scrapling import fetcher

    key = uuid4().hex
    sql(
        database,
        "create table if not exists social.ig_comment_rate_pace("
        "rate_key text primary key,last_start timestamptz,cooldown_until timestamptz,"
        "server_retry_after_until timestamptz)",
    )
    sql(
        database,
        "insert into social.ig_comment_rate_pace values("
        "%s,clock_timestamp()+interval '4 hour',clock_timestamp()+interval '4 hour',"
        "clock_timestamp()+make_interval(secs=>%s))",
        (key, server_seconds),
    )

    @contextmanager
    def cursor(*, conn, **_kwargs):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur

    monkeypatch.setattr(pg, "db_connection", lambda **_kwargs: database[1]())
    monkeypatch.setattr(pg, "db_cursor", cursor)
    monkeypatch.setattr(fetcher, "_wait_for_global_api_cooldown", lambda **_kwargs: True)
    monkeypatch.setattr(fetcher, "_persisted_global_api_cooldown_remaining", lambda _key: 0)
    waits = []
    monkeypatch.setattr(fetcher.time, "sleep", waits.append)
    result = fetcher._try_advisory_lock_pace(key=key, delay_seconds=0, deadline=None)
    assert result["acquired"] and result["paced"]
    saved = sql(
        database,
        "select extract(epoch from(last_start-clock_timestamp())),cooldown_until,"
        "server_retry_after_until>clock_timestamp() from social.ig_comment_rate_pace where rate_key=%s",
        (key,),
    )[0]
    assert saved[1] is None
    if server_seconds > 0:
        assert 25 < float(saved[0]) <= 30 and saved[2]
        assert len(waits) == 1 and 25 < waits[0] <= 30
    else:
        assert float(saved[0]) <= 0 and not saved[2] and waits == []


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


def test_full_history_validation_gate_denies_dispatch_before_capacity_probe(database):
    run = database[0].start(
        RunIntent(account="bravotv"),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="v1",
        initiated_by="test",
    )
    subject, probes = admission(database)
    assert not subject.admit(dict(run, account="bravotv", stage="post_details", target_key="candidate"))
    assert probes == []


@pytest.mark.parametrize("lease", ["malformed", " "])
def test_old_terminal_malformed_lease_remains_fail_closed(database, lease):
    item = candidate(database, stage="comments")
    legacy(
        database,
        "comments",
        status="completed",
        metadata={"dispatch": {"remote_invocation_status": "unknown", "lease_expires_at": lease}},
    )
    set_legacy_activity(database, hours_ago=25)
    subject, probes = admission(database)
    assert not subject.admit(item)
    with pytest.raises(ServiceUnavailableError):
        subject.validate_launch("bravotv", InstagramIngestionLaunchV2())
    assert probes == []


def test_candidate_prefilter_preserves_busy_counts_with_large_history(database):
    sql(
        database,
        """insert into social.scrape_jobs(id,platform,config,status,metadata,created_at)
        select gen_random_uuid(),'instagram','{"stage":"comments"}'::jsonb,'completed',
          jsonb_build_object('payload',repeat(md5(n::text),100),
            'dispatch',jsonb_build_object('remote_invocation_id','old','remote_invocation_status','unknown')),
          clock_timestamp()-interval '25 hours' from generate_series(1,2000) n""",
    )
    legacy(database, "comments", count=2, status="RuNnInG")
    legacy(database, "comments", status="unexpected", metadata={"dispatch": _HISTORICAL_DISPATCHES[1]})
    legacy(database, "comments", platform="youtube")
    subject, _ = admission(database)
    counts = subject._snapshot("bravotv")["counts"]
    assert sum(row["total"] for row in counts) == 4
    assert sum(row["total"] for row in counts if row["platform"] == "instagram") == 3
    assert sql(database, "select count(*) from social.scrape_jobs") == [(2004,)]


def test_current_local_policy_allows_two_but_preserves_physical_cap(database):
    from trr_backend.ingestion.execution_policy import ExecutionPolicy

    item = candidate(database, stage="comments", comments_workers=1)
    sql(
        database,
        "update social.instagram_ingestion_runs set execution_placement='local' where run_id=%s",
        (item["run_id"],),
    )
    original = sql(
        database, "select intent_hash from social.instagram_ingestion_runs where run_id=%s", (item["run_id"],)
    )[0][0]
    ExecutionPolicy(database[1]).set_limits(
        item["run_id"],
        item["account"],
        expected_generation=1,
        expected_intent_hash=original,
        expected_revision=0,
        comments_parallelism=2,
        idempotency_key=str(uuid4()),
        initiated_by="test",
    )
    reserve((test_persistence.Store(database[1], execution_placement="local"), database[1]), item)
    subject, _ = admission(database)
    assert subject.admit(item)
    capped, probes = admission(database, env={"TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT": "1"})
    assert not capped.admit(item)
    assert probes == []
    reserve((test_persistence.Store(database[1], execution_placement="local"), database[1]), item)
    assert not subject.admit(item)
    assert sql(
        database,
        "select intent_hash,intent->>'comments_worker_count' from social.instagram_ingestion_runs where run_id=%s",
        (item["run_id"],),
    ) == [(original, "1")]


def bounded_candidate(database, monkeypatch, *, useful=True):
    """Use the actual durable grant and phase commands, never a fake snapshot."""
    from trr_backend.ingestion.bounded_recovery import USEFUL_PROGRESS_POLICY

    intent = RunIntent(
        account="bravotv",
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
        source_profile=SourceProfile.for_mode("public"),
        selected_tasks=(Stage.COMMENTS, Stage.MEDIA),
        comments_worker_count=1,
    )
    store, tx = database
    run = store.start(
        intent, idempotency_key=str(uuid4()), engine="dbos", workflow_version="v1", initiated_by="fixture"
    )
    for stage in (Stage.COMMENTS, Stage.MEDIA):
        store.discover(
            run["run_id"],
            intent.account,
            generation=1,
            stage=stage,
            target_keys=["candidate"],
            max_attempts=12,
            cursor_state={},
            page_id=str(uuid4()),
            expected_cursor_state={},
            seal=True,
        )
    sql(
        database,
        "update social.instagram_ingestion_runs set execution_placement='local' where run_id=%s",
        (run["run_id"],),
    )
    local = test_persistence.Store(tx, execution_placement="local")
    cancelled = local.control(run["run_id"], intent.account, action="cancel", idempotency_key=str(uuid4()))
    local.hold_cancelled_run(
        run["run_id"],
        intent.account,
        cancelled["generation"],
        "bounded_recovery_window_expired",
        str(uuid4()),
        "fixture",
    )
    window = local.resume_bounded_recovery_window(
        run["run_id"],
        intent.account,
        expected_generation=cancelled["generation"],
        expected_intent_hash=intent.digest,
        expected_workflow_version="v1",
        source_revision="fixture-source",
        initiated_by="fixture",
        stop_policy=USEFUL_PROGRESS_POLICY if useful else None,
    )
    env = {
        "TRR_INSTAGRAM_RECOVERY_WINDOW_ID": window["window_id"],
        "TRR_INSTAGRAM_LOCAL_WORKER_RUN_ID": run["run_id"],
        "TRR_INSTAGRAM_LOCAL_WORKER_GENERATION": str(window["generation"]),
        "TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION": "fixture-source",
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    binding = {
        "run_id": run["run_id"],
        "account": intent.account,
        "generation": window["generation"],
        "intent_hash": intent.digest,
        "window_id": window["window_id"],
        "source_revision": "fixture-source",
    }
    item = dict(run, account=intent.account, generation=window["generation"], stage="media", target_key="candidate")
    return SimpleNamespace(store=local, item=item, binding=binding, env=env)


def advance_bounded(candidate, phase):
    from trr_backend.ingestion.bounded_recovery_phase import transition

    transition(candidate.store, **candidate.binding, to_phase="draining_comments", reason="comments_stalled")
    if phase == "media":
        transition(candidate.store, **candidate.binding, to_phase="media", reason="comments_drained")


def test_useful_window_admission_obeys_durable_phase_with_ready_comments(database, monkeypatch):
    bounded = bounded_candidate(database, monkeypatch)
    subject, probes = admission(database, env=bounded.env)
    comment = dict(bounded.item, stage="comments")
    assert subject._snapshot("bravotv", bounded.item["run_id"])["run"]["_comments_ready"] is True
    assert subject.admit(comment) and not subject.admit(bounded.item)
    advance_bounded(bounded, "draining_comments")
    assert not subject.admit(comment) and not subject.admit(bounded.item)
    from trr_backend.ingestion.bounded_recovery_phase import transition

    transition(bounded.store, **bounded.binding, to_phase="media", reason="comments_drained")
    snapshot = subject._snapshot("bravotv", bounded.item["run_id"])
    assert snapshot["run"]["_bounded_window"]["phase"] == "media"
    assert snapshot["run"]["_comments_ready"] is True
    assert subject.admit(bounded.item) and not subject.admit(comment)
    assert len(probes) == 2


def test_scheduler_with_real_admission_reserves_and_dispatches_proved_media_despite_ready_comments(
    database, monkeypatch
):
    from trr_backend.ingestion.workflow_store import WorkflowStore
    from trr_backend.ingestion.workflows import Scheduler

    bounded = bounded_candidate(database, monkeypatch)
    advance_bounded(bounded, "media")
    workflow = WorkflowStore(database[1], execution_placement="local")
    item = workflow.seed(bounded.item)
    subject, probes = admission(database, env=bounded.env)
    snapshot = subject._snapshot("bravotv", item["run_id"])
    assert snapshot["run"]["_comments_ready"] is True and snapshot["counts"] == []
    assert snapshot["run"]["_bounded_window"]["phase"] == "media"
    assert subject._fits(snapshot, "media_mirror", "bravotv", run_id=item["run_id"])
    proof_calls, dispatched = [], []

    def proved_media(_cur, run, _item, _now, *, advance_cursor=False, full_cycle=False):
        proof_calls.append((run["_bounded_window"]["phase"], advance_cursor, full_cycle))
        return {"kind": "target", "stage": "media", "target_key": "candidate"}

    monkeypatch.setattr(WorkflowStore, "_eligible_media", staticmethod(proved_media))
    scheduler = Scheduler(
        workflow,
        admit=subject.admit,
        dispatch=lambda reservation: dispatched.append(reservation) or "fixture-media",
        reconcile=lambda _: pytest.fail("Fresh media must not need reconciliation"),
    )
    assert scheduler.tick(item, 0) == {"ready": True}
    assert proof_calls == [("media", True, True)]
    assert len(probes) == len(dispatched) == 1 and dispatched[0]["stage"] == "media"
    assert sql(database, "select stage,state from social.instagram_ingestion_dispatches") == [("media", "accepted")]
    assert sql(
        database, "select outcome,owner_token from social.instagram_ingestion_targets where stage='comments'"
    ) == [("unresolved", None)]
    assert scheduler.tick(item, 0) == {"sleep": 1.0}
    assert len(probes) == len(dispatched) == 1


@pytest.mark.parametrize("guard", ["account", "identity", "server_retry", "capacity", "probe"])
def test_media_phase_keeps_existing_account_identity_capacity_and_probe_guards(database, monkeypatch, guard):
    bounded = bounded_candidate(database, monkeypatch)
    advance_bounded(bounded, "media")
    subject, probes = admission(
        database, env=bounded.env, identity_key="selected-identity", probe_available=guard != "probe"
    )
    if guard == "account":
        sql(
            database,
            "insert into social.account_auth_cooldown(platform,account_handle,blocker_kind) "
            "values('instagram','bravotv','checkpoint')",
        )
    elif guard == "identity":
        sql(
            database,
            "insert into social.instagram_detail_request_lanes(identity_key,blocked) values('selected-identity',true)",
        )
    elif guard == "server_retry":
        sql(
            database,
            "insert into social.instagram_detail_request_lanes(identity_key,server_retry_after_until) "
            "values('selected-identity',clock_timestamp()+interval '1 hour')",
        )
    elif guard == "capacity":
        legacy(database, "media_mirror")
    assert not subject.admit(bounded.item)
    assert bool(probes) is (guard == "probe")


@pytest.mark.parametrize("binding", ["generation", "source", "window", "account", "workflow"])
def test_media_phase_rejects_stale_generation_and_worker_bindings(database, monkeypatch, binding):
    bounded = bounded_candidate(database, monkeypatch)
    advance_bounded(bounded, "media")
    item, env = dict(bounded.item), dict(bounded.env)
    if binding == "generation":
        item["generation"] -= 1
    elif binding == "source":
        env["TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION"] = "stale-source"
    elif binding == "window":
        env["TRR_INSTAGRAM_RECOVERY_WINDOW_ID"] = "stale-window"
    elif binding == "account":
        item["account"] = "unrelated"
    else:
        item["workflow_version"] = "stale-workflow"
    subject, probes = admission(database, env=env)
    assert not subject.admit(item) and probes == []


def test_legacy_bounded_window_retains_comments_first_admission(database, monkeypatch):
    bounded = bounded_candidate(database, monkeypatch, useful=False)
    subject, probes = admission(database, env=bounded.env)
    assert not subject.admit(bounded.item) and probes == []
    sql(database, "update social.instagram_ingestion_targets set blocked_reason='retained_hold' where stage='comments'")
    assert subject.admit(bounded.item)
    assert sql(database, "select blocked_reason from social.instagram_ingestion_targets where stage='comments'") == [
        ("retained_hold",)
    ]
