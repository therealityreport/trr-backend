"""Profile observations against private PostgreSQL; source calls are local fakes."""

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion.test_persistence import database as database_fixture
from trr_backend.ingestion.avatar_resolution import (
    AVATAR_PARSER_VERSION,
    AVATAR_SOURCE_PREFIX,
    AvatarResolutionActivity,
    AvatarResolutionError,
    parse_avatar_evidence,
)
from trr_backend.ingestion.domain import LostOwnership, RunIntent, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.identity_policy import IdentityDeferred, SourceRequestGate
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

database = database_fixture
URL = "https://scontent.cdninstagram.com/avatar.jpg?oh=delivery-signature&oe=expiry&sessionid=secret"


@pytest.fixture(scope="module")
def avatars(database):
    _, tx = database
    root = Path(__file__).parents[2] / "supabase/migrations"
    with tx() as conn, conn.cursor() as cur:
        cur.execute((root / "0189_social_avatar_registry_repair.sql").read_text())
        cur.execute((root / "20260911121000_instagram_detail_request_lanes.sql").read_text())
        cur.execute((root / "20261022120000_instagram_server_retry_after.sql").read_text())
    return database


def owned_target(database):
    store, tx = database
    run = store.start(
        RunIntent(account="fixture_account", selected_tasks=(Stage.MEDIA,)),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="fixture",
        initiated_by="fixture",
    )
    owned = {
        "run_id": run["run_id"],
        "account": "fixture_account",
        "generation": run["generation"],
        "target_key": "Avatar" + uuid4().hex[:10],
    }
    store.discover(
        **{key: value for key, value in owned.items() if key != "target_key"},
        stage=Stage.MEDIA,
        target_keys=[owned["target_key"]],
        max_attempts=3,
        cursor_state={},
        expected_cursor_state={},
        page_id="one",
        seal=True,
    )
    owned["owner_token"] = str(store.reserve(**owned, stage=Stage.MEDIA)["owner_token"])
    store.acquire(**owned, stage=Stage.MEDIA)
    gate = SourceRequestGate(
        cookies={"ds_user_id": uuid4().hex, "sessionid": "synthetic-private-session"},
        transaction=tx,
        check_cancelled=lambda: store.check_owner(**owned, stage=Stage.MEDIA),
        deadline=time.monotonic() + 120,
    )
    return owned, AvatarResolutionActivity(store, gate=gate)


def profile(username, url=URL):
    return {"status": "ok", "data": {"user": {"id": "123", "username": username, "profile_pic_url_hd": url}}}


def source(payload, calls=None):
    def fetch(*, username, pre_source_request):
        pre_source_request()
        if calls is not None:
            calls.append(username)
        return payload(username) if callable(payload) else payload

    return fetch


def rows(database, owned):
    _, tx = database
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.instagram_ingestion_targets where run_id=%s", (owned["run_id"],))
        target = dict(cur.fetchone())
        cur.execute(
            "select * from social.instagram_ingestion_evidence where run_id=%s order by captured_at", (owned["run_id"],)
        )
        evidence = [dict(row) for row in cur.fetchall()]
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()["count"] == 0
    return target, evidence


def registry(database, username):
    _, tx = database
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.avatar_registry where account_handle=%s", (username,))
        return [dict(row) for row in cur.fetchall()]


def retained(database, row):
    return Evidence(
        row["source_identity"], row["captured_at"], row["parser_version"], row["payload"], row["content_hash"]
    )


def test_counted_source_retains_fetchable_safe_url_and_never_settles_media(avatars):
    owned, activity = owned_target(avatars)
    handle = "a" + uuid4().hex[:12]
    observations = activity.execute(**owned, handles=[handle.upper(), handle], fetch=source(profile))
    assert len(observations) == 1 and observations[0].username == handle
    assert observations[0].source_url == URL and "delivery-signature" not in repr(observations)
    target, evidence = rows(avatars, owned)
    assert target["requests"] == 1 and target["outcome"] == "unresolved"
    assert len(evidence) == 1 and evidence[0]["source_identity"] == AVATAR_SOURCE_PREFIX + handle
    assert "secret" not in evidence[0]["payload"] and "sessionid" not in evidence[0]["payload"]
    row = registry(avatars, handle)[0]
    assert row["status"] == "pending" and row["last_checked_at"] == evidence[0]["captured_at"]
    assert row["source_url"] == URL.split("&sessionid")[0]
    parsed = parse_avatar_evidence(retained(avatars, evidence[0]), username=handle, pinned=True)
    assert parsed.source_url == row["source_url"] and not parsed.unavailable
    assert activity.gate.request_count == 1


def test_explicit_unavailable_proof_is_retained_and_offline_replay_deduplicates_null(avatars):
    owned, activity = owned_target(avatars)
    handle = "b" + uuid4().hex[:12]
    result = activity.execute(**owned, handles=[handle], fetch=source(lambda username: profile(username, None)))[0]
    assert result.unavailable and result.source_url is None
    _, evidence = rows(avatars, owned)
    capture = retained(avatars, evidence[0])
    assert capture.read(pinned=True)["avatar_url_explicitly_missing"] is True
    activity.apply_evidence(**owned, username=handle, evidence=capture, evidence_id=result.evidence_id)
    found = registry(avatars, handle)
    assert len(found) == 1 and found[0]["status"] == "unsupported"
    assert found[0]["failure_reason"] == "profile_pic_not_available"
    assert activity.gate.request_count == 1  # Replay never invokes the source/gate.


@pytest.mark.parametrize(
    "change,code",
    [
        (lambda payload: payload["data"]["user"].pop("profile_pic_url_hd"), "avatar_profile_incomplete"),
        (lambda payload: payload["data"]["user"].update(username="different"), "avatar_identity_mismatch"),
        (lambda payload: payload.update(status="fail"), "avatar_profile_incomplete"),
        (lambda payload: payload["data"].update(user=None), "avatar_profile_incomplete"),
        (
            lambda payload: payload["data"]["user"].update(profile_pic_url_hd="http://127.0.0.1/private"),
            "avatar_reference_invalid",
        ),
    ],
)
def test_partial_malformed_or_wrong_user_never_becomes_unavailable(avatars, change, code):
    owned, activity = owned_target(avatars)
    handle = "c" + uuid4().hex[:12]
    payload = profile(handle)
    change(payload)
    with pytest.raises(AvatarResolutionError, match=code):
        activity.execute(**owned, handles=[handle], fetch=source(payload))
    assert registry(avatars, handle) == []
    target, evidence = rows(avatars, owned)
    assert target["requests"] == 1 and len(evidence) == 1
    with pytest.raises(AvatarResolutionError):
        parse_avatar_evidence(retained(avatars, evidence[0]), username=handle, pinned=True)


def test_cancelled_generation_before_request_never_calls_source_or_counts(avatars):
    owned, activity = owned_target(avatars)
    activity.store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
    with pytest.raises(LostOwnership):
        activity.execute(**owned, handles=["fixture"], fetch=lambda **kw: pytest.fail("Cancelled source ran"))
    assert rows(avatars, owned)[0]["requests"] == 0 and activity.gate.request_count == 0


def test_cancel_after_response_keeps_evidence_but_cannot_mutate_registry(avatars, monkeypatch):
    owned, activity = owned_target(avatars)
    handle = "d" + uuid4().hex[:12]
    original = activity.store.retain_evidence

    def retain(**kwargs):
        result = original(**kwargs)
        activity.store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
        return result

    monkeypatch.setattr(activity.store, "retain_evidence", retain)
    with pytest.raises(LostOwnership):
        activity.execute(**owned, handles=[handle], fetch=source(profile))
    assert registry(avatars, handle) == [] and len(rows(avatars, owned)[1]) == 1


def test_evidence_binding_rejects_other_target_account_and_changed_envelope(avatars):
    owned, activity = owned_target(avatars)
    handle = "e" + uuid4().hex[:12]
    observation = activity.execute(**owned, handles=[handle], fetch=source(profile))[0]
    _, stored = rows(avatars, owned)
    evidence = retained(avatars, stored[0])
    other, _ = owned_target(avatars)
    with pytest.raises(AvatarResolutionError, match="avatar_retained_evidence_required"):
        activity.apply_evidence(**other, username=handle, evidence=evidence, evidence_id=observation.evidence_id)
    with pytest.raises(LookupError):
        activity.apply_evidence(
            **{**owned, "account": "other_account"},
            username=handle,
            evidence=evidence,
            evidence_id=observation.evidence_id,
        )
    with pytest.raises(AvatarResolutionError, match="avatar_evidence_identity_mismatch"):
        parse_avatar_evidence(evidence, username="other_user", pinned=True)
    forged = Evidence.capture(
        source_identity=evidence.source_identity,
        captured_at=evidence.captured_at + timedelta(seconds=1),
        parser_version=AVATAR_PARSER_VERSION,
        payload=evidence.read(pinned=True),
    )
    with pytest.raises(AvatarResolutionError, match="avatar_retained_evidence_required"):
        activity.apply_evidence(**owned, username=handle, evidence=forged, evidence_id=observation.evidence_id)


def test_older_replay_cannot_overwrite_newer_registry_observation(avatars):
    owned, activity = owned_target(avatars)
    handle = "f" + uuid4().hex[:12]
    observation = activity.execute(**owned, handles=[handle], fetch=source(profile))[0]
    _, evidence = rows(avatars, owned)
    _, tx = avatars
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.avatar_registry set last_checked_at=%s where account_handle=%s",
            (datetime.now(UTC) + timedelta(hours=1), handle),
        )
    with pytest.raises(AvatarResolutionError, match="avatar_source_newer"):
        activity.apply_evidence(
            **owned, username=handle, evidence=retained(avatars, evidence[0]), evidence_id=observation.evidence_id
        )


def test_source_calls_outside_transactions_keep_own_probe_across_multiple_requests(avatars):
    owned, activity = owned_target(avatars)
    _, tx = avatars
    activity.gate.before_request()
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_detail_request_lanes "
            "set server_retry_after_until=clock_timestamp()-interval '1 sec'"
            " where identity_key=%s",
            (activity.gate.identity_key,),
        )
    probes = []

    def fetch(*, username, pre_source_request):
        pre_source_request()
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "select run_id from social.instagram_ingestion_runs where run_id=%s for update nowait",
                (owned["run_id"],),
            )
            cur.execute(
                "select probe_token from social.instagram_detail_request_lanes where identity_key=%s",
                (activity.gate.identity_key,),
            )
            probes.append(cur.fetchone()[0])
        return profile(username)

    handles = ["g" + uuid4().hex[:12], "h" + uuid4().hex[:12]]
    assert len(activity.execute(**owned, handles=handles, fetch=fetch)) == 2
    assert probes[0] and probes[0] == probes[1]
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select probe_token,cooldown_until from social.instagram_detail_request_lanes where identity_key=%s",
            (activity.gate.identity_key,),
        )
        assert cur.fetchone() == (None, None)


def test_partial_progress_then_auth_failure_is_safe_blocked_and_not_media_success(avatars):
    owned, activity = owned_target(avatars)
    handles = ["i" + uuid4().hex[:12], "j" + uuid4().hex[:12]]

    def fetch(*, username, pre_source_request):
        pre_source_request()
        if username == handles[1]:
            raise InstagramRequestFailure(
                "unauthorized", status_code=401, retryable=False, response_text="sessionid=must-not-be-returned"
            )
        return profile(username)

    with pytest.raises(AvatarResolutionError) as error:
        activity.execute(**owned, handles=handles, fetch=fetch)
    assert error.value.failure.identity_blocked and error.value.code == "unauthorized"
    assert "must-not-be-returned" not in repr(error.value)
    assert len(registry(avatars, handles[0])) == 1 and registry(avatars, handles[1]) == []
    target, evidence = rows(avatars, owned)
    assert target["requests"] == 2 and len(evidence) == 1 and target["outcome"] == "unresolved"


@pytest.mark.parametrize("calls", [0, 2])
def test_injected_source_must_use_one_explicit_request_boundary(avatars, calls):
    owned, activity = owned_target(avatars)

    def fetch(*, username, pre_source_request):
        for _ in range(calls):
            pre_source_request()
        return profile(username)

    with pytest.raises(AvatarResolutionError):
        activity.execute(**owned, handles=["fixture"], fetch=fetch)
    assert rows(avatars, owned)[0]["requests"] == min(calls, 1)


def test_parser_requires_version_identity_and_explicit_missing_marker():
    def capture(**kwargs):
        return Evidence.capture(
            source_identity=AVATAR_SOURCE_PREFIX + "fixture",
            captured_at=datetime.now(UTC),
            parser_version=AVATAR_PARSER_VERSION,
            **kwargs,
        )

    proof = capture(payload=profile("fixture", None))
    with pytest.raises(AvatarResolutionError, match="avatar_unavailable_proof_missing"):
        parse_avatar_evidence(proof, username="fixture")


def test_expired_deadline_never_sends_or_counts(avatars):
    owned, activity = owned_target(avatars)
    activity.gate.deadline = time.monotonic() - 1
    calls = []
    with pytest.raises(IdentityDeferred, match="target_deadline"):
        activity.execute(**owned, handles=["fixture"], fetch=source(profile, calls))
    assert calls == [] and rows(avatars, owned)[0]["requests"] == 0


def test_cancel_between_identity_reservation_and_count_never_sends(avatars, monkeypatch):
    owned, activity = owned_target(avatars)
    original = activity.gate.before_request

    def gate_then_cancel():
        permit = original()
        activity.store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
        return permit

    monkeypatch.setattr(activity.gate, "before_request", gate_then_cancel)
    calls = []
    with pytest.raises(LostOwnership):
        activity.execute(**owned, handles=["fixture"], fetch=source(profile, calls))
    assert calls == [] and rows(avatars, owned)[0]["requests"] == 0


def test_repeat_profile_observation_preserves_existing_hosted_identity(avatars):
    owned, activity = owned_target(avatars)
    handle = "k" + uuid4().hex[:12]
    _, tx = avatars
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,hosted_url,
            status,last_checked_at) values('instagram',%s,%s,'old-content-hash','https://cdn.example/old.jpg',
            'mirrored',%s)""",
            (handle, URL.split("&sessionid")[0], datetime.now(UTC) - timedelta(hours=1)),
        )
    activity.execute(**owned, handles=[handle], fetch=source(profile))
    row = registry(avatars, handle)[0]
    assert row["status"] == "mirrored" and row["content_hash"] == "old-content-hash"
    assert row["hosted_url"] == "https://cdn.example/old.jpg"


def test_simultaneous_unavailable_writes_deduplicate_nullable_registry_key(avatars):
    first, first_activity = owned_target(avatars)
    second, second_activity = owned_target(avatars)
    username = "l" + uuid4().hex[:12]
    evidence = Evidence.capture(
        source_identity=AVATAR_SOURCE_PREFIX + username,
        captured_at=datetime.now(UTC),
        parser_version=AVATAR_PARSER_VERSION,
        payload={**profile(username, None), "avatar_url_explicitly_missing": True},
    )

    def apply(item):
        owned, activity = item
        evidence_id = activity.store.retain_evidence(**owned, stage=Stage.MEDIA, evidence=evidence)
        return activity.apply_evidence(**owned, username=username, evidence=evidence, evidence_id=evidence_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(apply, [(first, first_activity), (second, second_activity)]))
    assert all(result.unavailable for result in results)
    assert len(registry(avatars, username)) == 1
    assert first_activity.gate.request_count == second_activity.gate.request_count == 0


@pytest.mark.parametrize("case", ["http_404", "null_user", "http_500"])
def test_missing_avatar_profiles_do_not_close_the_shared_local_lane(avatars, case):
    import hashlib

    store, tx = avatars
    key = hashlib.sha256(b"public:local").hexdigest()
    with tx() as conn, conn.cursor() as cur:
        cur.execute("delete from social.instagram_detail_request_lanes where identity_key=%s", (key,))

    def fetch(*, username, pre_source_request):
        pre_source_request()
        if case == "null_user":
            return {"status": "ok", "data": {"user": None}}
        raise InstagramRequestFailure(
            "request_failed", status_code=404 if case == "http_404" else 500, retryable=case == "http_500"
        )

    for _ in range(3):
        owned, _ = owned_target(avatars)
        gate = SourceRequestGate(
            cookies={},
            public_only=True,
            network_lane="local",
            transaction=tx,
            check_cancelled=lambda owned=owned: store.check_owner(**owned, stage=Stage.MEDIA),
            deadline=time.monotonic() + 120,
        )
        with pytest.raises(AvatarResolutionError):
            AvatarResolutionActivity(store, gate=gate).execute(**owned, handles=["m" + uuid4().hex[:12]], fetch=fetch)
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select consecutive_failures,last_error_code,cooldown_until from social.instagram_detail_request_lanes "
            "where identity_key=%s",
            (key,),
        )
        lane = dict(cur.fetchone())
        cur.execute("delete from social.instagram_detail_request_lanes where identity_key=%s", (key,))
    if case == "http_500":
        assert lane["consecutive_failures"] == 3 and lane["cooldown_until"] is None
    else:
        assert lane == {"consecutive_failures": 0, "last_error_code": None, "cooldown_until": None}
