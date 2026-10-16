"""Public commenter metadata against disposable PG; source/storage IO is fake."""

import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests.ingestion import test_comments, test_pipeline
from trr_backend.ingestion.domain import LostOwnership, Outcome, RunIntent, SourceProfile, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.identity_policy import SourceRequestGate
from trr_backend.ingestion.profiles import (
    PROFILE_KIND,
    PROFILE_PARSER,
    PROFILE_VERSION,
    PROFILE_WORKFLOW_VERSION,
    ProfileActivity,
    ProfileError,
    cached_profile,
    parse_profile,
    profile_target,
    project_profile_payload,
    write_profile,
)

base_database = test_comments.database
pipeline_database = test_pipeline.pipeline_database
ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 10, 16, 16, tzinfo=UTC)
URL = "https://scontent.cdninstagram.com/avatar.jpg?oh=delivery&oe=expiry&sessionid=forbidden"
HOSTED = "https://media.example/avatar.jpg"


@pytest.fixture(scope="module")
def profiles_db(pipeline_database):
    store, tx = pipeline_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute("create table social.shared_account_sources(id uuid primary key)")
        cur.execute((ROOT / "supabase/migrations/20260428145222_instagram_queryable_profile_schema.sql").read_text())
        cur.execute((ROOT / "supabase/migrations/20260911121000_instagram_detail_request_lanes.sql").read_text())
    return store, tx


@pytest.fixture(autouse=True)
def clear(profiles_db):
    with profiles_db[1]() as conn, conn.cursor() as cur:
        cur.execute(
            "truncate social.instagram_ingestion_runs,social.instagram_profiles,social.avatar_registry,"
            "social.instagram_detail_request_lanes cascade"
        )


def rows(database, query, params=()):
    return test_pipeline.rows(database, query, params)


def native(username="reader", identity="101", **values):
    return {"status": "ok", "data": {"user": {"id": identity, "username": username, **values}}}


def capture(payload, *, target="profile:id:101", at=NOW):
    return Evidence.capture(
        source_identity="instagram:commenter-profile:" + target,
        captured_at=at,
        parser_version=PROFILE_PARSER,
        payload=project_profile_payload(payload),
    )


def observation(payload=None, *, target="profile:id:101", at=NOW, expected_id="101", username="reader"):
    return parse_profile(
        capture(payload or native(), target=target, at=at),
        target_key=target,
        expected_id=expected_id,
        username=username,
        pinned=True,
    )


def owned_target(database, *, identity="101", username="reader"):
    store, tx = database
    intent = RunIntent(
        account="bravotv", selected_tasks=(Stage.COMMENTS, Stage.MEDIA), source_profile=SourceProfile.for_mode("public")
    )
    run = store.start(
        intent,
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version=PROFILE_WORKFLOW_VERSION,
        initiated_by="offline-fixture",
    )
    key = profile_target(identity, username)
    with tx() as conn, conn.cursor() as cur:
        from trr_backend.ingestion.domain import stable_json

        cur.execute(
            """insert into social.instagram_ingestion_targets
            (run_id,stage,target_key,max_attempts,checkpoint) values(%s,'media',%s,3,%s::jsonb)""",
            (
                run["run_id"],
                key,
                stable_json(
                    {"target_kind": PROFILE_KIND, "version": PROFILE_VERSION, "user_id": identity, "username": username}
                ),
            ),
        )
    token = store.reserve(run["run_id"], "bravotv", generation=1, stage=Stage.MEDIA, target_key=key)
    return {
        "run_id": run["run_id"],
        "account": "bravotv",
        "generation": 1,
        "target_key": key,
        "owner_token": str(token["owner_token"]),
    }


class Source:
    def __init__(self, payload):
        self.payload, self.calls, self.closed = payload, [], False

    def __call__(self, *, username, pre_source_request):
        pre_source_request()
        self.calls.append(username)
        return self.payload

    def close(self):
        self.closed = True


class Objects:
    def __init__(self, *, fail=False):
        self.fail, self.calls, self.closed = fail, [], False
        self.client = SimpleNamespace(close=lambda: setattr(self, "closed", True))

    def host(self, url, **callbacks):
        self.calls.append(url)
        callbacks["check"]()
        callbacks["request"]()
        if self.fail:
            raise OSError("synthetic object failure")
        return {"url": HOSTED, "sha256": "a" * 64}, {}


def provider(database, owned, source):
    store, tx = database
    return lambda: (
        source,
        SourceRequestGate(
            cookies={},
            public_only=True,
            transaction=tx,
            check_cancelled=lambda: store.check_owner(**owned, stage=Stage.MEDIA),
            deadline=time.monotonic() + 60,
        ),
    )


def execute(database, owned, payload, objects=None):
    source = Source(payload)
    result = ProfileActivity(database[0], clock=lambda: NOW).execute(
        **owned,
        provider=provider(database, owned, source),
        storage=lambda: objects or pytest.fail("unexpected object source"),
    )
    return result, source


def test_projection_contains_only_metadata_not_viewer_timeline_or_relationships():
    payload = native(
        biography="Public bio",
        full_name="Reader",
        follower_count=0,
        following_count=5,
        media_count=7,
        is_verified=False,
        is_private=True,
        viewer_has_liked="secret-viewer",
        edge_owner_to_timeline_media={"count": 7, "edges": [{"node": {"id": "must-not-collect-post"}}]},
        edge_follow={"count": 5, "edges": [{"node": {"username": "must-not-collect-followers"}}]},
    )
    retained = capture(payload)
    assert all(token not in retained.payload_json for token in ("secret-viewer", "must-not-collect", "edges"))
    value = observation(payload)
    assert value.values["followers_count"] == 0 and value.values["is_verified"] is False
    assert value.values["posts_count"] == 7 and value.values["follows_count"] == 5
    assert value.coverage["profile_pic_url"] == "unknown"
    with pytest.raises(TypeError):
        value.values["injected"] = True
    with pytest.raises(ProfileError):
        replace(value, values={**value.values, "unknown": True})


@pytest.mark.parametrize(
    "payload",
    [
        native(followers_count=-1),
        native(is_verified="false"),
        native(profile_pic_url="https://private.invalid/image.jpg"),
        native(identity="202"),
    ],
)
def test_invalid_types_or_identity_are_retained_but_never_parse_as_complete(payload):
    evidence = capture(payload)
    with pytest.raises(ProfileError):
        parse_profile(evidence, target_key="profile:id:101", expected_id="101", username="reader", pinned=True)


def test_rename_bound_to_native_id_and_handle_reuse_is_rejected():
    assert observation(native(username="renamed")).username == "renamed"
    with pytest.raises(ProfileError, match="mismatch"):
        observation(native(identity="202"))
    with pytest.raises(ProfileError, match="mismatch"):
        observation(native(username="renamed"), target="profile:handle:reader", expected_id=None)


def test_private_partial_profile_completes_collection_with_unknown_coverage_and_zero_false(profiles_db):
    owned = owned_target(profiles_db)
    result, source = execute(
        profiles_db, owned, native(is_private=True, is_verified=False, followers_count=0, biography="")
    )
    assert source.closed and source.calls == ["reader"] and result["outcome"] == Outcome.COMMITTED
    identity = result["canonical_identity"]
    assert identity["profile_collection_complete"] is True and identity["coverage"]["posts_count"] == "unknown"
    row = rows(
        profiles_db,
        "select biography,followers_count,follows_count,is_verified,raw_data,last_scraped_at "
        "from social.instagram_profiles",
    )[0]
    assert row["biography"] == "" and row["followers_count"] == 0 and row["follows_count"] is None
    assert row["is_verified"] is False and row["last_scraped_at"] == NOW
    assert row["raw_data"]["ingestion_public_profile"]["coverage"]["biography"] == "observed"
    assert rows(profiles_db, "select attempts,requests from social.instagram_ingestion_targets")[0] == {
        "attempts": 1,
        "requests": 1,
    }


def test_avatar_success_uses_exact_shared_version_proof_and_new_target_cache_has_zero_effects(profiles_db):
    first = owned_target(profiles_db)
    objects = Objects()
    receipt, source = execute(profiles_db, first, native(profile_pic_url=URL, full_name="Reader"), objects)
    assert source.closed and objects.closed and len(objects.calls) == 1
    assert receipt["canonical_identity"]["avatar"]["observed_at"] == NOW.isoformat()
    assert rows(profiles_db, "select source_url,hosted_url,status from social.avatar_registry") == [
        {"source_url": URL.split("?")[0], "hosted_url": HOSTED, "status": "mirrored"}
    ]
    second = owned_target(profiles_db)
    result = ProfileActivity(profiles_db[0], clock=lambda: NOW + timedelta(hours=1)).execute(
        **second,
        provider=lambda: pytest.fail("cached credential/source construction"),
        storage=lambda: pytest.fail("cached object IO"),
    )
    assert result["outcome"] == Outcome.CACHED and result["observed_at"] == NOW
    assert rows(
        profiles_db, "select requests from social.instagram_ingestion_targets where run_id=%s", (second["run_id"],)
    ) == [{"requests": 0}]


def test_partial_metadata_survives_avatar_error_without_false_receipt_and_source_closes(profiles_db):
    owned = owned_target(profiles_db)
    source, objects = Source(native(profile_pic_url=URL, biography="Retained")), Objects(fail=True)
    with pytest.raises(OSError):
        ProfileActivity(profiles_db[0], clock=lambda: NOW).execute(
            **owned, provider=provider(profiles_db, owned, source), storage=lambda: objects
        )
    assert source.closed and objects.closed
    proof = rows(profiles_db, "select raw_data from social.instagram_profiles")[0]["raw_data"][
        "ingestion_public_profile"
    ]
    assert proof["collection_complete"] is False
    assert rows(profiles_db, "select count(*) as n from social.instagram_ingestion_receipts") == [{"n": 0}]
    assert (
        cached_profile(profiles_db[0], expected_id="101", username="reader", target_key=owned["target_key"], now=NOW)
        is None
    )


def test_source_time_guard_no_history_auth_cache_and_freshness_24h(profiles_db):
    owned = owned_target(profiles_db)
    receipt, _ = execute(profiles_db, owned, native(biography="Newest", followers_count=9))
    row_id = receipt["canonical_identity"]["profile_row_id"]
    old = observation(native(biography="Old", followers_count=2), at=NOW - timedelta(days=1))
    with profiles_db[1]() as conn:
        value = write_profile(conn, observation=old, evidence_id=str(uuid4()), run_id=owned["run_id"])
    assert value["applied"] is False
    assert rows(
        profiles_db, "select biography,followers_count from social.instagram_profiles where id=%s", (row_id,)
    ) == [{"biography": "Newest", "followers_count": 9}]
    assert (
        cached_profile(
            profiles_db[0],
            expected_id="101",
            username="reader",
            target_key=owned["target_key"],
            now=NOW + timedelta(hours=24),
        )
        is None
    )
    with profiles_db[1]() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_profiles set raw_data=%s::jsonb",
            (json.dumps({"sessionid": "must-not-read", "user": {"id": "999"}}),),
        )
    assert (
        cached_profile(profiles_db[0], expected_id="101", username="reader", target_key=owned["target_key"], now=NOW)
        is None
    )


def test_fenced_profile_never_constructs_source_or_objects(profiles_db):
    owned = owned_target(profiles_db)
    profiles_db[0].control(owned["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    with pytest.raises(LostOwnership):
        ProfileActivity(profiles_db[0]).execute(
            **owned, provider=lambda: pytest.fail("fenced source"), storage=lambda: pytest.fail("fenced storage")
        )


def test_fresh_profile_reuses_existing_exact_avatar_bytes_without_storage(profiles_db):
    from trr_backend.ingestion.media import write_avatar_version

    proof_time = NOW - timedelta(days=2)
    with profiles_db[1]() as conn:
        write_avatar_version(
            conn,
            username="reader",
            source_url=URL.split("?")[0],
            hosted_url=HOSTED,
            content_hash="a" * 64,
            observed_at=proof_time,
        )
    owned = owned_target(profiles_db)
    result, source = execute(profiles_db, owned, native(profile_pic_url=URL))
    assert source.calls == ["reader"] and source.closed
    assert result["canonical_identity"]["avatar"]["observed_at"] == proof_time.isoformat()
    assert rows(profiles_db, "select last_scraped_at from social.instagram_profiles")[0]["last_scraped_at"] == NOW
    assert rows(profiles_db, "select requests from social.instagram_ingestion_targets") == [{"requests": 1}]


def test_new_capture_keeps_missing_fields_unknown_instead_of_stale_values(profiles_db):
    owned = owned_target(profiles_db)
    execute(profiles_db, owned, native(followers_count=12, biography="Prior"))
    newer = observation(native(is_verified=False), at=NOW + timedelta(hours=1))
    with profiles_db[1]() as conn:
        write_profile(conn, observation=newer, evidence_id=str(uuid4()), run_id=owned["run_id"])
    assert rows(profiles_db, "select biography,followers_count,is_verified from social.instagram_profiles") == [
        {"biography": None, "followers_count": None, "is_verified": False}
    ]


def test_legacy_metadata_mutation_cannot_reuse_stale_public_cache_proof(profiles_db):
    owned = owned_target(profiles_db)
    execute(profiles_db, owned, native(followers_count=12))
    assert (
        cached_profile(profiles_db[0], expected_id="101", username="reader", target_key=owned["target_key"], now=NOW)
        is not None
    )
    with profiles_db[1]() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_profiles set followers_count=99")
    assert (
        cached_profile(profiles_db[0], expected_id="101", username="reader", target_key=owned["target_key"], now=NOW)
        is None
    )


def test_public_backoff_preserves_shared_retry_after_and_never_writes_false_profile(profiles_db):
    from trr_backend.ingestion.post_transport import SourceFailure
    from trr_backend.socials.instagram.request_client import InstagramRequestFailure

    owned = owned_target(profiles_db)

    class Blocked(Source):
        def __call__(self, *, username, pre_source_request):
            pre_source_request()
            raise InstagramRequestFailure(
                "public_access_backoff", status_code=403, retryable=False, retry_after_seconds=37
            )

    source = Blocked(native())
    with pytest.raises(SourceFailure) as caught:
        ProfileActivity(profiles_db[0]).execute(
            **owned, provider=provider(profiles_db, owned, source), storage=lambda: pytest.fail("backoff storage")
        )
    assert caught.value.failure.code == "public_access_backoff" and source.closed
    assert rows(profiles_db, "select count(*) as n from social.instagram_profiles") == [{"n": 0}]
    lane = rows(
        profiles_db,
        "select blocked,last_error_code,cooldown_until,clock_timestamp() as now "
        "from social.instagram_detail_request_lanes",
    )[0]
    assert lane["blocked"] is False and lane["last_error_code"] == "public_access_backoff"
    assert 30 < (lane["cooldown_until"] - lane["now"]).total_seconds() <= 37
    assert rows(profiles_db, "select requests from social.instagram_ingestion_targets") == [{"requests": 1}]


def test_cancel_after_source_cannot_retain_or_write_profile(profiles_db):
    owned = owned_target(profiles_db)

    class Cancelled(Source):
        def __call__(self, *, username, pre_source_request):
            pre_source_request()
            profiles_db[0].control(owned["run_id"], "bravotv", action="cancel", idempotency_key="source-cancel")
            return self.payload

    source = Cancelled(native())
    with pytest.raises(LostOwnership):
        ProfileActivity(profiles_db[0]).execute(
            **owned, provider=provider(profiles_db, owned, source), storage=lambda: pytest.fail("cancel storage")
        )
    assert source.closed
    assert rows(profiles_db, "select count(*) as n from social.instagram_profiles") == [{"n": 0}]
    assert rows(profiles_db, "select count(*) as n from social.instagram_ingestion_receipts") == [{"n": 0}]


def test_source_without_counted_boundary_is_unresolved_without_identity_failure(profiles_db):
    owned = owned_target(profiles_db)

    class Uncounted(Source):
        def __call__(self, **kwargs):
            return self.payload

    source = Uncounted(native())
    with pytest.raises(ProfileError, match="profile_request_boundary_missing"):
        ProfileActivity(profiles_db[0]).execute(
            **owned, provider=provider(profiles_db, owned, source), storage=lambda: pytest.fail("uncounted storage")
        )
    assert source.closed
    assert rows(profiles_db, "select requests from social.instagram_ingestion_targets") == [{"requests": 0}]
    assert rows(profiles_db, "select count(*) as n from social.instagram_ingestion_receipts") == [{"n": 0}]
