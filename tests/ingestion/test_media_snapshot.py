"""Read-only inventory proof against private local PostgreSQL only."""

import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from tests.ingestion.test_media import OBSERVED, SOURCE, execute
from tests.ingestion.test_media import database as database_fixture
from tests.ingestion.test_media import media_database as media_database_fixture
from trr_backend.ingestion.canonical import normalize_post_source, write_post
from trr_backend.ingestion.domain import LostOwnership, RunIntent, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import MEDIA_VERSION, MediaIncomplete
from trr_backend.ingestion.media_snapshot import AvatarSourcesMissing, MediaSnapshotLoader
from trr_backend.ingestion.workflow_store import WorkflowStore

database = database_fixture
media_database = media_database_fixture


@pytest.fixture(scope="module")
def snapshot_database(media_database):
    _, tx = media_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute("alter table core.seasons add column show_id uuid,add column season_number integer")
        cur.execute("alter table social.scrape_jobs add column config jsonb")
    return media_database


def run(store, *, account="fixture_account", selected=(Stage.MEDIA,)):
    return store.start(
        RunIntent(account=account, selected_tasks=selected),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="fixture",
        initiated_by="fixture",
    )


def discover(store, run, stage, shortcode, *, seal=True):
    owned = {
        "run_id": run["run_id"],
        "account": "fixture_account",
        "generation": run["generation"],
        "stage": stage,
        "target_key": shortcode,
    }
    store.discover(
        **{k: v for k, v in owned.items() if k != "target_key"},
        target_keys=[shortcode],
        max_attempts=3,
        cursor_state={},
        expected_cursor_state={},
        page_id=str(uuid4()),
        seal=seal,
    )
    return owned


def seed(database, *, selected_comments=False, comments=1, proof=True):
    store, tx = database
    current = run(store, selected=(Stage.COMMENTS, Stage.MEDIA) if selected_comments else (Stage.MEDIA,))
    shortcode = "Snapshot" + uuid4().hex[:10]
    owned = discover(store, current, Stage.MEDIA, shortcode)
    show_id, season_id = str(uuid4()), str(uuid4())
    payload = {
        "shortcode": shortcode,
        "pk": "123",
        "owner": {"username": "fixture_account", "profile_pic_url": SOURCE},
        "caption": {"text": "Persisted capture"},
        "like_count": 1,
        "comment_count": comments,
        "media_urls": [SOURCE],
        "thumbnail_url": SOURCE,
    }
    with tx() as conn, conn.cursor() as cur:
        cur.execute("insert into core.shows values(%s)", (show_id,))
        cur.execute("insert into core.seasons(id,show_id,season_number) values(%s,%s,4)", (season_id, show_id))
        ids = write_post(
            conn,
            run_id=current["run_id"],
            account="fixture_account",
            observed_at=OBSERVED,
            normalized=normalize_post_source(payload, account="fixture_account"),
            source_payload=payload,
            assignment={"show_id": show_id, "season_id": season_id},
        )
        rows = []
        for index in range(comments):
            row_id, comment_id = str(uuid4()), "comment-" + str(index)
            cur.execute(
                """insert into social.instagram_comments
                (id,comment_id,post_id,username,text,scraped_at,last_seen_at,source_account,source_snapshot_type,
                 author_profile_pic_url,media_urls) values(%s,%s,%s,'commenter','Body',%s,%s,'fixture_account',
                 'full_comments_scrape',%s,%s::jsonb)""",
                (
                    row_id,
                    comment_id,
                    ids["post_id"],
                    OBSERVED + timedelta(minutes=2),
                    OBSERVED + timedelta(minutes=2),
                    SOURCE,
                    json.dumps([SOURCE]),
                ),
            )
            rows.append((row_id, comment_id))
    source_run = current if selected_comments else run(store, selected=(Stage.COMMENTS,))
    comment_owned = discover(store, source_run, Stage.COMMENTS, shortcode)
    if proof:
        token = str(store.reserve(**comment_owned)["owner_token"])
        store.acquire(**comment_owned, owner_token=token)
        store.commit_target(
            **comment_owned,
            owner_token=token,
            observed_at=OBSERVED + timedelta(minutes=3),
            write_canonical=lambda conn: {
                "post_id": ids["post_id"],
                "observed_comment_ids": [item[1] for item in rows],
            },
        )
    return owned, ids, rows, comment_owned


def load(database, owned, **kwargs):
    _, tx = database
    return MediaSnapshotLoader(tx, **kwargs).load(**{k: v for k, v in owned.items() if k != "stage"})


def test_media_only_uses_receipt_inventory_and_preserves_distinct_source_times(snapshot_database):
    owned, ids, rows, _ = seed(snapshot_database)
    snapshot = load(snapshot_database, owned)
    assert snapshot.post_id == ids["post_id"]
    assert snapshot.canonical_post_id == ids["canonical_post_id"]
    assert snapshot.post_id != snapshot.canonical_post_id
    assert snapshot.observed_at == OBSERVED
    assert snapshot.comments[0].observed_at == OBSERVED + timedelta(minutes=2)
    assert snapshot.avatars[0].observed_at == OBSERVED
    assert snapshot.comments[0].row_id == rows[0][0]
    assert snapshot.comments_complete and snapshot.avatars_complete
    assert snapshot.include_comment_avatars is False
    assert [avatar.role for avatar in snapshot.avatars] == ["owner"]
    assert snapshot.season_number == 4 and snapshot.week_index is None


def test_selected_comments_require_current_sealed_resolved_stage(snapshot_database):
    owned, _, _, _ = seed(snapshot_database, selected_comments=True, proof=False)
    with pytest.raises(MediaIncomplete, match="complete_comment_inventory_proof_required"):
        load(snapshot_database, owned)


def test_empty_inventory_requires_actual_complete_receipt(snapshot_database):
    owned, _, _, _ = seed(snapshot_database, comments=0, proof=False)
    with pytest.raises(MediaIncomplete, match="complete_comment_inventory_proof_required"):
        load(snapshot_database, owned)
    owned, _, _, _ = seed(snapshot_database, comments=0, proof=True)
    assert load(snapshot_database, owned).comments == ()


def test_source_account_and_run_target_isolation(snapshot_database):
    owned, _, _, _ = seed(snapshot_database)
    with pytest.raises(LookupError):
        load(snapshot_database, {**owned, "account": "another_account"})
    with pytest.raises(LookupError):
        load(snapshot_database, {**owned, "run_id": str(uuid4())})
    with pytest.raises(LookupError):
        load(snapshot_database, {**owned, "target_key": "AnotherPost"})
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set source_account='another_account' where shortcode=%s",
            (owned["target_key"],),
        )
    with pytest.raises(LookupError, match="Persisted media post"):
        load(snapshot_database, owned)


@pytest.mark.parametrize(
    "change,code",
    [
        ("update social.instagram_comments set source_snapshot_type='preview' where post_id=%s", "unproved_source"),
        ("update social.instagram_comments set source_account='other' where post_id=%s", "account_mismatch"),
        (
            "update social.instagram_comments set scraped_at=scraped_at+interval '1 day' where post_id=%s",
            "unproved_source",
        ),
        ("update social.instagram_comments set is_missing=true where post_id=%s", "inventory_changed"),
    ],
)
def test_changed_or_incomplete_comment_rows_cannot_become_complete(snapshot_database, change, code):
    owned, ids, _, _ = seed(snapshot_database)
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(change, (ids["post_id"],))
    with pytest.raises(MediaIncomplete, match=code):
        load(snapshot_database, owned)


def test_unresolved_mentions_do_not_fabricate_avatar_completeness(snapshot_database):
    owned, ids, _, _ = seed(snapshot_database)
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set mentions='[\"@unresolved\"]'::jsonb where id=%s", (ids["post_id"],)
        )
    with pytest.raises(MediaIncomplete, match="avatar_source_identity_unresolved"):
        load(snapshot_database, owned)


def test_comment_avatar_policy_is_explicit_and_keeps_original_observation(snapshot_database):
    owned, _, _, _ = seed(snapshot_database)
    snapshot = load(snapshot_database, owned, include_comment_avatars=True)
    assert snapshot.include_comment_avatars is True
    avatar = next(avatar for avatar in snapshot.avatars if avatar.role == "comment")
    assert avatar.observed_at == OBSERVED + timedelta(minutes=2)
    assert avatar.comment_id == snapshot.comments[0].comment_id


def test_proved_text_only_comments_are_excluded_unless_author_avatars_selected(snapshot_database):
    owned, ids, _, _ = seed(snapshot_database)
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_comments set media_urls='[]'::jsonb where post_id=%s", (ids["post_id"],))
    snapshot = load(snapshot_database, owned)
    assert snapshot.comments_complete and snapshot.comments == ()
    selected = load(snapshot_database, owned, include_comment_avatars=True)
    assert len(selected.comments) == 1
    assert any(avatar.role == "comment" for avatar in selected.avatars)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select media_mirror_status from social.instagram_comments where post_id=%s", (ids["post_id"],))
        assert cur.fetchone()[0] is None


def test_retained_key_survives_changed_signed_transport_url(snapshot_database):
    store, tx = snapshot_database
    owned, ids, rows, _ = seed(snapshot_database)
    token = str(store.reserve(**owned)["owner_token"])
    store.acquire(**owned, owner_token=token)
    key = "unknown-" + "a" * 24
    evidence = Evidence.capture(
        source_identity="instagram:media:" + owned["target_key"],
        captured_at=OBSERVED,
        parser_version=MEDIA_VERSION,
        payload={"comments": [{"row_id": rows[0][0], "comment_id": rows[0][1], "storage_source_key": key}]},
    )
    store.retain_evidence(**owned, owner_token=token, evidence=evidence)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_comments set media_urls=%s::jsonb where post_id=%s",
            (json.dumps([SOURCE + "-renewed"]), ids["post_id"]),
        )
    assert load(snapshot_database, owned).comments[0].storage_source_key == key


def test_existing_manifest_storage_identity_and_week_are_preserved(snapshot_database):
    owned, ids, _, _ = seed(snapshot_database)
    _, tx = snapshot_database
    manifest = {
        "originals": {
            "cover": {
                "storage_key": "social/instagram/show/4/week-7/Post/cover-original.png",
                "url": "https://media.example/cover.png",
            }
        }
    }
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_post_payloads set asset_manifest=%s::jsonb where post_id=%s",
            (json.dumps(manifest), ids["post_id"]),
        )
    snapshot = load(snapshot_database, owned)
    assert snapshot.existing_manifest == manifest and snapshot.week_index == 7


def test_run_cancellation_and_generation_are_checked_without_source_requests(snapshot_database):
    store, _ = snapshot_database
    owned, _, _, _ = seed(snapshot_database)
    with pytest.raises(LostOwnership):
        load(snapshot_database, {**owned, "generation": owned["generation"] + 1})
    store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
    with pytest.raises(LostOwnership):
        load(snapshot_database, owned)


def test_normal_details_then_comments_then_media_preserves_each_observation(snapshot_database):
    store, tx = snapshot_database
    owned, ids, _, _ = seed(snapshot_database, selected_comments=True)
    snapshot = load(snapshot_database, owned, include_comment_avatars=True)
    token = str(store.reserve(**owned)["owner_token"])
    started = datetime.now(OBSERVED.tzinfo)
    receipt = execute(snapshot_database, owned, token, snapshot)
    assert receipt["observed_at"] == OBSERVED
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select scraped_at,media_mirror_last_attempt_at from social.instagram_posts where id=%s", (ids["post_id"],)
        )
        source_time, attempt_time = cur.fetchone()
        assert source_time == OBSERVED and attempt_time >= started
        cur.execute(
            "select scraped_at,media_mirror_last_attempt_at from social.instagram_comments where post_id=%s",
            (ids["post_id"],),
        )
        expected = OBSERVED + timedelta(minutes=2)
        source_time, attempt_time = cur.fetchone()
        assert source_time == expected and attempt_time >= started
        cur.execute(
            "select payload from social.instagram_ingestion_evidence where run_id=%s and stage='media'",
            (owned["run_id"],),
        )
        retained = json.loads(cur.fetchone()[0])
        assert retained["comments"][0]["observed_at"] == expected.isoformat()
        assert retained["include_comment_avatars"] is True


def test_prior_receipt_from_other_account_cannot_certify_inventory(snapshot_database):
    store, _ = snapshot_database
    owned, ids, rows, _ = seed(snapshot_database, proof=False)
    other = run(store, account="other_account", selected=(Stage.COMMENTS,))
    foreign = {
        "run_id": other["run_id"],
        "account": "other_account",
        "generation": other["generation"],
        "stage": Stage.COMMENTS,
        "target_key": owned["target_key"],
    }
    store.discover(
        **{k: v for k, v in foreign.items() if k != "target_key"},
        target_keys=[foreign["target_key"]],
        max_attempts=3,
        cursor_state={},
        expected_cursor_state={},
        page_id="foreign",
        seal=True,
    )
    token = str(store.reserve(**foreign)["owner_token"])
    store.acquire(**foreign, owner_token=token)
    store.commit_target(
        **foreign,
        owner_token=token,
        observed_at=OBSERVED + timedelta(minutes=3),
        write_canonical=lambda conn: {"post_id": ids["post_id"], "observed_comment_ids": [row[1] for row in rows]},
    )
    with pytest.raises(MediaIncomplete, match="complete_comment_inventory_proof_required"):
        load(snapshot_database, owned)


def test_selected_comments_admit_own_inventory_despite_other_unresolved_targets(snapshot_database):
    owned, _, _, _ = seed(snapshot_database, selected_comments=True)
    # Preserve the sealed discovery set and make a previously discovered target
    # unresolved to model a different post still being processed in this stage.
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts)
            values(%s,'comments','another-post',3)""",
            (owned["run_id"],),
        )
    snapshot = load(snapshot_database, owned)
    assert snapshot.comments_complete
    assert snapshot.inventory_fingerprint["version"] == "instagram-media-inventory-v1"


def test_per_post_media_retains_public_exhaustion_count_gap(snapshot_database):
    owned, _, _, _ = seed(snapshot_database, selected_comments=True)
    _, tx = snapshot_database
    settlement = {"basis": "public_native_exhaustion", "expected_total": 9, "observed_total": 1, "count_gap": 8}
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_receipts set canonical_identity=canonical_identity ||
            jsonb_build_object('settlement',%s::jsonb) where run_id=%s and stage='comments'""",
            (json.dumps(settlement), owned["run_id"]),
        )
    assert load(snapshot_database, owned).comment_settlement == settlement


def test_reopened_own_comments_and_stale_receipt_generation_block_media(snapshot_database):
    owned, _, _, _ = seed(snapshot_database, selected_comments=True)
    _, tx = snapshot_database
    store = WorkflowStore(tx)
    item = {**owned, "engine": "dbos", "workflow_version": "fixture"}
    now = OBSERVED + timedelta(days=1)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts)
            values(%s,'comments','unrelated-unresolved',3)""",
            (owned["run_id"],),
        )
    assert load(snapshot_database, owned).comments_complete
    assert store.eligible_media(item, now)["target_key"] == owned["target_key"]
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set outcome='unresolved',observed_at=null,
            committed_at=null,owner_token=null,owner_started_at=null,owner_expires_at=null
            where run_id=%s and stage='comments' and target_key=%s""",
            (owned["run_id"], owned["target_key"]),
        )
    with pytest.raises(MediaIncomplete, match="complete_comment_inventory_proof_required"):
        load(snapshot_database, owned)
    assert store.eligible_media(item, now) is None
    # A settled target still cannot borrow an old generation's otherwise exact receipt.
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set outcome='committed',observed_at=%s,
            committed_at=clock_timestamp(),owner_generation=owner_generation+1
            where run_id=%s and stage='comments' and target_key=%s""",
            (OBSERVED + timedelta(minutes=3), owned["run_id"], owned["target_key"]),
        )
    with pytest.raises(MediaIncomplete, match="complete_comment_inventory_proof_required"):
        load(snapshot_database, owned)
    assert store.eligible_media(item, now) is None


def test_media_turn_is_durable_and_comments_keep_priority_between_batch_boundaries(snapshot_database):
    owned, _, _, _ = seed(snapshot_database, selected_comments=True)
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            (
                Path(__file__).parents[2] / "supabase/migrations/20261016070100_instagram_ingestion_workflows.sql"
            ).read_text()
        )
        cur.execute(
            """insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts)
            values(%s,'comments','another-post',3)""",
            (owned["run_id"],),
        )
        cur.execute(
            """update social.instagram_ingestion_manifests set discovery_complete=true,sealed_at=clock_timestamp()
            where run_id=%s and stage='listing'""",
            (owned["run_id"],),
        )
        cur.execute("select * from social.instagram_ingestion_runs where run_id=%s", (owned["run_id"],))
        run_row = dict(zip([column.name for column in cur.description], cur.fetchone(), strict=True))
    store = WorkflowStore(tx)
    part = store.seed(run_row)
    now = OBSERVED + timedelta(days=1)
    assert store.decision(part, 0, now)["stage"] == "comments"
    expected = {"kind": "target", "stage": "media", "target_key": owned["target_key"]}
    assert store.eligible_media(part, now) == expected
    assert store.decision({**part, "prefer_media": True}, 1, now) == expected
    assert store.decision(part, 1, now) == expected  # replay keeps the recorded choice
    assert store.decision(part, 2, now)["stage"] == "comments"
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_comments set is_missing=true where post_id in "
            "(select id from social.instagram_posts where shortcode=%s)",
            (owned["target_key"],),
        )
    assert store.eligible_media(part, now) is None


def test_selected_details_require_exact_current_post_receipt(snapshot_database):
    owned, ids, _, _ = seed(snapshot_database, selected_comments=True)
    store, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_runs set intent=jsonb_set(intent,'{selected_tasks}',
            '["post_details","comments","media"]'::jsonb) where run_id=%s""",
            (owned["run_id"],),
        )
        cur.execute(
            "insert into social.instagram_ingestion_manifests(run_id,stage) values(%s,'post_details')",
            (owned["run_id"],),
        )
    detail_owned = discover(
        store, {"run_id": owned["run_id"], "generation": owned["generation"]}, Stage.DETAILS, owned["target_key"]
    )
    with pytest.raises(MediaIncomplete, match="complete_detail_inventory_proof_required"):
        load(snapshot_database, owned)
    token = str(store.reserve(**detail_owned)["owner_token"])
    store.acquire(**detail_owned, owner_token=token)
    store.commit_target(
        **detail_owned, owner_token=token, observed_at=OBSERVED, write_canonical=lambda _: {"post_id": ids["post_id"]}
    )
    assert load(snapshot_database, owned).inventory_fingerprint


def test_repeatable_read_only_snapshot_accepts_pool_setup_without_network_callbacks(snapshot_database):
    _, tx = snapshot_database
    owned, _, _, _ = seed(snapshot_database)

    @contextmanager
    def pool_connection():
        with tx() as conn:
            with conn.cursor() as cur:
                cur.execute("set local search_path=social,public")
            yield conn
            with conn.cursor() as cur:
                cur.execute("show transaction_read_only")
                assert cur.fetchone()[0] == "on"
                cur.execute("show transaction_isolation")
                assert cur.fetchone()[0] == "repeatable read"

    calls = []
    MediaSnapshotLoader(pool_connection).load(
        **{key: value for key, value in owned.items() if key != "stage"},
        check_cancelled=lambda: calls.append("fence"),
        on_request=lambda: pytest.fail("Read-only SQL attempted a source request"),
    )
    assert calls == ["fence", "fence"]


def test_evidence_to_canonical_to_snapshot_keeps_cdn_delivery_signature(snapshot_database):
    from tests.ingestion.test_media import Response, storage
    from trr_backend.ingestion.media import mirror_snapshot

    store, tx = snapshot_database
    owned, ids, _, _ = seed(snapshot_database, comments=0)
    signed = "https://scontent.cdninstagram.com/image.png?oh=asset%2Bsignature&oe=ABC"
    raw = {
        "shortcode": owned["target_key"],
        "pk": "123",
        "owner": {"username": "fixture_account"},
        "caption": {"text": "Signed delivery"},
        "like_count": 1,
        "comment_count": 0,
        "media_urls": [signed],
        "thumbnail_url": signed,
    }
    evidence = Evidence.capture(
        source_identity="instagram:fixture",
        captured_at=OBSERVED + timedelta(seconds=1),
        parser_version="fixture",
        payload=raw,
    )
    retained = json.loads(evidence.payload_json)
    with tx() as conn:
        write_post(
            conn,
            run_id=owned["run_id"],
            account="fixture_account",
            observed_at=evidence.captured_at,
            normalized=normalize_post_source(retained, account="fixture_account"),
            source_payload=retained,
        )
    snapshot = load(snapshot_database, owned)
    assert snapshot.media_urls == (signed,)
    requested = []

    def opener(client, url, **kwargs):
        assert url in {signed, SOURCE}
        requested.append(url)
        return Response()

    mirror_snapshot(snapshot, storage=storage(opener=opener), check=lambda: None, request=lambda: None)
    assert signed in requested


def test_hd_avatar_fallback_keeps_delivery_parameters():
    from trr_backend.ingestion.media_snapshot import _profile_url

    assert _profile_url({"hd_profile_pic_url_info": {"url": SOURCE}}) == SOURCE


def test_unassigned_account_post_has_legacy_shared_storage_context(snapshot_database):
    import hashlib
    from uuid import UUID

    owned, ids, _, _ = seed(snapshot_database, comments=0)
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set show_id=null,season_id=null,posted_at=%s where id=%s",
            (OBSERVED, ids["post_id"]),
        )
    snapshot = load(snapshot_database, owned)
    expected = str(UUID(hashlib.md5(b"show:network:instagram:fixture_account").hexdigest()))
    assert snapshot.show_id == expected
    assert snapshot.season_number == 0 and snapshot.week_index == 1
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select show_id,season_id from social.instagram_posts where id=%s", (ids["post_id"],))
        assert cur.fetchone() == (None, None)


def test_retry_preserves_retained_media_storage_context(snapshot_database):
    store, tx = snapshot_database
    owned, ids, _, _ = seed(snapshot_database, comments=0)
    snapshot = load(snapshot_database, owned)
    token = str(store.reserve(**owned)["owner_token"])
    store.acquire(**owned, owner_token=token)
    evidence = Evidence.capture(
        source_identity="instagram:media:" + owned["target_key"],
        captured_at=OBSERVED,
        parser_version=MEDIA_VERSION,
        payload={"show_id": snapshot.show_id, "season_number": 4, "week_index": 8},
    )
    store.retain_evidence(**owned, owner_token=token, evidence=evidence)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_posts set show_id=null,season_id=null where id=%s", (ids["post_id"],))
    after = load(snapshot_database, owned)
    assert (after.show_id, after.season_number, after.week_index) == (snapshot.show_id, 4, 8)


def registry_mention(
    database, *, status, checked_at, source_url=None, hosted_url=None, content_hash=None, failure_reason=None
):
    owned, ids, _, _ = seed(database, comments=0)
    handle = "registry_" + uuid4().hex[:12]
    _, tx = database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set mentions=%s::jsonb where id=%s",
            (json.dumps(["@" + handle]), ids["post_id"]),
        )
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,hosted_url,
            content_hash,failure_reason,last_checked_at) values('instagram',%s,%s,%s,%s,%s,%s,%s)""",
            (handle, source_url, status, hosted_url, content_hash, failure_reason, checked_at),
        )
    return owned, handle


def assert_no_snapshot_source_requests(database, owned):
    _, tx = database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select requests from social.instagram_ingestion_targets where run_id=%s and stage='media'",
            (owned["run_id"],),
        )
        assert cur.fetchone()[0] == 0


def test_missing_avatar_handles_are_aggregated_without_source_requests(snapshot_database):
    owned, ids, _, _ = seed(snapshot_database, comments=0)
    handles = ["a" + uuid4().hex[:12], "b" + uuid4().hex[:12]]
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set mentions=%s::jsonb,collaborators=%s::jsonb where id=%s",
            (json.dumps(["@" + handles[1], "@" + handles[0]]), json.dumps([handles[0]]), ids["post_id"]),
        )
    with pytest.raises(AvatarSourcesMissing) as error:
        load(snapshot_database, owned)
    assert error.value.handles == tuple(sorted(handles))
    assert error.value.code == "avatar_source_identity_unresolved"
    assert_no_snapshot_source_requests(snapshot_database, owned)


def test_pending_avatar_registry_reloads_sanitized_fetchable_url(snapshot_database):
    signed = "https://scontent.cdninstagram.com/avatar.png?oh=asset%2Bsignature&oe=ABC"
    observed = OBSERVED + timedelta(minutes=4)
    owned, handle = registry_mention(snapshot_database, status="pending", checked_at=observed, source_url=signed)
    snapshot = load(snapshot_database, owned)
    avatar = next(item for item in snapshot.avatars if item.username == handle)
    assert avatar.source_url == signed and avatar.observed_at == observed
    assert avatar.role == "mention" and avatar.cached_hosted_url is avatar.cached_sha256 is None
    assert snapshot.avatars_complete and snapshot.unavailable_avatars == ()
    assert "asset%2Bsignature" not in repr(avatar)
    assert_no_snapshot_source_requests(snapshot_database, owned)


@pytest.mark.parametrize("age_hours,accepted", [(0, True), (167, True), (168, False), (169, False), (-1, False)])
def test_unsupported_avatar_registry_proof_respects_ttl_and_future_clock(snapshot_database, age_hours, accepted):
    clock = OBSERVED + timedelta(days=10)
    owned, handle = registry_mention(
        snapshot_database,
        status="unsupported",
        checked_at=clock - timedelta(hours=age_hours),
        failure_reason="profile_pic_not_available",
    )
    if accepted:
        snapshot = load(snapshot_database, owned, clock=lambda: clock)
        assert snapshot.unavailable_avatars == (handle,)
        assert snapshot.avatars_complete and all(item.username != handle for item in snapshot.avatars)
    else:
        with pytest.raises(AvatarSourcesMissing) as error:
            load(snapshot_database, owned, clock=lambda: clock)
        assert error.value.handles == (handle,)
    assert_no_snapshot_source_requests(snapshot_database, owned)


def test_unsupported_avatar_registry_uses_explicit_configured_ttl_and_reason(snapshot_database):
    clock = OBSERVED + timedelta(days=10)
    owned, handle = registry_mention(
        snapshot_database,
        status="unsupported",
        checked_at=clock - timedelta(hours=2),
        failure_reason="profile_pic_not_available",
    )
    assert load(snapshot_database, owned, clock=lambda: clock, unsupported_ttl_hours=3).unavailable_avatars == (handle,)
    with pytest.raises(AvatarSourcesMissing):
        load(snapshot_database, owned, clock=lambda: clock, unsupported_ttl_hours=1)
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.avatar_registry set failure_reason='network_error' where account_handle=%s", (handle,)
        )
    with pytest.raises(AvatarSourcesMissing):
        load(snapshot_database, owned, clock=lambda: clock)
    assert_no_snapshot_source_requests(snapshot_database, owned)


@pytest.mark.parametrize(
    "age_hours,reason", [(169, "profile_pic_not_available"), (-1, "profile_pic_not_available"), (1, "network_error")]
)
def test_invalid_unsupported_proof_never_falls_back_to_its_old_source_url(snapshot_database, age_hours, reason):
    clock = OBSERVED + timedelta(days=10)
    owned, handle = registry_mention(
        snapshot_database,
        status="unsupported",
        checked_at=clock - timedelta(hours=age_hours),
        source_url=SOURCE,
        failure_reason=reason,
    )
    with pytest.raises(AvatarSourcesMissing) as error:
        load(snapshot_database, owned, clock=lambda: clock)
    assert error.value.handles == (handle,)
    assert_no_snapshot_source_requests(snapshot_database, owned)


def test_mirrored_registry_pair_becomes_cached_avatar_and_skips_avatar_cdn(snapshot_database):
    from tests.ingestion.test_media import Response, storage
    from trr_backend.ingestion.media import mirror_snapshot

    origin = "https://scontent.cdninstagram.com/legacy-queryless-avatar.png"
    hosted = "https://media.example/social/instagram/profiles/registry/avatar.png"
    digest = "a" * 64
    observed = OBSERVED + timedelta(minutes=4)
    owned, handle = registry_mention(
        snapshot_database,
        status="mirrored",
        checked_at=observed,
        source_url=origin,
        hosted_url=hosted,
        content_hash=digest,
    )
    snapshot = load(snapshot_database, owned)
    avatar = next(item for item in snapshot.avatars if item.username == handle)
    assert (avatar.cached_hosted_url, avatar.cached_sha256, avatar.observed_at) == (hosted, digest, observed)
    requested = []

    def opener(client, url, **kwargs):
        assert url != origin, "Queryless legacy avatar must use its persisted hosted proof"
        requested.append(url)
        return Response()

    result = mirror_snapshot(snapshot, storage(opener=opener), check=lambda: None, request=lambda: None)
    mirrored = next(item for item in result.avatars if item["username"] == handle)
    assert mirrored["hosted_url"] == hosted and mirrored["sha256"] == digest
    assert result.post_fields["hosted_tagged_profile_pics"][handle] == hosted
    assert origin not in requested and requested
    assert_no_snapshot_source_requests(snapshot_database, owned)


def test_older_post_reuses_newer_matching_owner_proof_without_avatar_download(snapshot_database):
    from tests.ingestion.test_media import Response, storage
    from trr_backend.ingestion.media import MediaActivity

    store, tx = snapshot_database
    handle = "ordered_" + uuid4().hex[:12]
    avatar_url = "https://scontent.cdninstagram.com/ordered-owner.png?oh=delivery"
    older, older_ids, _, _ = seed(snapshot_database, comments=0)
    newer, newer_ids, _, _ = seed(snapshot_database, comments=0)
    newer_time = OBSERVED + timedelta(minutes=1)
    with tx() as conn, conn.cursor() as cur:
        for ids, stamp in ((older_ids, OBSERVED), (newer_ids, newer_time)):
            cur.execute(
                """update social.instagram_posts set owner_username=%s,owner_profile_pic_url=%s,
                owner_profile_pic_url_hd=null,scraped_at=%s where id=%s""",
                (handle, avatar_url, stamp, ids["post_id"]),
            )
    downloaded = []

    def opener(client, url, **kwargs):
        downloaded.append(url)
        return Response()

    service = storage(opener=opener)

    def settle(owned):
        source = load(snapshot_database, owned)
        token = str(store.reserve(**owned)["owner_token"])
        return source, MediaActivity(store, storage=service).execute(
            **owned, owner_token=token, load=lambda **callbacks: source
        )

    _, newer_receipt = settle(newer)
    assert newer_receipt["outcome"] == "committed" and downloaded.count(avatar_url) == 1
    downloaded.clear()
    source, older_receipt = settle(older)
    assert older_receipt["outcome"] == "committed"
    assert older_receipt["observed_at"] == source.observed_at == OBSERVED
    assert source.avatars[0].observed_at == newer_time and source.avatars[0].cached_hosted_url
    assert downloaded == [SOURCE]  # Only this post's cover; its owner avatar is already hosted.
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select last_checked_at from social.avatar_registry where account_handle=%s", (handle,))
        assert cur.fetchone()[0] == newer_time


@pytest.mark.parametrize("status", ["mirrored", "pending", "unsupported"])
def test_newer_different_owner_version_does_not_invalidate_explicit_source(snapshot_database, status):
    owned, ids, _, _ = seed(snapshot_database, comments=0)
    _, tx = snapshot_database
    handle = "conflict_" + uuid4().hex[:12]
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_posts set owner_username=%s where id=%s", (handle, ids["post_id"]))
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,hosted_url,
            content_hash,failure_reason,last_checked_at) values('instagram',%s,%s,%s,%s,%s,%s,%s)""",
            (
                handle,
                None if status == "unsupported" else "https://scontent.cdninstagram.com/other-owner.png",
                status,
                "https://media.example/other-owner.png",
                "a" * 64,
                "profile_pic_not_available" if status == "unsupported" else None,
                OBSERVED + timedelta(minutes=1),
            ),
        )
    snapshot = load(snapshot_database, owned)
    avatar = next(item for item in snapshot.avatars if item.username == handle)
    assert avatar.source_url == SOURCE and avatar.observed_at == OBSERVED
    assert avatar.cached_hosted_url is avatar.cached_sha256 is None
    assert_no_snapshot_source_requests(snapshot_database, owned)


def test_newer_matching_owner_requires_complete_hosted_proof(snapshot_database):
    owned, ids, _, _ = seed(snapshot_database, comments=0)
    _, tx = snapshot_database
    handle = "incomplete_" + uuid4().hex[:12]
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_posts set owner_username=%s where id=%s", (handle, ids["post_id"]))
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,hosted_url,
            last_checked_at) values('instagram',%s,%s,'mirrored',%s,%s)""",
            (handle, SOURCE, "https://media.example/owner.png", OBSERVED + timedelta(minutes=1)),
        )
    with pytest.raises(MediaIncomplete, match="avatar_source_newer"):
        load(snapshot_database, owned)
    assert_no_snapshot_source_requests(snapshot_database, owned)


@pytest.mark.parametrize(
    "hosted,digest",
    [
        (None, "a" * 64),
        ("https://media.example/avatar.png", None),
        ("https://media.example/avatar.png", "invalid-hash"),
    ],
)
def test_incomplete_mirrored_registry_pair_cannot_claim_cached_hosting(snapshot_database, hosted, digest):
    owned, handle = registry_mention(
        snapshot_database,
        status="mirrored",
        checked_at=OBSERVED,
        source_url=SOURCE,
        hosted_url=hosted,
        content_hash=digest,
    )
    avatar = next(item for item in load(snapshot_database, owned).avatars if item.username == handle)
    assert avatar.cached_hosted_url is avatar.cached_sha256 is None
    assert avatar.source_url == SOURCE
    assert_no_snapshot_source_requests(snapshot_database, owned)


@pytest.mark.parametrize("pending_newer", [False, True])
def test_registry_prefers_newest_observation_then_mirrored_proof_for_equal_timestamp(snapshot_database, pending_newer):
    origin = "https://scontent.cdninstagram.com/avatar.png"
    hosted = "https://media.example/profile/avatar.png"
    owned, handle = registry_mention(
        snapshot_database,
        status="pending",
        checked_at=OBSERVED + timedelta(seconds=1) if pending_newer else OBSERVED,
        source_url=origin + "?oh=delivery&oe=ABC",
    )
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        # Pending and mirrored rows are created by the resolver and media
        # writer from the same source capture. UUID ordering is not evidence.
        cur.execute(
            "update social.avatar_registry set id=%s where account_handle=%s",
            ("ffff" + uuid4().hex[4:], handle),
        )
        cur.execute(
            """insert into social.avatar_registry(id,platform,account_handle,source_url,status,hosted_url,
            content_hash,last_checked_at) values(%s,'instagram',%s,%s,'mirrored',%s,%s,%s)""",
            ("0000" + uuid4().hex[4:], handle, origin, hosted, "a" * 64, OBSERVED),
        )
    avatar = next(item for item in load(snapshot_database, owned).avatars if item.username == handle)
    if pending_newer:
        assert avatar.cached_hosted_url is avatar.cached_sha256 is None
        assert avatar.source_url == origin + "?oh=delivery&oe=ABC"
        assert avatar.observed_at == OBSERVED + timedelta(seconds=1)
    else:
        assert avatar.cached_hosted_url == hosted and avatar.cached_sha256 == "a" * 64
    assert_no_snapshot_source_requests(snapshot_database, owned)


def test_valid_cached_pair_outranks_null_hash_at_equal_registry_observation(snapshot_database):
    origin = "https://scontent.cdninstagram.com/avatar.png"
    hosted = "https://media.example/profile/complete-avatar.png"
    owned, handle = registry_mention(
        snapshot_database,
        status="mirrored",
        checked_at=OBSERVED,
        source_url=origin,
        hosted_url=hosted,
        content_hash="a" * 64,
    )
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,hosted_url,
            content_hash,last_checked_at) values('instagram',%s,%s,'mirrored',%s,null,%s)""",
            (handle, origin + "?oh=delivery", "https://media.example/profile/incomplete-avatar.png", OBSERVED),
        )
    avatar = next(item for item in load(snapshot_database, owned).avatars if item.username == handle)
    assert avatar.cached_hosted_url == hosted and avatar.cached_sha256 == "a" * 64


def test_explicit_unavailable_avatar_inventory_is_retained_without_post_unavailable(snapshot_database):
    clock = OBSERVED + timedelta(days=10)
    owned, handle = registry_mention(
        snapshot_database,
        status="unsupported",
        checked_at=clock - timedelta(hours=1),
        failure_reason="profile_pic_not_available",
    )
    snapshot = load(snapshot_database, owned, clock=lambda: clock)
    store, tx = snapshot_database
    token = str(store.reserve(**owned)["owner_token"])
    receipt = execute(snapshot_database, owned, token, snapshot)
    assert receipt["outcome"] == "committed" and snapshot.unavailable is None
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select payload from social.instagram_ingestion_evidence where run_id=%s and stage='media'",
            (owned["run_id"],),
        )
        retained = json.loads(cur.fetchone()[0])
    assert retained["unavailable_avatars"] == [handle]
    assert retained["avatars_complete"] is True


def test_existing_manifest_root_precedes_new_shared_context(snapshot_database):
    owned, ids, _, _ = seed(snapshot_database, comments=0)
    _, tx = snapshot_database
    historical_show = str(uuid4())
    manifest = {
        "originals": {
            "cover": {
                "storage_key": (
                    f"prefix/social/instagram/{historical_show}/0/week-unknown/{owned['target_key']}/cover-original.png"
                )
            }
        }
    }
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set show_id=null,season_id=null,posted_at=%s where id=%s",
            (OBSERVED, ids["post_id"]),
        )
        cur.execute(
            "update social.instagram_post_payloads set asset_manifest=%s::jsonb where post_id=%s",
            (json.dumps(manifest), ids["post_id"]),
        )
    snapshot = load(snapshot_database, owned)
    assert (snapshot.show_id, snapshot.season_number, snapshot.week_index) == (historical_show, 0, None)


@pytest.mark.parametrize("proof_offset_minutes", [-1, 1])
def test_comment_exact_old_version_reuses_proof_despite_newer_profile_image(snapshot_database, proof_offset_minutes):
    from tests.ingestion.test_media import Response, storage
    from trr_backend.ingestion.media import MediaActivity

    store, tx = snapshot_database
    owned, ids, comments, _ = seed(snapshot_database, comments=1)
    handle = "commentver_" + uuid4().hex[:12]
    old_source = "https://scontent.cdninstagram.com/retained-comment-version.png?oh=old-delivery"
    old_reference = old_source.split("?")[0]
    new_reference = "https://scontent.cdninstagram.com/new-profile-version.png"
    old_hosted = "https://media.example/profiles/retained-comment-version.png"
    new_hosted = "https://media.example/profiles/new-profile-version.png"
    slot_time = OBSERVED + timedelta(minutes=2)
    proof_time = slot_time + timedelta(minutes=proof_offset_minutes)
    newer_time = slot_time + timedelta(minutes=2)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_comments set username=%s,author_profile_pic_url=%s,
            author_profile_pic_url_hd=null where id=%s""",
            (handle, old_source, comments[0][0]),
        )
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,hosted_url,
            content_hash,last_checked_at) values('instagram',%s,%s,'mirrored',%s,%s,%s),
            ('instagram',%s,%s,'mirrored',%s,%s,%s)""",
            (
                handle,
                old_reference,
                old_hosted,
                "a" * 64,
                proof_time,
                handle,
                new_reference,
                new_hosted,
                "b" * 64,
                newer_time,
            ),
        )
    source = load(snapshot_database, owned, include_comment_avatars=True)
    avatar = next(item for item in source.avatars if item.role == "comment")
    assert source.observed_at == OBSERVED and source.comments[0].observed_at == slot_time
    assert avatar.source_url == old_source and avatar.observed_at == proof_time
    assert (avatar.cached_hosted_url, avatar.cached_sha256) == (old_hosted, "a" * 64)
    requested = []

    def opener(client, url, **kwargs):
        requested.append(url)
        return Response()

    token = str(store.reserve(**owned)["owner_token"])
    receipt = MediaActivity(store, storage=storage(opener=opener)).execute(
        **owned, owner_token=token, load=lambda **callbacks: source
    )
    assert receipt["outcome"] == "committed" and receipt["observed_at"] == OBSERVED
    assert old_source not in requested and new_reference not in requested
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select hosted_author_profile_pic_url,scraped_at from social.instagram_comments
            where id=%s""",
            (comments[0][0],),
        )
        assert cur.fetchone() == (old_hosted, slot_time)
        cur.execute(
            """select source_url,hosted_url,content_hash,last_checked_at from social.avatar_registry
            where account_handle=%s order by last_checked_at""",
            (handle,),
        )
        assert cur.fetchall() == [
            (old_reference, old_hosted, "a" * 64, proof_time),
            (new_reference, new_hosted, "b" * 64, newer_time),
        ]


def test_signed_exact_version_newer_incomplete_proof_does_not_fall_back_to_other_version(snapshot_database):
    owned, ids, _, _ = seed(snapshot_database, comments=0)
    _, tx = snapshot_database
    handle = "signedver_" + uuid4().hex[:12]
    reference = SOURCE.split("?")[0]
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_posts set owner_username=%s where id=%s", (handle, ids["post_id"]))
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,hosted_url,
            content_hash,last_checked_at) values('instagram',%s,%s,'mirrored',%s,%s,%s),
            ('instagram',%s,%s,'pending',null,null,%s),('instagram',%s,%s,'mirrored',%s,%s,%s)""",
            (
                handle,
                reference,
                "https://media.example/profiles/old.png",
                "a" * 64,
                OBSERVED,
                handle,
                reference + "?oh=new-observation",
                OBSERVED + timedelta(minutes=1),
                handle,
                "https://scontent.cdninstagram.com/different.png",
                "https://media.example/profiles/new.png",
                "b" * 64,
                OBSERVED + timedelta(minutes=2),
            ),
        )
    with pytest.raises(MediaIncomplete, match="avatar_source_newer"):
        load(snapshot_database, owned)
    assert_no_snapshot_source_requests(snapshot_database, owned)
