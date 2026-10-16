"""Private SQL and mock storage only; use --confcutdir=tests/ingestion."""

import hashlib
import io
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import requests
from botocore.exceptions import ClientError
from PIL import Image

from tests.ingestion.test_persistence import database as database_fixture
from trr_backend.ingestion.canonical import OlderObservation, normalize_post_source, write_post
from trr_backend.ingestion.domain import LostOwnership, RunIntent, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import (
    AvatarSource,
    CommentMedia,
    MediaActivity,
    MediaIncomplete,
    MediaSnapshot,
    MediaStorage,
    legacy_comment_source_key,
    mirror_snapshot,
    write_media,
)
from trr_backend.media.social_image_variants import SocialDisplayVariant

database = database_fixture
OBSERVED = datetime(2026, 10, 16, 12, tzinfo=UTC)
SOURCE = "https://scontent.cdninstagram.com/image.png?oh=private-signature"


def image_bytes(color="red"):
    output = io.BytesIO()
    Image.new("RGB", (12, 8), color).save(output, "PNG")
    return output.getvalue()


class Response:
    def __init__(self, data=None, *, status=200, headers=None):
        self.data = image_bytes() if data is None else data
        self.status_code = status
        self.headers = {"Content-Type": "image/png", **(headers or {})}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("transport URL must not enter errors", response=self)

    def iter_content(self, **kwargs):
        yield self.data


class Storage:
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.after_put = None

    def head_object(self, *, Bucket, Key):  # noqa: N803 - boto3 API
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        data, metadata = self.objects[Key]
        return {"ContentLength": len(data), "Metadata": metadata}

    def get_object(self, *, Bucket, Key):  # noqa: N803 - boto3 API
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        assert kwargs["IfNoneMatch"] == "*"
        if key in self.objects:
            raise ClientError(
                {"Error": {"Code": "PreconditionFailed"}, "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject"
            )
        self.objects[key] = kwargs["Body"], kwargs["Metadata"]
        self.writes.append(key)
        if self.after_put:
            self.after_put()
        return {"ETag": '"test"'}


def quick_variants(*, image_path, s3_client, bucket, key_prefix, build_hosted_url):
    data = Path(image_path).read_bytes()
    digest = hashlib.sha1(data).hexdigest()[:16]
    result = []
    for name in ("thumb", "card", "detail", "poster_card", "poster_detail"):
        for fmt, extension in (("webp", "webp"), ("jpeg", "jpg")):
            variant = name + "_" + fmt
            key = f"{key_prefix}/{digest}/{variant}.{extension}"
            s3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="image/" + fmt)
            result.append(
                SocialDisplayVariant(variant, fmt, build_hosted_url(key), "image/" + fmt, 12, 8, len(data), key)
            )
    return result


def storage(client=None, **kwargs):
    return MediaStorage(
        client=client or Storage(),
        bucket="private-fixture",
        build_hosted_url=lambda key: "https://media.example/" + key,
        opener=kwargs.pop("opener", lambda *a, **k: Response()),
        variant_generator=kwargs.pop("variant_generator", quick_variants),
        **kwargs,
    )


def snapshot(**kwargs):
    values = {
        "observed_at": OBSERVED,
        "post_id": str(uuid4()),
        "canonical_post_id": str(uuid4()),
        "shortcode": "MediaFixture",
        "show_id": str(uuid4()),
        "season_number": 2,
        "week_index": 3,
        "media_urls": (SOURCE,),
        "thumbnail_url": SOURCE,
        "expected_media_count": 1,
        "comments": (),
        "avatars": (),
        "comments_complete": True,
        "avatars_complete": True,
    }
    return MediaSnapshot(**(values | kwargs))


def noop():
    pass


def test_exact_source_reuse_downloads_once_and_preserves_distinct_manifest_roles():
    client = Storage()
    downloaded = []

    def opener(client, url, **kwargs):
        downloaded.append(url)
        return Response()

    result = mirror_snapshot(snapshot(), storage(client, opener=opener), check=noop, request=noop)
    manifest = result.post_fields["asset_manifest"]
    cover, media = manifest["originals"]["cover"], manifest["originals"]["media"][0]
    assert downloaded == [SOURCE]
    assert len(client.writes) == len(client.objects) == 11  # One original and ten display variants.
    assert result.post_fields["hosted_media_urls"] == [result.post_fields["hosted_thumbnail_url"]]
    assert cover is not media and cover["storage_key"] == media["storage_key"]
    assert (cover["source_role"], cover["variant_key"]) == ("cover", "cover_original")
    assert (media["source_role"], media["variant_key"]) == ("media", "media_original_01")
    assert cover["sha256"] == media["sha256"] and len(manifest["display_variants"]) == 10


def test_distinct_source_urls_with_identical_bytes_keep_separate_objects():
    client = Storage()
    other = SOURCE + "&oe=different-delivery"
    downloaded = []

    def opener(client, url, **kwargs):
        downloaded.append(url)
        return Response()

    result = mirror_snapshot(snapshot(media_urls=(other,)), storage(client, opener=opener), check=noop, request=noop)
    originals = result.post_fields["asset_manifest"]["originals"]
    cover, media = originals["cover"], originals["media"]
    assert downloaded == [SOURCE, other]
    assert cover["sha256"] == media[0]["sha256"]
    assert cover["storage_key"] != media[0]["storage_key"]
    assert len(client.writes) == 12


@pytest.mark.parametrize("shared_key", [False, True])
def test_exact_source_reuse_respects_preserved_role_storage_identity(shared_key):
    client = Storage()
    cover_key = "legacy/cover-original.png"
    media_key = cover_key if shared_key else "legacy/media-01.png"
    source = snapshot(
        existing_manifest={"originals": {"cover": {"storage_key": cover_key}, "media": [{"storage_key": media_key}]}}
    )
    downloaded = []

    def opener(client, url, **kwargs):
        downloaded.append(url)
        return Response()

    result = mirror_snapshot(source, storage(client, opener=opener), check=noop, request=noop)
    originals = result.post_fields["asset_manifest"]["originals"]
    assert originals["cover"]["storage_key"] == cover_key
    assert originals["media"][0]["storage_key"] == media_key
    assert downloaded == [SOURCE] * (1 if shared_key else 2)
    assert cover_key in client.objects and media_key in client.objects
    assert len(client.writes) == (11 if shared_key else 12)


def test_cached_avatar_proof_reuses_hosted_identity_without_cdn_or_storage_write():
    origin = "https://scontent.cdninstagram.com/old-queryless-avatar.png"
    hosted = "https://media.example/social/instagram/profiles/owner/fixed.png"
    digest = "a" * 64
    original = OBSERVED - timedelta(days=1)
    source = snapshot(
        avatars=(
            AvatarSource(
                "owner", origin, "owner", observed_at=original, cached_hosted_url=hosted, cached_sha256=digest
            ),
        )
    )
    downloaded = []

    def opener(client, url, **kwargs):
        assert url != origin
        downloaded.append(url)
        return Response()

    client = Storage()
    result = mirror_snapshot(source, storage(client, opener=opener), check=noop, request=noop)
    assert result.post_fields["hosted_owner_profile_pic_url"] == hosted
    assert result.avatars == (
        {
            "username": "owner",
            "source_url": origin,
            "hosted_url": hosted,
            "sha256": digest,
            "observed_at": original.isoformat(),
        },
    )
    assert origin not in downloaded and downloaded == [SOURCE]
    assert all("/profiles/" not in key for key in client.writes)


@pytest.mark.parametrize(
    "hosted,digest",
    [
        (None, "a" * 64),
        ("https://media.example/avatar.png", None),
        ("https://media.example/avatar.png", "invalid-hash"),
    ],
)
def test_incomplete_cached_avatar_proof_fails_before_any_source_download(hosted, digest):
    source = snapshot(avatars=(AvatarSource("owner", SOURCE, "owner", cached_hosted_url=hosted, cached_sha256=digest),))
    with pytest.raises(MediaIncomplete, match="cached_avatar_proof_incomplete"):
        mirror_snapshot(
            source,
            storage(opener=lambda *a, **kw: pytest.fail("Incomplete cached proof fetched source")),
            check=noop,
            request=noop,
        )


def test_existing_keys_comment_hash_avatar_paths_and_signed_url_redaction():
    client = Storage()
    comment_id = str(uuid4())
    comment = CommentMedia("comment1", comment_id, (SOURCE,), legacy_comment_source_key(comment_id, (SOURCE,)))
    source = snapshot(
        comments=(comment,),
        include_comment_avatars=True,
        avatars=(AvatarSource("owner", SOURCE, "owner"), AvatarSource("commenter", SOURCE, "comment", "comment1")),
    )
    result = mirror_snapshot(source, storage(client), check=noop, request=noop)
    root = f"social/instagram/{source.show_id}/2/week-3"
    assert root + "/MediaFixture/cover-original.png" in client.objects
    assert root + "/MediaFixture/media-01.png" not in client.objects
    assert result.post_fields["hosted_media_urls"] == [result.post_fields["hosted_thumbnail_url"]]
    assert root + "/" + comment.storage_source_key + "/media-01.png" in client.objects
    assert f"social/instagram/profile-pics/owner/{hashlib.sha256(image_bytes()).hexdigest()}.png" in client.objects
    assert result.comment_fields["comment1"]["hosted_author_profile_pic_url"]
    assert len(result.post_fields["asset_manifest"]["display_variants"]) == 10
    assert "private-signature" not in repr(source)
    assert "private-signature" not in repr(result)
    assert "private-signature" not in json.dumps(result.post_fields)
    assert "?" not in result.avatars[0]["source_url"]


def test_real_cover_variants_and_repeated_storage_writes_are_idempotent():
    from trr_backend.media.social_image_variants import generate_social_cover_display_variants

    client = Storage()
    service = storage(client, variant_generator=generate_social_cover_display_variants)
    source = snapshot()
    first = mirror_snapshot(source, service, check=noop, request=noop)
    count = len(client.writes)
    second = mirror_snapshot(source, service, check=noop, request=noop)
    assert first.post_fields == second.post_fields
    assert len(client.writes) == count == 11
    variants = first.post_fields["asset_manifest"]["display_variants"]
    assert variants["poster_card_webp"]["width"] == 720
    assert variants["poster_card_webp"]["height"] == 960


def test_existing_legacy_object_without_digest_compares_bytes_and_rejects_changed_content():
    client = Storage()
    service = storage(client)
    client.objects["old.png"] = image_bytes(), {}
    service.put("old.png", image_bytes(), "image/png", check=noop, request=noop)
    assert client.writes == []
    with pytest.raises(MediaIncomplete, match="existing_object_content_conflict"):
        service.put("old.png", image_bytes("blue"), "image/png", check=noop, request=noop)
    assert client.objects["old.png"][0] == image_bytes()


def test_storage_lost_response_and_competing_upload_are_idempotent():
    client = Storage()
    original = client.put_object

    def lose_response(**kwargs):
        original(**kwargs)
        raise ClientError(
            {"Error": {"Code": "InternalError"}, "ResponseMetadata": {"HTTPStatusCode": 503}}, "PutObject"
        )

    client.put_object = lose_response
    storage(client).put("image.png", image_bytes(), "image/png", check=noop, request=noop)
    assert client.writes == ["image.png"]


@pytest.mark.parametrize(
    "change,code",
    [
        ({"comments_complete": False}, "inventory_not_sealed"),
        ({"avatars_complete": False}, "inventory_not_sealed"),
        ({"expected_media_count": 2}, "coverage_incomplete"),
        ({"thumbnail_url": ""}, "coverage_incomplete"),
        ({"observed_at": OBSERVED.replace(tzinfo=None)}, "original_observation_required"),
        ({"comments": (CommentMedia("one", str(uuid4()), (SOURCE,)),)}, "storage_key_required"),
    ],
)
def test_incomplete_inventory_cannot_start_network(change, code):
    with pytest.raises(MediaIncomplete, match=code):
        mirror_snapshot(
            snapshot(**change),
            storage(opener=lambda *a, **k: pytest.fail("Unexpected network")),
            check=noop,
            request=noop,
        )


def test_redirects_refresh_and_transient_retries_are_fenced_and_bounded():
    calls = []
    attempts = iter(
        [
            Response(status=302, headers={"location": "https://scontent.cdninstagram.com/new.png"}),
            Response(status=403),
            Response(status=503),
            Response(),
        ]
    )

    def opener(*args, **kwargs):
        calls.append(args[1])
        return next(attempts)

    checks, requests_count = [], []
    service = storage(opener=opener, refresh=lambda **kwargs: SOURCE + "-fresh")
    data, content_type = service._download(
        SOURCE, check=lambda: checks.append(1), request=lambda: requests_count.append(1)
    )
    assert data == image_bytes() and content_type == "image/png"
    assert len(calls) == len(requests_count) == 4
    assert len(checks) >= 4


@pytest.mark.parametrize(
    "response,code",
    [
        (Response(status=404), "media_http_404"),
        (Response(data=b"<html>sign in</html>"), "wrong_content_type"),
        (Response(headers={"Content-Length": "999999999"}), "asset_too_large"),
    ],
)
def test_cdn_failures_never_mean_source_unavailable(response, code):
    with pytest.raises(MediaIncomplete, match=code):
        storage(opener=lambda *a, **k: response)._download(SOURCE, check=noop, request=noop)


def test_private_or_cross_platform_redirect_is_rejected_before_next_request():
    calls = []

    def opener(*args, **kwargs):
        calls.append(1)
        return Response(status=302, headers={"location": "http://127.0.0.1/private"})

    with pytest.raises(MediaIncomplete, match="unsafe_media_url"):
        storage(opener=opener)._download(SOURCE, check=noop, request=noop)
    assert len(calls) == 1


@pytest.fixture(scope="module")
def media_database(database):
    store, tx = database
    schema = (Path(__file__).parents[1] / "fixtures/ingestion/canonical_schema.sql").read_text()
    schema = schema.replace("create schema core; create schema social;", "create schema core;")
    schema = schema.replace("create table social.scrape_runs(id uuid primary key);", "")
    with tx() as conn, conn.cursor() as cur:
        cur.execute(schema)
        cur.execute("""create table social.avatar_registry(id uuid primary key default gen_random_uuid(),platform text,
            account_handle text,source_url text,content_hash text,hosted_url text,status text,failure_reason text,
            last_checked_at timestamptz,created_at timestamptz default now(),updated_at timestamptz,
            unique(platform,account_handle,source_url));""")
    return store, tx


def launch_media(database, *, comments=False):
    store, tx = database
    source = snapshot(shortcode="Media" + uuid4().hex[:10])
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
        "stage": Stage.MEDIA,
        "target_key": source.shortcode,
    }
    store.discover(
        **{k: v for k, v in owned.items() if k != "target_key"},
        target_keys=[source.shortcode],
        max_attempts=3,
        cursor_state={},
        expected_cursor_state={},
        page_id="one",
        seal=True,
    )
    token = str(store.reserve(**owned)["owner_token"])
    payload = {
        "shortcode": source.shortcode,
        "pk": "101",
        "owner": {"username": "fixture_account"},
        "caption": {"text": "Preserve caption"},
        "like_count": 9,
        "comment_count": 0,
        "display_url": SOURCE.split("?")[0],
        "media_urls": [SOURCE.split("?")[0]],
    }
    with tx() as conn:
        ids = write_post(
            conn,
            run_id=run["run_id"],
            account="fixture_account",
            observed_at=OBSERVED,
            normalized=normalize_post_source(payload, account="fixture_account"),
            source_payload=payload,
        )
    source = replace(source, post_id=ids["post_id"], canonical_post_id=ids["canonical_post_id"])
    if comments:
        row_id = str(uuid4())
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into social.instagram_comments(id,comment_id,post_id,username,text,scraped_at,
                last_seen_at,media_urls) values(%s,'comment1',%s,'commenter','Preserve comment',%s,%s,%s::jsonb)""",
                (row_id, source.post_id, OBSERVED, OBSERVED, json.dumps([SOURCE.split("?")[0]])),
            )
        comment = CommentMedia("comment1", row_id, (SOURCE,), legacy_comment_source_key(row_id, (SOURCE,)))
        source = replace(
            source,
            comments=(comment,),
            include_comment_avatars=True,
            avatars=(
                AvatarSource("fixture_account", SOURCE, "owner"),
                AvatarSource("commenter", SOURCE, "comment", "comment1"),
            ),
        )
    return owned, token, source


def execute(database, owned, token, source, *, client=None, writer=write_media):
    store, tx = database

    def opener(*a, **k):
        # A separate transaction can take the business lock while downloading.
        with tx() as conn, conn.cursor() as cur:
            cur.execute("set local lock_timeout='100ms'")
            cur.execute("select id from social.instagram_posts where id=%s for update", (source.post_id,))
        return Response()

    return MediaActivity(store, storage=storage(client, opener=opener), writer=writer).execute(
        **owned, owner_token=token, load=lambda **callbacks: source
    )


def test_real_sql_media_and_receipt_commit_together_preserving_source(media_database):
    owned, token, source = launch_media(media_database, comments=True)
    auth_bearing = (
        SOURCE + "&sessionid=must-not-retain-session&csrftoken=must-not-retain-csrf&access_token=must-not-retain-token"
    )
    source = replace(
        source,
        media_urls=(auth_bearing,),
        thumbnail_url=auth_bearing,
        comments=tuple(replace(comment, media_urls=(auth_bearing,)) for comment in source.comments),
        avatars=tuple(replace(avatar, source_url=auth_bearing) for avatar in source.avatars),
    )
    receipt = execute(media_database, owned, token, source)
    assert receipt["outcome"] == "committed" and receipt["observed_at"] == OBSERVED
    _, tx = media_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select caption,likes,scraped_at,hosted_media_urls,hosted_owner_profile_pic_url
            from social.instagram_posts where id=%s""",
            (source.post_id,),
        )
        caption, likes, observed, hosted, avatar = cur.fetchone()
        assert (caption, likes, observed) == ("Preserve caption", 9, OBSERVED)
        assert len(hosted) == 1 and avatar
        cur.execute(
            "select hosted_url from social.social_post_media_assets where post_id=%s", (source.canonical_post_id,)
        )
        assert cur.fetchone()[0] == hosted[0]
        cur.execute(
            "select text,hosted_media_urls,hosted_author_profile_pic_url from social.instagram_comments where id=%s",
            (source.comments[0].row_id,),
        )
        text, comment_hosted, comment_avatar = cur.fetchone()
        assert text == "Preserve comment" and len(comment_hosted) == 1 and comment_avatar
        cur.execute(
            "select payload from social.instagram_ingestion_evidence where run_id=%s and parser_version=%s",
            (owned["run_id"], "instagram-media-v1"),
        )
        evidence = cur.fetchone()[0]
        if isinstance(evidence, str):
            evidence = json.loads(evidence)
        assert "must-not-retain" not in str(evidence)
        assert not any(key in str(evidence) for key in ("sessionid", "csrftoken", "access_token"))
        assert evidence["media_urls"] == [SOURCE] and evidence["thumbnail_url"] == SOURCE
        assert evidence["post_id"] == source.post_id
        assert evidence["canonical_post_id"] == source.canonical_post_id
        assert evidence["comments"][0]["storage_source_key"] == source.comments[0].storage_source_key
        assert evidence["avatars"][0]["profile_pic_url"] == SOURCE
        cur.execute(
            "select count(*) from social.avatar_registry where account_handle in ('fixture_account','commenter')"
        )
        assert cur.fetchone()[0] == 2


@pytest.mark.parametrize("when", ["before_download", "after_upload"])
def test_cancellation_fences_network_and_canonical_receipt(media_database, when):
    store, tx = media_database
    owned, token, source = launch_media(media_database)
    client = Storage()

    def cancel():
        store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))

    if when == "before_download":
        cancel()
    else:
        client.after_put = cancel
    with pytest.raises(LostOwnership):
        execute(media_database, owned, token, source, client=client)
    assert bool(client.writes) == (when == "after_upload")
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0
        cur.execute("select hosted_media_urls from social.instagram_posts where id=%s", (source.post_id,))
        assert cur.fetchone()[0] == []


def test_writer_failure_rolls_back_every_media_column_and_receipt(media_database):
    _, tx = media_database
    owned, token, source = launch_media(media_database)

    def fail(conn, **kwargs):
        write_media(conn, **kwargs)
        raise RuntimeError("Injected after canonical media writes")

    with pytest.raises(RuntimeError, match="Injected"):
        execute(media_database, owned, token, source, writer=fail)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select hosted_media_urls from social.instagram_posts where id=%s", (source.post_id,))
        assert cur.fetchone()[0] == []
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0


def test_old_replay_cannot_replace_newer_observation(media_database):
    _, tx = media_database
    owned, token, source = launch_media(media_database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set scraped_at=%s where id=%s",
            (OBSERVED + timedelta(hours=1), source.post_id),
        )
    with pytest.raises(OlderObservation):
        execute(media_database, owned, token, source)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0


def test_explicit_source_unavailable_proof_settles_without_network(media_database):
    store, _ = media_database
    owned, token, source = launch_media(media_database)
    proof = Evidence.capture(
        source_identity=f"instagram:media:{source.shortcode}",
        captured_at=OBSERVED,
        parser_version="source-proof-v1",
        payload={"status": "media_unavailable", "shortcode": source.shortcode},
    )
    source = replace(source, unavailable=proof, media_urls=(), thumbnail_url="", expected_media_count=0)
    receipt = MediaActivity(
        store, storage=storage(opener=lambda *a, **k: pytest.fail("Unavailable source downloaded"))
    ).execute(**owned, owner_token=token, load=lambda **callbacks: source)
    assert receipt["outcome"] == "source_unavailable"


def test_cdn_error_retains_evidence_but_cannot_settle_media_target(media_database):
    store, tx = media_database
    owned, token, source = launch_media(media_database)
    with pytest.raises(MediaIncomplete, match="media_http_404"):
        MediaActivity(store, storage=storage(opener=lambda *a, **k: Response(status=404))).execute(
            **owned, owner_token=token, load=lambda **callbacks: source
        )
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select outcome from social.instagram_ingestion_targets where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == "unresolved"
        cur.execute("select count(*) from social.instagram_ingestion_evidence where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 1


def test_expired_owner_after_upload_cannot_commit_or_continue_storage(media_database):
    _, tx = media_database
    owned, token, source = launch_media(media_database)
    client = Storage()

    def expire_owner():
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """update social.instagram_ingestion_targets
                set owner_expires_at=clock_timestamp()-interval '1 second' where run_id=%s""",
                (owned["run_id"],),
            )

    client.after_put = expire_owner
    with pytest.raises(LostOwnership):
        execute(media_database, owned, token, source, client=client)
    assert len(client.writes) == 1
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0


def test_newer_canonical_asset_fences_old_media_snapshot(media_database):
    _, tx = media_database
    owned, token, source = launch_media(media_database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.social_post_media_assets
            set media_payload=jsonb_set(media_payload,'{ingestion_media}',%s::jsonb,true) where post_id=%s""",
            (
                json.dumps(
                    {"version": "instagram-media-v1", "observed_at": (OBSERVED + timedelta(hours=1)).isoformat()}
                ),
                source.canonical_post_id,
            ),
        )
    with pytest.raises(OlderObservation):
        execute(media_database, owned, token, source)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select hosted_media_urls from social.instagram_posts where id=%s", (source.post_id,))
        assert cur.fetchone()[0] == []


@pytest.mark.parametrize("newer_source", [None, "https://scontent.cdninstagram.com/new-avatar.png"])
def test_newer_handle_observation_fences_cached_avatar_and_rolls_back_prior_post_writes(media_database, newer_source):
    _, tx = media_database
    owned, token, source = launch_media(media_database)
    handle = "guard_" + uuid4().hex[:12]
    origin = "https://scontent.cdninstagram.com/old-avatar.png"
    hosted = "https://media.example/profile/old-avatar.png"
    digest = "a" * 64
    newer = OBSERVED + timedelta(hours=1)
    source = replace(
        source,
        avatars=(
            AvatarSource(handle, origin, "owner", observed_at=OBSERVED, cached_hosted_url=hosted, cached_sha256=digest),
        ),
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,hosted_url,
            status,last_checked_at) values('instagram',%s,%s,%s,%s,'mirrored',%s)""",
            (handle, origin, digest, hosted, OBSERVED),
        )
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,failure_reason,
            last_checked_at) values('instagram',%s,%s,%s,%s,%s)""",
            (
                handle,
                newer_source,
                "unsupported" if newer_source is None else "pending",
                "profile_pic_not_available" if newer_source is None else None,
                newer,
            ),
        )
        cur.execute("select to_jsonb(p) from social.instagram_posts p where id=%s", (source.post_id,))
        original_post = cur.fetchone()[0]
        cur.execute("select to_jsonb(p) from social.instagram_post_payloads p where post_id=%s", (source.post_id,))
        original_sidecar = cur.fetchone()[0]
        cur.execute(
            "select to_jsonb(a) from social.social_post_media_assets a where post_id=%s", (source.canonical_post_id,)
        )
        original_asset = cur.fetchone()[0]
    changed_inside_transaction = []

    def write_then_observe_guard(conn, **kwargs):
        try:
            return write_media(conn, **kwargs)
        except OlderObservation:
            with conn.cursor() as cur:
                cur.execute("select hosted_media_urls from social.instagram_posts where id=%s", (source.post_id,))
                changed_inside_transaction.append(bool(cur.fetchone()[0]))
            raise

    with pytest.raises(OlderObservation):
        execute(media_database, owned, token, source, writer=write_then_observe_guard)
    assert changed_inside_transaction == [True]
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select to_jsonb(p) from social.instagram_posts p where id=%s", (source.post_id,))
        assert cur.fetchone()[0] == original_post
        cur.execute("select to_jsonb(p) from social.instagram_post_payloads p where post_id=%s", (source.post_id,))
        assert cur.fetchone()[0] == original_sidecar
        cur.execute(
            "select to_jsonb(a) from social.social_post_media_assets a where post_id=%s", (source.canonical_post_id,)
        )
        assert cur.fetchone()[0] == original_asset
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0
        cur.execute(
            "select status,last_checked_at from social.avatar_registry where account_handle=%s and source_url=%s",
            (handle, origin),
        )
        assert cur.fetchone() == ("mirrored", OBSERVED)


def test_legacy_attempt_wall_clocks_do_not_relabel_or_block_source_observation(media_database):
    _, tx = media_database
    owned, token, source = launch_media(media_database, comments=True)
    legacy_attempt = OBSERVED + timedelta(days=1)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set media_mirror_last_attempt_at=%s where id=%s",
            (legacy_attempt, source.post_id),
        )
        cur.execute(
            "update social.instagram_post_payloads set payload_updated_at=%s where post_id=%s",
            (legacy_attempt, source.post_id),
        )
        cur.execute(
            "update social.social_post_media_assets set mirror_last_attempt_at=%s where post_id=%s",
            (legacy_attempt, source.canonical_post_id),
        )
        cur.execute(
            "update social.instagram_comments set media_mirror_last_attempt_at=%s where post_id=%s",
            (legacy_attempt, source.post_id),
        )
    receipt = execute(media_database, owned, token, source)
    assert receipt["observed_at"] == OBSERVED
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select scraped_at,asset_manifest from social.instagram_posts where id=%s", (source.post_id,))
        observed, manifest = cur.fetchone()
        assert observed == OBSERVED
        assert manifest["ingestion_media"]["observed_at"] == OBSERVED.isoformat()
        cur.execute("select scraped_at,raw_data from social.instagram_comments where post_id=%s", (source.post_id,))
        observed, raw = cur.fetchone()
        assert observed == OBSERVED and raw["ingestion_media"]["observed_at"] == OBSERVED.isoformat()


def test_partial_variant_failure_retains_objects_for_safe_retry():
    client = Storage()
    source = snapshot()

    def fail_variants(**kwargs):
        quick_variants(**kwargs)
        kwargs["s3_client"].delete_object(Bucket=kwargs["bucket"], Key=client.writes[1])
        raise MediaIncomplete("variant_generation_failed")

    with pytest.raises(MediaIncomplete, match="variant_generation_failed"):
        mirror_snapshot(source, storage(client, variant_generator=fail_variants), check=noop, request=noop)
    retained = len(client.objects)
    assert retained == 11
    mirror_snapshot(source, storage(client), check=noop, request=noop)
    assert len(client.objects) == len(client.writes) == retained


def test_conditional_race_cannot_replace_another_workers_object():
    client = Storage()
    original = client.put_object

    def competing_put(**kwargs):
        client.objects[kwargs["Key"]] = image_bytes("blue"), {}
        return original(**kwargs)

    client.put_object = competing_put
    with pytest.raises(MediaIncomplete, match="existing_object_content_conflict"):
        storage(client).put("shared.png", image_bytes(), "image/png", check=noop, request=noop)
    assert client.objects["shared.png"][0] == image_bytes("blue")


def test_existing_cross_platform_reuse_manifest_keeps_original_and_variant_keys():
    source = snapshot()
    client = Storage()
    initial = mirror_snapshot(source, storage(client), check=noop, request=noop)
    manifest = initial.post_fields["asset_manifest"]
    # Model maintained cross-platform SHA reuse: storage keys predate this lane.
    cover = manifest["originals"]["cover"]
    cover["storage_key"] = "existing/facebook/reused-cover.png"
    manifest["originals"]["media"][0]["storage_key"] = "existing/facebook/reused-media.png"
    manifest["display_variants"]["card_webp"]["storage_key"] = "existing/facebook/reused-card.webp"
    source = replace(source, existing_manifest=manifest)
    result = mirror_snapshot(source, storage(client), check=noop, request=noop)
    assert result.post_fields["hosted_thumbnail_url"] == "https://media.example/existing/facebook/reused-cover.png"
    assert result.post_fields["hosted_media_urls"] == ["https://media.example/existing/facebook/reused-media.png"]
    assert (
        result.post_fields["asset_manifest"]["display_variants"]["card_webp"]["storage_key"]
        == "existing/facebook/reused-card.webp"
    )


def test_rate_limit_exposes_typed_durable_retry_without_immediate_retry():
    calls = []

    def opener(*args, **kwargs):
        calls.append(1)
        return Response(status=429, headers={"Retry-After": "120"})

    before = datetime.now(UTC)
    with pytest.raises(MediaIncomplete) as failure:
        storage(opener=opener)._download(SOURCE, check=noop, request=noop)
    assert failure.value.failure.code == "media_source_throttled"
    assert failure.value.failure.retry_at >= before + timedelta(seconds=120)
    assert not failure.value.failure.source_unavailable
    assert calls == [1]
