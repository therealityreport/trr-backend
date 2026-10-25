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
from trr_backend.ingestion.domain import Failure, LostOwnership, RunIntent, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import (
    PRIOR_MANIFEST_VERSION,
    AvatarSource,
    CommentMedia,
    MediaActivity,
    MediaIncomplete,
    MediaOwnershipCheck,
    MediaSnapshot,
    MediaStorage,
    invalidate_media_inventory,
    inventory_fingerprint,
    legacy_comment_source_key,
    mirror_snapshot,
    read_inventory_fingerprint,
    write_avatar_version,
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


def test_inventory_fingerprint_tracks_text_only_topology_but_excludes_hosted_outputs():
    post = {"id": "post", "shortcode": "proof", "scraped_at": OBSERVED, "media_urls": [SOURCE]}
    row = {"id": "row", "comment_id": "comment", "parent_comment_id": None, "media_urls": []}
    original = inventory_fingerprint(post, [row])
    assert inventory_fingerprint({**post, "hosted_media_urls": ["https://media.example/hosted.png"]}, [row]) == original
    assert inventory_fingerprint({**post, "media_urls": [SOURCE + "&new-signature=ignored"]}, [row]) == original
    assert inventory_fingerprint(post, [{**row, "parent_comment_id": "parent"}]) != original
    assert inventory_fingerprint(post, [row, {**row, "id": "row2", "comment_id": "second"}]) != original


def test_new_text_only_comment_fences_media_result_and_receipt(media_database):
    owned, token, source = launch_media(media_database)
    _, tx = media_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_comments(id,comment_id,post_id,username,text,scraped_at,last_seen_at)
            values(%s,'new-text-only',%s,'reader','new source',%s,%s)""",
            (str(uuid4()), source.post_id, OBSERVED, OBSERVED),
        )
    with pytest.raises(ValueError, match="inventory fingerprint changed"):
        execute(media_database, owned, token, source)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0


def test_inventory_invalidation_retains_receipts_evidence_attempts_and_checkpoint(media_database):
    owned, token, source = launch_media(media_database)
    receipt = execute(media_database, owned, token, source)
    store, tx = media_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set checkpoint='{"preserved":"proof"}'::jsonb,
            blocked_reason='old-block' where run_id=%s""",
            (owned["run_id"],),
        )
        invalidate_media_inventory(cur, owned["run_id"], owned["target_key"])
        cur.execute(
            """select outcome,attempts,max_attempts,owner_token,blocked_reason,checkpoint
            from social.instagram_ingestion_targets where run_id=%s""",
            (owned["run_id"],),
        )
        outcome, attempts, budget, owner, blocked, checkpoint = cur.fetchone()
        assert (outcome, attempts, owner, blocked) == ("unresolved", 1, None, "old-block")
        assert budget >= attempts + 12 and checkpoint["preserved"] == "proof"
        assert checkpoint["media_inventory_invalidations"][0]["blocked_reason"] == "old-block"
        cur.execute(
            "select receipt_id,evidence_id from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],)
        )
        assert tuple(map(str, cur.fetchone())) == (str(receipt["receipt_id"]), str(receipt["evidence_id"]))
    assert store.reserve(**owned) is None


def test_inventory_invalidation_preserves_source_retry_after_and_prevents_early_media_requests(media_database):
    owned, token, source = launch_media(media_database)
    store, tx = media_database
    store.acquire(**owned, owner_token=token)
    deadline = datetime.now(UTC) + timedelta(hours=2)
    store.checkpoint_failure(
        **owned,
        owner_token=token,
        failure=Failure(code="media_http_429", retry_at=deadline),
        checkpoint={"source": "public_cdn"},
        requests=0,
    )
    with tx() as conn, conn.cursor() as cur:
        invalidate_media_inventory(cur, owned["run_id"], owned["target_key"])
        cur.execute(
            """select outcome,next_attempt_at,checkpoint,requests from social.instagram_ingestion_targets
            where run_id=%s and stage='media' and target_key=%s""",
            (owned["run_id"], owned["target_key"]),
        )
        outcome, retained_deadline, checkpoint, requests = cur.fetchone()
    assert outcome == "unresolved" and retained_deadline == deadline and requests == 0
    assert checkpoint["last_error_code"] == "media_http_429"
    assert datetime.fromisoformat(checkpoint["media_inventory_invalidations"][0]["next_attempt_at"]) == deadline
    assert store.reserve(**owned) is None
    guarded_storage = storage(opener=lambda *args, **kwargs: pytest.fail("Media requested source before Retry-After"))
    with pytest.raises(LostOwnership):
        MediaActivity(store, storage=guarded_storage).execute(
            **owned,
            owner_token=token,
            load=lambda **callbacks: source,
        )
    # Move the isolated fixture beyond its deadline. The ordinary reservation
    # predicate then permits the next media activity without an invalidation bypass.
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set next_attempt_at=clock_timestamp()-interval '1 second'
            where run_id=%s and stage='media' and target_key=%s""",
            (owned["run_id"], owned["target_key"]),
        )
    replacement = store.reserve(**owned)
    assert replacement is not None and str(replacement["owner_token"]) != token
    assert execute(media_database, owned, str(replacement["owner_token"]), source)["outcome"] == "committed"


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
    summary = manifest["storage_summary"]
    assert summary["object_count"] == summary["cdn_url_count"] == len(client.objects) == 11
    assert summary["original_bytes"] == cover["bytes"]
    assert summary["variant_bytes"] == sum(
        len(data) for key, (data, _) in client.objects.items() if key != cover["storage_key"]
    )


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
    summary = result.post_fields["asset_manifest"]["storage_summary"]
    assert summary["object_count"] == summary["cdn_url_count"] == len(client.objects)
    assert summary["original_bytes"] == len(image_bytes()) * (1 if shared_key else 2)


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


def test_changed_post_cover_and_variants_keep_old_bytes_and_resolve_actual_keys():
    from trr_backend.ingestion.replay import serialize_media_result

    client = Storage()
    source = snapshot(inventory_fingerprint={"version": "instagram-media-inventory-v1", "sha256": "a" * 64})
    first = mirror_snapshot(source, storage(client), check=noop, request=noop)
    old_objects = dict(client.objects)
    retained = replace(source, existing_manifest=first.post_fields["asset_manifest"])
    blue = storage(client, opener=lambda *args, **kwargs: Response(image_bytes("blue")))
    result = mirror_snapshot(retained, blue, check=noop, request=noop)
    for attribute in (
        "inventory_fingerprint",
        "result_inventory_fingerprint",
        "observed_at",
        "thumbnail_url",
        "media_urls",
        "expected_media_count",
        "comments",
        "avatars",
        "comment_settlement",
    ):
        assert getattr(result.snapshot, attribute) == getattr(retained, attribute)
    manifest = result.post_fields["asset_manifest"]
    digest = hashlib.sha256(image_bytes("blue")).hexdigest()
    for old_key, value in old_objects.items():
        assert client.objects[old_key] == value
    for entry in [
        manifest["originals"]["cover"],
        *manifest["originals"]["media"],
        *manifest["display_variants"].values(),
    ]:
        assert f".sha256-{digest}." in entry["storage_key"]
        assert entry["url"] == "https://media.example/" + entry["storage_key"]
        assert client.objects[entry["storage_key"]][0] == image_bytes("blue")
        assert entry["sha256"] == digest
    assert result.post_fields["hosted_thumbnail_url"] == manifest["originals"]["cover"]["url"]
    assert result.post_fields["hosted_media_urls"] == [manifest["originals"]["media"][0]["url"]]
    serialize_media_result(result)  # Strict existing result-v1 schema accepts resolved identities.
    writes = list(client.writes)
    assert mirror_snapshot(retained, blue, check=noop, request=noop).post_fields == result.post_fields
    newest = replace(source, existing_manifest=manifest)
    assert mirror_snapshot(newest, blue, check=noop, request=noop).post_fields == result.post_fields
    assert client.writes == writes


def test_changed_post_video_retains_original_and_uses_one_content_version_on_retry():
    client = Storage()
    old = b"\x00\x00\x00\x18ftypmp42" + b"old-video"
    new = b"\x00\x00\x00\x18ftypmp42" + b"new-video"
    key = "legacy/post/media-01.mp4"
    client.objects[key] = old, {}
    service = storage(client, opener=lambda *args, **kwargs: Response(new, headers={"Content-Type": "video/mp4"}))
    entry, _ = service.host(
        "https://scontent.cdninstagram.com/video.mp4",
        root="legacy/post",
        name="media-01",
        preserved_key=key,
        version_post_role="media_original_01",
        check=noop,
        request=noop,
    )
    expected = "legacy/post/media-01.sha256-" + hashlib.sha256(new).hexdigest() + ".mp4"
    assert entry["storage_key"] == expected and entry["url"] == "https://media.example/" + expected
    assert client.objects[key][0] == old and client.objects[expected][0] == new
    repeated, _ = service.host(
        "https://scontent.cdninstagram.com/video.mp4",
        root="legacy/post",
        name="media-01",
        preserved_key=expected,
        version_post_role="media_original_01",
        check=noop,
        request=noop,
    )
    assert repeated == entry and client.writes == [expected]


@pytest.mark.parametrize("identical", [False, True])
def test_post_content_version_conditional_race_never_overwrites_competing_bytes(identical):
    client = Storage()
    new = image_bytes("blue")
    client.objects["legacy/cover.png"] = image_bytes(), {}
    expected = "legacy/cover.sha256-" + hashlib.sha256(new).hexdigest() + ".png"
    original = client.put_object

    def competing_put(**kwargs):
        assert kwargs["Key"] == expected
        client.objects[expected] = (new if identical else image_bytes("green")), {}
        return original(**kwargs)

    client.put_object = competing_put
    service = storage(client, opener=lambda *args, **kwargs: Response(new))
    if identical:
        entry, _ = service.host(
            SOURCE,
            root="legacy",
            name="cover",
            preserved_key="legacy/cover.png",
            version_post_role="cover_original",
            check=noop,
            request=noop,
        )
        assert entry["storage_key"] == expected
    else:
        with pytest.raises(MediaIncomplete, match="existing_object_content_conflict"):
            service.host(
                SOURCE,
                root="legacy",
                name="cover",
                preserved_key="legacy/cover.png",
                version_post_role="cover_original",
                check=noop,
                request=noop,
            )
    assert client.objects["legacy/cover.png"][0] == image_bytes()
    assert client.objects[expected][0] == (new if identical else image_bytes("green"))


def test_comment_and_avatar_hosting_do_not_enable_post_content_version_resolution():
    client = Storage()
    client.objects["comment/media-01.png"] = image_bytes(), {}
    service = storage(client, opener=lambda *args, **kwargs: Response(image_bytes("blue")))
    with pytest.raises(MediaIncomplete, match="existing_object_content_conflict"):
        service.host(SOURCE, root="comment", name="media-01", check=noop, request=noop)
    with pytest.raises(MediaIncomplete, match="avatar_content_version_forbidden"):
        service.host(
            SOURCE, root="", name="", avatar="viewer", version_post_role="cover_original", check=noop, request=noop
        )
    assert client.writes == []


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


def test_native_unavailable_gif_cannot_be_hosted_as_comment_media():
    # Public Instagram static asset xg_5YoVlvjp.gif, observed 2026-10-17.
    # It is a valid GIF whose rendered content says "Content unavailable".
    data = (Path(__file__).parents[1] / "fixtures/ingestion/instagram-content-unavailable.gif").read_bytes()
    client = Storage()
    service = storage(client, opener=lambda *a, **k: Response(data=data, headers={"Content-Type": "image/gif"}))
    with pytest.raises(MediaIncomplete, match="media_source_placeholder"):
        service.host(SOURCE, root="comments", name="media-01", check=noop, request=noop)
    assert client.writes == []


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
    with tx() as conn:
        source = replace(source, inventory_fingerprint=read_inventory_fingerprint(conn, source.post_id))
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


def test_refresh_binds_validated_snapshot_once_before_any_download(media_database):
    store, _ = media_database
    owned, token, source = launch_media(media_database)
    bound = []

    class Refresh:
        def bind_snapshot(self, value):
            value.validate()
            bound.append(value)

        def __call__(self, **kwargs):
            pytest.fail("Fresh source unexpectedly required URL refresh")

    def opener(*args, **kwargs):
        assert bound == [source]
        return Response()

    receipt = MediaActivity(store, storage=storage(opener=opener, refresh=Refresh())).execute(
        **owned, owner_token=token, load=lambda **callbacks: source
    )
    assert receipt["outcome"] == "committed" and bound == [source]


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


class ChunkClock:
    def __init__(self):
        self.milliseconds = 0

    def __call__(self):
        return self.milliseconds / 1000


def test_fifty_mib_stream_uses_bounded_durable_sql_checks(media_database):
    store, _ = media_database
    owned, token, _ = launch_media(media_database)
    store.acquire(**owned, owner_token=token)
    clock = ChunkClock()
    durable_checks, requests_sent = [], []
    chunk = b"\x00\x00\x00\x18ftypmp42" + b"x" * (64 * 1024 - 12)

    class ChunkedResponse(Response):
        def iter_content(self, *, chunk_size):
            assert chunk_size == len(chunk)
            for _ in range(800):
                clock.milliseconds += 10
                yield chunk

    def durable():
        durable_checks.append(clock())
        store.check_owner(**owned, owner_token=token)

    def request():
        requests_sent.append(clock())
        store.check_owner(**owned, owner_token=token, record_request=True)

    data, content_type = storage(
        opener=lambda *a, **kw: ChunkedResponse(headers={"Content-Type": "video/mp4"})
    )._download(
        "https://scontent.cdninstagram.com/video.mp4",
        check=MediaOwnershipCheck(durable, deadline=590, clock=clock),
        request=request,
    )
    assert len(data) == 50 * 1024 * 1024 and content_type == "video/mp4"
    assert durable_checks == list(range(9))  # Initial fence plus one per second; not 800 SQL transactions.
    assert requests_sent == [0]


def test_stream_cancellation_is_observed_within_one_second(media_database):
    store, _ = media_database
    owned, token, _ = launch_media(media_database)
    store.acquire(**owned, owner_token=token)
    clock = ChunkClock()
    cancelled_at = None

    class CancelledResponse(Response):
        def iter_content(self, *, chunk_size):
            nonlocal cancelled_at
            for position in range(800):
                clock.milliseconds += 10
                if position == 10:
                    cancelled_at = clock()
                    store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
                yield b"x" * chunk_size

    with pytest.raises(LostOwnership):
        storage(opener=lambda *a, **kw: CancelledResponse())._download(
            SOURCE,
            check=MediaOwnershipCheck(lambda: store.check_owner(**owned, owner_token=token), deadline=590, clock=clock),
            request=noop,
        )
    assert cancelled_at is not None and 0 < clock() - cancelled_at <= 1
    assert clock() == 1


def test_stream_checks_local_deadline_on_every_chunk():
    clock = ChunkClock()
    durable_checks = []

    class SlowResponse(Response):
        def iter_content(self, **kwargs):
            for _ in range(100):
                clock.milliseconds += 10
                yield b"x"

    with pytest.raises(MediaIncomplete, match="target_deadline"):
        storage(opener=lambda *a, **kw: SlowResponse())._download(
            SOURCE,
            check=MediaOwnershipCheck(lambda: durable_checks.append(clock()), deadline=0.25, clock=clock),
            request=noop,
        )
    assert clock() == 0.25 and durable_checks == [0]


def test_new_generation_after_last_upload_cannot_commit_media(media_database):
    _, tx = media_database
    owned, token, source = launch_media(media_database)
    client = Storage()

    def replace_generation():
        if len(client.writes) == 11:
            with tx() as conn, conn.cursor() as cur:
                cur.execute(
                    "update social.instagram_ingestion_runs set generation=generation+1 where run_id=%s",
                    (owned["run_id"],),
                )

    client.after_put = replace_generation
    with pytest.raises(LostOwnership):
        execute(media_database, owned, token, source, client=client)
    assert len(client.writes) == 11
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0
        cur.execute("select hosted_media_urls from social.instagram_posts where id=%s", (source.post_id,))
        assert cur.fetchone()[0] == []


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


@pytest.mark.parametrize(
    "newer_source",
    [
        "https://scontent.cdninstagram.com/old-avatar.png?oh=new-proof",
        "https://scontent.cdninstagram.com/old-avatar.png#new-proof",
    ],
)
def test_newer_same_version_observation_fences_cached_avatar_and_rolls_back_prior_post_writes(
    media_database, newer_source
):
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


def test_versioned_avatar_writer_preserves_newer_different_image_and_exact_proof_time(media_database):
    _, tx = media_database
    handle = "versions_" + uuid4().hex[:12]
    old_source = "https://scontent.cdninstagram.com/old-retained.png"
    new_source = "https://scontent.cdninstagram.com/new-current.png"
    old_hosted = "https://media.example/profiles/old-retained.png"
    new_hosted = "https://media.example/profiles/new-current.png"
    newer = OBSERVED + timedelta(hours=1)
    with tx() as conn:
        write_avatar_version(
            conn,
            username=handle,
            source_url=new_source,
            hosted_url=new_hosted,
            content_hash="b" * 64,
            observed_at=newer,
        )
        proof = write_avatar_version(
            conn,
            username=handle,
            source_url=old_source,
            hosted_url=old_hosted,
            content_hash="a" * 64,
            observed_at=OBSERVED,
        )
        assert proof["observed_at"] == OBSERVED
        # Same bytes prove an immutable existing version; an older consumer
        # cannot relabel the retained proof or the latest profile version.
        reused = write_avatar_version(
            conn,
            username=handle,
            source_url=new_source,
            hosted_url=new_hosted,
            content_hash="b" * 64,
            observed_at=OBSERVED,
        )
        assert reused["observed_at"] == newer
        with conn.cursor() as cur:
            cur.execute(
                """select source_url,hosted_url,content_hash,last_checked_at from social.avatar_registry
                where account_handle=%s order by last_checked_at""",
                (handle,),
            )
            assert cur.fetchall() == [
                (old_source, old_hosted, "a" * 64, OBSERVED),
                (new_source, new_hosted, "b" * 64, newer),
            ]


@pytest.mark.parametrize("signed", [False, True])
def test_avatar_writer_rejects_newer_same_version_conflicting_hash(media_database, signed):
    _, tx = media_database
    handle = "hashguard_" + uuid4().hex[:12]
    reference = "https://scontent.cdninstagram.com/same-version.png"
    newer = OBSERVED + timedelta(hours=1)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,
            hosted_url,content_hash,last_checked_at) values('instagram',%s,%s,'mirrored',%s,%s,%s)""",
            (
                handle,
                reference + ("?oh=legacy" if signed else ""),
                "https://media.example/profiles/new-bytes.png",
                "b" * 64,
                newer,
            ),
        )
    with pytest.raises(OlderObservation):
        with tx() as conn:
            write_avatar_version(
                conn,
                username=handle,
                source_url=reference,
                hosted_url="https://media.example/profiles/old-bytes.png",
                content_hash="a" * 64,
                observed_at=OBSERVED,
            )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select hosted_url,content_hash,last_checked_at from social.avatar_registry where account_handle=%s",
            (handle,),
        )
        assert cur.fetchall() == [("https://media.example/profiles/new-bytes.png", "b" * 64, newer)]


def test_cancelled_media_cannot_publish_avatar_version_proof(media_database):
    store, tx = media_database
    owned, token, source = launch_media(media_database)
    handle = "cancelledver_" + uuid4().hex[:12]
    source = replace(source, avatars=(AvatarSource(handle, SOURCE, "owner", observed_at=OBSERVED),))
    store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
    with pytest.raises(LostOwnership):
        execute(media_database, owned, token, source)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.avatar_registry where account_handle=%s", (handle,))
        assert cur.fetchone()[0] == 0
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0


def test_prior_manifest_is_retained_before_hosting_deduplicated_and_separate_from_result_receipt(media_database):
    from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION

    store, tx = media_database
    owned, token, source = launch_media(media_database)
    client = Storage()
    old = mirror_snapshot(source, storage(client), check=noop, request=noop).post_fields["asset_manifest"]
    prior_time = OBSERVED - timedelta(days=1)
    archival_started = datetime.now(UTC)
    old["updated_at"] = prior_time.isoformat()
    old["ingestion_media"]["observed_at"] = prior_time.isoformat()
    descriptors = [old["originals"]["cover"], *old["originals"]["media"], *old["display_variants"].values()]
    for item in descriptors:
        item["generated_at"] = prior_time.isoformat()
        item["url"] += "?oh=must-not-retain&access_token=must-not-retain"
    old_objects = dict(client.objects)
    source = replace(source, existing_manifest=old)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set asset_manifest=%s::jsonb where id=%s", (json.dumps(old), source.post_id)
        )

    def retained():
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """select captured_at,payload from social.instagram_ingestion_evidence
                where run_id=%s and target_key=%s and parser_version=%s""",
                (owned["run_id"], source.shortcode, PRIOR_MANIFEST_VERSION),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        captured, payload = rows[0]
        payload = json.loads(payload)
        assert archival_started <= captured <= datetime.now(UTC)
        assert payload["captured_at"] == prior_time.isoformat()
        assert payload["updated_at"] == prior_time.isoformat()
        assert "must-not-retain" not in json.dumps(payload) and "?" not in json.dumps(payload)
        assert {item["variant_key"] for item in payload["media"]} == {item["variant_key"] for item in descriptors}
        for prior in payload["media"]:
            expected = next(item for item in descriptors if item["variant_key"] == prior["variant_key"])
            for field in ("storage_key", "bytes", "sha256", "width", "height", "generated_at"):
                assert prior[field] == expected[field]
        return payload

    def fail_before_download(*args, **kwargs):
        retained()
        raise MediaIncomplete("media_download_failed")

    with pytest.raises(MediaIncomplete, match="media_download_failed"):
        MediaActivity(store, storage=storage(client, opener=fail_before_download)).execute(
            **owned, owner_token=token, load=lambda **callbacks: source
        )
    store.checkpoint_failure(
        **owned, owner_token=token, failure=Failure("media_download_failed"), checkpoint={}, requests=0
    )
    token = str(store.reserve(**owned)["owner_token"])

    def fresh_bytes(*args, **kwargs):
        retained()
        return Response(image_bytes("blue"))

    receipt = MediaActivity(store, storage=storage(client, opener=fresh_bytes)).execute(
        **owned, owner_token=token, load=lambda **callbacks: source
    )
    assert receipt["outcome"] == "committed"
    assert receipt["observed_at"] == source.observed_at
    assert receipt["canonical_identity"]["inventory_fingerprint"] == source.inventory_fingerprint
    retained()
    for key, old_value in old_objects.items():
        assert client.objects[key] == old_value
    with tx() as conn, conn.cursor() as cur:
        # Final certifier's source is the exact receipt-linked evidence, so
        # auxiliary prior-manifest evidence cannot become completion proof.
        cur.execute(
            """select e.parser_version,p.asset_manifest from social.instagram_ingestion_receipts r
            join social.instagram_ingestion_evidence e using(evidence_id,run_id,stage,target_key)
            join social.instagram_posts p on p.shortcode=r.target_key where r.run_id=%s""",
            (owned["run_id"],),
        )
        parser, manifest = cur.fetchone()
        assert parser == MEDIA_RESULT_REPLAY_VERSION
        assert ".sha256-" in manifest["originals"]["cover"]["storage_key"]
        assert "prior_manifest" not in manifest and "history" not in manifest
        cur.execute(
            """select payload from social.instagram_ingestion_evidence
            where run_id=%s and parser_version='instagram-media-v1'""",
            (owned["run_id"],),
        )
        for (payload,) in cur.fetchall():
            payload = json.loads(payload)
            assert {entry["id"]: entry["storage_source_key"] for entry in payload["media"]} == source.storage_keys()
            assert payload["inventory_fingerprint"] == source.inventory_fingerprint
            assert payload["ingestion_media"]["observed_at"] == source.observed_at.isoformat()


def test_old_prior_manifest_uses_archival_retention_clock_and_keeps_source_clocks():
    from trr_backend.ingestion.media import prior_manifest_evidence

    archived = datetime.now(UTC)
    old = archived - timedelta(days=100)
    source = snapshot()
    manifest = mirror_snapshot(source, storage(), check=noop, request=noop).post_fields["asset_manifest"]
    manifest["updated_at"] = old.isoformat()
    manifest["ingestion_media"]["observed_at"] = old.isoformat()
    for item in [
        manifest["originals"]["cover"],
        *manifest["originals"]["media"],
        *manifest["display_variants"].values(),
    ]:
        item["generated_at"] = old.isoformat()
    source = replace(source, existing_manifest=manifest)
    first = prior_manifest_evidence(source, captured_at=archived)
    second = prior_manifest_evidence(source, captured_at=archived + timedelta(days=1))
    assert first.captured_at == archived and first.expires_at == archived + timedelta(days=30)
    payload = first.read(now=archived + timedelta(days=1))
    assert payload["updated_at"] == payload["captured_at"] == old.isoformat()
    assert all(item["generated_at"] == old.isoformat() for item in payload["media"])
    assert first.content_hash == second.content_hash and first.payload_json == second.payload_json


def test_first_snapshot_default_cover_video_and_variant_conflicts_round_trip():
    from trr_backend.ingestion.replay import _decode_media_result, serialize_media_result

    source = snapshot(media_urls=("https://scontent.cdninstagram.com/video.mp4",))
    client = Storage()
    root = f"social/instagram/{source.show_id}/{source.season_number}/week-{source.week_index}/{source.shortcode}"
    red, blue = image_bytes(), image_bytes("blue")
    old_video, new_video = b"\x00\x00\x00\x18ftypmp42old-video", b"\x00\x00\x00\x18ftypmp42new-video"
    client.objects[root + "/cover-original.png"] = blue, {}
    client.objects[root + "/media-01.mp4"] = old_video, {}
    cover_hash = hashlib.sha1(red).hexdigest()[:16]
    for role in ("thumb", "card", "detail", "poster_card", "poster_detail"):
        for fmt, ext in (("webp", "webp"), ("jpeg", "jpg")):
            client.objects[f"social-variants/instagram/{source.post_id}/{cover_hash}/{role}_{fmt}.{ext}"] = blue, {}
    old_objects = dict(client.objects)

    def opener(_client, url, **kwargs):
        return Response(new_video, headers={"Content-Type": "video/mp4"}) if url.endswith(".mp4") else Response(red)

    result = mirror_snapshot(source, storage(client, opener=opener), check=noop, request=noop)
    decoded = _decode_media_result(serialize_media_result(result))
    assert source.storage_keys() == {}
    assert decoded.post_fields == result.post_fields
    manifest = result.post_fields["asset_manifest"]
    entries = [manifest["originals"]["cover"], *manifest["originals"]["media"], *manifest["display_variants"].values()]
    assert len(entries) == 12 and all(".sha256-" in item["storage_key"] for item in entries)
    assert all(client.objects[key] == value for key, value in old_objects.items())


def test_avatar_ephemeral_download_url_preserves_canonical_evidence_and_identity():
    from trr_backend.ingestion.replay import serialize_media_result

    canonical = "https://scontent.cdninstagram.com/avatar.png"
    download = canonical + "?oh=ephemeral-secret"
    avatar = AvatarSource("viewer", canonical, "owner", download_url=download)
    assert avatar == replace(avatar, download_url=None) and "ephemeral-secret" not in repr(avatar)
    requested = []

    def opener(_client, url, **kwargs):
        requested.append(url)
        return Response()

    result = mirror_snapshot(snapshot(avatars=(avatar,)), storage(opener=opener), check=noop, request=noop)
    assert download in requested
    assert result.avatars[0]["source_url"] == canonical
    assert "ephemeral-secret" not in json.dumps(serialize_media_result(result))
    with pytest.raises(MediaIncomplete, match="avatar_download_identity_mismatch"):
        snapshot(avatars=(replace(avatar, download_url="https://scontent.cdninstagram.com/other.png"),)).validate()


@pytest.mark.parametrize("request_elapsed", [5, 10])
def test_cdn_request_uses_remaining_budget_after_request_callback(request_elapsed):
    clock = ChunkClock()
    sent = []

    def request():
        clock.milliseconds = request_elapsed * 1000

    def opener(*args, **kwargs):
        sent.append(kwargs)
        return Response()

    check = MediaOwnershipCheck(noop, deadline=10, clock=clock)
    service = storage(opener=opener)
    if request_elapsed == 10:
        with pytest.raises(MediaIncomplete, match="target_deadline"):
            service._download(SOURCE, check=check, request=request)
        assert sent == []
    else:
        service._download(SOURCE, check=check, request=request)
        assert sent[0]["timeout"] == (5, 5)
        assert sent[0]["body_timeout"] == sent[0]["total_timeout"] == 5


def test_cdn_body_stall_closes_response_and_classifies_soft_deadline():
    clock = ChunkClock()
    closed = []

    class StalledResponse(Response):
        def iter_content(self, **kwargs):
            clock.milliseconds = 10_000
            raise requests.Timeout("private URL excluded")
            yield b""  # Keep the same iterator contract as a streamed response.

        def __exit__(self, *_):
            closed.append(True)

    with pytest.raises(MediaIncomplete, match="target_deadline"):
        storage(opener=lambda *args, **kwargs: StalledResponse())._download(
            SOURCE, check=MediaOwnershipCheck(noop, deadline=10, clock=clock), request=noop
        )
    assert closed == [True]


def sdk_budget_client():
    from types import SimpleNamespace

    from botocore.config import Config

    client = Storage()
    client.meta = SimpleNamespace(config=Config(connect_timeout=10, read_timeout=60, retries={"total_max_attempts": 1}))
    return client


@pytest.mark.parametrize("operation", ["head", "get", "put"])
def test_r2_physical_calls_recheck_known_budget_after_request_callback(operation):
    client = sdk_budget_client()
    service = storage(client)
    clock = ChunkClock()
    calls, requests = [], []
    original_head = client.head_object

    def head(**kwargs):
        calls.append("head")
        return original_head(**kwargs)

    client.head_object = head
    client.get_object = lambda **kwargs: calls.append("get")
    client.put_object = lambda **kwargs: calls.append("put")

    def request():
        requests.append(True)
        if operation != "put" or len(requests) == 2:
            clock.milliseconds = 45_000

    check = MediaOwnershipCheck(noop, deadline=100, clock=clock)
    with pytest.raises(MediaIncomplete, match="target_deadline"):
        if operation == "head":
            service._head("role.png", check=check, request=request)
        elif operation == "get":
            service._same(
                "role.png", image_bytes(), {"ContentLength": len(image_bytes())}, check=check, request=request
            )
        else:
            service.put("role.png", image_bytes(), "image/png", check=check, request=request)
    assert calls == (["head"] if operation == "put" else [])
    assert client.meta.config.retries == {"total_max_attempts": 1}


def test_r2_body_budget_closes_received_body_before_an_unbounded_read():
    client = sdk_budget_client()
    service = storage(client)
    clock = ChunkClock()
    reads = []

    class Body(io.BytesIO):
        def read(self, *args):
            reads.append(True)
            return super().read(*args)

    body = Body(image_bytes())

    def get(**kwargs):
        clock.milliseconds = 45_000
        return {"Body": body}

    client.get_object = get
    with pytest.raises(MediaIncomplete, match="target_deadline"):
        service._same(
            "role.png",
            image_bytes(),
            {"ContentLength": len(image_bytes())},
            check=MediaOwnershipCheck(noop, deadline=100, clock=clock),
            request=noop,
        )
    assert body.closed and reads == []


def test_soft_deadline_keeps_completed_immutable_upload_for_owned_retry():
    client = sdk_budget_client()
    service = storage(client)
    clock = ChunkClock()
    original_put = client.put_object

    def lost_response(**kwargs):
        original_put(**kwargs)
        clock.milliseconds = 40_000
        raise ClientError(
            {"Error": {"Code": "InternalError"}, "ResponseMetadata": {"HTTPStatusCode": 503}}, "PutObject"
        )

    client.put_object = lost_response
    with pytest.raises(MediaIncomplete, match="target_deadline"):
        service.put(
            "role.png",
            image_bytes(),
            "image/png",
            check=MediaOwnershipCheck(noop, deadline=100, clock=clock),
            request=noop,
        )
    assert client.objects["role.png"][0] == image_bytes()
    clock.milliseconds = 0
    service.put(
        "role.png", image_bytes(), "image/png", check=MediaOwnershipCheck(noop, deadline=100, clock=clock), request=noop
    )
    assert client.writes == ["role.png"]


def test_media_activity_inherits_cleanup_reserve_and_keeps_real_ownership_failure(media_database):
    from trr_backend.ingestion.reply_context import execution_deadline

    store, tx = media_database
    owned, token, source = launch_media(media_database)
    clock = ChunkClock()
    context = execution_deadline.set(30)

    def load(**callbacks):
        clock.milliseconds = 10_000
        return source

    try:
        with pytest.raises(MediaIncomplete, match="target_deadline"):
            MediaActivity(store, storage=storage(), clock=clock).execute(**owned, owner_token=token, load=load)
    finally:
        execution_deadline.reset(context)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_evidence where run_id=%s", (owned["run_id"],))
        assert cur.fetchone()[0] == 0
    check = MediaOwnershipCheck(
        lambda: (_ for _ in ()).throw(LostOwnership("fixture cancelled")), deadline=100, clock=clock
    )
    with pytest.raises(LostOwnership):
        check()
