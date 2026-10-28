"""Attachment changes retain source inventory and reject stale fenced writes."""

from dataclasses import replace
from datetime import timedelta

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_media as fixtures
from trr_backend.ingestion.canonical import CanonicalWriteError
from trr_backend.ingestion.comment_attachment_refresh import parse_targeted_rest_attachment
from trr_backend.ingestion.media import mirror_snapshot, read_inventory_fingerprint
from trr_backend.ingestion.replay import _decode_media_result, serialize_media_result

database = fixtures.database
media_database = fixtures.media_database
GIF = "https://cdn.fbsbx.com/native.gif"


def prepare(database):
    owned, token, source = fixtures.launch_media(database, comments=True)
    with database[1]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "update social.instagram_comments set comment_id='123',is_missing=false where id=%s returning *",
            (source.comments[0].row_id,),
        )
        row = dict(cur.fetchone())
    native_id = 0
    for char in source.shortcode:
        native_id = native_id * 64 + "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".index(char)
    proof = parse_targeted_rest_attachment(
        {
            "status": "ok",
            "comments": [
                {
                    "pk": "123",
                    "media_id": str(native_id),
                    "giphy_media_info": {"id": "456", "images": {"fixed_height": {"url": GIF}}},
                }
            ],
        },
        row=row,
        shortcode=source.shortcode,
        expected_gif_id="456",
        captured_at=fixtures.OBSERVED + timedelta(hours=1),
    )
    comment = replace(source.comments[0], comment_id="123", media_urls=(GIF,), attachment_refresh=proof)
    source = replace(source, comments=(comment,), avatars=(), include_comment_avatars=False)
    with database[1]() as conn:
        source = replace(
            source,
            inventory_fingerprint=read_inventory_fingerprint(conn, source.post_id),
            result_inventory_fingerprint=read_inventory_fingerprint(
                conn, source.post_id, attachment_refreshes=(proof,)
            ),
        )
    return owned, token, source, row


def test_attachment_round_trip_and_commit_preserves_inventory_observation(media_database):
    owned, token, source, before = prepare(media_database)
    result = mirror_snapshot(source, fixtures.storage(), check=fixtures.noop, request=fixtures.noop)
    decoded = _decode_media_result(serialize_media_result(result))
    assert decoded.snapshot.comments[0].attachment_refresh == source.comments[0].attachment_refresh
    receipt = fixtures.execute(media_database, owned, token, source)
    assert receipt["outcome"] == "committed"
    assert receipt["canonical_identity"]["inventory_fingerprint"] == source.result_inventory_fingerprint
    with media_database[1]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.instagram_comments where id=%s", (before["id"],))
        row = dict(cur.fetchone())
    for name in ("id", "post_id", "comment_id", "text", "scraped_at", "last_seen_at"):
        assert row[name] == before[name]
    assert row["media_urls"] == [GIF]
    assert len(row["hosted_media_urls"]) == 1
    assert row["raw_data"]["ingestion_attachment"]["native_gif_id"] == "456"


def test_arbitrary_alternate_inventory_fingerprint_cannot_authorize_attachment_apply(media_database):
    owned, token, source, _ = prepare(media_database)
    source = replace(
        source, result_inventory_fingerprint={"version": "instagram-media-inventory-v1", "sha256": "a" * 64}
    )
    with pytest.raises(CanonicalWriteError, match="inventory fingerprint"):
        fixtures.execute(media_database, owned, token, source)


@pytest.mark.parametrize("change", ["baseline", "version"])
def test_attachment_cas_rejects_changed_row(media_database, change):
    owned, token, source, before = prepare(media_database)
    with media_database[1]() as conn, conn.cursor() as cur:
        if change == "baseline":
            cur.execute("update social.instagram_comments set text='changed' where id=%s", (before["id"],))
        else:
            cur.execute(
                "update social.instagram_comments set raw_data=%s::jsonb where id=%s",
                ('{"ingestion_attachment":{"evidence_sha256":"' + "a" * 64 + '"}}', before["id"]),
            )
    with pytest.raises(CanonicalWriteError, match="attachment"):
        fixtures.execute(media_database, owned, token, source)


@pytest.mark.parametrize("clear_hosted", [False, True])
def test_same_proof_replay_restores_only_its_committed_attachment(media_database, clear_hosted):
    from trr_backend.ingestion.media import write_media

    owned, token, source, before = prepare(media_database)
    result = mirror_snapshot(source, fixtures.storage(), check=fixtures.noop, request=fixtures.noop)
    retained = _decode_media_result(serialize_media_result(result))
    fixtures.execute(media_database, owned, token, source)
    with media_database[1]() as conn, conn.cursor() as cur:
        if clear_hosted:
            cur.execute(
                "update social.instagram_comments set hosted_media_urls='[]'::jsonb where id=%s", (before["id"],)
            )
        write_media(
            conn,
            run_id=owned["run_id"],
            account=owned["account"],
            target_key=source.shortcode,
            observed_at=source.observed_at,
            result=retained,
        )
        cur.execute("select hosted_media_urls,raw_data from social.instagram_comments where id=%s", (before["id"],))
        hosted, raw = cur.fetchone()
    assert hosted == result.comment_fields["123"]["hosted_media_urls"]
    assert source.comments[0].attachment_refresh is not None
    assert raw["ingestion_attachment"] == source.comments[0].attachment_refresh.to_payload()
