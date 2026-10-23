"""Private PostgreSQL proof that attachment work leaves crawl claims intact."""

from copy import deepcopy
from datetime import timedelta

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_media as media_fixtures
from tests.ingestion import test_media_snapshot as snapshot_fixtures
from trr_backend.ingestion.canonical import normalize_comment_source, write_comments
from trr_backend.ingestion.comment_attachment_refresh import parse_targeted_rest_attachment
from trr_backend.ingestion.domain import Stage
from trr_backend.ingestion.media import MediaIncomplete
from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader

database = media_fixtures.database
media_database = media_fixtures.media_database
snapshot_database = snapshot_fixtures.snapshot_database

COMMENT_ID = "18029668664695097"
GIF_ID = "1072011872270984"
REAL = "https://cdn.fbsbx.com/native.gif"
PLACEHOLDER = "https://static.cdninstagram.com/rsrc.php/v4/yr/r/xg_5YoVlvjp.gif"
SCRAPED = media_fixtures.OBSERVED + timedelta(minutes=2)
CAPTURED = media_fixtures.OBSERVED + timedelta(hours=1)


def native_comment(shortcode, *, url=PLACEHOLDER, text="Original text"):
    media_id = 0
    for char in shortcode:
        media_id = media_id * 64 + "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".index(char)
    return {
        "pk": COMMENT_ID,
        "media_id": str(media_id),
        "user_id": "58758231298",
        "user": {"pk": "58758231298", "username": "commenter"},
        "text": text,
        "child_comment_count": 0,
        "media_urls": [url],
        "giphy_media_info": {"id": GIF_ID, "images": {"fixed_height": {"url": url}}},
    }


def read_comment(tx, row_id):
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.instagram_comments where id=%s", (row_id,))
        return dict(cur.fetchone())


def read_receipts(tx, owned):
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select * from social.instagram_ingestion_receipts
            where run_id=%s and stage='comments' order by receipt_id""",
            (owned["run_id"],),
        )
        return [dict(row) for row in cur.fetchall()]


def prepare(database, *, receipt=True):
    store, tx = database
    owned, ids, _, comment_owned = snapshot_fixtures.seed(database, comments=0, proof=False)
    raw = native_comment(owned["target_key"])
    with tx() as conn:
        result = write_comments(
            conn,
            run_id=comment_owned["run_id"],
            account=owned["account"],
            post_id=ids["post_id"],
            observed_at=SCRAPED,
            comments=[normalize_comment_source(raw)],
            source_payloads=[raw],
        )
    row_id = result["comment_ids"][COMMENT_ID]
    if receipt:
        token = str(store.reserve(**comment_owned)["owner_token"])
        store.acquire(**comment_owned, owner_token=token)
        store.commit_target(
            **comment_owned,
            owner_token=token,
            observed_at=media_fixtures.OBSERVED + timedelta(minutes=3),
            write_canonical=lambda conn: {"post_id": ids["post_id"], "observed_comment_ids": [COMMENT_ID]},
        )
    baseline = read_comment(tx, row_id)
    proof = parse_targeted_rest_attachment(
        {"status": "ok", "comments": [native_comment(owned["target_key"], url=REAL)]},
        row=baseline,
        shortcode=owned["target_key"],
        expected_gif_id=GIF_ID,
        captured_at=CAPTURED,
    )
    return owned, comment_owned, baseline, proof


def overlay(database, owned, proof):
    return MediaSnapshotLoader(database[1]).load(
        **{key: value for key, value in owned.items() if key != "stage"}, attachment_refreshes=(proof,)
    )


def test_loader_overlay_preserves_complete_receipt_inventory_and_each_source_time(snapshot_database):
    owned, comment_owned, before, proof = prepare(snapshot_database)
    store, tx = snapshot_database
    receipts = read_receipts(tx, comment_owned)
    assert receipts[0]["canonical_identity"]["observed_comment_ids"] == [COMMENT_ID]
    snapshot = overlay(snapshot_database, owned, proof)
    assert snapshot.comments_complete
    assert snapshot.observed_at == media_fixtures.OBSERVED
    assert snapshot.comments[0].observed_at == SCRAPED
    assert snapshot.comments[0].media_urls == (REAL,)
    assert snapshot.comments[0].attachment_refresh == proof
    assert read_comment(tx, before["id"]) == before
    assert read_receipts(tx, comment_owned) == receipts

    token = str(store.reserve(**owned)["owner_token"])
    media_fixtures.execute(snapshot_database, owned, token, snapshot)
    after = read_comment(tx, before["id"])
    for name in (
        "id",
        "post_id",
        "comment_id",
        "user_id",
        "username",
        "text",
        "parent_comment_id",
        "is_reply",
        "is_missing",
        "deleted_at",
        "scraped_at",
        "last_seen_at",
        "last_seen_run_id",
        "source_snapshot_type",
    ):
        assert after[name] == before[name]
    assert after["media_urls"] == [REAL]
    assert after["raw_data"]["ingestion_attachment"] == proof.to_payload()
    assert read_receipts(tx, comment_owned) == receipts


@pytest.mark.parametrize("change", ["text", "source_urls", "hosted_urls"])
def test_loader_overlay_requires_unchanged_attachment_baseline(snapshot_database, change):
    owned, _, before, proof = prepare(snapshot_database)
    with snapshot_database[1]() as conn, conn.cursor() as cur:
        if change == "text":
            cur.execute("update social.instagram_comments set text='changed' where id=%s", (before["id"],))
        elif change == "source_urls":
            cur.execute("update social.instagram_comments set media_urls='[]'::jsonb where id=%s", (before["id"],))
        else:
            cur.execute(
                """update social.instagram_comments
                set hosted_media_urls='[\"https://cdn.thereality.report/other.gif\"]'::jsonb where id=%s""",
                (before["id"],),
            )
    with pytest.raises(MediaIncomplete, match="attachment_refresh_baseline_changed"):
        overlay(snapshot_database, owned, proof)


def test_loader_attachment_proof_does_not_replace_missing_full_inventory_receipt(snapshot_database):
    owned, _, _, proof = prepare(snapshot_database, receipt=False)
    with pytest.raises(MediaIncomplete, match="complete_comment_inventory_proof_required"):
        overlay(snapshot_database, owned, proof)


@pytest.mark.parametrize("offset_minutes", [-1, 0, 1])
def test_full_comment_replay_respects_independent_attachment_version(snapshot_database, offset_minutes):
    owned, comment_owned, before, proof = prepare(snapshot_database)
    store, tx = snapshot_database
    receipts = read_receipts(tx, comment_owned)
    snapshot = overlay(snapshot_database, owned, proof)
    token = str(store.reserve(**owned)["owner_token"])
    media_fixtures.execute(snapshot_database, owned, token, snapshot)
    attached = read_comment(tx, before["id"])
    retained = deepcopy(attached["raw_data"])
    replay_at = CAPTURED + timedelta(minutes=offset_minutes)
    raw = native_comment(owned["target_key"], text="New full observation")
    raw["hosted_media_urls"] = ["https://cdn.thereality.report/replay.gif"]
    raw["media_mirror_status"] = "pending"
    writer_run = snapshot_fixtures.run(store, selected=(Stage.COMMENTS,))
    with tx() as conn:
        write_comments(
            conn,
            run_id=writer_run["run_id"],
            account=owned["account"],
            post_id=before["post_id"],
            observed_at=replay_at,
            comments=[normalize_comment_source(raw)],
            source_payloads=[raw],
        )
    after = read_comment(tx, before["id"])
    assert after["text"] == "New full observation" and after["scraped_at"] == replay_at
    if offset_minutes <= 0:
        for column in ("media_urls", "hosted_media_urls", "media_mirror_status", "media_mirror_error"):
            assert after[column] == attached[column]
        for key in ("giphy_media_info", "media_urls", "ingestion_attachment", "ingestion_media"):
            assert after["raw_data"].get(key) == retained.get(key)
    else:
        assert "ingestion_attachment" not in after["raw_data"]
        assert after["media_urls"] == [PLACEHOLDER]
        assert after["raw_data"]["giphy_media_info"]["images"]["fixed_height"]["url"] == PLACEHOLDER
    assert read_receipts(tx, comment_owned) == receipts
