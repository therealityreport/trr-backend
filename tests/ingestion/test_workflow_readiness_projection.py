"""Readiness uses proof fields while the media activity retains full source rows."""

import json
from contextlib import contextmanager
from datetime import timedelta

import pytest
from psycopg2.extras import register_default_jsonb

from tests.ingestion import test_media_snapshot as existing
from trr_backend.ingestion.domain import Stage
from trr_backend.ingestion.media import MediaIncomplete
from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader
from trr_backend.ingestion.workflow_store import WorkflowStore

database = existing.database
media_database = existing.media_database
snapshot_database = existing.snapshot_database


@pytest.mark.parametrize("selected_details", [False, True])
def test_readiness_does_not_decode_unused_large_json_fields(snapshot_database, monkeypatch, selected_details):
    owned, ids, comments, _ = existing.seed(snapshot_database, selected_comments=True)
    store, tx = snapshot_database
    if selected_details:
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
        details = existing.discover(
            store, {"run_id": owned["run_id"], "generation": owned["generation"]}, Stage.DETAILS, owned["target_key"]
        )
        token = str(store.reserve(**details)["owner_token"])
        store.acquire(**details, owner_token=token)
        store.commit_target(
            **details,
            owner_token=token,
            observed_at=existing.OBSERVED,
            write_canonical=lambda _: {"post_id": ids["post_id"]},
        )
    before = existing.load(snapshot_database, owned)
    marker = "unused-readiness-json-field"
    extra = json.dumps({marker: "x" * (512 * 1024)})
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set raw_data=coalesce(raw_data,'{}'::jsonb)||%s::jsonb where id=%s",
            (extra, ids["post_id"]),
        )
        cur.execute("update social.instagram_comments set raw_data=%s::jsonb where id=%s", (extra, comments[0][0]))

    def proof_json_only(value):
        if marker in value:
            raise ValueError("Readiness decoded an unused large JSON source field")
        return json.loads(value)

    @contextmanager
    def readiness_transaction():
        with tx() as conn:
            register_default_jsonb(conn, loads=proof_json_only)
            yield conn

    completion, details = MediaSnapshotLoader._completion, MediaSnapshotLoader._details
    calls = []

    def completion_fields(cur, run_id, account, key, post, rows, *, selected):
        assert set(post) == {"target_key", "id", "scraped_at"}
        assert rows and all(
            set(row) == {"comment_id", "source_account", "source_snapshot_type", "scraped_at"} for row in rows
        )
        calls.append(key)
        return completion(cur, run_id, account, key, post, rows, selected=selected)

    def detail_fields(cur, run_id, key, post, *, selected):
        assert set(post) == {"target_key", "id", "scraped_at"}
        return details(cur, run_id, key, post, selected=selected)

    monkeypatch.setattr(MediaSnapshotLoader, "_completion", staticmethod(completion_fields))
    monkeypatch.setattr(MediaSnapshotLoader, "_details", staticmethod(detail_fields))
    item = {**owned, "engine": "dbos", "workflow_version": "fixture"}
    decision = WorkflowStore(readiness_transaction).eligible_media(item, existing.OBSERVED + timedelta(days=1))
    assert decision == {"kind": "target", "stage": "media", "target_key": owned["target_key"]}
    assert calls == [owned["target_key"]]
    monkeypatch.setattr(MediaSnapshotLoader, "_completion", staticmethod(completion))
    monkeypatch.setattr(MediaSnapshotLoader, "_details", staticmethod(details))
    # The actual child loader still reads its full snapshot, including media
    # source fields that readiness does not consume.
    after = existing.load(snapshot_database, owned)
    assert (
        after.media_urls == before.media_urls and after.comments == before.comments and after.avatars == before.avatars
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select length(raw_data->>%s) from social.instagram_posts where id=%s", (marker, ids["post_id"]))
        assert cur.fetchone()[0] == 512 * 1024
        cur.execute("select length(raw_data->>%s) from social.instagram_comments where id=%s", (marker, comments[0][0]))
        assert cur.fetchone()[0] == 512 * 1024


@pytest.mark.parametrize(
    "change,reason",
    [
        ("identity", "persisted_comment_inventory_changed"),
        ("account", "comment_inventory_account_mismatch"),
        ("source", "comment_inventory_has_unproved_source"),
        ("comment_clock", "comment_inventory_has_unproved_source"),
        ("post_clock", "comment_inventory_predates_post_source"),
    ],
)
def test_projected_readiness_keeps_full_loader_proof_rejections(snapshot_database, change, reason):
    owned, ids, comments, _ = existing.seed(snapshot_database, selected_comments=True)
    _, tx = snapshot_database
    with tx() as conn, conn.cursor() as cur:
        if change == "identity":
            cur.execute(
                "update social.instagram_comments set comment_id=comment_id||'-changed' where id=%s", (comments[0][0],)
            )
        elif change == "account":
            cur.execute(
                "update social.instagram_comments set source_account='other_account' where id=%s", (comments[0][0],)
            )
        elif change == "source":
            cur.execute(
                "update social.instagram_comments set source_snapshot_type='unproved' where id=%s", (comments[0][0],)
            )
        elif change == "comment_clock":
            cur.execute(
                "update social.instagram_comments set scraped_at=%s where id=%s",
                (existing.OBSERVED + timedelta(minutes=4), comments[0][0]),
            )
        else:
            cur.execute(
                "update social.instagram_posts set scraped_at=%s where id=%s",
                (existing.OBSERVED + timedelta(minutes=4), ids["post_id"]),
            )
    with pytest.raises(MediaIncomplete, match=reason):
        existing.load(snapshot_database, owned)
    item = {**owned, "engine": "dbos", "workflow_version": "fixture"}
    assert WorkflowStore(tx).eligible_media(item, existing.OBSERVED + timedelta(days=1)) is None
