"""Attachment-only activity with fakes: no source, storage or database calls."""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from tests.ingestion import test_comment_attachment_refresh as fixtures
from trr_backend.ingestion import attachment_activity as activity_module
from trr_backend.ingestion.attachment_activity import (
    COMMENT_ATTACHMENT_RESULT_VERSION,
    CommentAttachmentActivity,
    write_comment_attachments,
)
from trr_backend.ingestion.canonical import CanonicalWriteError
from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.media import (
    AvatarSource,
    CommentMedia,
    MediaIncomplete,
    MediaSnapshot,
    legacy_comment_source_key,
)


class Connection:
    autocommit = False

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def execute(self, *args):
        pass


class Store:
    def __init__(self):
        self.evidence = []
        self.receipts = []
        self.active = True
        self.requests = 0

    def acquire(self, **owned):
        self.check_owner(**owned)
        return {"owner_expires_at": datetime.now(UTC) + timedelta(minutes=5)}

    def check_owner(self, **owned):
        if not self.active:
            raise LostOwnership("fixture owner lost")
        self.requests += bool(owned.get("record_request"))

    def retain_evidence(self, *, evidence, **owned):
        self.check_owner(**owned)
        evidence.read(pinned=True)
        self.evidence.append(evidence)
        return str(len(self.evidence))

    def commit_target(self, *, write_canonical, **owned):
        self.check_owner(**owned)
        identity = write_canonical(Connection())
        receipt = {"outcome": "committed", "canonical_identity": identity, **owned}
        self.receipts.append(receipt)
        return receipt


class Storage:
    prefix = "private"

    def __init__(self, *, fail_at=None, after=None):
        self.calls = []
        self.fail_at, self.after = fail_at, after

    def host(self, url, **kwargs):
        kwargs["check"]()
        kwargs["request"]()
        self.calls.append((url, kwargs))
        if self.fail_at == len(self.calls):
            raise MediaIncomplete("fixture_download_failed")
        if self.after:
            self.after()
        return {
            "url": "https://cdn.thereality.report/" + str(len(self.calls)) + ".gif",
            "storage_key": kwargs["root"] + "/" + kwargs["name"] + ".gif",
            "sha256": "a" * 64,
            "bytes": 100,
            "content_type": "image/gif",
        }, {}


def snapshot():
    comments = []
    for index in range(2):
        row, native = fixtures.row(), fixtures.payload()
        if index:
            row.update(id="5eeea0dd-4021-4ef1-bd23-730b05cbda95", comment_id="18139996450549826")
            native["comments"][0]["pk"] = row["comment_id"]
        proof = fixtures.parse(native, row)
        comments.append(
            CommentMedia(
                proof.comment_id,
                proof.row_id,
                (proof.source_url,),
                legacy_comment_source_key(proof.row_id, (fixtures.PLACEHOLDER,)),
                row["scraped_at"],
                proof,
            )
        )
    return MediaSnapshot(
        observed_at=fixtures.CAPTURED - timedelta(days=1),
        post_id=comments[0].attachment_refresh.post_id,
        canonical_post_id="c533831e-866f-4d5b-ac1a-80683adb9bc7",
        shortcode=fixtures.CODE,
        show_id="fixture-show",
        season_number=1,
        week_index=None,
        media_urls=("https://cdninstagram.com/unrelated-post.jpg",),
        thumbnail_url="https://cdninstagram.com/unrelated-post.jpg",
        expected_media_count=1,
        comments=tuple(comments),
        avatars=(AvatarSource("owner", "https://cdninstagram.com/stale-avatar.jpg"),),
        comments_complete=True,
        avatars_complete=True,
    )


def run(source, *, store=None, storage=None, writer=None):
    store, storage = store or Store(), storage or Storage()
    options = {"writer": writer} if writer else {}
    result = CommentAttachmentActivity(store, storage=storage, **options).execute(
        run_id="fixture-run",
        account="bravotv",
        generation=1,
        target_key=fixtures.CODE,
        owner_token="fixture-owner",
        comment_ids=tuple(comment.comment_id for comment in source.comments),
        load=lambda **callbacks: source,
    )
    return result, store, storage


def patch_writer_dependencies(monkeypatch):
    calls = []
    monkeypatch.setattr(activity_module, "_locked", lambda conn, table, keys: {"source_account": "bravotv"})
    monkeypatch.setattr(activity_module, "write_comment_media", lambda conn, **kwargs: calls.append(kwargs))
    return calls


def test_exact_two_attachments_mirror_without_post_or_avatar_requests(monkeypatch):
    calls = patch_writer_dependencies(monkeypatch)
    source = snapshot()
    original = deepcopy(source)
    receipt, store, storage = run(source)
    assert source == original
    assert len(storage.calls) == 2 and store.requests == 2
    assert [call[0] for call in storage.calls] == [comment.attachment_refresh.source_url for comment in source.comments]  # pyright: ignore[reportOptionalMemberAccess]
    assert all("unknown-" in call[1]["root"] and call[1]["name"] == "media-01" for call in storage.calls)
    identity = receipt["canonical_identity"]
    assert identity["kind"] == "comment_attachments"
    assert identity["comment_ids"] == [comment.comment_id for comment in source.comments]
    assert not {"post_fields", "asset_manifest", "comments_complete", "avatars_complete"} & identity.keys()
    assert len(calls) == 1 and calls[0]["comments"] == source.comments
    assert len(store.evidence) == 3
    result = store.evidence[-1]
    assert result.parser_version == COMMENT_ATTACHMENT_RESULT_VERSION
    retained = result.read(pinned=True)
    assert retained["kind"] == "comment_attachments"
    assert len(retained["comments"]) == 2
    assert "private-delivery" not in result.payload_json


@pytest.mark.parametrize("change", ["incomplete", "missing_proof", "extra_proof", "target"])
def test_invalid_inventory_or_selection_fails_before_storage(change):
    source = snapshot()
    if change == "incomplete":
        source = replace(source, comments_complete=False)
    elif change == "missing_proof":
        source = replace(source, comments=(replace(source.comments[0], attachment_refresh=None), source.comments[1]))
    elif change == "extra_proof":
        source = replace(source, comments=source.comments + (replace(source.comments[0], comment_id="999"),))
    else:
        source = replace(source, shortcode="OtherCode")
    store, storage = Store(), Storage()
    with pytest.raises((MediaIncomplete, ValueError)):
        run(source, store=store, storage=storage)
    assert not storage.calls and not store.receipts


def test_second_asset_failure_cannot_commit_any_attachment():
    store, storage = Store(), Storage(fail_at=2)
    with pytest.raises(MediaIncomplete, match="fixture_download_failed"):
        run(snapshot(), store=store, storage=storage)
    assert len(storage.calls) == 2 and not store.receipts


def test_owner_loss_after_hosting_prevents_commit():
    store = Store()
    storage = Storage(after=lambda: setattr(store, "active", False))
    with pytest.raises(LostOwnership):
        run(snapshot(), store=store, storage=storage)
    assert not store.receipts


def test_private_transaction_guard_can_wrap_maintained_writer(monkeypatch):
    calls = patch_writer_dependencies(monkeypatch)
    guarded = []

    def writer(conn, **kwargs):
        guarded.append(conn)
        return write_comment_attachments(conn, **kwargs)

    receipt, _, _ = run(snapshot(), writer=writer)
    assert receipt["outcome"] == "committed" and len(guarded) == len(calls) == 1


def test_locked_account_mismatch_prevents_comment_writer(monkeypatch):
    calls = patch_writer_dependencies(monkeypatch)
    monkeypatch.setattr(activity_module, "_locked", lambda conn, table, keys: {"source_account": "other"})
    store = Store()
    with pytest.raises(CanonicalWriteError, match="account"):
        run(snapshot(), store=store)
    assert not calls and not store.receipts


def test_attachment_writer_rejects_non_attachment_canonical_fields(monkeypatch):
    calls = patch_writer_dependencies(monkeypatch)

    def writer(conn, **kwargs):
        fields = deepcopy(kwargs["comment_fields"])
        fields[next(iter(fields))]["text"] = "must not write"
        return write_comment_attachments(conn, **{**kwargs, "comment_fields": fields})

    store = Store()
    with pytest.raises(CanonicalWriteError, match="unsupported canonical fields"):
        run(snapshot(), store=store, writer=writer)
    assert not calls and not store.receipts
