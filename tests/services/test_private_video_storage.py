# ruff: noqa: N803 -- boto3 keyword names are part of the client contract
from datetime import UTC, datetime
from io import BytesIO

import pytest
from botocore.exceptions import ClientError

from trr_backend.services import private_video_migration as migration
from trr_backend.video_storage import private_video_bucket, source_reference, video_url_ttl


@pytest.fixture(autouse=True)
def storage_env(monkeypatch):
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "media")
    monkeypatch.setenv("OBJECT_STORAGE_PROVIDER", "r2")
    monkeypatch.delenv("OBJECT_STORAGE_VIDEO_BUCKET", raising=False)
    monkeypatch.delenv("OBJECT_STORAGE_VIDEO_URL_TTL_SECONDS", raising=False)


def test_private_writes_fail_closed(monkeypatch):
    with pytest.raises(RuntimeError, match="OBJECT_STORAGE_VIDEO_BUCKET"):
        private_video_bucket()
    monkeypatch.setenv("OBJECT_STORAGE_VIDEO_BUCKET", "media")
    with pytest.raises(RuntimeError, match="differ"):
        private_video_bucket()
    monkeypatch.setenv("OBJECT_STORAGE_VIDEO_BUCKET", "video")
    assert private_video_bucket() == "video"


def test_url_lifetime_bounded(monkeypatch):
    assert video_url_ttl() == 3600
    monkeypatch.setenv("OBJECT_STORAGE_VIDEO_URL_TTL_SECONDS", "3601")
    with pytest.raises(RuntimeError):
        video_url_ttl()


def test_stored_bucket_wins_and_conflicts_rejected():
    asset = {"source_json": {"bucket": "video", "object_key": "source/a", "access": "private"}}
    assert source_reference(asset) == ("video", "source/a")
    assert source_reference({"source_url": "s3://legacy/source/a"}) == ("legacy", "source/a")
    assert source_reference({"source_json": {"object_key": "source/a"}}) == ("media", "source/a")
    with pytest.raises(ValueError, match="disagree"):
        source_reference({**asset, "source_url": "s3://media/source/a"})
    with pytest.raises(ValueError, match="missing its bucket"):
        source_reference({"source_json": {"object_key": "source/a", "access": "private"}})


class Store:
    def __init__(self, destination=None):
        self.objects = {"media": b"video"}
        if destination is not None:
            self.objects["video"] = destination
        self.modified = datetime(2026, 10, 31, tzinfo=UTC)
        self.copies = 0

    def head_object(self, *, Bucket, Key):
        if Bucket not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Bucket]), "ETag": '"etag"', "LastModified": self.modified}

    def get_object(self, *, Bucket, Key, IfMatch=None):
        assert IfMatch == '"etag"'
        return {"Body": BytesIO(self.objects[Bucket])}

    def copy_object(self, *, Bucket, Key, CopySource, CopySourceIfMatch):
        assert CopySourceIfMatch == '"etag"'
        self.copies += 1
        self.objects[Bucket] = self.objects[CopySource["Bucket"]]


def item(store):
    import hashlib

    return {
        "source_bucket": "media",
        "destination_bucket": "video",
        "object_key": "source/a",
        "source_etag": '"etag"',
        "expected_size_bytes": 5,
        "source_last_modified": store.modified,
        "expected_sha256": hashlib.sha256(b"video").hexdigest(),
    }


def test_copy_verifies_content_and_preserves_original():
    store = Store()
    assert migration.copy_verified(store, item(store))["size"] == 5
    assert store.objects["media"] == b"video"
    migration.copy_verified(store, item(store))
    assert store.copies == 1


def test_existing_mismatched_destination_never_overwritten():
    store = Store(b"other")
    with pytest.raises(RuntimeError, match="mismatch"):
        migration.copy_verified(store, item(store))
    assert store.copies == 0
    assert store.objects["video"] == b"other"


def test_changed_source_blocks_copy():
    store = Store()
    entry = item(store)
    entry["source_etag"] = '"old"'
    with pytest.raises(RuntimeError, match="Source changed"):
        migration.copy_verified(store, entry)
    assert store.copies == 0


def test_final_reconciliation_failure_marks_run_failed(monkeypatch):
    updates = []
    monkeypatch.setattr(
        migration.pg, "fetch_one", lambda *args: {"source_bucket": "media", "destination_bucket": "video"}
    )
    monkeypatch.setattr(migration.pg, "fetch_all", lambda *args: [])
    monkeypatch.setattr(migration.pg, "execute", lambda sql, params: updates.append(sql))
    monkeypatch.setattr(migration, "references", lambda bucket: [{"id": "new-reference"}])
    with pytest.raises(RuntimeError, match="remain"):
        migration.execute_run(Store(), "run", "video")
    assert "status='failed'" in updates[-1]
    assert not any("status='complete'" in sql for sql in updates)
