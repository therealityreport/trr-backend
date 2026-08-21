from __future__ import annotations

import hashlib
import io
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import URLError

import pytest

from scripts.media import sync_watched_media_to_desktop as sync


class Response(io.BytesIO):
    def __init__(self, body: bytes, *, status: int = 200, headers: dict[str, str] | None = None) -> None:
        super().__init__(body)
        self.status = status
        self.headers = headers or {}

    def getcode(self) -> int:
        return self.status


class FakeOpener:
    def __init__(self, responses: dict[str, Response | BaseException]) -> None:
        self.responses = responses
        self.requests = []

    def open(self, request: object, timeout: int) -> Response:
        self.requests.append(request)
        url = request.full_url  # type: ignore[attr-defined]
        outcome = self.responses.get(url)
        if outcome is None:
            raise AssertionError(f"unexpected request: {url}")
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _config(root: Path) -> sync.SyncConfig:
    return sync.SyncConfig(
        api_base_url="https://api.example.test",
        allowed_api_hosts=frozenset({"api.example.test"}),
        allowed_download_hosts=frozenset({"media.example.test"}),
        destinations={"RHOSLC-S7": root},
        max_download_bytes=1024 * 1024,
        allowed_content_types=frozenset({"image/jpeg", "image/png"}),
        timeout_seconds=5,
    )


def _revision(
    *, body: bytes = b"image-bytes", url: str = "https://media.example.test/revisions/one.jpg"
) -> sync.ManifestRevision:
    return sync.ManifestRevision(
        revision_id="revision-1",
        media_asset_id="asset-1",
        sha256=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        content_type="image/jpeg",
        hosted_url=url,
    )


def _download_response(body: bytes, *, content_type: str = "image/jpeg", length: int | None = None) -> Response:
    return Response(
        body,
        headers={"Content-Type": content_type, "Content-Length": str(len(body) if length is None else length)},
    )


def test_fetches_authenticated_manifest_and_syncs_verified_revision(tmp_path: Path) -> None:
    config = _config(tmp_path)
    body = b"verified-image"
    revision = _revision(body=body)
    watch_id = "11111111-1111-1111-1111-111111111111"
    manifest_url = f"https://api.example.test/api/v1/admin/media-watchers/{watch_id}/manifest"
    manifest = {
        "watch_id": watch_id,
        "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        "revisions": [
            {
                "revision_id": revision.revision_id,
                "media_asset_id": revision.media_asset_id,
                "sha256": revision.sha256,
                "size_bytes": revision.size_bytes,
                "content_type": revision.content_type,
                "hosted_url": revision.hosted_url,
            }
        ],
    }
    opener = FakeOpener(
        {
            manifest_url: Response(json.dumps(manifest).encode(), headers={"Content-Type": "application/json"}),
            revision.hosted_url: _download_response(body),
        }
    )

    revisions = sync.fetch_manifest(config, watch_id, "token-value", opener=opener)
    result = sync.sync_revisions(revisions, config, "RHOSLC-S7", opener=opener)

    target = tmp_path / sync._filename(revision)
    assert result == {"downloaded": 1, "skipped": 0}
    assert target.read_bytes() == body
    saved_state = json.loads((tmp_path / sync.STATE_FILE_NAME).read_text())
    assert saved_state["revisions"][revision.revision_id]["sha256"] == revision.sha256
    assert opener.requests[0].get_header("Authorization") == "Bearer token-value"


def test_rejects_non_uuid_watch_id_before_request(tmp_path: Path) -> None:
    with pytest.raises(sync.SyncError, match="watch_id must be a UUID"):
        sync.fetch_manifest(_config(tmp_path), "../status", "token", opener=FakeOpener({}))


def test_rejects_expired_manifest(tmp_path: Path) -> None:
    watch_id = "11111111-1111-1111-1111-111111111111"
    manifest_url = f"https://api.example.test/api/v1/admin/media-watchers/{watch_id}/manifest"
    opener = FakeOpener(
        {
            manifest_url: Response(
                json.dumps(
                    {
                        "watch_id": watch_id,
                        "expires_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                        "revisions": [],
                    }
                ).encode(),
                headers={"Content-Type": "application/json"},
            )
        }
    )

    with pytest.raises(sync.SyncError, match="manifest has expired"):
        sync.fetch_manifest(_config(tmp_path), watch_id, "token", opener=opener)


def test_rejects_redirect_to_unallowlisted_download_host(tmp_path: Path) -> None:
    revision = _revision()
    opener = FakeOpener(
        {
            revision.hosted_url: Response(b"", status=302, headers={"Location": "https://evil.example/download"}),
        }
    )

    with pytest.raises(sync.SyncError, match="not allowlisted"):
        sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=opener)


def test_rejects_manifest_download_domain_before_request(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw = {
        "revision_id": "r1",
        "media_asset_id": "a1",
        "sha256": "a" * 64,
        "size_bytes": 1,
        "content_type": "image/jpeg",
        "hosted_url": "https://untrusted.example/file.jpg",
    }

    with pytest.raises(sync.SyncError, match="not allowlisted"):
        sync._parse_revision(raw, config)


def test_rejects_numeric_download_host_even_if_misconfigured_as_allowed(tmp_path: Path) -> None:
    config = sync.SyncConfig(
        api_base_url="https://api.example.test",
        allowed_api_hosts=frozenset({"api.example.test"}),
        allowed_download_hosts=frozenset({"127.0.0.1"}),
        destinations={"RHOSLC-S7": tmp_path},
        max_download_bytes=1024,
        allowed_content_types=frozenset({"image/jpeg"}),
    )
    raw = {
        "revision_id": "r1",
        "media_asset_id": "a1",
        "sha256": "a" * 64,
        "size_bytes": 1,
        "content_type": "image/jpeg",
        "hosted_url": "https://127.0.0.1/file.jpg",
    }

    with pytest.raises(sync.SyncError, match="DNS host"):
        sync._parse_revision(raw, config)


def test_rejects_symlink_destination_and_target(tmp_path: Path) -> None:
    real_root = tmp_path / "real"
    real_root.mkdir()
    root_link = tmp_path / "destination-link"
    root_link.symlink_to(real_root, target_is_directory=True)
    with pytest.raises(sync.SyncError, match="non-symlink directory"):
        sync.resolve_destination(_config(root_link), "RHOSLC-S7")

    revision = _revision()
    target = tmp_path / sync._filename(revision)
    target.symlink_to(tmp_path / "outside")
    with pytest.raises(sync.SyncError, match="symlink"):
        sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=FakeOpener({}))


def test_server_identifiers_cannot_escape_local_destination(tmp_path: Path) -> None:
    body = b"contained"
    revision = sync.ManifestRevision(
        revision_id="../../revision",
        media_asset_id="/tmp/server-path-is-not-a-destination",
        sha256=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        content_type="image/jpeg",
        hosted_url="https://media.example.test/revisions/contained.jpg",
    )
    opener = FakeOpener({revision.hosted_url: _download_response(body)})

    sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=opener)

    assert (tmp_path / sync._filename(revision)).read_bytes() == body
    assert not (tmp_path.parent / "revision").exists()


def test_hash_failure_leaves_no_final_or_state_or_partial_file(tmp_path: Path) -> None:
    revision = _revision(body=b"expected")
    opener = FakeOpener({revision.hosted_url: _download_response(b"wrong!!!")})

    with pytest.raises(sync.SyncError, match="SHA-256"):
        sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=opener)

    assert not (tmp_path / sync._filename(revision)).exists()
    assert not (tmp_path / sync.STATE_FILE_NAME).exists()
    assert not list(tmp_path.glob("*.partial"))


def test_rejects_download_larger_than_manifest_limit(tmp_path: Path) -> None:
    revision = _revision(body=b"small")
    opener = FakeOpener({revision.hosted_url: _download_response(b"too-long", length=len(b"too-long"))})

    with pytest.raises(sync.SyncError, match="Content-Length disagrees"):
        sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=opener)

    assert not (tmp_path / sync._filename(revision)).exists()


def test_rejects_mime_mismatch(tmp_path: Path) -> None:
    revision = _revision()
    opener = FakeOpener({revision.hosted_url: _download_response(b"image-bytes", content_type="text/html")})

    with pytest.raises(sync.SyncError, match="MIME type"):
        sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=opener)


def test_preserves_existing_collision_with_deterministic_suffix(tmp_path: Path) -> None:
    body = b"new-verified-image"
    revision = _revision(body=body)
    original = tmp_path / sync._filename(revision)
    original.write_bytes(b"operator-file")
    opener = FakeOpener({revision.hosted_url: _download_response(body)})

    sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=opener)

    collision = tmp_path / sync._filename(revision, 1)
    assert original.read_bytes() == b"operator-file"
    assert collision.read_bytes() == body


def test_offline_download_is_not_recorded_and_retries(tmp_path: Path) -> None:
    body = b"retryable"
    revision = _revision(body=body)
    offline = FakeOpener({revision.hosted_url: URLError("network unavailable")})

    with pytest.raises(sync.OfflineSyncError):
        sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=offline)
    assert not (tmp_path / sync.STATE_FILE_NAME).exists()

    online = FakeOpener({revision.hosted_url: _download_response(body)})
    assert sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=online) == {
        "downloaded": 1,
        "skipped": 0,
    }


def test_idempotent_rerun_uses_durable_revision_and_hash_state(tmp_path: Path) -> None:
    body = b"only-once"
    revision = _revision(body=body)
    first = FakeOpener({revision.hosted_url: _download_response(body)})
    assert sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=first)["downloaded"] == 1

    second = FakeOpener({})
    assert sync.sync_revisions([revision], _config(tmp_path), "RHOSLC-S7", opener=second) == {
        "downloaded": 0,
        "skipped": 1,
    }
    assert len(list(tmp_path.glob("*.jpg"))) == 1
    assert not os.path.islink(tmp_path / sync.STATE_FILE_NAME)
