"""Tests for media asset mirroring logic (Phase 3)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest


def test_is_allowed_domain_valid() -> None:
    """Test domain allowlist validation for allowed domains."""
    from scripts.mirror_media_assets_to_s3 import is_allowed_domain

    # These should be allowed (default allowlist)
    assert is_allowed_domain("https://image.tmdb.org/t/p/original/foo.jpg") is True
    assert is_allowed_domain("https://m.media-amazon.com/images/M/abc.jpg") is True
    assert is_allowed_domain("https://static.wikia.nocookie.net/images/foo.png") is True


def test_is_allowed_domain_blocked() -> None:
    """Test domain allowlist validation for blocked domains."""
    from scripts.mirror_media_assets_to_s3 import is_allowed_domain

    # These should NOT be allowed
    assert is_allowed_domain("https://evil.com/image.jpg") is False
    assert is_allowed_domain("https://random-cdn.net/foo.png") is False
    assert is_allowed_domain("http://localhost/image.jpg") is False


def test_is_allowed_domain_invalid_url() -> None:
    """Test domain allowlist validation for malformed URLs."""
    from scripts.mirror_media_assets_to_s3 import is_allowed_domain

    assert is_allowed_domain("") is False
    assert is_allowed_domain("not-a-url") is False
    assert is_allowed_domain(None) is False  # type: ignore[arg-type]


def test_compute_next_retry_at_exponential_backoff() -> None:
    """Test exponential backoff calculation for retry scheduling."""
    from scripts.mirror_media_assets_to_s3 import _compute_next_retry_at

    base_hours = 1.0
    now = datetime.now(UTC)

    # First retry: 1 hour
    next1 = datetime.fromisoformat(_compute_next_retry_at(1, base_hours))
    delta1 = next1 - now
    assert 0.9 <= delta1.total_seconds() / 3600 <= 1.1

    # Second retry: 2 hours
    next2 = datetime.fromisoformat(_compute_next_retry_at(2, base_hours))
    delta2 = next2 - now
    assert 1.9 <= delta2.total_seconds() / 3600 <= 2.1

    # Third retry: 4 hours
    next3 = datetime.fromisoformat(_compute_next_retry_at(3, base_hours))
    delta3 = next3 - now
    assert 3.9 <= delta3.total_seconds() / 3600 <= 4.1

    # Fourth retry: 8 hours
    next4 = datetime.fromisoformat(_compute_next_retry_at(4, base_hours))
    delta4 = next4 - now
    assert 7.9 <= delta4.total_seconds() / 3600 <= 8.1


def test_build_s3_key_content_addressed() -> None:
    """Test S3 key generation uses sha256 prefix for partitioning."""
    from scripts.mirror_media_assets_to_s3 import _build_s3_key

    sha256 = "abc123def456789"
    key = _build_s3_key(sha256, "image/jpeg")

    # Should use first 2 chars as prefix
    assert key.startswith("media/ab/")
    assert sha256 in key
    assert key.endswith(".jpg")


def test_build_s3_key_different_content_types() -> None:
    """Test S3 key generation handles various content types."""
    from scripts.mirror_media_assets_to_s3 import _build_s3_key

    sha256 = "deadbeef12345678"

    assert _build_s3_key(sha256, "image/jpeg").endswith(".jpg")
    assert _build_s3_key(sha256, "image/png").endswith(".png")
    assert _build_s3_key(sha256, "image/webp").endswith(".webp")
    assert _build_s3_key(sha256, "image/gif").endswith(".gif")

    # Unknown type gets no extension
    key_unknown = _build_s3_key(sha256, None)
    assert key_unknown == f"media/de/{sha256}"


def test_guess_extension_normalizes_jpeg() -> None:
    """Test .jpe is normalized to .jpg."""
    from scripts.mirror_media_assets_to_s3 import _guess_extension

    # Some systems return .jpe for image/jpeg
    # Our code normalizes to .jpg
    ext = _guess_extension("image/jpeg")
    assert ext == ".jpg"


def test_mirror_single_asset_domain_not_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test mirroring skips assets with disallowed domains."""
    from scripts.mirror_media_assets_to_s3 import mirror_single_asset

    db = MagicMock()
    # Mock the update_ingest_status response chain
    mock_response = MagicMock()
    mock_response.data = [{"id": "test-asset-1", "ingest_status": "skipped"}]
    mock_response.error = None
    db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute.return_value = mock_response

    asset = {
        "id": "test-asset-1",
        "source_url": "https://evil.com/malware.jpg",
        "ingest_retry_count": 0,
    }

    result = mirror_single_asset(
        db,
        asset,
        s3_client=None,
        bucket="test-bucket",
        cdn_base_url="https://cdn.example.com",
        max_retries=3,
        backoff_hours=1.0,
        dry_run=False,
        verbose=False,
    )

    assert result.status == "skipped"
    assert "allowlist" in result.error.lower()


def test_mirror_single_asset_dry_run_no_changes() -> None:
    """Test dry run mode doesn't make actual changes."""
    from scripts.mirror_media_assets_to_s3 import mirror_single_asset

    db = MagicMock()
    asset = {
        "id": "test-asset-2",
        "source_url": "https://image.tmdb.org/t/p/original/test.jpg",
        "ingest_retry_count": 0,
    }

    result = mirror_single_asset(
        db,
        asset,
        s3_client=None,
        bucket="test-bucket",
        cdn_base_url="https://cdn.example.com",
        max_retries=3,
        backoff_hours=1.0,
        dry_run=True,
        verbose=False,
    )

    # Dry run returns success without actually downloading
    assert result.status == "hosted"
    assert result.bytes_transferred == 0

    # Database should not be called
    db.schema.assert_not_called()


def test_handle_retryable_error_increments_count() -> None:
    """Test retryable errors increment retry count."""
    from scripts.mirror_media_assets_to_s3 import _handle_retryable_error

    db = MagicMock()
    # Mock the update_ingest_status call
    db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
        data=[{"id": "test"}], error=None
    )

    result = _handle_retryable_error(
        db,
        "test-asset",
        current_retry_count=1,
        error="Connection timeout",
        max_retries=5,
        backoff_hours=1.0,
        dry_run=False,
        verbose=False,
    )

    assert result.status == "failed"
    assert result.retry_count == 2


def test_handle_retryable_error_max_retries_exceeded() -> None:
    """Test max retries results in skipped status."""
    from scripts.mirror_media_assets_to_s3 import _handle_retryable_error

    db = MagicMock()
    db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
        data=[{"id": "test"}], error=None
    )

    result = _handle_retryable_error(
        db,
        "test-asset",
        current_retry_count=4,  # Already at 4, next would be 5
        error="Persistent failure",
        max_retries=5,
        backoff_hours=1.0,
        dry_run=False,
        verbose=False,
    )

    assert result.status == "skipped"
    assert "Max retries" in result.error


def test_update_summary_tracks_bytes() -> None:
    """Test summary correctly aggregates bytes transferred."""
    from scripts.mirror_media_assets_to_s3 import MirrorResult, MirrorSummary, _update_summary

    summary = MirrorSummary()

    _update_summary(summary, MirrorResult(asset_id="1", status="hosted", bytes_transferred=1000))
    _update_summary(summary, MirrorResult(asset_id="2", status="hosted", bytes_transferred=2000))
    _update_summary(summary, MirrorResult(asset_id="3", status="failed", error="test"))
    _update_summary(summary, MirrorResult(asset_id="4", status="skipped"))

    assert summary.hosted == 2
    assert summary.failed == 1
    assert summary.skipped == 1
    assert summary.bytes_transferred == 3000
    assert len(summary.errors) == 1


def test_update_ingest_status_clears_error_on_hosted() -> None:
    """Test that marking as hosted clears error fields."""
    from trr_backend.repositories.media_assets import update_ingest_status

    db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [{"id": "test", "ingest_status": "hosted"}]
    mock_response.error = None
    db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute.return_value = mock_response

    update_ingest_status(db, "test-asset", "hosted")

    # Verify the update call includes clearing error fields
    call_args = db.schema.return_value.table.return_value.update.call_args
    payload = call_args[0][0]

    assert payload["ingest_status"] == "hosted"
    assert payload["ingest_last_error"] is None
    assert payload["ingest_failed_at"] is None
    assert payload["ingest_next_retry_at"] is None


def test_fetch_assets_for_mirroring_requires_source_url() -> None:
    """Test that fetching excludes assets without source_url."""
    from trr_backend.repositories.media_assets import fetch_assets_for_mirroring

    db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = []
    mock_response.error = None

    # Chain the mock
    query_mock = MagicMock()
    query_mock.select.return_value = query_mock
    query_mock.eq.return_value = query_mock
    query_mock.not_.is_.return_value = query_mock
    query_mock.order.return_value = query_mock
    query_mock.limit.return_value = query_mock
    query_mock.execute.return_value = mock_response

    db.schema.return_value.table.return_value = query_mock

    fetch_assets_for_mirroring(db, status="pending", limit=10)

    # Verify not_.is_("source_url", "null") was called
    query_mock.not_.is_.assert_called()
