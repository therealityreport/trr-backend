"""Unit tests for user media uploads."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image

from trr_backend.media.user_uploads import (
    MAX_UPLOAD_BYTES,
    FinalizedUpload,
    MediaUploadSession,
    _asset_id_for_sha256,
    _build_canonical_key,
    _build_temp_key,
    _extract_image_dimensions,
    _sha256_bytes,
    _validate_content_type,
    _validate_entity_type,
    _validate_expected_bytes,
    _validate_kind,
    cancel_media_upload_session,
    create_media_upload_session,
    finalize_media_upload_session,
    generate_presigned_post,
    set_primary_media_link,
)


def _create_test_image(width: int = 100, height: int = 100, format: str = "JPEG") -> bytes:
    """Create a test image and return its bytes."""
    img = Image.new("RGB", (width, height), color="red")
    buffer = BytesIO()
    img.save(buffer, format=format)
    return buffer.getvalue()


class TestHelperFunctions:
    """Tests for helper/utility functions."""

    def test_sha256_bytes(self) -> None:
        """Test SHA256 computation."""
        data = b"test data"
        result = _sha256_bytes(data)
        assert len(result) == 64  # SHA256 hex is 64 chars
        assert result.isalnum()

    def test_build_temp_key(self) -> None:
        """Test temp S3 key construction."""
        key = _build_temp_key("show", "uuid-123", "upload-456", "poster.jpg")
        assert key == "uploads/show/uuid-123/upload-456/poster.jpg"

    def test_build_temp_key_sanitizes_filename(self) -> None:
        """Test that dangerous characters are removed from filename."""
        key = _build_temp_key("show", "uuid-123", "upload-456", "../../../etc/passwd")
        assert ".." not in key
        assert "/" not in key.split("/")[-1]

    def test_build_canonical_key(self) -> None:
        """Test content-addressed canonical key construction."""
        sha256 = "abcd1234567890" + "0" * 50  # 64 char hash
        key = _build_canonical_key(sha256, ".jpg")
        assert key == f"media/ab/{sha256}.jpg"

    def test_asset_id_for_sha256_deterministic(self) -> None:
        """Test that asset IDs are deterministic for same SHA256."""
        sha256 = "abc123def456" + "0" * 52
        id1 = _asset_id_for_sha256(sha256)
        id2 = _asset_id_for_sha256(sha256)
        assert id1 == id2

    def test_extract_image_dimensions(self) -> None:
        """Test dimension extraction from image bytes."""
        img_bytes = _create_test_image(width=200, height=150)
        width, height = _extract_image_dimensions(img_bytes)
        assert width == 200
        assert height == 150

    def test_extract_image_dimensions_invalid_data(self) -> None:
        """Test dimension extraction returns None for invalid data."""
        width, height = _extract_image_dimensions(b"not an image")
        assert width is None
        assert height is None


class TestValidation:
    """Tests for input validation functions."""

    def test_validate_content_type_accepts_jpeg(self) -> None:
        """Test that image/jpeg is accepted."""
        _validate_content_type("image/jpeg")  # Should not raise

    def test_validate_content_type_accepts_png(self) -> None:
        """Test that image/png is accepted."""
        _validate_content_type("image/png")  # Should not raise

    def test_validate_content_type_accepts_webp(self) -> None:
        """Test that image/webp is accepted."""
        _validate_content_type("image/webp")  # Should not raise

    def test_validate_content_type_rejects_gif(self) -> None:
        """Test that image/gif is rejected."""
        with pytest.raises(ValueError, match="Invalid content_type"):
            _validate_content_type("image/gif")

    def test_validate_content_type_rejects_pdf(self) -> None:
        """Test that application/pdf is rejected."""
        with pytest.raises(ValueError, match="Invalid content_type"):
            _validate_content_type("application/pdf")

    def test_validate_entity_type_accepts_show(self) -> None:
        """Test that 'show' is accepted."""
        _validate_entity_type("show")  # Should not raise

    def test_validate_entity_type_accepts_person(self) -> None:
        """Test that 'person' is accepted."""
        _validate_entity_type("person")  # Should not raise

    def test_validate_entity_type_rejects_invalid(self) -> None:
        """Test that invalid entity types are rejected."""
        with pytest.raises(ValueError, match="Invalid entity_type"):
            _validate_entity_type("movie")

    def test_validate_kind_accepts_poster(self) -> None:
        """Test that 'poster' is accepted."""
        _validate_kind("poster")  # Should not raise

    def test_validate_kind_rejects_invalid(self) -> None:
        """Test that invalid kinds are rejected."""
        with pytest.raises(ValueError, match="Invalid kind"):
            _validate_kind("thumbnail")

    def test_validate_expected_bytes_accepts_under_limit(self) -> None:
        """Test that bytes under limit are accepted."""
        _validate_expected_bytes(MAX_UPLOAD_BYTES - 1)  # Should not raise

    def test_validate_expected_bytes_rejects_over_limit(self) -> None:
        """Test that bytes over limit are rejected."""
        with pytest.raises(ValueError, match="exceeds maximum"):
            _validate_expected_bytes(MAX_UPLOAD_BYTES + 1)

    def test_validate_expected_bytes_accepts_none(self) -> None:
        """Test that None is accepted."""
        _validate_expected_bytes(None)  # Should not raise


class TestCreateMediaUploadSession:
    """Tests for create_media_upload_session function."""

    def test_rejects_bad_content_type(self) -> None:
        """Test that invalid content types are rejected."""
        mock_db = MagicMock()

        with pytest.raises(ValueError, match="Invalid content_type"):
            create_media_upload_session(
                mock_db,
                entity_type="show",
                entity_id="uuid-123",
                kind="poster",
                content_type="image/gif",  # Invalid
            )

    def test_rejects_too_large_expected_bytes(self) -> None:
        """Test that expected_bytes > 10MB is rejected."""
        mock_db = MagicMock()

        with pytest.raises(ValueError, match="exceeds maximum"):
            create_media_upload_session(
                mock_db,
                entity_type="show",
                entity_id="uuid-123",
                kind="poster",
                content_type="image/jpeg",
                expected_bytes=MAX_UPLOAD_BYTES + 1,
            )

    def test_rejects_invalid_entity_type(self) -> None:
        """Test that invalid entity types are rejected."""
        mock_db = MagicMock()

        with pytest.raises(ValueError, match="Invalid entity_type"):
            create_media_upload_session(
                mock_db,
                entity_type="movie",  # Invalid
                entity_id="uuid-123",
                kind="poster",
                content_type="image/jpeg",
            )

    def test_rejects_invalid_kind(self) -> None:
        """Test that invalid kinds are rejected."""
        mock_db = MagicMock()

        with pytest.raises(ValueError, match="Invalid kind"):
            create_media_upload_session(
                mock_db,
                entity_type="show",
                entity_id="uuid-123",
                kind="thumbnail",  # Invalid
                content_type="image/jpeg",
            )

    @patch("trr_backend.media.user_uploads.get_s3_bucket")
    @patch("trr_backend.media.user_uploads.generate_presigned_post")
    def test_creates_session_successfully(
        self,
        mock_presigned: MagicMock,
        mock_bucket: MagicMock,
    ) -> None:
        """Test successful session creation."""
        mock_bucket.return_value = "test-bucket"
        mock_presigned.return_value = {
            "url": "https://test-bucket.s3.amazonaws.com",
            "fields": {"key": "uploads/show/uuid-123/upload-id/test.jpg"},
        }

        mock_db = MagicMock()
        mock_db.schema.return_value.table.return_value.insert.return_value.execute.return_value = MagicMock(
            error=None, data={}
        )

        result = create_media_upload_session(
            mock_db,
            entity_type="show",
            entity_id="uuid-123",
            kind="poster",
            content_type="image/jpeg",
        )

        assert isinstance(result, MediaUploadSession)
        assert result.bucket == "test-bucket"
        assert result.presigned_url == "https://test-bucket.s3.amazonaws.com"


class TestFinalizeMediaUploadSession:
    """Tests for finalize_media_upload_session function."""

    def _mock_db_session(
        self,
        status: str = "initiated",
        expires_at: datetime | None = None,
    ) -> MagicMock:
        """Create a mock DB with a session."""
        if expires_at is None:
            expires_at = datetime.now(UTC) + timedelta(hours=1)

        mock_db = MagicMock()
        session_data = {
            "id": "upload-123",
            "status": status,
            "expires_at": expires_at.isoformat(),
            "s3_bucket": "test-bucket",
            "s3_temp_key": "uploads/show/uuid-123/upload-123/test.jpg",
            "entity_type": "show",
            "entity_id": "uuid-123",
            "kind": "poster",
            "content_type": "image/jpeg",
            "make_primary": False,
            "caption": None,
            "alt_text": None,
        }

        # Mock the select query for loading session
        select_chain = mock_db.schema.return_value.table.return_value.select.return_value
        select_chain.eq.return_value.single.return_value.execute.return_value = MagicMock(error=None, data=session_data)

        return mock_db

    @patch("trr_backend.media.user_uploads.get_s3_client")
    def test_finalize_expired_session_fails(self, mock_s3_client: MagicMock) -> None:
        """Test that expired sessions cannot be finalized."""
        expired_at = datetime.now(UTC) - timedelta(hours=1)  # Already expired
        mock_db = self._mock_db_session(status="initiated", expires_at=expired_at)

        # Mock the update for marking expired
        mock_db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute.return_value = (
            MagicMock(error=None)
        )

        with pytest.raises(ValueError, match="expired"):
            finalize_media_upload_session(mock_db, "upload-123")

    @patch("trr_backend.media.user_uploads.get_s3_client")
    def test_finalize_already_finalized_fails(self, mock_s3_client: MagicMock) -> None:
        """Test that already finalized sessions cannot be finalized again."""
        mock_db = self._mock_db_session(status="finalized")

        with pytest.raises(ValueError, match="already finalized"):
            finalize_media_upload_session(mock_db, "upload-123")

    @patch("trr_backend.media.user_uploads.get_s3_client")
    def test_finalize_canceled_session_fails(self, mock_s3_client: MagicMock) -> None:
        """Test that canceled sessions cannot be finalized."""
        mock_db = self._mock_db_session(status="canceled")

        with pytest.raises(ValueError, match="canceled"):
            finalize_media_upload_session(mock_db, "upload-123")

    @patch("trr_backend.media.user_uploads.get_s3_client")
    @patch("trr_backend.media.user_uploads.get_cdn_base_url")
    def test_finalize_rejects_missing_temp_object(
        self,
        mock_cdn: MagicMock,
        mock_s3_client: MagicMock,
    ) -> None:
        """Test that missing temp object causes failure."""
        mock_db = self._mock_db_session()
        mock_cdn.return_value = "https://cdn.example.com"

        # Mock S3 client that returns 404 for HEAD
        mock_client = MagicMock()
        mock_client.head_object.side_effect = self._not_found_error()
        mock_s3_client.return_value = mock_client

        # Mock update for marking failed
        mock_db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute.return_value = (
            MagicMock(error=None)
        )

        with pytest.raises(RuntimeError, match="not found"):
            finalize_media_upload_session(mock_db, "upload-123")

    def _not_found_error(self) -> Exception:
        """Create a ClientError for 404 Not Found."""
        from botocore.exceptions import ClientError

        return ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )

    @patch("trr_backend.media.user_uploads.get_s3_client")
    @patch("trr_backend.media.user_uploads.get_cdn_base_url")
    def test_finalize_creates_asset_with_null_source_url(
        self,
        mock_cdn: MagicMock,
        mock_s3_client: MagicMock,
    ) -> None:
        """Test that finalized uploads have source_url=NULL."""
        mock_db = self._mock_db_session()
        mock_cdn.return_value = "https://cdn.example.com"

        # Create test image
        img_bytes = _create_test_image()
        sha256 = _sha256_bytes(img_bytes)

        # Mock S3 client
        mock_client = MagicMock()
        mock_client.head_object.side_effect = [
            # First call: HEAD temp object (exists)
            {"ContentLength": len(img_bytes), "ContentType": "image/jpeg"},
            # Second call: HEAD canonical object (doesn't exist)
            self._not_found_error(),
        ]
        mock_client.get_object.return_value = {"Body": BytesIO(img_bytes)}
        mock_client.copy_object.return_value = {}
        mock_client.delete_object.return_value = {}
        mock_s3_client.return_value = mock_client

        # Mock DB operations
        upsert_mock = MagicMock(error=None, data=[{}])
        insert_mock = MagicMock(error=None, data=[{}])
        update_mock = MagicMock(error=None)

        # Track upsert calls to verify source_url is NULL
        upsert_calls: list[dict[str, Any]] = []

        def capture_upsert(payload: dict, **kwargs: Any) -> MagicMock:
            upsert_calls.append(payload)
            mock = MagicMock()
            mock.execute.return_value = upsert_mock
            return mock

        mock_db.schema.return_value.table.return_value.upsert.side_effect = capture_upsert
        mock_db.schema.return_value.table.return_value.insert.return_value.execute.return_value = insert_mock
        mock_db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute.return_value = (
            update_mock
        )

        result = finalize_media_upload_session(mock_db, "upload-123")

        # Verify result
        assert isinstance(result, FinalizedUpload)
        assert result.sha256 == sha256
        assert result.hosted_url.startswith("https://cdn.example.com/media/")

        # Verify source_url is explicitly NULL in upsert
        assert len(upsert_calls) == 1
        asset_payload = upsert_calls[0]
        assert "source_url" in asset_payload
        assert asset_payload["source_url"] is None
        assert asset_payload["source"] == "user_upload"
        assert asset_payload["ingest_status"] == "hosted"


class TestSetPrimaryMediaLink:
    """Tests for set_primary_media_link function."""

    def test_calls_rpc_with_correct_params(self) -> None:
        """Test that the RPC is called with correct parameters."""
        mock_db = MagicMock()
        mock_db.schema.return_value.rpc.return_value.execute.return_value = MagicMock(error=None)

        set_primary_media_link(
            mock_db,
            entity_type="show",
            entity_id="uuid-123",
            kind="poster",
            media_link_id="link-456",
        )

        mock_db.schema.return_value.rpc.assert_called_once_with(
            "set_primary_media_link",
            {
                "p_entity_type": "show",
                "p_entity_id": "uuid-123",
                "p_kind": "poster",
                "p_media_link_id": "link-456",
            },
        )

    def test_raises_on_rpc_error(self) -> None:
        """Test that RPC errors are raised."""
        mock_db = MagicMock()
        mock_db.schema.return_value.rpc.return_value.execute.return_value = MagicMock(
            error="Link does not belong to entity"
        )

        with pytest.raises(RuntimeError, match="Failed to set primary"):
            set_primary_media_link(
                mock_db,
                entity_type="show",
                entity_id="uuid-123",
                kind="poster",
                media_link_id="wrong-link",
            )


class TestCancelMediaUploadSession:
    """Tests for cancel_media_upload_session function."""

    @patch("trr_backend.media.user_uploads.get_s3_client")
    def test_cancel_finalized_fails(self, mock_s3_client: MagicMock) -> None:
        """Test that finalized sessions cannot be canceled."""
        mock_db = MagicMock()
        select_chain = mock_db.schema.return_value.table.return_value.select.return_value
        select_chain.eq.return_value.single.return_value.execute.return_value = MagicMock(
            error=None,
            data={
                "id": "upload-123",
                "status": "finalized",
                "s3_bucket": "test-bucket",
                "s3_temp_key": "uploads/test/key",
            },
        )

        with pytest.raises(ValueError, match="Cannot cancel finalized"):
            cancel_media_upload_session(mock_db, "upload-123")

    @patch("trr_backend.media.user_uploads.get_s3_client")
    def test_cancel_deletes_temp_object(
        self,
        mock_s3_client: MagicMock,
    ) -> None:
        """Test that canceling deletes the temp S3 object."""
        mock_db = MagicMock()
        select_chain = mock_db.schema.return_value.table.return_value.select.return_value
        select_chain.eq.return_value.single.return_value.execute.return_value = MagicMock(
            error=None,
            data={
                "id": "upload-123",
                "status": "initiated",
                "s3_bucket": "test-bucket",
                "s3_temp_key": "uploads/test/key",
            },
        )
        mock_db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute.return_value = (
            MagicMock(error=None)
        )

        mock_client = MagicMock()
        mock_s3_client.return_value = mock_client

        cancel_media_upload_session(mock_db, "upload-123")

        mock_client.delete_object.assert_called_once_with(Bucket="test-bucket", Key="uploads/test/key")


class TestGeneratePresignedPost:
    """Tests for generate_presigned_post function."""

    def test_generates_presigned_post_with_conditions(self) -> None:
        """Test that presigned POST is generated with size/type conditions."""
        import boto3

        # Create a real S3 client
        s3_client = boto3.client("s3", region_name="us-east-1")

        # We can't stub generate_presigned_post directly, but we can test
        # the function signature and that it returns the expected structure
        with patch.object(s3_client, "generate_presigned_post") as mock_presigned:
            mock_presigned.return_value = {
                "url": "https://test-bucket.s3.amazonaws.com",
                "fields": {"key": "test/key"},
            }

            result = generate_presigned_post(
                bucket="test-bucket",
                key="test/key",
                content_type="image/jpeg",
                max_bytes=1024,
                s3_client=s3_client,
            )

            assert "url" in result
            assert "fields" in result

            # Verify conditions were passed
            mock_presigned.assert_called_once()
            call_kwargs = mock_presigned.call_args[1]
            assert "Conditions" in call_kwargs

            conditions = call_kwargs["Conditions"]
            # Check content-type condition
            assert {"Content-Type": "image/jpeg"} in conditions
            # Check content-length-range condition
            assert ["content-length-range", 0, 1024] in conditions
