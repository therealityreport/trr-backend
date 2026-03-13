from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from api.routers import admin_scrape


def test_import_non_show_logo_target_keeps_base_r2_mirror_when_logo_variant_decode_fails() -> None:
    db = MagicMock()
    execute_response = SimpleNamespace(data=[{"id": "asset-1"}], error=None)
    upsert_query = MagicMock()
    upsert_query.execute.return_value = execute_response
    table_query = MagicMock()
    table_query.upsert.return_value = upsert_query
    schema_query = MagicMock()
    schema_query.table.return_value = table_query
    db.schema.return_value = schema_query

    with (
        patch("trr_backend.media.s3_mirror.get_s3_client", return_value=object()),
        patch("trr_backend.media.s3_mirror.get_s3_bucket", return_value="trr-media-prod"),
        patch("trr_backend.media.s3_mirror.guess_ext_from_content_type", return_value=".svg"),
        patch(
            "trr_backend.media.s3_mirror.build_logo_s3_key",
            return_value="brands-publication/imdb.com/sha256/logo.svg",
        ),
        patch("trr_backend.media.s3_mirror.upload_bytes_to_s3", return_value=("etag-1", 321)),
        patch(
            "trr_backend.media.s3_mirror.mirror_logo_monochrome_variants_row",
            side_effect=RuntimeError("logo_decode_failed"),
        ),
        patch(
            "api.routers.admin_show_sync.build_hosted_url",
            return_value="https://cdn.example.com/brands-publication/imdb.com/sha256/logo.svg",
        ),
        patch("api.routers.admin_show_sync._detect_base_logo_format", return_value="svg"),
        patch("api.routers.admin_show_sync._upsert_logo_import_audit") as audit_mock,
    ):
        status, hosted_logo_url, created_asset_id = admin_scrape._import_non_show_logo_target(
            db=db,
            target_type="publication",
            target_key="imdb.com",
            target_label="IMDb",
            set_primary=False,
            image_data=b"<svg />",
            sha256="sha256",
            content_type="image/svg+xml",
            source_url="https://logos.fandom.com/wiki/IMDb",
            source_page_url="https://logos.fandom.com/wiki/IMDb",
            source_domain="logos.fandom.com",
            metadata={"source_provider": "logos_fandom"},
        )

    assert status == "imported"
    assert hosted_logo_url == "https://cdn.example.com/brands-publication/imdb.com/sha256/logo.svg"
    assert created_asset_id == "asset-1"

    payload = table_query.upsert.call_args.args[0]
    assert payload["hosted_logo_key"] == "brands-publication/imdb.com/sha256/logo.svg"
    assert payload["hosted_logo_url"] == "https://cdn.example.com/brands-publication/imdb.com/sha256/logo.svg"
    assert payload["hosted_logo_black_key"] is None
    assert payload["hosted_logo_white_key"] is None
    assert payload["mirror_status"] == "mirrored"
    assert payload["failure_reason"] is None
    audit_mock.assert_called_once()
