from __future__ import annotations

import importlib

import pytest

mod = importlib.import_module("scripts.media.rebuild_hosted_urls")


@pytest.fixture(autouse=True)
def stub_build_hosted_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        mod,
        "build_hosted_url",
        lambda hosted_key: f"https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/{str(hosted_key).lstrip('/')}",
    )


def test_resolve_desired_hosted_url_prefers_hosted_key() -> None:
    assert mod.resolve_desired_hosted_url(
        hosted_key="media-variants/asset-1/base/card.webp",
        current_url="https://d1fmdyqfafwim3.cloudfront.net/media-variants/asset-1/base/card.webp",
    ) == "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/media-variants/asset-1/base/card.webp"


def test_rewrite_metadata_urls_rewrites_nested_legacy_gallery_hosts() -> None:
    metadata = {
        "display_url": "https://d1fmdyqfafwim3.cloudfront.net/media-variants/asset-1/base/card.webp",
        "variants": {
            "base": {
                "card": {
                    "webp": {
                        "url": "https://d1fmdyqfafwim3.cloudfront.net/media-variants/asset-1/base/card.webp",
                    },
                },
            },
        },
        "source_page_url": "https://www.bravotv.com/people/lisa-barlow",
    }

    rewritten, changed = mod.rewrite_metadata_urls(metadata)

    assert changed is True
    assert rewritten["display_url"] == (
        "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/media-variants/asset-1/base/card.webp"
    )
    assert rewritten["variants"]["base"]["card"]["webp"]["url"] == (
        "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/media-variants/asset-1/base/card.webp"
    )
    assert rewritten["source_page_url"] == "https://www.bravotv.com/people/lisa-barlow"


def test_rewrite_metadata_urls_rewrites_cast_photo_variant_hosts() -> None:
    metadata = {
        "display_url": "https://d1fmdyqfafwim3.cloudfront.net/cast-photo-variants/asset-1/base/card.webp",
        "variants": {
            "base": {
                "detail": {
                    "webp": {
                        "url": "https://d1fmdyqfafwim3.cloudfront.net/cast-photo-variants/asset-1/base/detail.webp",
                    },
                },
            },
        },
    }

    rewritten, changed = mod.rewrite_metadata_urls(metadata)

    assert changed is True
    assert rewritten["display_url"] == (
        "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/cast-photo-variants/asset-1/base/card.webp"
    )
    assert rewritten["variants"]["base"]["detail"]["webp"]["url"] == (
        "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/cast-photo-variants/asset-1/base/detail.webp"
    )


def test_rewrite_metadata_urls_rewrites_face_crop_hosts() -> None:
    metadata = {
        "face_crops": [
            {
                "variant_url": (
                    "https://d1fmdyqfafwim3.cloudfront.net/face-crops/cast_photo/asset-1/example.jpg"
                ),
            }
        ]
    }

    rewritten, changed = mod.rewrite_metadata_urls(metadata)

    assert changed is True
    assert rewritten["face_crops"][0]["variant_url"] == (
        "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/face-crops/cast_photo/asset-1/example.jpg"
    )


def test_build_row_patch_updates_media_asset_hosted_url_and_metadata() -> None:
    row = {
        "id": "asset-1",
        "hosted_key": "images/people/example/photo.jpg",
        "hosted_url": "https://d1fmdyqfafwim3.cloudfront.net/images/people/example/photo.jpg",
        "metadata": {
            "detail_url": "https://d1fmdyqfafwim3.cloudfront.net/media-variants/asset-1/base/detail.webp",
        },
    }

    patch = mod.build_row_patch(table="media_assets", row=row)

    assert patch == {
        "hosted_url": "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/images/people/example/photo.jpg",
        "metadata": {
            "detail_url": (
                "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/media-variants/asset-1/base/detail.webp"
            ),
        },
    }


def test_build_row_patch_updates_media_asset_variant_hosted_url_only() -> None:
    row = {
        "id": "variant-1",
        "hosted_key": "media-variants/asset-1/base/detail.webp",
        "hosted_url": "https://d1fmdyqfafwim3.cloudfront.net/media-variants/asset-1/base/detail.webp",
    }

    patch = mod.build_row_patch(table="media_asset_variants", row=row)

    assert patch == {
        "hosted_url": "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/media-variants/asset-1/base/detail.webp",
    }
