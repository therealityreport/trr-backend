from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram import catalog_ingest as catalog


def _post(raw_data: dict[str, Any], **overrides: Any) -> SimpleNamespace:
    values = {
        "shortcode": "THIN123",
        "taken_at": 1_700_000_000,
        "media_urls": [],
        "thumbnail_url": None,
        "profile_tags": [],
        "collaborators": [],
        "hashtags": [],
        "mentions": [],
        "hosted_media_urls": [],
        "likes": 1,
        "comments": 2,
        "video_views_observed": None,
        "caption": None,
        "post_type": "image",
        "username": "bravotv",
        "child_posts_data": [],
        "sponsored": False,
    }
    values.update(overrides)
    post = SimpleNamespace(**values)
    post.to_dict = lambda: dict(raw_data)
    return post


def test_post_thin_update_prefers_sidecar_preservation_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(catalog, "_instagram_posts_has_column", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(catalog._core, "_instagram_posts_has_column", lambda *_args, **_kwargs: True)
    rich_children = [{"slide_index": 0, "image": {"url": "https://cdn.test/slide.jpg"}}]
    rich_manifest = {"original": {"sha256": "abc123", "bytes": 12345}}
    payload = catalog._instagram_post_payload(
        None,
        job_id=None,
        account="bravotv",
        post=_post({"has_more_comments": True}),
        conn=object(),
        existing_row={
            "views": 12,
            "raw_data": {"image_versions2": {"candidates": [{"url": "rich"}]}},
            "child_posts_data": rich_children,
            "asset_manifest": rich_manifest,
            "media_urls": ["https://cdn.test/rich.jpg"],
            "thumbnail_url": "https://cdn.test/rich.jpg",
        },
    )
    assert payload is not None
    assert payload["raw_data"]["image_versions2"]["candidates"][0]["url"] == "rich"
    assert payload["media_urls"] == ["https://cdn.test/rich.jpg"]
    assert payload["views"] == 12
    assert payload["child_posts_data"] == rich_children
    assert payload["asset_manifest"] == rich_manifest

    sidecar = catalog._payload_sidecars.post_sidecar_payload(
        legacy_row={"id": "11111111-1111-4111-8111-111111111111"},
        payload=payload,
    )
    assert sidecar is not None
    assert sidecar["child_posts_data"] == rich_children
    assert sidecar["asset_manifest"] == rich_manifest


def test_catalog_thin_update_preserves_rich_raw_and_child_payload() -> None:
    payload = catalog._shared_catalog_instagram_post_payload(
        run_id="run-1",
        account_handle="bravotv",
        post=_post({"has_more_comments": True}),
        existing_row={
            "raw_data": {"image_versions2": {"candidates": [{"url": "rich"}]}},
            "child_posts_data": [{"slide_index": 0}],
        },
    )
    assert payload is not None
    assert payload["raw_data"]["image_versions2"]["candidates"][0]["url"] == "rich"
    assert payload["child_posts_data"] == [{"slide_index": 0}]


def test_single_post_dual_write_opens_managed_transaction_for_patched_upsert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed_conn = object()
    observed_at = datetime(2026, 7, 13, 18, 30, tzinfo=UTC)
    captured: dict[str, Any] = {}

    @contextmanager
    def _managed_transaction(conn: Any, *, label: str):
        captured["transaction"] = (conn, label)
        yield managed_conn

    def _fake_payload(*_args: Any, conn: Any, **_kwargs: Any) -> dict[str, Any]:
        assert conn is managed_conn
        return {
            "shortcode": "POST123",
            "raw_data": {"shortcode": "POST123"},
            "asset_manifest": {},
            "child_posts_data": [],
            "metadata_scraped_at": observed_at,
        }

    def _fake_upsert(*_args: Any, conn: Any, **_kwargs: Any) -> dict[str, Any]:
        assert conn is managed_conn
        return {
            "id": "11111111-1111-4111-8111-111111111111",
            "shortcode": "POST123",
        }

    def _fake_sidecar_upsert(payloads: list[dict[str, Any]], *, conn: Any) -> list[dict[str, Any]]:
        captured["sidecar_payloads"] = payloads
        captured["sidecar_conn"] = conn
        return []

    monkeypatch.setattr(catalog._payload_sidecars, "payload_write_transaction", _managed_transaction)
    monkeypatch.setattr(catalog, "_instagram_post_payload", _fake_payload)
    monkeypatch.setattr(catalog._core, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(catalog._payload_sidecars, "upsert_post_payloads", _fake_sidecar_upsert)
    monkeypatch.setattr(catalog._core, "_sync_instagram_canonical_post", lambda **_kwargs: None)

    row = catalog._upsert_instagram_post(
        None,
        job_id="job-1",
        account="bravotv",
        post=SimpleNamespace(shortcode="POST123"),
        conn=None,
    )

    assert row == {
        "id": "11111111-1111-4111-8111-111111111111",
        "shortcode": "POST123",
    }
    assert captured["transaction"] == (None, "instagram_post_payload_dual_write")
    assert captured["sidecar_conn"] is managed_conn
    assert captured["sidecar_payloads"][0]["payload_updated_at"] == observed_at


def test_single_catalog_dual_write_rolls_back_managed_transaction_for_patched_upsert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed_conn = object()
    observed_at = datetime(2026, 7, 13, 18, 45, tzinfo=UTC)
    captured: dict[str, Any] = {"rolled_back": False}

    @contextmanager
    def _managed_transaction(conn: Any, *, label: str):
        captured["transaction"] = (conn, label)
        try:
            yield managed_conn
        except RuntimeError:
            captured["rolled_back"] = True
            raise

    def _fake_payload(**_kwargs: Any) -> dict[str, Any]:
        return {
            "source_id": "CATALOG123",
            "raw_data": {"shortcode": "CATALOG123"},
            "child_posts_data": [],
            "updated_at": observed_at,
        }

    def _fake_upsert(*_args: Any, conn: Any, **_kwargs: Any) -> dict[str, Any]:
        assert conn is managed_conn
        return {
            "id": "22222222-2222-4222-8222-222222222222",
            "source_id": "CATALOG123",
        }

    def _fake_sidecar_upsert(payloads: list[dict[str, Any]], *, conn: Any) -> list[dict[str, Any]]:
        captured["sidecar_payloads"] = payloads
        captured["sidecar_conn"] = conn
        raise RuntimeError("catalog sidecar failed")

    monkeypatch.setattr(catalog._payload_sidecars, "payload_write_transaction", _managed_transaction)
    monkeypatch.setattr(catalog, "_shared_catalog_instagram_post_payload", _fake_payload)
    monkeypatch.setattr(catalog._core, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(
        catalog._payload_sidecars,
        "fetch_catalog_preservation_rows",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(catalog._payload_sidecars, "upsert_catalog_payloads", _fake_sidecar_upsert)

    with pytest.raises(RuntimeError, match="catalog sidecar failed"):
        catalog._upsert_shared_catalog_instagram_post(
            run_id="run-1",
            account_handle="bravotv",
            post=SimpleNamespace(shortcode="CATALOG123"),
            conn=None,
        )

    assert captured["transaction"] == (None, "instagram_catalog_payload_dual_write")
    assert captured["sidecar_conn"] is managed_conn
    assert captured["sidecar_payloads"][0]["payload_updated_at"] == observed_at
    assert captured["rolled_back"] is True


def test_post_batch_dual_writes_sidecars_in_same_transaction_without_n_plus_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = object()
    captured: dict[str, Any] = {"sidecar_calls": 0}

    def _fake_payload(_context: Any, *, account: str, post: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            "shortcode": post.shortcode,
            "source_account": account,
            "raw_data": {"shortcode": post.shortcode},
            "asset_manifest": {},
            "child_posts_data": [],
        }

    def _fake_upsert_many(_table: str, payloads: list[dict[str, Any]], **kwargs: Any) -> list[dict[str, Any]]:
        assert kwargs["conn"] is fake_conn
        return [
            {
                "id": f"00000000-0000-4000-8000-{index:012d}",
                "shortcode": payload["shortcode"],
            }
            for index, payload in enumerate(payloads, start=1)
        ]

    def _fake_sidecar_upsert(payloads: list[dict[str, Any]], *, conn: Any) -> list[dict[str, Any]]:
        captured["sidecar_calls"] += 1
        captured["payload_count"] = len(payloads)
        captured["conn"] = conn
        return []

    monkeypatch.setattr(catalog, "_instagram_post_payload", _fake_payload)
    monkeypatch.setattr(catalog._core, "_pg_upsert_many", _fake_upsert_many)
    monkeypatch.setattr(catalog._core, "_sync_instagram_canonical_post", lambda **_kwargs: None)
    monkeypatch.setattr(catalog._payload_sidecars, "upsert_post_payloads", _fake_sidecar_upsert)

    rows = catalog._batch_upsert_instagram_posts(
        None,
        job_id="job-1",
        account="bravotv",
        posts=[SimpleNamespace(shortcode="A"), SimpleNamespace(shortcode="B")],
        conn=fake_conn,
    )
    assert len(rows) == 2
    assert captured == {"sidecar_calls": 1, "payload_count": 2, "conn": fake_conn}
