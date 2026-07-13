from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    InstagramCommentsScraplingFetcher,
    _status_only_fetch_reason,
    _target_metadata_context,
    _target_metadata_indicates_coauthor,
)


def _build_fetcher() -> InstagramCommentsScraplingFetcher:
    with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
        fetcher = InstagramCommentsScraplingFetcher(
            cookies=[],
            raw_cookies={"csrftoken": "initial"},
            browser_account_id="thetraitorsus",
        )
        asyncio.run(fetcher._rebuild_http_client())
        return fetcher


def test_collaborators_detail_metadata_marks_status_only_as_coauthor() -> None:
    metadata = {
        "source_id": "ABC123",
        "selected_profile_account": "thetraitorsus",
        "profile_account": "thetraitorsus",
        "source_account": "peacock",
        "caption_author": "peacock",
        "owner_username": "peacock",
        "collaborators_detail": [
            {"username": "thetraitorsus", "full_name": "The Traitors"},
        ],
        "profile_source_surface": "catalog",
        "profile_match_mode": "catalog_collaborator",
    }

    context = _target_metadata_context(metadata)

    assert context["selected_profile_account"] == "thetraitorsus"
    assert context["caption_author"] == "peacock"
    assert context["collaborator_handles"] == ["thetraitorsus"]
    assert _target_metadata_indicates_coauthor(metadata) is True
    assert _status_only_fetch_reason(metadata) == "coauthor_comments_endpoint_empty"


def test_collaborator_status_only_response_runs_coauthor_fallbacks() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(return_value=([], {}))
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=3,
            target_metadata={
                "selected_profile_account": "thetraitorsus",
                "profile_account": "thetraitorsus",
                "source_account": "peacock",
                "caption_author": "peacock",
                "owner_username": "peacock",
                "collaborators_detail": [{"username": "thetraitorsus"}],
                "profile_match_mode": "catalog_collaborator",
            },
        )
    )

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "coauthor_comments_endpoint_empty"
    assert result.diagnostic_metadata["is_coauthor_context"] is True
    assert result.diagnostic_metadata["owner_context"]["selected_profile_account"] == "thetraitorsus"
    assert result.diagnostic_metadata["owner_context"]["caption_author"] == "peacock"
    assert result.diagnostic_metadata["owner_context"]["collaborator_handles"] == ["thetraitorsus"]
    fetcher._fetch_graphql_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_load_comment_target_metadata_uses_catalog_collaborator_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    captured: dict[str, Any] = {}

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "shortcode": "ABC123",
                "materialized_post_id": "catalog-post-1",
                "source_account": "peacock",
                "username": "peacock",
                "owner_username": "peacock",
                "collaborators": [{"username": "TheTraitorsUS"}],
                "collaborators_detail": [],
                "media_type": "video",
                "product_type": "clips",
                "profile_source_surface": "catalog",
                "profile_match_mode": "catalog_collaborator",
                "catalog_collaborator_handle": "thetraitorsus",
            }
        ]

    monkeypatch.setattr(jr.pg, "fetch_all", fake_fetch_all)

    metadata_by_shortcode = jr._load_comment_target_metadata(
        account_handle="@thetraitorsus",
        target_source_ids=["ABC123"],
    )

    assert "instagram_account_catalog_post_collaborators" in captured["sql"]
    assert "instagram_account_catalog_posts" in captured["sql"]
    assert "ltrim(lower(coalesce(nullif(m.collaborator_handle, ''), '')), '@') = %s" in captured["sql"]
    assert "to_jsonb" not in captured["sql"]
    assert "when ltrim(lower(coalesce(nullif(p.source_account, ''), '')), '@') = %s then 4" in captured["sql"]
    assert "then 3" in captured["sql"]
    assert "else 1" in captured["sql"]
    assert "5 as profile_match_rank" in captured["sql"]
    assert captured["params"][0] == ["ABC123"]
    assert captured["params"][-2:] == ["thetraitorsus", "thetraitorsus"]

    metadata = metadata_by_shortcode["ABC123"]
    assert metadata["selected_profile_account"] == "thetraitorsus"
    assert metadata["profile_account"] == "thetraitorsus"
    assert metadata["account_handle"] == "thetraitorsus"
    assert metadata["source_account"] == "peacock"
    assert metadata["owner_username"] == "peacock"
    assert metadata["caption_author"] == "peacock"
    assert metadata["original_author"] == "peacock"
    assert metadata["collaborators"] == ["TheTraitorsUS"]
    assert metadata["collaborator_handles"] == ["thetraitorsus"]
    assert metadata["collaborators_detail"] == []
    assert metadata["profile_source_surface"] == "catalog"
    assert metadata["profile_match_mode"] == "catalog_collaborator"
    assert metadata["is_collaborator_post"] is True
    assert metadata["is_collaborator"] is True
    assert metadata["has_collaborators"] is True


def test_load_comment_target_metadata_marks_authored_posts_with_collaborators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "shortcode": "DTRdpWtjbz5",
                "materialized_post_id": "post-1",
                "source_account": "thetraitorsus",
                "username": "thetraitorsus",
                "owner_username": "thetraitorsus",
                "collaborators": [
                    {"username": "johnnygweir"},
                    {"username": "nbcsports"},
                    {"username": "taralipinski"},
                ],
                "collaborators_detail": [],
                "media_type": "video",
                "product_type": "clips",
                "profile_source_surface": "materialized",
                "profile_match_mode": "profile_source_account",
                "catalog_collaborator_handle": None,
            }
        ]

    monkeypatch.setattr(jr.pg, "fetch_all", fake_fetch_all)

    metadata_by_shortcode = jr._load_comment_target_metadata(
        account_handle="@thetraitorsus",
        target_source_ids=["DTRdpWtjbz5"],
    )

    metadata = metadata_by_shortcode["DTRdpWtjbz5"]
    assert metadata["original_author"] == "thetraitorsus"
    assert metadata["collaborator_handles"] == ["johnnygweir", "nbcsports", "taralipinski"]
    assert metadata["is_collaborator_post"] is False
    assert metadata["is_collaborator"] is False
    assert metadata["has_collaborators"] is True
    assert _target_metadata_indicates_coauthor(metadata) is True
