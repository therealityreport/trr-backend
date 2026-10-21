"""Keep bounded media reuse lookups on indexed, narrow post projections."""

from datetime import date
from typing import Any

import pytest

import trr_backend.socials.social_season_analytics_impl as social_repo
from trr_backend.socials.control_plane import SeasonContext


@pytest.mark.parametrize("platform", list(social_repo.PLATFORM_POST_TABLES))
def test_reuse_query_uses_native_scope_columns_and_only_manifest_fields(
    monkeypatch: pytest.MonkeyPatch, platform: str
) -> None:
    source_column = social_repo.PLATFORM_SOURCE_ID_COLUMN[platform]
    timestamp_column = social_repo.PLATFORM_POSTED_AT_COLUMN[platform]
    columns = {"asset_manifest", "hosted_media_urls", "season_id", "show_id", source_column, timestamp_column}
    monkeypatch.setattr(social_repo, "_social_media_reuse_platforms", lambda: (platform,))
    monkeypatch.setattr(social_repo, "_platform_posts_has_column", lambda _platform, column: column in columns)
    captured: list[tuple[str, Any]] = []
    digest = "a" * 64

    def fetch(sql: str, params: Any) -> list[dict[str, Any]]:
        captured.append((sql, params))
        return [
            {
                "id": "candidate-id",
                source_column: "source-id",
                "asset_manifest": {"originals": {"media": [{"sha256": digest, "url": "https://cdn.test/image.jpg"}]}},
            }
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", fetch)
    result = social_repo._find_social_media_reuse_asset_by_sha256(
        context=SeasonContext(
            season_id="e9161955-6ee4-4985-865e-3386a0f670fb",
            show_id="7782652f-783a-488b-8860-41b97de32e75",
            show_name="Test",
            season_number=1,
            anchor_date=date(2026, 1, 1),
        ),
        current_platform=platform,
        current_post_id="other-id",
        sha256=digest,
    )
    assert result is not None
    assert result["post_id"] == "source-id"
    assert result["sha256"] == digest
    assert len(captured) == 1
    sql, params = captured[0]
    assert "where (p.season_id = %s or p.show_id = %s)" in sql
    assert params == ["e9161955-6ee4-4985-865e-3386a0f670fb", "7782652f-783a-488b-8860-41b97de32e75"]
    assert f"order by p.{timestamp_column} desc nulls last" in sql
    assert "limit 200" in sql
    assert "to_jsonb" not in sql
    assert "raw_data" not in sql


def test_reuse_skips_tables_without_scope_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(social_repo, "_social_media_reuse_platforms", lambda: ("instagram",))
    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda _platform, column: column in {"asset_manifest", "hosted_media_urls"},
    )

    def fail_fetch(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("A reuse lookup without a show/season predicate must never scan the table")

    monkeypatch.setattr(social_repo.pg, "fetch_all", fail_fetch)
    assert (
        social_repo._find_social_media_reuse_asset_by_sha256(
            context=SeasonContext(
                season_id="season-1", show_id="show-1", show_name="Test", season_number=1, anchor_date=date(2026, 1, 1)
            ),
            current_platform="instagram",
            current_post_id="current",
            sha256="a" * 64,
        )
        is None
    )
