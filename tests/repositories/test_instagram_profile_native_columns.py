"""Profile pagination must retain owner membership without serializing whole rows."""

import pytest

import trr_backend.socials.social_season_analytics_impl as repo


def test_native_owner_membership_keeps_raw_owner_fallbacks():
    sql = repo._instagram_owner_account_match_sql(native_columns={"source_account", "username", "raw_data"})
    assert "to_jsonb" not in sql
    assert "p.source_account::text" in sql
    assert "p.username::text" in sql
    assert "p.owner_username::text" not in sql
    for field in ("source_account", "owner_username", "username"):
        assert f"p.raw_data ->> '{field}'" in sql
    assert "collaborator" not in sql
    assert sql.count("%s") == 1


@pytest.mark.parametrize("optional_present", [False, True])
def test_paged_profile_uses_available_native_fields_and_keeps_pagination(monkeypatch, optional_present):
    columns = {"source_account", "raw_data"}
    if optional_present:
        columns |= {"hosted_media_urls", "hosted_thumbnail_url", "post_format", "collaborators_detail"}
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: columns)
    monkeypatch.setattr(repo, "_instagram_catalog_collaborator_membership_available", lambda **k: True)
    monkeypatch.setattr(repo, "_comment_lifecycle_supported", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_instagram_post_comment_rollups_available", lambda **k: True)
    queries = []
    monkeypatch.setattr(
        repo.pg, "fetch_all", lambda sql, params: queries.append((sql, params)) or [{"_total_count": 40}]
    )
    rows, total = repo._fetch_instagram_comments_only_profile_rows_page(
        "BravoTV", page=2, page_size=25, comment_filter="incomplete"
    )
    sql, params = queries[0]
    assert total == 40 and len(rows) == 1
    assert params == ["bravotv", "bravotv", "bravotv", 25, 25]
    assert "to_jsonb(p)" not in sql
    assert "p.raw_data -> 'collaborators_detail'" in sql
    assert "count(*) over()" in sql
    assert "limit %s offset %s" in sql
    assert ("p.hosted_media_urls" in sql) == optional_present
    assert ("null::jsonb" in sql) != optional_present
