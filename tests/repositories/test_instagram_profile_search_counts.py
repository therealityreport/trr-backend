"""Search must narrow comment aggregation without changing the rich matcher."""

import pytest

import trr_backend.socials.social_season_analytics_impl as repo


@pytest.mark.parametrize("search", ["TARGET", "#Bravo_Con", "@foo_bar"])
def test_search_counts_only_deduped_matches(monkeypatch, search):
    winner = {"id": "matched", "source_id": "TARGET", "caption": "#Bravo_Con @foo.bar", "comments_count": 9}
    other = {"id": "other", "source_id": "OTHER", "caption": "unrelated", "comments_count": 999}
    monkeypatch.setattr(repo, "_fetch_social_account_profile_rows", lambda *a, **k: [winner, other])
    monkeypatch.setattr(
        repo,
        "_fetch_instagram_owner_catalog_rows",
        lambda *a, **k: [{**other, "id": "loser", "caption": winner["caption"] + " TARGET"}],
    )
    monkeypatch.setattr(repo, "_fetch_instagram_collaborator_catalog_rows", lambda *a, **k: [])
    seen = []

    def counts(ids, **kwargs):
        seen.extend(ids)
        return {"matched": {"saved_comments": 2, "saved_parent_comments": 1, "saved_child_replies": 1}}

    monkeypatch.setattr(repo, "_count_stored_instagram_comment_breakdown", counts)
    rows = repo._instagram_social_account_profile_dataset_rows("bravotv", search=search, comments_only=True)
    assert seen == ["matched"]
    assert [row["id"] for row in rows] == ["matched"]


@pytest.mark.parametrize(
    "comment_filter, expected", [("commentable", ["a"]), ("not_commentable", ["b"]), ("incomplete", [])]
)
def test_search_honors_comment_filter(monkeypatch, comment_filter, expected):
    rows = [
        {"id": "a", "source_id": "TARGETa", "comments_count": 5, "fb_comment_count": 1},
        {"id": "b", "source_id": "TARGETb", "comments_count": 0},
    ]
    monkeypatch.setattr(repo, "_fetch_social_account_profile_rows", lambda *a, **k: rows)
    monkeypatch.setattr(repo, "_fetch_instagram_owner_catalog_rows", lambda *a, **k: [])
    monkeypatch.setattr(repo, "_fetch_instagram_collaborator_catalog_rows", lambda *a, **k: [])
    monkeypatch.setattr(
        repo,
        "_count_stored_instagram_comment_breakdown",
        lambda *a, **k: {"a": {"saved_comments": 2, "classified_missing_comments": 1}, "b": {"saved_comments": 1}},
    )
    result = repo._instagram_social_account_profile_dataset_rows(
        "bravotv", search="TARGET", comments_only=True, comment_filter=comment_filter
    )
    assert [row["id"] for row in result] == expected


def test_search_hydrates_only_matched_source_ids(monkeypatch):
    selected = {"id": "a", "source_id": "TARGET", "comments_count": 2}
    other = {"id": "b", "source_id": "OTHER", "comments_count": 999}
    calls = []

    def materialized(*args, **kwargs):
        calls.append(kwargs)
        if kwargs.get("projection") == "search":
            assert "source_ids" not in kwargs
            return [selected, other]
        assert kwargs["source_ids"] == ["TARGET"]
        return [selected]

    def catalog(*args, **kwargs):
        if not kwargs.get("search_projection"):
            assert kwargs["source_ids"] == ["TARGET"]
        return []

    monkeypatch.setattr(repo, "_fetch_social_account_profile_rows", materialized)
    monkeypatch.setattr(repo, "_fetch_instagram_owner_catalog_rows", catalog)
    monkeypatch.setattr(repo, "_fetch_instagram_collaborator_catalog_rows", catalog)
    monkeypatch.setattr(repo, "_count_stored_instagram_comment_breakdown", lambda *a, **k: {})
    result = repo._instagram_social_account_profile_dataset_rows("bravotv", search="TARGET", comments_only=True)
    assert [row["id"] for row in result] == ["a"]
    assert len(calls) == 2


@pytest.mark.parametrize("kind", ["materialized", "owner_catalog", "collaborator_catalog"])
def test_selected_source_query_binds_ids_before_limit(monkeypatch, kind):
    seen = []
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"source_account", "raw_data"})
    monkeypatch.setattr(repo.pg, "fetch_all", lambda sql, params: seen.append((sql, params)) or [])
    kwargs = {"limit": 10, "source_ids": ["TARGET"]}
    if kind == "materialized":
        repo._fetch_social_account_profile_rows("instagram", "bravotv", **kwargs)
    elif kind == "owner_catalog":
        repo._fetch_instagram_owner_catalog_rows("bravotv", **kwargs)
    else:
        repo._fetch_instagram_collaborator_catalog_rows("bravotv", **kwargs)
    sql, params = seen[0]
    assert "= any(%s::text[])" in sql
    assert params[-3:] == [["TARGET"], 10, 0] if kind == "materialized" else params[-2:] == [["TARGET"], 10]
