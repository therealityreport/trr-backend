from __future__ import annotations

from trr_backend.socials import social_season_analytics_impl as social_repo


def test_default_renders_all_excluded() -> None:
    clause = social_repo._build_upsert_update_clause(
        "instagram_comments",
        ["text", "likes", "author_full_name"],
        None,
    )
    assert clause == ("text = EXCLUDED.text, likes = EXCLUDED.likes, author_full_name = EXCLUDED.author_full_name")


def test_preserve_columns_render_coalesce_and_others_render_excluded() -> None:
    updates = ["text", "author_full_name", "author_profile_pic_url", "likes"]
    preserve = ["author_full_name", "author_profile_pic_url"]

    clause = social_repo._build_upsert_update_clause("instagram_comments", updates, preserve)

    # Non-preserve columns render plain EXCLUDED assignments.
    assert "text = EXCLUDED.text" in clause
    assert "likes = EXCLUDED.likes" in clause
    # Preserve columns render COALESCE(EXCLUDED.c, social.<table>.c).
    assert (
        "author_full_name = COALESCE(EXCLUDED.author_full_name, social.instagram_comments.author_full_name)" in clause
    )
    assert (
        "author_profile_pic_url = "
        "COALESCE(EXCLUDED.author_profile_pic_url, social.instagram_comments.author_profile_pic_url)" in clause
    )
    # Column order is preserved relative to the updates list.
    assert clause.index("text = EXCLUDED.text") < clause.index("author_full_name = COALESCE")
    assert clause.index("author_full_name = COALESCE") < clause.index("author_profile_pic_url = COALESCE")
    assert clause.index("author_profile_pic_url = COALESCE") < clause.index("likes = EXCLUDED.likes")


def test_preserve_column_not_in_updates_is_ignored() -> None:
    # A preserve column that is not among the update columns must not appear.
    clause = social_repo._build_upsert_update_clause(
        "instagram_comments",
        ["text", "likes"],
        ["author_full_name", "comment_url"],
    )
    assert clause == "text = EXCLUDED.text, likes = EXCLUDED.likes"


def test_preserve_set_uses_provided_table_name() -> None:
    clause = social_repo._build_upsert_update_clause(
        "twitter_tweets",
        ["author_full_name"],
        ["author_full_name"],
    )
    assert clause == ("author_full_name = COALESCE(EXCLUDED.author_full_name, social.twitter_tweets.author_full_name)")


def test_empty_and_whitespace_preserve_names_are_ignored() -> None:
    clause = social_repo._build_upsert_update_clause(
        "instagram_comments",
        ["text"],
        ["", "   "],
    )
    assert clause == "text = EXCLUDED.text"


def test_instagram_preserve_constant_matches_persistence_constant() -> None:
    from trr_backend.socials.instagram.comments_scrapling import persistence

    # The no-season persistence path and the season-context batch upsert must
    # agree on the author/url preserve set so both pass the same COALESCE guard.
    assert (
        persistence._INSTAGRAM_COMMENT_COALESCE_PRESERVE_COLS == social_repo._INSTAGRAM_COMMENT_COALESCE_PRESERVE_COLS
    )
