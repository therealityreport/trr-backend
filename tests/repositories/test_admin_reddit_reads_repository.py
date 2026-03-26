from __future__ import annotations

from typing import Any

from trr_backend.repositories import admin_reddit_reads as repo


def test_list_communities_binds_all_sql_parameters(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch_all(query: str, params: list[Any] | tuple[Any, ...] | None = None):
        captured["query"] = query
        captured["params"] = list(params or [])
        return []

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.list_reddit_communities(
        trr_show_id="show-1",
        include_inactive=True,
        trr_season_id="season-1",
        include_global_threads_for_season=False,
        include_assigned_threads=True,
    )

    assert query_count == 1
    assert payload == {"communities": []}
    assert captured["query"].count("%s") == len(captured["params"]) == 6
    assert captured["params"] == [
        "season-1",
        "season-1",
        False,
        "show-1",
        "show-1",
        True,
    ]


def test_list_threads_binds_all_sql_parameters(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch_all(query: str, params: list[Any] | tuple[Any, ...] | None = None):
        captured["query"] = query
        captured["params"] = list(params or [])
        return []

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.list_reddit_threads(
        community_id="community-1",
        trr_show_id="show-1",
        trr_season_id="season-1",
        include_global_threads_for_season=False,
    )

    assert query_count == 1
    assert payload == {"threads": []}
    assert captured["query"].count("%s") == len(captured["params"]) == 7
    assert captured["params"] == [
        "community-1",
        "community-1",
        "show-1",
        "show-1",
        "season-1",
        "season-1",
        False,
    ]


def test_stored_post_counts_contract(monkeypatch) -> None:
    captured: dict[str, Any] = {"all_calls": [], "one_calls": 0}

    def fake_fetch_all(query: str, params: list[Any] | tuple[Any, ...] | None = None):
        captured["all_calls"].append((query, list(params or [])))
        normalized_query = query.lower()
        if "container_post_count" in normalized_query:
            return [
                {
                    "flair_key": "cast",
                    "flair_label": "Cast",
                    "post_count": 2,
                    "container_key": "episode-1",
                    "container_post_count": 2,
                },
                {
                    "flair_key": "cast",
                    "flair_label": "Cast",
                    "post_count": 2,
                    "container_key": "period-preseason",
                    "container_post_count": 1,
                },
            ]
        if "group by container_key" in normalized_query:
            return [
                {"container_key": "episode-1", "post_count": 3},
                {"container_key": "period-preseason", "post_count": 1},
            ]
        if "unassigned" in normalized_query:
            return [
                {"container_key": "episode-1", "flair_key": "cast", "flair_label": "Cast", "post_count": 1},
            ]
        return []

    def fake_fetch_one(query: str, params: list[Any] | tuple[Any, ...] | None = None):
        captured["one_calls"] += 1
        return {"total_posts": 4, "tracked_total_posts": 3}

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(repo.pg, "fetch_one", fake_fetch_one)

    payload, query_count = repo.get_stored_post_counts_by_community_and_season("community-1", "season-1")

    assert query_count == 4
    assert captured["all_calls"][-3][0].count("%s") == len(captured["all_calls"][-3][1]) == 2
    assert captured["all_calls"][-2][0].count("%s") == len(captured["all_calls"][-2][1]) == 2
    assert captured["all_calls"][-1][0].count("%s") == len(captured["all_calls"][-1][1]) == 4
    assert payload["counts"] == {"episode-1": 3, "period-preseason": 1}
    assert payload["total_posts"] == 4
    assert payload["tracked_total_posts"] == 3
    assert payload["tracked_flair_counts"][0]["flair_key"] == "cast"
    assert payload["pending_tracked_flair_counts"][0]["container_key"] == "episode-1"
    assert payload["flair_counts"] == [{"flair": "Cast", "post_count": 2}]


def test_stored_window_posts_contract(monkeypatch) -> None:
    captured: dict[str, Any] = {"calls": []}

    def fake_fetch_one(query: str, params: list[Any] | tuple[Any, ...] | None = None):
        captured["calls"].append(("one", query, list(params or [])))
        return {"total_count": 2}

    def fake_fetch_all(query: str, params: list[Any] | tuple[Any, ...] | None = None):
        captured["calls"].append(("all", query, list(params or [])))
        return [
            {
                "reddit_post_id": "post-1",
                "title": "Alpha",
                "text": "Body",
                "url": "https://reddit.com/r/show/comments/abc123",
                "permalink": "https://reddit.com/r/show/comments/abc123",
                "author": "author",
                "score": 12,
                "num_comments": 4,
                "posted_at": "2026-03-26T00:00:00Z",
                "link_flair_text": "Cast",
                "is_show_match": True,
                "match_score": 41,
            }
        ]

    monkeypatch.setattr(repo.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.get_stored_window_posts_by_community_and_season(
        "community-1",
        "season-1",
        "episode-1",
        page=1,
        per_page=50,
    )

    assert query_count == 2
    assert payload["pagination"] == {"page": 1, "per_page": 50, "total_count": 2}
    assert payload["posts"][0]["match_type"] == "flair"
    assert payload["posts"][0]["passes_flair_filter"] is True


def test_resolve_post_detail_contract(monkeypatch) -> None:
    def fake_fetch_all(query: str, params: list[Any] | tuple[Any, ...] | None = None):
        return [
            {
                "reddit_post_id": "post-1",
                "title": "My Title",
                "author": "BravoFan",
                "posted_at": "2026-03-26T00:00:00Z",
                "url": "https://reddit.com/r/show/comments/abc123",
                "permalink": "https://reddit.com/r/show/comments/abc123",
            },
            {
                "reddit_post_id": "post-2",
                "title": "My Title",
                "author": "BravoFan",
                "posted_at": "2026-03-26T01:00:00Z",
                "url": "https://reddit.com/r/show/comments/def456",
                "permalink": "https://reddit.com/r/show/comments/def456",
            },
        ]

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.resolve_reddit_post_detail_by_slug(
        community_id="community-1",
        season_id="season-1",
        container_key="episode-1",
        title_slug="my-title",
        author_slug="bravofan",
    )

    assert query_count == 1
    assert payload is None

    payload, query_count = repo.resolve_reddit_post_detail_by_slug(
        community_id="community-1",
        season_id="season-1",
        container_key="episode-1",
        reddit_post_id="post-1",
    )

    assert query_count == 1
    assert payload["reddit_post_id"] == "post-1"
    assert payload["detail_slug"].startswith("my-title--u-bravofan")


def test_post_detail_contract(monkeypatch) -> None:
    fetch_one_calls = {"count": 0}

    def fake_fetch_one(query: str, params=None):
        fetch_one_calls["count"] += 1
        if "from social.reddit_posts p" in query.lower():
            return {
                "reddit_post_id": "post-1",
                "subreddit": "BravoRealHousewives",
                "title": "Episode Thread",
                "selftext": "Body",
                "url": "https://reddit.com/r/show/comments/abc123",
                "permalink": "https://reddit.com/r/show/comments/abc123",
                "author": "BravoFan",
                "score": 12,
                "num_comments": 4,
                "posted_at": "2026-03-26T00:00:00Z",
                "link_flair_text": "Cast",
                "canonical_flair_key": "cast",
                "upvote_ratio": 0.98,
                "is_self": True,
                "post_type": "self",
                "thumbnail": None,
                "content_url": None,
                "is_nsfw": False,
                "is_spoiler": False,
                "author_flair_text": None,
                "detail_scraped_at": "2026-03-26T00:10:00Z",
                "source_sorts": ["hot"],
                "media_metadata": {"images": 1},
                "poll_data": {},
            }
        if "from social.reddit_comments" in query.lower():
            return {
                "total_comments": 2,
                "top_level_comments": 1,
                "earliest_comment_at": "2026-03-26T00:05:00Z",
                "latest_comment_at": "2026-03-26T00:06:00Z",
            }
        return {
            "total_media": 1,
            "mirrored_media": 1,
            "pending_media": 0,
            "failed_media": 0,
        }

    def fake_fetch_all(query: str, params=None):
        normalized = query.lower()
        if "from social.reddit_period_post_matches" in normalized:
            return [
                {
                    "period_key": "episode-1",
                    "period_start": None,
                    "period_end": None,
                    "is_show_match": True,
                    "passes_flair_filter": True,
                    "match_score": 41,
                    "match_type": "flair",
                    "admin_approved": True,
                    "flair_mode": "analysis",
                    "source_sorts": ["hot"],
                    "matched_terms": ["bravo"],
                    "matched_cast_terms": [],
                    "cross_show_terms": [],
                    "link_flair_text": "Cast",
                    "canonical_flair_key": "cast",
                    "created_at": "2026-03-26T00:00:00Z",
                    "updated_at": "2026-03-26T00:00:00Z",
                }
            ]
        if "from social.reddit_comments" in normalized:
            return [
                {
                    "reddit_comment_id": "c1",
                    "parent_comment_id": None,
                    "author": "user",
                    "body": "Great episode",
                    "score": 5,
                    "depth": 0,
                    "created_at_utc": "2026-03-26T00:05:00Z",
                    "author_flair_text": None,
                    "is_submitter": False,
                    "controversiality": 0,
                    "ups": 5,
                    "downs": 0,
                    "gildings": {},
                }
            ]
        if "from social.reddit_media_mirrors" in normalized:
            return [
                {
                    "id": "media-1",
                    "reddit_comment_id": None,
                    "source_url": "https://i.redd.it/example.jpg",
                    "media_type": "image",
                    "hosted_url": "https://cdn.example.com/example.jpg",
                    "status": "mirrored",
                    "content_type": "image/jpeg",
                    "size_bytes": 1234,
                    "error_message": None,
                    "created_at": "2026-03-26T00:05:00Z",
                }
            ]
        return [
            {
                "id": "thread-1",
                "community_id": "community-1",
                "reddit_post_id": "post-1",
                "trr_season_id": None,
                "title": "Episode Thread",
            }
        ]

    monkeypatch.setattr(repo.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.get_reddit_post_details_by_community_and_season(
        community_id="community-1",
        season_id="season-1",
        reddit_post_id="post-1",
        comments_limit=100,
    )

    assert query_count == 7
    assert payload is not None
    assert payload["reddit_post_id"] == "post-1"
    assert payload["comment_summary"]["total_comments"] == 2
    assert payload["media_summary"]["mirrored_media"] == 1
    assert payload["comments"][0]["gildings"] == {}


def test_get_reddit_post_details_by_community_and_season_contract(monkeypatch) -> None:
    def fake_fetch_one(query: str, params=None):
        normalized = query.lower()
        if "from social.reddit_posts" in normalized:
            return {
                "reddit_post_id": "post-1",
                "subreddit": "BravoRealHousewives",
                "title": "Episode Thread",
                "selftext": "Body",
                "url": "https://reddit.com/r/show/comments/abc123",
                "permalink": "https://reddit.com/r/show/comments/abc123",
                "author": "BravoFan",
                "score": 12,
                "num_comments": 4,
                "posted_at": "2026-03-26T00:00:00Z",
                "link_flair_text": "Cast",
                "canonical_flair_key": "cast",
                "upvote_ratio": 0.9,
                "is_self": True,
                "post_type": "self",
                "thumbnail": None,
                "content_url": None,
                "is_nsfw": False,
                "is_spoiler": False,
                "author_flair_text": None,
                "detail_scraped_at": "2026-03-26T01:00:00Z",
                "source_sorts": ["new"],
                "media_metadata": {"kind": "gallery"},
                "poll_data": None,
            }
        if "from social.reddit_comments" in normalized:
            return {
                "total_comments": 4,
                "top_level_comments": 3,
                "earliest_comment_at": "2026-03-26T00:10:00Z",
                "latest_comment_at": "2026-03-26T00:20:00Z",
            }
        return {
            "total_media": 2,
            "mirrored_media": 1,
            "pending_media": 1,
            "failed_media": 0,
        }

    def fake_fetch_all(query: str, params=None):
        normalized = query.lower()
        if "from social.reddit_period_post_matches" in normalized:
            return [
                {
                    "period_key": "episode-1",
                    "match_score": 41,
                    "source_sorts": ["new"],
                    "matched_terms": ["term"],
                    "matched_cast_terms": [],
                    "cross_show_terms": [],
                }
            ]
        if "from social.reddit_comments" in normalized:
            return [{"reddit_comment_id": "c1", "score": 5, "depth": 0, "gildings": {"gid_1": 1}}]
        if "from social.reddit_media_mirrors" in normalized:
            return [{"id": "media-1", "size_bytes": 1024}]
        return [{"id": "thread-1", "reddit_post_id": "post-1"}]

    monkeypatch.setattr(repo.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.get_reddit_post_details_by_community_and_season(
        community_id="community-1",
        season_id="season-1",
        reddit_post_id="post-1",
        comments_limit=100,
    )

    assert query_count == 7
    assert payload["reddit_post_id"] == "post-1"
    assert payload["comment_summary"]["reply_comments"] == 1
    assert payload["media_summary"]["mirrored_media"] == 1
    assert payload["assigned_threads"][0]["id"] == "thread-1"
