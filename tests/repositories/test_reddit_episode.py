from __future__ import annotations

import copy
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import pytest

from trr_backend.repositories import reddit_capture, reddit_episode, reddit_transport
from trr_backend.repositories.reddit_transport import RedditTransportError


class Client:
    def __init__(self, listings: dict[str, Any] | None = None, searches: dict[str, Any] | None = None) -> None:
        self.listings = listings or {}
        self.searches = searches or {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get_json(self, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((path, copy.deepcopy(params)))
        if path.endswith("/search.json"):
            response = self.searches.get((params["q"], params.get("after")), self.searches.get(params["q"], page()))
        else:
            response = self.listings.get(path.rsplit("/", 1)[-1].replace(".json", ""), page())
        if isinstance(response, Exception):
            raise response
        return copy.deepcopy(response)


def post(
    post_id: str = "a", title: str = "RHOSLC Season 6 Episode 1 Live Episode Discussion", **extra: Any
) -> dict[str, Any]:
    return {
        "kind": "t3",
        "data": {
            "id": post_id,
            "title": title,
            "url": f"/r/test/comments/{post_id}",
            "permalink": f"/r/test/comments/{post_id}",
            "selftext": "",
            "author": "AutoModerator",
            "score": 10,
            "num_comments": 20,
            "created_utc": datetime(2026, 10, 21, 1, tzinfo=UTC).timestamp(),
            "link_flair_text": "Salt Lake City",
            **extra,
        },
    }


def page(*posts: dict[str, Any], after: str | None = None) -> dict[str, Any]:
    return {"kind": "Listing", "data": {"children": list(posts), "after": after}}


def payload(**extra: Any) -> dict[str, Any]:
    return {
        "subreddit": "BravoRealHousewives",
        "showName": "The Real Housewives of Salt Lake City",
        "showAliases": ["RHOSLC"],
        "seasonNumber": 6,
        "sortModes": ["new"],
        "episodeTitlePatterns": ["Live Episode Discussion", "Post Episode Discussion", "Weekly Episode Discussion"],
        "episodeRequiredFlairs": ["Salt Lake City"],
        **extra,
    }


def test_listing_candidate_shape_matrix_and_merging_match_app_service() -> None:
    client = Client({"new": page(post()), "hot": page(post(score=15)), "top": page(post("b", num_comments=30))})
    result = reddit_episode.discover_episode(payload(sortModes=["new", "hot", "top"]), client=client)
    assert [row["reddit_post_id"] for row in result["candidates"]] == ["b", "a"]
    candidate = result["candidates"][1]
    assert candidate == {
        "reddit_post_id": "a",
        "title": "RHOSLC Season 6 Episode 1 Live Episode Discussion",
        "text": None,
        "url": "https://www.reddit.com/r/test/comments/a",
        "permalink": "https://www.reddit.com/r/test/comments/a",
        "author": "AutoModerator",
        "score": 15,
        "num_comments": 20,
        "posted_at": "2026-10-21T01:00:00.000Z",
        "link_flair_text": "Salt Lake City",
        "source_sorts": ["new", "hot"],
        "episode_number": 1,
        "discussion_type": "live",
        "match_reasons": [
            "title pattern: Live Episode Discussion",
            "show term: rhoslc",
            "season: 6",
            "episode: 1",
            "type: live",
            "flair: Salt Lake City",
        ],
    }
    row = result["episode_matrix"][0]
    assert row["live"] == {
        "post_count": 2,
        "total_comments": 50,
        "total_upvotes": 25,
        "top_post_id": "b",
        "top_post_url": "https://www.reddit.com/r/test/comments/b",
    }
    assert row["post"]["post_count"] == 0
    assert result["coverage_expected_slots"] == 0
    assert result["coverage_found_slots"] == 0
    assert result["coverage_found_episode_count"] == 1
    assert result["discovery_source_summary"]["listing_count"] == 2
    assert len(client.calls) == 3
    assert client.calls[-1][1]["t"] == "all"


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("RHOSLC S06E01 live thread", (1, "live")),
        ("RHOSLC Season 6 EP 02 Weekly Thread", (2, "weekly")),
        ("RHOSLC season 6 episode 3 Post-Episode Discussion", (3, "post")),
        ("RHOSLC season 6 E04 Post - Episode Discussion", None),
        ("RHOSLC season 6 episode 1 and S06E02 Live Episode Discussion", None),
        ("RHOBH season 6 episode 1 Live Episode Discussion", None),
        ("RHOSLC season 16 episode 1 Live Episode Discussion", None),
        ("RHOSLC season 6 Live Episode Discussion", None),
    ],
)
def test_episode_aliases_season_show_and_ambiguous_titles(title: str, expected: tuple[int, str] | None) -> None:
    result = reddit_episode.discover_episode(payload(), client=Client({"new": page(post(title=title))}))
    actual = [(row["episode_number"], row["discussion_type"]) for row in result["candidates"]]
    assert actual == ([] if expected is None else [expected])


def test_expected_episodes_broad_search_gap_fill_and_zero_matrix_rows() -> None:
    value = payload(seasonEpisodes=[{"episode_number": 1}, {"episode_number": 2}])
    client = Client({"new": page(post(), post("out", title="RHOSLC season 6 episode 3 live thread"))})
    result = reddit_episode.discover_episode(value, client=client)
    assert result["expected_episode_numbers"] == [1, 2]
    assert result["coverage_expected_slots"] == 6
    assert result["coverage_found_slots"] == 1
    assert len(result["coverage_missing_slots"]) == 5
    assert result["episode_matrix"][1]["total_posts"] == 0
    assert result["discovery_source_summary"] == {
        "listing_count": 2,
        "search_count": 0,
        "search_pages_fetched": 8,
        "gap_fill_queries_run": 5,
    }
    queries = [params["q"] for path, params in client.calls if path.endswith("/search.json")]
    assert len(queries) == 8
    assert queries[0].endswith('("season 6" OR "s6") "live episode discussion"')
    assert '"season 6" ("episode 1" OR "e1" OR "s6e1") "post episode discussion"' in queries[3]


def test_search_results_fill_all_expected_slots_and_skip_gap_queries() -> None:
    value = payload(seasonEpisodes=[{"episode_number": 1}])
    client = Client()
    for kind in ("live", "post", "weekly"):
        query = reddit_episode._query(["the real housewives of salt lake city", "rhoslc"], 6, kind)
        client.searches[query] = page(post(kind, title=f"RHOSLC season 6 episode 1 {kind} episode discussion"))
    result = reddit_episode.discover_episode(value, client=client)
    assert result["coverage_found_slots"] == 3
    assert result["coverage_missing_slots"] == []
    assert result["discovery_source_summary"]["gap_fill_queries_run"] == 0
    assert [row["discussion_type"] for row in result["candidates"]] == ["live", "post", "weekly"]


def test_flair_assignment_filters_episode_and_season() -> None:
    value = payload(
        resolvedSeasonId="season-a",
        seasonEpisodes=[{"id": "episode-a", "episode_number": 1}],
        episodeFlairAssignments={"salt lake city": {"season_ids": ["season-a"], "episode_ids": ["episode-a"]}},
    )
    assert len(reddit_episode.discover_episode(value, client=Client({"new": page(post())}))["candidates"]) == 1
    value["episodeFlairAssignments"]["salt lake city"]["episode_ids"] = ["episode-b"]
    assert reddit_episode.discover_episode(value, client=Client({"new": page(post())}))["candidates"] == []
    value["isShowFocused"] = True
    assert len(reddit_episode.discover_episode(value, client=Client({"new": page(post())}))["candidates"]) == 1


def test_show_focused_can_omit_patterns_and_flair_but_still_requires_show_term() -> None:
    value = payload(isShowFocused=True, episodeTitlePatterns=[], episodeRequiredFlairs=["Other"])
    result = reddit_episode.discover_episode(value, client=Client({"new": page(post(link_flair_text=None))}))
    assert len(result["candidates"]) == 1
    assert result["candidates"][0]["match_reasons"][0] == "title pattern: live episode discussion"


def test_period_window_boundaries_are_inclusive() -> None:
    value = payload(periodStart="2026-10-21T01:00:00Z", periodEnd="2026-10-21T01:00:00Z")
    result = reddit_episode.discover_episode(value, client=Client({"new": page(post(), post("old", created_utc=1))}))
    assert [row["reddit_post_id"] for row in result["candidates"]] == ["a"]


def test_rhoslc_flair_normalization_and_removed_whitney_marker() -> None:
    value = payload(subreddit="realhousewivesofslc", episodeRequiredFlairs=["SALT LAKE CITY"])
    result = reddit_episode.discover_episode(
        value,
        client=Client(
            {
                "new": page(
                    post(link_flair_text="❄️ :snow: SALT LAKE CITY ❄️"),
                    post("removed", link_flair_text=":whitney: SALT LAKE CITY"),
                )
            }
        ),
    )
    assert [row["reddit_post_id"] for row in result["candidates"]] == ["a"]
    assert result["candidates"][0]["link_flair_text"] == "Salt Lake City"


def test_rules_seed_bravo_patterns_and_salt_lake_city_flair_only_when_unset() -> None:
    rules = reddit_episode.resolve_episode_rules(payload(analysisAllFlairs=[]))
    assert rules["effectiveRequiredFlairs"] == ["Salt Lake City"]
    assert rules["autoSeededRequiredFlairs"] is True
    assert len(rules["effectiveEpisodeTitlePatterns"]) == 3
    assert reddit_episode.resolve_episode_rules(payload(analysisAllFlairs=["Custom"]))["effectiveRequiredFlairs"] == [
        "Custom"
    ]
    assert reddit_episode.resolve_episode_rules(payload(isShowFocused=True))["autoSeededRequiredFlairs"] is False


@pytest.mark.parametrize(
    "extra",
    [
        {"showName": "", "showAliases": []},
        {"episodeTitlePatterns": []},
        {"periodStart": "invalid"},
        {"periodEnd": "invalid"},
        {"periodStart": "2026-02-02", "periodEnd": "2026-02-01"},
        {"subreddit": ""},
        {"sortModes": ["invalid"]},
    ],
)
def test_invalid_inputs_fail_before_http(extra: dict[str, Any]) -> None:
    client = Client()
    with pytest.raises(RedditTransportError) as error:
        reddit_episode.discover_episode(payload(**extra), client=client)
    assert error.value.status == 400
    assert client.calls == []


def test_partial_sort_failure_preserves_successful_candidates_and_diagnostics() -> None:
    result = reddit_episode.discover_episode(
        payload(sortModes=["new", "hot"]),
        client=Client({"new": page(post()), "hot": RedditTransportError("limited", status=429)}),
    )
    assert result["successful_sorts"] == ["new"]
    assert result["failed_sorts"] == ["hot"]
    assert result["rate_limited_sorts"] == ["hot"]
    assert len(result["candidates"]) == 1
    assert result["collection_complete"] is False


def test_initial_failure_exposes_checkpoint_and_resumes_without_refetching_successes() -> None:
    value = payload(sortModes=["new", "hot"], seasonEpisodes=[{"episode_number": 1}])
    failing = Client({"new": page(post()), "hot": page()}, searches={})
    failing.searches = {
        reddit_episode._query(["the real housewives of salt lake city", "rhoslc"], 6, "live"): RedditTransportError(
            "bad gateway", status=502
        )
    }
    checkpoints: list[dict[str, Any]] = []
    with pytest.raises(RedditTransportError) as error:
        reddit_episode.discover_episode(value, client=failing, on_checkpoint=checkpoints.append)
    assert error.value.checkpoint == checkpoints[-1]
    preserved = copy.deepcopy(checkpoints[-1])
    resumed = Client()
    result = reddit_episode.discover_episode(value, client=resumed, checkpoint=checkpoints[-1])
    assert all(path.endswith("/search.json") for path, _params in resumed.calls)
    assert result["candidates"][0]["reddit_post_id"] == "a"
    assert checkpoints[-1] == preserved


def test_all_sort_non_rate_limit_failures_raise_with_checkpoint() -> None:
    with pytest.raises(RedditTransportError) as error:
        reddit_episode.discover_episode(
            payload(), client=Client({"new": RedditTransportError("not found", status=404)})
        )
    assert error.value.status == 404
    assert error.value.checkpoint["queries"]["listing:new"]["error"]["status"] == 404


def test_capped_query_cursor_is_saved_and_never_reported_complete() -> None:
    value = payload(seasonEpisodes=[{"episode_number": 1}])
    query = reddit_episode._query(["the real housewives of salt lake city", "rhoslc"], 6, "live")
    client = Client(
        searches={
            (query, None): page(post(), after="t3_next1"),
            (query, "t3_next1"): page(after="t3_next2"),
            (query, "t3_next2"): page(after="t3_next3"),
        }
    )
    result = reddit_episode.discover_episode(value, client=client)
    assert result["collection_complete"] is False
    assert "search:" + query in result["unresolved_queries"]
    assert result["checkpoint"]["queries"]["search:" + query]["after"] == "t3_next3"


def test_malformed_search_does_not_erase_saved_listing_results() -> None:
    value = payload(seasonEpisodes=[{"episode_number": 1}])
    query = reddit_episode._query(["the real housewives of salt lake city", "rhoslc"], 6, "live")
    with pytest.raises(RedditTransportError) as error:
        reddit_episode.discover_episode(value, client=Client({"new": page(post())}, {query: {"data": {}}}))
    assert error.value.checkpoint["queries"]["listing:new"]["rows"][0]["reddit_post_id"] == "a"


def test_checkpoint_wrong_config_is_rejected_before_http() -> None:
    result = reddit_episode.discover_episode(payload(), client=Client())
    client = Client()
    with pytest.raises(RedditTransportError, match="does not match"):
        reddit_episode.discover_episode(payload(seasonNumber=7), client=client, checkpoint=result["checkpoint"])
    assert client.calls == []


@pytest.mark.parametrize(
    ("extra", "eligible", "reason"),
    [
        ({}, True, "auto_saved_success"),
        ({"author": "another"}, False, "author_not_automoderator"),
        ({"title": "Live Thread"}, False, "title_missing_episode_discussion"),
        ({"postedAt": None}, False, "missing_post_timestamp"),
        ({"postedAt": "invalid"}, False, "invalid_post_timestamp"),
        ({"episodeAirDateKey": None}, False, "missing_episode_air_date"),
        ({"postedAt": "2026-10-22T01:00:00Z"}, False, "posted_date_mismatch"),
        ({"postedAt": "2026-10-22T01:00:00Z", "discussionType": "weekly"}, True, "auto_saved_success"),
    ],
)
def test_sync_gate_preserves_author_title_and_new_york_date_rules(
    extra: dict[str, Any], eligible: bool, reason: str
) -> None:
    value = {
        "author": "AutoModerator",
        "title": "Live Episode Discussion",
        "postedAt": "2026-10-21T01:00:00Z",
        "episodeAirDateKey": "2026-10-20",
        "discussionType": "live",
        "episodeNumber": 1,
        "seasonNumber": 6,
        **extra,
    }
    result = reddit_episode.evaluate_sync_eligibility(value)
    assert result["eligible"] is eligible
    assert result["reason_code"] == reason


def test_default_transport_binding_uses_shared_identity_and_requires_oauth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDDIT_CLIENT_ID", "fake-client")
    events: list[Any] = []

    @contextmanager
    def guard(identity: str, lane: str):
        events.append((identity, lane))
        yield

    monkeypatch.setattr(reddit_capture, "request_guard", guard)
    monkeypatch.setattr(reddit_capture, "observe_response", lambda *args: events.append(args))
    client = reddit_transport.build_client()
    assert client.require_oauth is True
    with client.request_guard("morechildren"):
        pass
    import hashlib

    assert events == [(hashlib.sha256(b"fake-client").hexdigest(), "morechildren")]


def test_resume_keeps_saved_search_rows_when_recovered_sort_fills_all_slots() -> None:
    value = payload(sortModes=["new", "hot"], seasonEpisodes=[{"episode_number": 1}])
    first = reddit_episode.discover_episode(
        value,
        client=Client(
            {"new": page(), "hot": RedditTransportError("limited", status=429)},
            {
                reddit_episode._query(["the real housewives of salt lake city", "rhoslc"], 6, "live"): page(
                    post("saved-search")
                )
            },
        ),
    )
    recovered = Client(
        {
            "hot": page(
                *(
                    post(kind, title=f"RHOSLC season 6 episode 1 {kind} episode discussion")
                    for kind in ("live", "post", "weekly")
                )
            )
        }
    )
    result = reddit_episode.discover_episode(value, client=recovered, checkpoint=first["checkpoint"])
    assert {row["reddit_post_id"] for row in result["candidates"]} == {"saved-search", "live", "post", "weekly"}
    assert (
        result["discovery_source_summary"]["search_pages_fetched"]
        == first["discovery_source_summary"]["search_pages_fetched"]
    )


def test_non_string_post_fields_are_explicit_malformed_error() -> None:
    with pytest.raises(RedditTransportError, match="post fields"):
        reddit_episode.discover_episode(payload(), client=Client({"new": page(post(url=123))}))
