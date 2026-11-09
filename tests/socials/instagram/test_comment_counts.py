"""Observed counters stay separate from canonical saved inventory and progress."""

from datetime import UTC, datetime

import pytest

from trr_backend.socials.instagram import comment_counts as counts


def detail(raw):
    from types import SimpleNamespace

    post = SimpleNamespace()
    counts.capture_detail_observations(post, raw, observed_at="2026-01-01T00:00:00Z")
    return {
        **raw,
        counts.OBSERVATION_KEY: post.comment_count_observations,
        "comment_count_observation_conflicts": post.comment_count_observation_conflicts,
    }


@pytest.mark.parametrize(
    "raw,expected",
    [
        ({}, None),
        ({"comments": 0}, None),
        ({"comment_count": 0}, 0),
        ({"comment_count": "7"}, 7),
        ({"comment_count": -1}, None),
        ({"comment_count": False}, None),
        ({"comment_count": 1.5}, None),
        ({"edge_media_to_comment": {"count": 9}}, 9),
        ({"comments": 0, "detail_snapshot": {"comments_present": True}}, None),
    ],
)
def test_only_observed_source_counts_are_known(raw, expected):
    contract = counts.build_comment_counts({"comments_count": 999, "raw_data": detail(raw)})
    assert contract["post_reported_count"] == expected
    assert contract["instagram_saved_count"] is None
    assert contract["post_minus_saved_difference"] is None
    assert contract["inventory_status"] == "unavailable"


def test_omission_preserves_timestamp_and_conflicts_retain_previous_evidence():
    old = counts.merge_observations(detail({"comment_count": 5}), {}, observed_at="2026-01-01T00:00:00Z")
    omitted = counts.merge_observations({}, old, observed_at="2026-02-01T00:00:00Z")
    observation = omitted[counts.OBSERVATION_KEY]["post_reported_count"]
    assert observation["value"] == 5
    assert observation["observed_at"] == "2026-01-01T00:00:00Z"
    assert observation["observed_in_latest_response"] is False
    changed = counts.merge_observations(detail({"comment_count": 0}), omitted, observed_at="2026-03-01T00:00:00Z")
    assert changed[counts.OBSERVATION_KEY]["post_reported_count"]["value"] == 0
    assert changed[counts.OBSERVATION_KEY]["post_reported_count"]["observed_in_latest_response"] is True
    assert changed["comment_count_observation_conflicts"][0]["value"] == 5


def test_facebook_zero_is_observed_threads_stays_unknown_and_difference_is_signed():
    inventory = dict.fromkeys(counts.INVENTORY_FIELDS, 0) | {"instagram_saved_count": 8}
    contract = counts.build_comment_counts(
        {"raw_data": detail({"comment_count": 3, "fb_comment_count": 0, "threads_comment_count": 14})}, inventory
    )
    assert contract["facebook_count_observed_on_instagram"] == 0
    assert contract["threads_count_observed_on_instagram"] is None
    assert contract["post_minus_saved_difference"] == -5
    assert contract["observations"]["facebook_count_observed_on_instagram"]["source_field"] == "fb_comment_count"
    assert counts.InstagramCommentCounts.model_validate(contract)


def test_canonical_inventory_success_zero_and_unavailable_are_distinct(monkeypatch):
    captured = []

    def query(sql, params, **kwargs):
        captured.append((sql, params))
        return [
            {
                "shortcode": "a",
                "instagram_saved_count": 3,
                "instagram_saved_top_level_count": 1,
                "instagram_saved_reply_count": 1,
                "instagram_saved_unclassified_count": 1,
                "instagram_marked_missing_count": 2,
            }
        ]

    monkeypatch.setattr(counts, "_query", query)
    inventory = counts.load_inventory(["a", "a", "b"])
    assert inventory["a"]["instagram_saved_reply_count"] == 1
    assert inventory["b"]["instagram_saved_count"] == 0
    sql, params = captured[0]
    assert params == [["a", "b"]]
    assert "count(*) filter" in sql
    assert "c.*" not in sql
    assert "to_jsonb" not in sql
    assert "<> 'fb_crosspost'" in sql
    assert "is_missing is not true" in sql

    def failed(*args, **kwargs):
        raise RuntimeError("inventory offline")

    monkeypatch.setattr(counts, "_query", failed)
    assert counts.load_inventory(["a"]) is None


def test_summary_known_subtotals_do_not_convert_unknown_posts_to_zero():
    inventory = dict.fromkeys(counts.INVENTORY_FIELDS, 0)
    contracts = {
        "a": counts.build_comment_counts({"raw_data": detail({"comment_count": 0})}, inventory),
        "b": counts.build_comment_counts({"raw_data": {"fb_comment_count": 4}}, inventory),
    }
    summary = counts.summarize_comment_counts(contracts)
    assert summary["posts_count"] == 2
    assert summary["post_reported_known_subtotal"] == 0
    assert summary["post_reported_known_posts_count"] == 1
    assert summary["post_reported_unknown_posts_count"] == 1
    assert summary["facebook_known_subtotal"] == 4
    assert summary["facebook_unknown_posts_count"] == 1
    assert summary["threads_unknown_posts_count"] == 2
    assert counts.InstagramCommentCountsSummary.model_validate(summary)


def test_source_observation_timestamp_does_not_use_post_timestamp():
    observed = counts.observations_from_raw(
        {"comment_count": 1}, observed_at=datetime(2026, 1, 1, tzinfo=UTC), source_surface="instagram_post_details"
    )
    assert observed["post_reported_count"]["observed_at"] == "2026-01-01T00:00:00+00:00"


def test_new_sql_filters_use_observation_presence_and_preserve_old_semantics():
    import trr_backend.socials.social_season_analytics_impl as repo

    args = {"reported_comments_sql": "d.comments_count", "saved_comments_sql": "cc.saved_comments"}
    assert repo._comments_only_profile_filter_where_sql(comment_filter="not_commentable", **args) == (
        "greatest(coalesce(d.comments_count, 0), 0) = 0"
    )
    unknown = repo._comments_only_profile_filter_where_sql(comment_filter="reported_unknown", **args)
    assert unknown.endswith("is null")
    assert "comment_count_observations,post_reported_count" in unknown
    difference = repo._comments_only_profile_filter_where_sql(comment_filter="count_difference", **args)
    assert "count(*)" in difference
    assert "<> 'fb_crosspost'" in difference
    for direction in ("asc", "desc"):
        order = repo._comments_only_profile_order_by_sql(sort_by="count_difference", sort_dir=direction)
        assert f"{direction} nulls last" in order


@pytest.mark.parametrize(
    "direction,expected", [("asc", ["negative", "positive", "unknown"]), ("desc", ["positive", "negative", "unknown"])]
)
def test_signed_difference_sort_keeps_unknown_last(direction, expected):
    import trr_backend.socials.social_season_analytics_impl as repo

    rows = [
        {"source_id": name, "comment_counts": {"post_minus_saved_difference": value}}
        for name, value in [("unknown", None), ("negative", -2), ("positive", 3)]
    ]
    assert [
        row["source_id"]
        for row in repo._sort_social_account_profile_post_rows(rows, sort_by="count_difference", sort_dir=direction)
    ] == expected


@pytest.mark.parametrize(
    "stored",
    [
        {"value": "bad", "status": "observed"},
        {"value": 8, "status": "unknown"},
        {"value": True, "status": "observed"},
        {"value": 8, "status": "observed", "source": "unverified"},
    ],
)
def test_malformed_stored_observation_remains_consistently_unknown(stored):
    contract = counts.build_comment_counts({"raw_data": {counts.OBSERVATION_KEY: {"post_reported_count": stored}}})
    assert contract["post_reported_count"] is None
    assert contract["observations"]["post_reported_count"]["value"] is None
    assert contract["observations"]["post_reported_count"]["status"] == "unknown"


def test_same_response_conflicts_remain_inspectable_and_parent_edges_are_not_totals():
    raw = {"comment_count": 0, "commentsCount": 7, "edge_media_to_comment": {"count": 9}}
    merged = counts.merge_observations(detail(raw), {}, observed_at="2026-01-01T00:00:00Z")
    contract = counts.build_comment_counts({"raw_data": merged})
    assert contract["post_reported_count"] == 0
    assert {item["value"] for item in contract["observation_conflicts"]} == {7, 9}
    assert (
        counts.build_comment_counts(
            {"raw_data": {"edge_media_to_parent_comment": {"count": 12}, "edge_media_preview_comment": {"count": 3}}}
        )["post_reported_count"]
        is None
    )


def test_timeline_counts_and_legacy_presence_marker_do_not_prove_post_details():
    for raw in (
        {"comment_count": 8},
        {"commentsCount": 8},
        {"comments": 8, "detail_snapshot": {"comments_present": True}},
    ):
        assert counts.build_comment_counts({"raw_data": raw})["post_reported_count"] is None


def test_catalog_refresh_preserves_prior_detail_observation_without_changing_legacy_metric():
    from types import SimpleNamespace

    from trr_backend.socials.instagram import catalog_ingest as catalog

    prior = detail({"comment_count": 6})
    post = SimpleNamespace(
        shortcode="COUNT123", comments=99, taken_at=1700000000, to_dict=lambda: {"comment_count": 99}
    )
    payload = catalog._shared_catalog_instagram_post_payload(
        run_id=None, account_handle="bravotv", post=post, existing_row={"raw_data": prior}
    )
    assert payload["comments_count"] == 99
    contract = counts.build_comment_counts(payload)
    assert contract["post_reported_count"] == 6
    assert contract["observations"]["post_reported_count"]["observed_in_latest_response"] is False
    assert contract["observations"]["post_reported_count"]["observed_at"] == "2026-01-01T00:00:00Z"


def test_summary_cards_use_source_counts_and_exact_inventory():
    empty = dict.fromkeys(counts.INVENTORY_FIELDS, 0)
    rows = {
        "positive": counts.build_comment_counts({"raw_data": detail({"comment_count": 4})}, empty),
        "zero": counts.build_comment_counts({"raw_data": detail({"comment_count": 0})}, empty),
        "unknown": counts.build_comment_counts({}, empty),
    }
    summary = counts.summarize_comment_counts(rows)
    assert summary["posts_with_reported_comments_count"] == 1
    assert summary["posts_reporting_zero_comments_count"] == 1
    assert summary["reported_count_unknown_posts_count"] == 1
    assert summary["posts_with_reported_comments_but_no_saved_records_count"] == 1
    rows["positive"] = counts.build_comment_counts({"raw_data": detail({"comment_count": 4})})
    assert counts.summarize_comment_counts(rows)["posts_with_reported_comments_but_no_saved_records_count"] is None


def test_profile_summary_projects_only_count_evidence_and_deduplicates_posts(monkeypatch):
    seen = []

    def query(sql, params, **kwargs):
        seen.append((sql, params))
        row = {"source_id": "same", "raw_data": detail({"comment_count": 2})}
        return [row, row]

    monkeypatch.setattr(counts, "_query", query)
    monkeypatch.setattr(counts, "load_inventory", lambda *a, **k: {"same": dict.fromkeys(counts.INVENTORY_FIELDS, 0)})
    summary = counts.profile_summary("bravotv")
    assert summary["posts_count"] == 1
    assert summary["post_reported_known_subtotal"] == 2
    assert "jsonb_path_query_array" in seen[0][0]
    assert "p.*" not in seen[0][0]
    assert seen[0][1] == ["bravotv"] * 5


def test_unknown_page_selects_keys_without_counting_unknown_post_inventory(monkeypatch):
    seen = []

    def query(sql, params, **kwargs):
        seen.append((sql, params))
        return [{"source_id": "only-page-key", "position": 1, "total": 18490}]

    monkeypatch.setattr(counts, "_query", query)
    keys, total = counts.profile_post_keys(
        "bravotv",
        page=2,
        page_size=25,
        comment_filter="reported_unknown",
        sort_by="count_difference",
        sort_dir="desc",
    )
    assert keys == ["only-page-key"] and total == 18490
    sql, params = seen[0]
    assert "reported_count is null" in sql
    assert "case when e.reported_count is not null then" in sql
    assert "limit %s offset %s" in sql
    assert params == ["bravotv"] * 5 + [25, 25]
    assert "media_urls" not in sql


def test_profile_route_hydrates_only_selected_keys_and_preserves_database_order(monkeypatch):
    from contextlib import nullcontext

    from trr_backend.socials.read_models.account_profile import common

    common._sync_core_overrides()
    monkeypatch.setattr(common, "_sync_core_overrides", lambda: None)
    monkeypatch.setattr(common, "_social_account_profile_summary_connection", lambda *a: nullcontext(object()))
    monkeypatch.setattr(common, "_assert_social_account_profile_exists", lambda *a, **k: [])
    monkeypatch.setattr(common, "_build_social_account_profile_known_handle_identity_index", lambda *a: {})
    monkeypatch.setattr(common, "_log_social_profile_perf", lambda **k: None)
    monkeypatch.setattr(counts, "profile_post_keys", lambda *a, **k: (["b", "a"], 18490))
    contract = counts.build_comment_counts({}, dict.fromkeys(counts.INVENTORY_FIELDS, 0))
    calls = []

    def hydrate(account, **kwargs):
        calls.append(kwargs)
        return [{"source_id": key, "comment_counts": contract} for key in ("a", "b")]

    monkeypatch.setattr(common, "_instagram_social_account_profile_dataset_rows", hydrate)
    monkeypatch.setattr(common, "_social_account_profile_post_item", lambda platform, row, **k: dict(row))
    payload = common.get_social_account_profile_posts(
        "instagram", "bravotv", comments_only=True, comment_filter="reported_unknown", sort_by="count_difference"
    )
    assert [item["source_id"] for item in payload["items"]] == ["b", "a"]
    assert calls[0]["source_ids"] == ["b", "a"]
    assert payload["pagination"]["total"] == 18490


def test_single_pass_projection_keeps_count_evidence_and_excludes_media():
    raw = detail({"comment_count": 0, "fb_comment_count": 0})
    projected = counts._projected_raw(
        [
            {"key": counts.OBSERVATION_KEY, "value": raw[counts.OBSERVATION_KEY]},
            {"key": "fb_comment_count", "value": 0},
            {"key": "media_urls", "value": ["private-unused-media"]},
        ]
    )
    assert "media_urls" not in projected
    contract = counts.build_comment_counts({"raw_data": projected})
    assert contract["post_reported_count"] == 0
    assert contract["facebook_count_observed_on_instagram"] == 0
