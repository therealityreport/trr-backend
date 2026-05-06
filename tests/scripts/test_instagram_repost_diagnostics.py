from __future__ import annotations

from trr_backend.socials.instagram import repost_diagnostics as subject


def test_classify_raw_data_source_shape_uses_expected_key_fingerprints() -> None:
    assert (
        subject.classify_raw_data_source_shape(
            {
                "pk": "3885259576224942959",
                "code": "DXrNv_lEotv",
                "caption": {"text": "commissions are open!"},
                "user": {"username": "jographicss"},
                "media_type": 2,
                "original_width": 1440,
                "original_height": 1800,
                "can_viewer_reshare": True,
                "repostCount": 7,
            }
        )
        == "xdt-like"
    )
    assert (
        subject.classify_raw_data_source_shape(
            {
                "items": [
                    {
                        "pk": "3885259576224942959",
                        "code": "DXrNv_lEotv",
                        "image_versions2": {"candidates": []},
                    }
                ],
                "status": "ok",
            }
        )
        == "v1-info-like"
    )
    assert (
        subject.classify_raw_data_source_shape(
            {
                "url": "https://www.instagram.com/p/DXrNv_lEotv/",
                "og:image": "https://cdn.test/thumb.jpg",
                "json_ld": {"@type": "ImageObject"},
            }
        )
        == "permalink-like"
    )
    assert (
        subject.classify_raw_data_source_shape(
            {
                "status": "ok",
                "comment_filter_param": "headload",
                "has_more_comments": False,
                "has_more_headload_comments": True,
                "next_min_id": "cursor",
                "comments": [{"pk": "comment-1", "text": "Thin comment header"}],
            }
        )
        == "comments-header-like"
    )
    assert subject.classify_raw_data_source_shape({"unexpected": True}) == "unknown"


def test_detect_repost_alias_reports_required_alias_buckets() -> None:
    assert subject.detect_repost_alias({"media_repost_count": 1}) == "media_repost_count"
    assert subject.detect_repost_alias({"repostCount": 2}) == "repostCount"
    assert subject.detect_repost_alias({"reshare_count": 3}) == "reshareCount"
    assert subject.detect_repost_alias({"nested": {"shareCount": 4}}) == "shareCount"
    assert subject.detect_repost_alias({"like_count": 5}) == "source_absent"


def test_summarize_repost_diagnostics_builds_coverage_histogram_aliases_and_thin_samples() -> None:
    rows = [
        {
            "id": "row-1",
            "shortcode": "XDT1",
            "source_account": "bravotv",
            "username": "bravotv",
            "media_repost_count": 0,
            "raw_data": {
                "pk": "1",
                "code": "XDT1",
                "original_width": 1080,
                "original_height": 1350,
                "can_viewer_reshare": True,
                "repostCount": 12,
            },
        },
        {
            "id": "row-2",
            "shortcode": "V1A",
            "source_account": "bravotv",
            "username": "bravotv",
            "media_repost_count": None,
            "raw_data": {"items": [{"pk": "2", "code": "V1A", "media_repost_count": 9}]},
        },
        {
            "id": "row-3",
            "shortcode": "HEAD",
            "source_account": "bravotv",
            "username": "bravotv",
            "media_repost_count": None,
            "raw_data": {
                "status": "ok",
                "comment_filter_param": "headload",
                "has_more_comments": False,
                "comments": [],
            },
        },
    ]

    payload = subject.summarize_repost_diagnostics(rows, sample_limit=1)

    assert payload["coverage"] == {
        "total_instagram_posts": 3,
        "posts_with_media_repost_count": 1,
        "percent_populated": 33.3,
    }
    assert payload["source_shape_histogram"] == [
        {"source_shape": "xdt-like", "rows": 1},
        {"source_shape": "v1-info-like", "rows": 1},
        {"source_shape": "comments-header-like", "rows": 1},
    ]
    assert payload["repost_alias_counters"] == {
        "media_repost_count": 1,
        "repostCount": 1,
        "reshareCount": 0,
        "shareCount": 0,
        "source_absent": 1,
    }
    assert payload["thin_source_repost_gaps"] == {
        "comments_header_like_without_media_repost_count": 1,
        "sample_limit": 1,
        "samples": [
            {
                "id": "row-3",
                "shortcode": "HEAD",
                "source_account": "bravotv",
                "username": "bravotv",
            }
        ],
    }


def test_get_repost_coverage_returns_total_populated_and_percent(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_all(query, params, *, conn=None, pool_name="default"):
        captured["query"] = query
        captured["params"] = params
        captured["conn"] = conn
        captured["pool_name"] = pool_name
        return [
            {
                "total_instagram_posts": 4,
                "posts_with_media_repost_count": 3,
                "percent_populated": "75.0",
            }
        ]

    monkeypatch.setattr(subject.pg, "fetch_all", fake_fetch_all)

    payload = subject.get_repost_coverage(pool_name="social_profile", statement_timeout_ms=0)

    assert payload == {
        "total_instagram_posts": 4,
        "posts_with_media_repost_count": 3,
        "percent_populated": 75.0,
    }
    normalized_sql = " ".join(str(captured["query"]).lower().split())
    assert "from social.instagram_posts" in normalized_sql
    assert "media_repost_count is not null" in normalized_sql
    assert captured["params"] == []
    assert captured["pool_name"] == "social_profile"


def test_get_repost_alias_counters_returns_required_per_source_alias_shape(monkeypatch) -> None:
    def fake_fetch_all(query, params, *, conn=None, pool_name="default"):
        del params, conn, pool_name
        normalized_sql = " ".join(query.lower().split())
        assert "media_repost_count" in normalized_sql
        assert "repostcount" in normalized_sql
        assert "resharecount" in normalized_sql
        assert "sharecount" in normalized_sql
        assert "source_absent" in normalized_sql
        return [
            {"source_shape": "xdt-like", "repost_alias": "repostCount", "rows": 2},
            {"source_shape": "v1-info-like", "repost_alias": "media_repost_count", "rows": 1},
            {"source_shape": "comments-header-like", "repost_alias": "source_absent", "rows": 3},
        ]

    monkeypatch.setattr(subject.pg, "fetch_all", fake_fetch_all)

    payload = subject.get_repost_alias_counters(statement_timeout_ms=0)

    assert payload == {
        "totals": {
            "media_repost_count": 1,
            "repostCount": 2,
            "reshareCount": 0,
            "shareCount": 0,
            "source_absent": 3,
        },
        "by_source_shape": {
            "comments-header-like": {
                "media_repost_count": 0,
                "repostCount": 0,
                "reshareCount": 0,
                "shareCount": 0,
                "source_absent": 3,
            },
            "v1-info-like": {
                "media_repost_count": 1,
                "repostCount": 0,
                "reshareCount": 0,
                "shareCount": 0,
                "source_absent": 0,
            },
            "xdt-like": {
                "media_repost_count": 0,
                "repostCount": 2,
                "reshareCount": 0,
                "shareCount": 0,
                "source_absent": 0,
            },
        },
    }


def test_build_repost_diagnostics_report_is_read_only_and_bounded(monkeypatch) -> None:
    monkeypatch.setattr(
        subject,
        "get_repost_coverage",
        lambda **kwargs: {
            "kwargs": kwargs,
            "total_instagram_posts": 1,
            "posts_with_media_repost_count": 0,
            "percent_populated": 0.0,
        },
    )
    monkeypatch.setattr(subject, "get_source_shape_histogram", lambda **kwargs: [{"kwargs": kwargs}])
    monkeypatch.setattr(subject, "get_repost_alias_counters", lambda **kwargs: {"kwargs": kwargs})
    monkeypatch.setattr(
        subject,
        "get_thin_source_repost_gaps",
        lambda **kwargs: {"kwargs": kwargs, "comments_header_like_without_media_repost_count": 0},
    )

    payload = subject.build_repost_diagnostics_report(sample_limit=999, statement_timeout_ms=999999)

    assert payload["mode"] == "read_only"
    assert payload["write_repair"] == {
        "implemented": False,
        "reason": "repair writes are intentionally out of scope for this diagnostics slice",
        "required_future_counters": {
            "attempted": 0,
            "updated": 0,
            "skipped_thin_source": 0,
            "failed": 0,
            "rate_limited": 0,
        },
    }
    assert payload["bounds"] == {
        "sample_limit": subject.MAX_SAMPLE_LIMIT,
        "statement_timeout_ms": subject.MAX_STATEMENT_TIMEOUT_MS,
        "pool_name": subject.DEFAULT_POOL_NAME,
    }
    assert payload["thin_source_repost_gaps"]["kwargs"]["sample_limit"] == 999
