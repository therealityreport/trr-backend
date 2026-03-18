"""Tests for Facebook engagement metric extraction from SSR HTML."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from trr_backend.socials.facebook.scraper import FacebookScraper, FacebookSearchConfig

# Minimal SSR HTML fragment that mirrors the real Facebook feedback JSON structure
_SAMPLE_FEEDBACK_HTML = """
<html><head></head><body><script>
{"data":{"node":{"id":"12345","feedback":{"id":"ZmVlZGJhY2s6MTIzNA==","comment_rendering_instance":{"comments":{"total_count":151}},"i18n_reaction_count":"3.4K","important_reactors":{"nodes":[]},"reaction_count":{"count":3495,"is_empty":false},"reaction_display_config":{"reaction_display_strategy":"USE_REACTION_SHEET_STRING_ONLY"},"viewer_actor":null,"viewer_feedback_reaction_info":null,"top_reactions":{"edges":[{"visible_in_bling_bar":true,"node":{"id":"1635855486666999","localized_name":"Like"},"i18n_reaction_count":"2.9K","reaction_count":2924},{"visible_in_bling_bar":true,"node":{"id":"115940658764963","localized_name":"Haha"},"i18n_reaction_count":"400","reaction_count":400},{"visible_in_bling_bar":false,"node":{"id":"1678524932434102","localized_name":"Love"},"i18n_reaction_count":"154","reaction_count":154},{"visible_in_bling_bar":false,"node":{"id":"478547315650144","localized_name":"Wow"},"i18n_reaction_count":"11","reaction_count":11},{"visible_in_bling_bar":false,"node":{"id":"613557422527858","localized_name":"Care"},"i18n_reaction_count":"3","reaction_count":3},{"visible_in_bling_bar":false,"node":{"id":"444813342392137","localized_name":"Angry"},"i18n_reaction_count":"2","reaction_count":2},{"visible_in_bling_bar":false,"node":{"id":"908563459236466","localized_name":"Sad"},"i18n_reaction_count":"1","reaction_count":1}]},"total_comment_count":151,"video_view_count_renderer":{"__typename":"UFI2ViewCountRenderer","feedback":{"associated_video":{"is_live_streaming":false,"is_profile_video":false,"id":"1272066031284653"},"video_view_count":152062,"video_view_count_reduced":"152K","total_video_posts":1,"video_post_view_count":152062,"should_show_play_count":false,"play_count_reduced":"282K","play_count":282150,"is_play_count_supported":true,"id":"ZmVlZGJhY2s6MTIzNA=="}}}}}
</script></body></html>
"""

# HTML with share_count present
_SAMPLE_WITH_SHARES_HTML = """
<html><head></head><body><script>
{"data":{"node":{"feedback":{"id":"ZmVlZGJhY2s6MTIzNA==","comment_rendering_instance":{"comments":{"total_count":42}},"reaction_count":{"count":500,"is_empty":false},"share_count":{"count":54},"top_reactions":{"edges":[{"visible_in_bling_bar":true,"node":{"id":"1635855486666999","localized_name":"Like"},"i18n_reaction_count":"450","reaction_count":450},{"visible_in_bling_bar":false,"node":{"id":"1678524932434102","localized_name":"Love"},"i18n_reaction_count":"50","reaction_count":50}]},"total_comment_count":42}}}}
</script></body></html>
"""

# HTML with reduced metric fields and edge-only i18n reaction counts
_SAMPLE_REDUCED_OR_OBJECT_HTML = """
<html><head></head><body><script>
{"data":{"node":{"feedback":{"id":"ZmVlZGJhY2s6MTIzNA==","comment_count":{"count":"1.5K"},"video_view_count_renderer":{"__typename":"UFI2ViewCountRenderer","feedback":{"video_view_count_reduced":"120K","play_count_reduced":"45K","id":"abc"}},"share_count":{"count_reduced":"7.5K"},"top_reactions":{"edges":[{"visible_in_bling_bar":true,"node":{"id":"1635855486666999","localized_name":"Like"},"i18n_reaction_count":"3K"},{"visible_in_bling_bar":false,"node":{"id":"115940658764963","name":"Haha"},"i18n_reaction_count":"1.2K"}]}}}}
</script></body></html>
"""

# Minimal HTML with no engagement data
_SAMPLE_NO_ENGAGEMENT_HTML = """
<html><head>
<meta property="og:url" content="https://www.facebook.com/TestPage/posts/abc123" />
<meta property="og:title" content="Test Post" />
</head><body></body></html>
"""

# HTML with only fallback patterns (no feedback block)
_SAMPLE_FALLBACK_HTML = """
<html><head></head><body><script>
{"reaction_count":{"count":100},"total_comment_count":25,"video_view_count":5000}
</script></body></html>
"""

_SAMPLE_VARIANT_HTML = """
<html><head></head><body><script>
{"data":{"node":{"id":"12345","feedback":{"comment_rendering_instance":{"comments":{"total_count":"151"}},
"reaction_count":{"count":"3.4K"},
"video_view_count_renderer":{"__typename":"UFI2ViewCountRenderer","feedback":{"video_view_count":"152.5K","play_count":"282.150K","id":"12345"}},
"share_count":{"count":"1.2M"},
"top_reactions":{"edges":[{"visible_in_bling_bar":true,"node":{"id":"1635855486666999","localized_name":"Like"},"reaction_count":"2.9K"},{"visible_in_bling_bar":false,"node":{"id":"115940658764963","localized_name":"Haha"},"reaction_count":400}]},
"total_comment_count":"151"}}}}
</script></body></html>
"""


class TestExtractEngagement:
    def test_extracts_reaction_count(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_FEEDBACK_HTML)
        assert result["reaction_count"] == 3495

    def test_extracts_comment_count(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_FEEDBACK_HTML)
        assert result["comment_count"] == 151

    def test_extracts_video_view_count(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_FEEDBACK_HTML)
        assert result["view_count"] == 152062

    def test_extracts_play_count(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_FEEDBACK_HTML)
        assert result["play_count"] == 282150

    def test_extracts_reactions_breakdown(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_FEEDBACK_HTML)
        reactions = result["reactions"]
        assert reactions["Like"] == 2924
        assert reactions["Haha"] == 400
        assert reactions["Love"] == 154
        assert reactions["Wow"] == 11
        assert reactions["Care"] == 3
        assert reactions["Angry"] == 2
        assert reactions["Sad"] == 1

    def test_extracts_share_count(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_WITH_SHARES_HTML)
        assert result["share_count"] == 54

    def test_extracts_reduced_and_object_counts(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_REDUCED_OR_OBJECT_HTML)
        assert result["comment_count"] == 1500
        assert result["share_count"] == 7500
        assert result["view_count"] == 120000
        assert result["play_count"] == 45000

    def test_extracts_shares_with_reactions(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_WITH_SHARES_HTML)
        assert result["reaction_count"] == 500
        assert result["comment_count"] == 42
        reactions = result["reactions"]
        assert reactions["Like"] == 450
        assert reactions["Love"] == 50

    def test_extracts_reaction_breakdown_from_i18n_without_reaction_count(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_REDUCED_OR_OBJECT_HTML)
        reactions = result["reactions"]
        assert reactions["Like"] == 3000
        assert reactions["Haha"] == 1200

    def test_no_engagement_returns_zeros(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_NO_ENGAGEMENT_HTML)
        assert result["reaction_count"] == 0
        assert result["comment_count"] == 0
        assert result["share_count"] == 0
        assert result["view_count"] == 0
        assert result["reactions"] == {}

    def test_fallback_extraction_without_feedback_block(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_FALLBACK_HTML)
        assert result["reaction_count"] == 100
        assert result["comment_count"] == 25
        assert result["view_count"] == 5000

    def test_extracts_counts_from_quoted_and_scientific_like_payload(self) -> None:
        result = FacebookScraper._extract_engagement(_SAMPLE_VARIANT_HTML)
        assert result["reaction_count"] == 3400
        assert result["comment_count"] == 151
        assert result["share_count"] == 1200000
        assert result["view_count"] == 152500
        assert result["play_count"] == 282150
        assert result["reactions"]["Like"] == 2900
        assert result["reactions"]["Haha"] == 400


class TestBuildPostEngagement:
    def test_build_post_populates_engagement(self) -> None:
        scraper = FacebookScraper()
        html = (
            "<html><head>"
            '<meta property="og:url" content="https://www.facebook.com/Bravo/videos/123" />'
            '<meta property="og:title" content="Test Video" />'
            '<meta property="og:description" content="A test video" />'
            "</head><body><script>"
            '{"feedback":{"id":"abc","comment_rendering_instance":{"comments":{"total_count":10}}'
            ',"reaction_count":{"count":200,"is_empty":false}'
            ',"top_reactions":{"edges":[{"visible_in_bling_bar":true,"node":{"id":"1","localized_name":"Like"},"i18n_reaction_count":"180","reaction_count":180}'
            ',{"visible_in_bling_bar":false,"node":{"id":"2","localized_name":"Love"},"i18n_reaction_count":"20","reaction_count":20}]}'
            ',"total_comment_count":10'
            ',"video_view_count_renderer":{"__typename":"UFI2ViewCountRenderer","feedback":{"video_view_count":5000,"play_count":8000,"id":"abc"}}'
            "}}"
            "</script></body></html>"
        )
        post = scraper._build_post_from_html(
            url="https://www.facebook.com/Bravo/videos/123",
            html_text=html,
            username="Bravo",
            post_type_hint="reel",
        )
        assert post.likes == 200
        assert post.comments == 10
        assert post.views == 5000
        assert post.reactions["Like"] == 180
        assert post.reactions["Love"] == 20
        assert post.post_type == "reel"

    def test_build_post_zero_engagement_when_no_data(self) -> None:
        scraper = FacebookScraper()
        html = (
            "<html><head>"
            '<meta property="og:url" content="https://www.facebook.com/TestPage/posts/456" />'
            '<meta property="og:title" content="Simple Post" />'
            "</head><body></body></html>"
        )
        post = scraper._build_post_from_html(
            url="https://www.facebook.com/TestPage/posts/456",
            html_text=html,
            username="TestPage",
            post_type_hint="feed",
        )
        assert post.likes == 0
        assert post.comments == 0
        assert post.shares == 0
        assert post.views == 0
        assert post.reactions == {}

    def test_build_post_uses_deterministic_fallback_post_id(self) -> None:
        scraper = FacebookScraper()
        html = (
            '<html><head><meta property="og:url" '
            'content="https://www.facebook.com/?utm_source=one&fbclid=abc" /></head><body></body></html>'
        )
        post_one = scraper._build_post_from_html(
            url="https://www.facebook.com/?utm_source=one&fbclid=abc",
            html_text=html,
            username="Bravo",
            post_type_hint="feed",
        )
        post_two = scraper._build_post_from_html(
            url="https://www.facebook.com/?utm_source=two&utm_medium=email&fbclid=xyz",
            html_text=html,
            username="Bravo",
            post_type_hint="feed",
        )
        assert post_one.post_id == post_two.post_id
        assert post_one.post_id.startswith("fb_")


class TestFacebookPostDataclass:
    def test_reactions_field_exists(self) -> None:
        from trr_backend.socials.facebook.scraper import FacebookPost

        post = FacebookPost(
            post_id="1",
            username="test",
            post_type="feed",
            caption="hello",
            media_urls=[],
            thumbnail_url=None,
            reactions={"Like": 100, "Love": 20},
        )
        assert post.reactions == {"Like": 100, "Love": 20}

    def test_to_dict_includes_reactions(self) -> None:
        from trr_backend.socials.facebook.scraper import FacebookPost

        post = FacebookPost(
            post_id="1",
            username="test",
            post_type="feed",
            caption="hello",
            media_urls=[],
            thumbnail_url=None,
            reactions={"Like": 50},
        )
        d = post.to_dict()
        assert d["reactions"] == {"Like": 50}


class TestFacebookSearchAndShareHelpers:
    def test_build_search_url_from_profile_and_dates(self) -> None:
        config = FacebookSearchConfig(
            profile_url="https://www.facebook.com/profile/100059495681624",
            query="#RHOSLC",
            date_start=datetime(2025, 1, 1, tzinfo=UTC),
            date_end=datetime(2025, 12, 31, tzinfo=UTC),
        )
        url = FacebookScraper._build_search_url(config)
        assert "/profile/100059495681624/search" in url
        assert "q=%23RHOSLC" in url
        assert "filters=" in url

    def test_extract_post_urls_supports_group_posts(self) -> None:
        scraper = FacebookScraper()
        html = """
        <a href="/groups/1432923047421128/posts/1722908395089257/">group post</a>
        <a href="https://www.facebook.com/TestPage/posts/12345">feed post</a>
        """
        pairs = scraper._extract_post_urls(html, handle="")
        urls = {url for url, _kind in pairs}
        assert "https://www.facebook.com/groups/1432923047421128/posts/1722908395089257/" in urls
        assert "https://www.facebook.com/TestPage/posts/12345" in urls

    def test_extract_share_details_from_html(self) -> None:
        html = """
        <div data-ad-rendering-role="profile_name">
          <a href="https://www.facebook.com/kyle.d.karnes"><span>Kyle Davis Karnes</span></a>
          <title>Shared with Public</title>
          <div data-ad-rendering-role="story_message">Opa! The moment you've been waiting for is finally here.</div>
          <a href="https://www.facebook.com/kyle.d.karnes/posts/pfbid033MGB4hDUWwRo31hcXacsqDQZSuFSDogHFSy1jhfzVc92SWXdee7fpNF1rQsJBPVLl">30w</a>
          <img src="https://scontent.xx.fbcdn.net/preview.jpg" />
        </div>
        """
        shares = FacebookScraper._extract_share_details_from_html(html, max_shares=10)
        assert len(shares) == 1
        assert shares[0].sharer_name == "Kyle Davis Karnes"
        assert shares[0].profile_url == "https://www.facebook.com/kyle.d.karnes"
        assert shares[0].post_url == (
            "https://www.facebook.com/kyle.d.karnes/posts/pfbid033MGB4hDUWwRo31hcXacsqDQZSuFSDogHFSy1jhfzVc92SWXdee7fpNF1rQsJBPVLl"
        )
        assert shares[0].privacy_label == "Shared with Public"
        assert shares[0].caption_snippet == "Opa! The moment you've been waiting for is finally here."

    def test_search_posts_uses_discovery_and_post_scrape(self, monkeypatch: pytest.MonkeyPatch) -> None:
        scraper = FacebookScraper()
        post = scraper._build_post_from_html(
            url="https://www.facebook.com/TestPage/posts/123",
            html_text=(
                '<meta property="og:url" content="https://www.facebook.com/TestPage/posts/123" />'
                '<meta property="og:title" content="A title" />'
                '<meta property="og:description" content="#RHOSLC hello" />'
                '{"creation_time":1736035200}'
            ),
            username="TestPage",
            post_type_hint="feed",
        )

        monkeypatch.setattr(scraper, "_discover_search_post_urls", lambda config: [post.url])
        monkeypatch.setattr(scraper, "scrape_post", lambda *args, **kwargs: (post, []))

        results = scraper.search_posts(
            FacebookSearchConfig(
                profile_url="https://www.facebook.com/TestPage",
                query="#RHOSLC",
                date_start=datetime(2025, 1, 1, tzinfo=UTC),
                date_end=datetime(2025, 1, 31, tzinfo=UTC),
                max_posts=5,
            )
        )
        assert len(results) == 1
        assert results[0].post_id == post.post_id

    def test_cross_platform_media_fallback_accepts_exact_caption_same_day(self, monkeypatch: pytest.MonkeyPatch) -> None:
        scraper = FacebookScraper()
        post = scraper._build_post_from_html(
            url="https://www.facebook.com/TestPage/posts/123",
            html_text=(
                '<meta property="og:url" content="https://www.facebook.com/TestPage/posts/123" />'
                '<meta property="og:description" '
                'content="Opa! The moment you&apos;ve been waiting for is finally here. ❄️ A new season of #RHOSLC goes the distance starting September 16th!" />'
                '{"creation_time":1757980800}'
            ),
            username="TestPage",
            post_type_hint="reel",
        )
        post.media_urls = []
        post.thumbnail_url = None

        class FakeInstagramScraper:
            def __init__(self) -> None:
                self.session = None
                self.request_timeout = (10, 45)
                self.cookies = {}

            def _get_headers(self, referer: str | None = None) -> dict[str, str]:
                return {}

            def fetch_post_info(self, shortcode: str) -> dict[str, str]:
                return {}

            def _extract_caption(self, raw_media: dict[str, str]) -> str:
                return str(raw_media.get("caption") or "")

        resolution = SimpleNamespace(
            source="html_json",
            media_type="reel",
            media_urls=["https://cdn.instagram.test/video.mp4"],
            thumbnail_url="https://cdn.instagram.test/thumb.jpg",
            attempts=[{"source": "html_json", "success": True}],
            metadata=SimpleNamespace(
                taken_at=datetime(2025, 9, 16, 15, 0, tzinfo=UTC),
                duration_seconds=35.0,
                raw_media={
                    "caption": (
                        "Opa! The moment you've been waiting for is finally here. ❄️ "
                        "A new season of #RHOSLC goes the distance starting September 16th!"
                    )
                },
            ),
        )

        monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", FakeInstagramScraper)
        monkeypatch.setattr("trr_backend.socials.instagram.resolve_instagram_media", lambda *args, **kwargs: resolution)

        scraper._resolve_cross_platform_media_fallback(
            post=post,
            html_text='https://www.instagram.com/reel/ABC123/',
            allow_fallback=True,
        )

        assert post.media_urls == ["https://cdn.instagram.test/video.mp4"]
        assert post.thumbnail_url == "https://cdn.instagram.test/thumb.jpg"
        assert post.media_provenance.platform == "instagram"
        assert post.media_provenance.fallback_used is True

    def test_cross_platform_media_fallback_rejects_caption_mismatch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        scraper = FacebookScraper()
        post = scraper._build_post_from_html(
            url="https://www.facebook.com/TestPage/posts/123",
            html_text=(
                '<meta property="og:url" content="https://www.facebook.com/TestPage/posts/123" />'
                '<meta property="og:description" content="Original caption" />'
                '{"creation_time":1757980800}'
            ),
            username="TestPage",
            post_type_hint="feed",
        )
        post.media_urls = []

        class FakeInstagramScraper:
            def __init__(self) -> None:
                self.session = None
                self.request_timeout = (10, 45)
                self.cookies = {}

            def _get_headers(self, referer: str | None = None) -> dict[str, str]:
                return {}

            def fetch_post_info(self, shortcode: str) -> dict[str, str]:
                return {}

            def _extract_caption(self, raw_media: dict[str, str]) -> str:
                return str(raw_media.get("caption") or "")

        resolution = SimpleNamespace(
            source="html_json",
            media_type="image",
            media_urls=["https://cdn.instagram.test/fallback.jpg"],
            thumbnail_url=None,
            attempts=[],
            metadata=SimpleNamespace(
                taken_at=datetime(2025, 9, 16, 12, 0, tzinfo=UTC),
                duration_seconds=None,
                raw_media={"caption": "Different caption"},
            ),
        )
        monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", FakeInstagramScraper)
        monkeypatch.setattr("trr_backend.socials.instagram.resolve_instagram_media", lambda *args, **kwargs: resolution)

        scraper._resolve_cross_platform_media_fallback(
            post=post,
            html_text='https://www.instagram.com/p/ABC123/',
            allow_fallback=True,
        )

        assert post.media_urls == []
        assert post.media_provenance.platform == "facebook"
        assert post.media_provenance.fallback_used is False
