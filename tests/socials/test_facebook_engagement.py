"""Tests for Facebook engagement metric extraction from SSR HTML."""

from __future__ import annotations

import pytest

from trr_backend.socials.facebook.scraper import FacebookScraper


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
            '<html><head>'
            '<meta property="og:url" content="https://www.facebook.com/Bravo/videos/123" />'
            '<meta property="og:title" content="Test Video" />'
            '<meta property="og:description" content="A test video" />'
            '</head><body><script>'
            '{"feedback":{"id":"abc","comment_rendering_instance":{"comments":{"total_count":10}}'
            ',"reaction_count":{"count":200,"is_empty":false}'
            ',"top_reactions":{"edges":[{"visible_in_bling_bar":true,"node":{"id":"1","localized_name":"Like"},"i18n_reaction_count":"180","reaction_count":180}'
            ',{"visible_in_bling_bar":false,"node":{"id":"2","localized_name":"Love"},"i18n_reaction_count":"20","reaction_count":20}]}'
            ',"total_comment_count":10'
            ',"video_view_count_renderer":{"__typename":"UFI2ViewCountRenderer","feedback":{"video_view_count":5000,"play_count":8000,"id":"abc"}}'
            '}}'
            '</script></body></html>'
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
            '<html><head>'
            '<meta property="og:url" content="https://www.facebook.com/TestPage/posts/456" />'
            '<meta property="og:title" content="Simple Post" />'
            '</head><body></body></html>'
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
