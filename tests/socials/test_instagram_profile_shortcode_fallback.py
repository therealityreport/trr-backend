from __future__ import annotations

from trr_backend.socials.instagram.profile_shortcode_fallback import extract_profile_shortcodes


def test_extract_profile_shortcodes_from_minimal_profile_html() -> None:
    html = """
    <a href="/p/ABC1234/">one</a>
    <a href="/reel/XYZ9876/">two</a>
    """

    assert extract_profile_shortcodes(html) == ["ABC1234", "XYZ9876"]


def test_extract_profile_shortcodes_ignores_invalid_and_duplicate_values() -> None:
    html = """
    <a href="/p/ABC1234/">one</a>
    <a href="/p/ABC1234/">dup</a>
    <a href="/p/not-a-valid-shortcode!!!/">bad</a>
    <a href="/tv/QWE4321/">three</a>
    """

    assert extract_profile_shortcodes(html) == ["ABC1234", "QWE4321"]
