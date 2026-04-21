from __future__ import annotations

from trr_backend.repositories import social_season_analytics as social_repo


def test_build_mirror_source_key_uses_post_uuid_in_unknown_instagram_fallback() -> None:
    first = social_repo._build_mirror_source_key(  # noqa: SLF001
        "instagram",
        type("P", (), {"id": "11111111-1111-1111-1111-111111111111", "shortcode": ""})(),
        source_urls=["https://cdn.test/a.jpg"],
    )
    second = social_repo._build_mirror_source_key(  # noqa: SLF001
        "instagram",
        type("P", (), {"id": "22222222-2222-2222-2222-222222222222", "shortcode": ""})(),
        source_urls=["https://cdn.test/a.jpg"],
    )

    assert first != second
