from __future__ import annotations

import asyncio

from api.routers import socials as socials_router


def test_preview_tiktok_profile_uses_auth_preflight(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeScraper:
        def __init__(self, *, cookies=None):  # noqa: ANN003
            captured["cookies"] = cookies

        def fetch_user_detail(self, username: str, *, delay: float):
            captured["username"] = username
            captured["delay"] = delay
            return {
                "userInfo": {
                    "user": {
                        "uniqueId": username,
                        "nickname": "Bravo",
                        "signature": "bio",
                        "verified": True,
                        "privateAccount": False,
                    },
                    "stats": {
                        "followerCount": 10,
                        "followingCount": 20,
                        "heartCount": 30,
                        "videoCount": 40,
                    },
                }
            }

    monkeypatch.setattr(
        socials_router,
        "_load_social_auth_or_503",
        lambda **_kwargs: {"sessionid": "cookie"},
    )
    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeScraper)

    payload = asyncio.run(socials_router.preview_tiktok_profile("creator", {"email": "admin@example.com"}))

    assert captured["cookies"] == {"sessionid": "cookie"}
    assert payload["username"] == "creator"
    assert payload["followers"] == 10
