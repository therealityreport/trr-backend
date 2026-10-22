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
    monkeypatch.setattr("trr_backend.socials.tiktok.scraper.TikTokScraper", _FakeScraper)

    payload = asyncio.run(socials_router.preview_tiktok_profile("creator", {"email": "admin@example.com"}))

    assert captured["cookies"] == {"sessionid": "cookie"}
    assert payload["username"] == "creator"
    assert payload["followers"] == 10
    assert payload["likes"] == 30


def test_preview_tiktok_profile_runs_off_the_event_loop_thread(monkeypatch) -> None:
    import asyncio
    import threading

    from api.routers import socials as socials_router

    captured: dict[str, int] = {}

    class _FakeScraper:
        def __init__(self, *, cookies=None):  # noqa: ANN003
            pass

        def fetch_user_detail(self, username, delay=0):  # noqa: ANN001
            captured["fetch_thread"] = threading.get_ident()
            return {"userInfo": {"user": {"uniqueId": username, "nickname": "Bravo"}, "stats": {"followerCount": 5}}}

    monkeypatch.setattr(socials_router, "_load_social_auth_or_503", lambda **_kwargs: {"sessionid": "c"})
    monkeypatch.setattr("trr_backend.socials.tiktok.scraper.TikTokScraper", _FakeScraper)

    async def _call():  # noqa: ANN202
        captured["loop_thread"] = threading.get_ident()
        return await socials_router.preview_tiktok_profile("bravotv", {"email": "admin@example.com"})

    payload = asyncio.run(_call())

    assert payload["username"] == "bravotv"
    assert payload["followers"] == 5
    assert captured["fetch_thread"] != captured["loop_thread"]
