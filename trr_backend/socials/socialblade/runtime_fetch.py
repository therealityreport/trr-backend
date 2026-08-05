from __future__ import annotations

import asyncio
from typing import Any


def run_socialblade_scrapling_fetch(
    handle: str,
    cookies: Any,
    *,
    platform: str,
) -> dict[str, Any]:
    from trr_backend.socials.socialblade.fetcher import SocialBladeScraplingFetcher
    from trr_backend.socials.socialblade.proxy import select_socialblade_proxy
    from trr_backend.socials.socialblade.session import resolve_socialblade_scrapling_session

    session = resolve_socialblade_scrapling_session(cookies)
    proxy_config = select_socialblade_proxy(session_key=f"{platform}:{handle}")

    async def _run() -> dict[str, Any]:
        fetcher = SocialBladeScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.raw_cookies,
            platform=platform,
            proxy_config=proxy_config,
        )
        try:
            return await fetcher.scrape(handle)
        finally:
            await fetcher.aclose()

    return asyncio.run(_run())
