from __future__ import annotations

import threading

from trr_backend.socials.instagram.scraper import InstagramScraper


def test_profile_page_context_cache_mutations_are_locked() -> None:
    scraper = InstagramScraper.__new__(InstagramScraper)
    scraper._profile_page_context_cache = {}
    scraper._context_cache_lock = threading.RLock()

    failures: list[Exception] = []

    def _hammer() -> None:
        try:
            for idx in range(200):
                scraper._set_profile_page_context_cache_entry("bravotv", {"cursor": str(idx)})
                scraper._get_profile_page_context_cache_entry("bravotv")
                if idx % 50 == 0:
                    scraper._clear_profile_page_context_cache()
                if idx % 75 == 0:
                    scraper._pop_profile_page_context_cache_entry("bravotv")
        except Exception as exc:  # noqa: BLE001
            failures.append(exc)

    threads = [threading.Thread(target=_hammer) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert failures == []
