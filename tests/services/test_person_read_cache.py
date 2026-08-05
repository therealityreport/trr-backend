from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from trr_backend.services.person_read_cache import (
    cache_get,
    invalidate_person_read_cache,
    resolve_person_read_singleflight,
)

PERSON_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
CACHE_KEY = f"person:{PERSON_ID}:cover-photo"


@pytest.fixture(autouse=True)
def clear_person_read_cache() -> None:
    invalidate_person_read_cache()
    yield
    invalidate_person_read_cache()


def test_person_invalidation_detaches_stale_inflight_loader_and_preserves_fresh_cache() -> None:
    old_loader_started = Event()
    release_old_loader = Event()

    def load_old_payload() -> tuple[dict[str, str], int]:
        old_loader_started.set()
        assert release_old_loader.wait(timeout=2), "old person read loader was never released"
        return {"version": "old"}, 1

    def load_fresh_payload() -> tuple[dict[str, str], int]:
        return {"version": "fresh"}, 1

    with ThreadPoolExecutor(max_workers=2) as executor:
        old_read = executor.submit(
            resolve_person_read_singleflight,
            cache_key=CACHE_KEY,
            ttl_seconds=60,
            loader=load_old_payload,
        )
        assert old_loader_started.wait(timeout=2), "old person read loader never started"

        invalidate_person_read_cache(person_id=PERSON_ID)

        fresh_payload, fresh_queries, fresh_cache, fresh_singleflight = resolve_person_read_singleflight(
            cache_key=CACHE_KEY,
            ttl_seconds=60,
            loader=load_fresh_payload,
        )
        assert fresh_payload == {"version": "fresh"}
        assert (fresh_queries, fresh_cache, fresh_singleflight) == (1, "miss", "owner")

        release_old_loader.set()
        old_payload, old_queries, old_cache, old_singleflight = old_read.result(timeout=2)

    assert old_payload == {"version": "old"}
    assert (old_queries, old_cache, old_singleflight) == (1, "miss", "owner")
    assert cache_get(CACHE_KEY) == {"version": "fresh"}
