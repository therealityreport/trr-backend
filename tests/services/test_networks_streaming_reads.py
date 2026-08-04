from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from trr_backend.repositories import admin_networks_streaming_reads as repository
from trr_backend.services import networks_streaming_reads as service


def _summary_payload(*, added_shows: int = 7) -> dict[str, object]:
    return {
        "totals": {
            "total_available_shows": 18,
            "total_added_shows": added_shows,
        },
        "rows": [],
        "generated_at": "2026-07-16T12:00:00Z",
    }


def _detail_payload(*, added_shows: int = 5) -> dict[str, object]:
    return {
        "entity_type": "network",
        "entity_key": "bravo",
        "entity_slug": "bravo",
        "display_name": "Bravo",
        "available_show_count": 10,
        "added_show_count": added_shows,
    }


@pytest.fixture(autouse=True)
def clear_summary_cache():
    service.invalidate_networks_streaming_summary_cache()
    yield
    service.invalidate_networks_streaming_summary_cache()


def test_summary_cache_and_invalidation_share_one_repository_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def fake_summary():
        calls["count"] += 1
        return _summary_payload(added_shows=calls["count"]), 2

    monkeypatch.setattr(repository, "get_networks_streaming_summary", fake_summary)

    first = service.get_networks_streaming_summary()
    second = service.get_networks_streaming_summary()
    service.invalidate_networks_streaming_summary_cache()
    third = service.get_networks_streaming_summary()

    assert first == (_summary_payload(added_shows=1), 2, "miss")
    assert second == (_summary_payload(added_shows=1), 0, "hit")
    assert third == (_summary_payload(added_shows=2), 2, "miss")
    assert calls["count"] == 2


def test_summary_singleflight_collapses_concurrent_cold_misses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def fake_summary():
        calls["count"] += 1
        time.sleep(0.05)
        return _summary_payload(), 2

    monkeypatch.setattr(repository, "get_networks_streaming_summary", fake_summary)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: service.get_networks_streaming_summary(), range(2)))

    assert calls["count"] == 1
    assert {result[2] for result in results} == {"miss", "deduped"}
    assert sorted(result[1] for result in results) == [0, 2]
    assert all(result[0] == _summary_payload() for result in results)


def test_invalidation_does_not_repopulate_cache_from_older_inflight_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}
    first_load_started = Event()
    release_first_load = Event()

    def fake_summary():
        calls["count"] += 1
        call_number = calls["count"]
        if call_number == 1:
            first_load_started.set()
            assert release_first_load.wait(timeout=1)
        return _summary_payload(added_shows=call_number), 2

    monkeypatch.setattr(repository, "get_networks_streaming_summary", fake_summary)

    with ThreadPoolExecutor(max_workers=1) as executor:
        first_future = executor.submit(service.get_networks_streaming_summary)
        assert first_load_started.wait(timeout=1)
        service.invalidate_networks_streaming_summary_cache()
        release_first_load.set()
        first = first_future.result(timeout=1)

    second = service.get_networks_streaming_summary()

    assert first == (_summary_payload(added_shows=1), 2, "miss")
    assert second == (_summary_payload(added_shows=2), 2, "miss")
    assert calls["count"] == 2


def test_detail_singleflight_collapses_concurrent_cold_misses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def fake_detail(**_kwargs):
        calls["count"] += 1
        time.sleep(0.05)
        return _detail_payload(), 4

    monkeypatch.setattr(service, "_build_networks_streaming_detail", fake_detail)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _index: service.get_networks_streaming_detail(
                    entity_type="network",
                    entity_key=" Bravo ",
                ),
                range(2),
            )
        )

    assert calls["count"] == 1
    assert {result[2] for result in results} == {"miss", "deduped"}
    assert sorted(result[1] for result in results) == [0, 4]
    assert all(result[0] == _detail_payload() for result in results)


def test_detail_singleflight_propagates_one_concurrent_not_found_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def missing_detail(**_kwargs):
        calls["count"] += 1
        time.sleep(0.05)
        raise service.NetworksStreamingDetailNotFoundError(
            suggestions=[{"entity_type": "network", "entity_slug": "bravo"}],
            query_count=5,
        )

    monkeypatch.setattr(service, "_build_networks_streaming_detail", missing_detail)

    def load_missing(_index: int) -> service.NetworksStreamingDetailNotFoundError:
        with pytest.raises(service.NetworksStreamingDetailNotFoundError) as caught:
            service.get_networks_streaming_detail(
                entity_type="network",
                entity_slug="brva",
            )
        return caught.value

    with ThreadPoolExecutor(max_workers=5) as executor:
        errors = list(executor.map(load_missing, range(5)))

    assert calls["count"] == 1
    assert all(error.query_count == 5 for error in errors)
    assert all(error.suggestions == [{"entity_type": "network", "entity_slug": "bravo"}] for error in errors)


def test_detail_cache_normalizes_equivalent_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def fake_detail(**_kwargs):
        calls["count"] += 1
        return _detail_payload(), 4

    monkeypatch.setattr(service, "_build_networks_streaming_detail", fake_detail)

    first = service.get_networks_streaming_detail(
        entity_type="NETWORK",
        entity_key="  Bravo   TV ",
    )
    second = service.get_networks_streaming_detail(
        entity_type="network",
        entity_key="bravo tv",
    )

    assert first == (_detail_payload(), 4, "miss")
    assert second == (_detail_payload(), 0, "hit")
    assert calls["count"] == 1


def test_detail_invalidation_does_not_repopulate_from_older_inflight_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}
    first_load_started = Event()
    release_first_load = Event()

    def fake_detail(**_kwargs):
        calls["count"] += 1
        call_number = calls["count"]
        if call_number == 1:
            first_load_started.set()
            assert release_first_load.wait(timeout=1)
        return _detail_payload(added_shows=call_number), 4

    monkeypatch.setattr(service, "_build_networks_streaming_detail", fake_detail)

    with ThreadPoolExecutor(max_workers=1) as executor:
        first_future = executor.submit(
            service.get_networks_streaming_detail,
            entity_type="network",
            entity_slug="bravo",
        )
        assert first_load_started.wait(timeout=1)
        service.invalidate_networks_streaming_cache()
        release_first_load.set()
        first = first_future.result(timeout=1)

    second = service.get_networks_streaming_detail(
        entity_type="network",
        entity_slug="bravo",
    )

    assert first == (_detail_payload(added_shows=1), 4, "miss")
    assert second == (_detail_payload(added_shows=2), 4, "miss")
    assert calls["count"] == 2
