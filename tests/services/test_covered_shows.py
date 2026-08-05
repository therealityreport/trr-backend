from __future__ import annotations

import pytest

from trr_backend.repositories import covered_shows as covered_shows_repo
from trr_backend.services import covered_shows as service


def test_add_covered_show_invalidates_covered_and_network_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        covered_shows_repo,
        "add_covered_show",
        lambda **_kwargs: ({"show_id": "show-1", "show_name": "Top Chef"}, 1),
    )
    covered_invalidation = {"count": 0}
    network_invalidation = {"count": 0}
    monkeypatch.setattr(
        service,
        "invalidate_cache",
        lambda: covered_invalidation.__setitem__("count", covered_invalidation["count"] + 1),
    )
    monkeypatch.setattr(
        service.networks_streaming_reads_service,
        "invalidate_networks_streaming_cache",
        lambda: network_invalidation.__setitem__("count", network_invalidation["count"] + 1),
    )

    result = service.add_covered_show(
        show_id="show-1",
        show_name="Top Chef",
        actor_uid="admin-1",
    )

    assert result == ({"show_id": "show-1", "show_name": "Top Chef"}, 1)
    assert covered_invalidation["count"] == 1
    assert network_invalidation["count"] == 1


@pytest.mark.parametrize("deleted", [True, False])
def test_remove_covered_show_invalidates_network_reads_only_after_delete(
    monkeypatch: pytest.MonkeyPatch,
    deleted: bool,
) -> None:
    monkeypatch.setattr(
        covered_shows_repo,
        "remove_covered_show",
        lambda _show_id: (deleted, 1),
    )
    covered_invalidation = {"count": 0}
    network_invalidation = {"count": 0}
    monkeypatch.setattr(
        service,
        "invalidate_cache",
        lambda: covered_invalidation.__setitem__("count", covered_invalidation["count"] + 1),
    )
    monkeypatch.setattr(
        service.networks_streaming_reads_service,
        "invalidate_networks_streaming_cache",
        lambda: network_invalidation.__setitem__("count", network_invalidation["count"] + 1),
    )

    assert service.remove_covered_show("show-1") == (deleted, 1)
    assert covered_invalidation["count"] == int(deleted)
    assert network_invalidation["count"] == int(deleted)
