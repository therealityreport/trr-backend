from __future__ import annotations

import json
from pathlib import Path

import scripts.sync_tmdb_watch_providers as mod


def test_parse_watch_providers_payload_builds_rows() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    payload = json.loads((repo_root / "tests" / "fixtures" / "tmdb" / "tv_watch_providers_sample.json").read_text())

    provider_rows, show_provider_rows, ids_by_group = mod._parse_watch_providers_payload(
        payload,
        show_id="show-1",
        fetched_at="2025-01-01T00:00:00Z",
    )

    provider_ids = {row.get("provider_id") for row in provider_rows}
    assert provider_ids == {15, 531}
    assert all(row.get("provider_name") for row in provider_rows)

    assert len(show_provider_rows) == 2
    assert {row.get("provider_id") for row in show_provider_rows} == {15, 531}
    assert {row.get("region") for row in show_provider_rows} == {"US"}
    assert {row.get("offer_type") for row in show_provider_rows} == {"flatrate"}
    assert all(row.get("show_id") == "show-1" for row in show_provider_rows)
    assert all(row.get("link") for row in show_provider_rows)

    assert ids_by_group.get(("US", "flatrate")) == {15, 531}


def test_compute_stale_provider_ids() -> None:
    assert mod._compute_stale_provider_ids({1, 2, 3}, {2, 3}) == [1]
