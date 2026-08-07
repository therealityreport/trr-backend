from __future__ import annotations

import json
from typing import Any, cast
from uuid import uuid4

import pytest

from api import deps as deps_mod
from api.routers import admin_person_profile as mod


def _parse_sse_events(events: list[str]) -> list[tuple[str, dict[str, Any]]]:
    parsed: list[tuple[str, dict[str, Any]]] = []
    for raw in events:
        blocks = [block for block in raw.strip().split("\n") if block]
        event_name = "message"
        payload: dict[str, Any] = {}
        for block in blocks:
            if block.startswith("event:"):
                event_name = block.split(":", 1)[1].strip()
            elif block.startswith("data:"):
                payload = json.loads(block.split(":", 1)[1].strip())
        parsed.append((event_name, payload))
    return parsed


def test_merge_source_aliases_preserves_per_source_records() -> None:
    merged = mod._merge_source_aliases(
        {"tmdb": ["Heather Gay"]},
        source="imdb",
        values=["H. Gay", "Heather Gay", "  H.   Gay  "],
    )

    assert merged == {
        "tmdb": ["Heather Gay"],
        "imdb": ["H. Gay", "Heather Gay"],
    }
    assert mod._flatten_aliases(merged) == ["Heather Gay", "H. Gay"]


def test_load_related_shows_for_person_falls_back_to_v_show_cast(monkeypatch: pytest.MonkeyPatch) -> None:
    person_id = str(uuid4())
    fallback_rows: list[dict[str, object]] = [
        {"show_id": "show-1", "show_name": "Watch What Happens Live", "show_imdb_id": "tt123"}
    ]
    fetch_all_calls: list[str] = []

    def _fake_fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
        assert params == [person_id]
        fetch_all_calls.append(query)
        return [] if len(fetch_all_calls) == 1 else fallback_rows

    monkeypatch.setattr(mod.pg, "fetch_all", _fake_fetch_all)

    rows = mod._load_related_shows_for_person(person_id)

    assert rows == fallback_rows
    assert "FROM core.credits" in fetch_all_calls[0]
    assert "FROM core.v_show_cast" in fetch_all_calls[1]


def test_load_person_uses_direct_pg_fetch_one(monkeypatch: pytest.MonkeyPatch) -> None:
    person_id = str(uuid4())
    person_row = {
        "id": person_id,
        "full_name": "Heather Gay",
        "external_ids": {},
        "birthday": {},
        "gender": {},
        "biography": {},
        "place_of_birth": {},
        "homepage": {},
        "profile_image_url": {},
        "alternative_names": {},
    }
    fetch_one_calls: list[tuple[str, list[object] | None, object | None]] = []

    def _fake_fetch_one(query: str, params: list[object] | None = None, *, conn=None) -> dict[str, object]:
        fetch_one_calls.append((query, params, conn))
        return person_row

    monkeypatch.setattr(mod.pg, "fetch_one", _fake_fetch_one)

    row = mod._load_person(person_id=person_id)

    assert row == person_row
    assert len(fetch_one_calls) == 1
    query, params, conn = fetch_one_calls[0]
    assert "from core.people" in query.lower()
    assert "external_ids" in query.lower()
    assert params == [person_id]
    assert conn is None


def test_load_approved_person_links_uses_direct_pg_fetch_all(monkeypatch: pytest.MonkeyPatch) -> None:
    person_id = str(uuid4())
    show_ids = [str(uuid4()), str(uuid4())]
    approved_links = [
        {
            "show_id": show_ids[0],
            "link_kind": "imdb",
            "url": "https://www.imdb.com/name/nm123",
            "status": "approved",
            "label": "IMDb",
            "metadata": {"source": "seed"},
        }
    ]
    fetch_all_calls: list[tuple[str, list[object] | None, object | None]] = []

    def _fake_fetch_all(query: str, params: list[object] | None = None, *, conn=None) -> list[dict[str, object]]:
        fetch_all_calls.append((query, params, conn))
        return approved_links

    monkeypatch.setattr(mod.pg, "fetch_all", _fake_fetch_all)

    rows = mod._load_approved_person_links(person_id=person_id, show_ids=show_ids)

    assert rows == approved_links
    assert len(fetch_all_calls) == 1
    query, params, conn = fetch_all_calls[0]
    assert "from core.entity_links" in query.lower()
    assert "entity_type = 'person'" in query.lower()
    assert "status = 'approved'" in query.lower()
    assert params == [person_id, show_ids]
    assert conn is None


def test_get_supabase_admin_client_warns_and_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    monkeypatch.setattr(deps_mod, "get_postgrest_admin_client", lambda: sentinel)

    with pytest.warns(DeprecationWarning, match="get_postgrest_admin_client"):
        client = deps_mod.get_supabase_admin_client()

    assert client is sentinel


def test_refresh_credits_for_related_shows_fans_out_per_show(monkeypatch: pytest.MonkeyPatch) -> None:
    cast_calls: list[list[str]] = []
    episode_calls: list[list[str]] = []
    monkeypatch.setattr(mod.sync_show_cast, "main", lambda argv=None: cast_calls.append(list(argv or [])) or 0)
    monkeypatch.setattr(
        mod.sync_episode_appearances,
        "main",
        lambda argv=None: episode_calls.append(list(argv or [])) or 0,
    )

    processed, failures = mod._refresh_credits_for_related_shows([{"show_id": "show-1"}, {"show_id": "show-2"}])

    assert processed == 2
    assert failures == []
    assert cast_calls == [["--show-id", "show-1"], ["--show-id", "show-2"]]
    assert episode_calls == [
        ["--show-id", "show-1", "--concurrency", "2"],
        ["--show-id", "show-2", "--concurrency", "2"],
    ]


def test_run_person_profile_refresh_keeps_going_when_one_source_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    person_id = str(uuid4())
    monkeypatch.setattr(
        mod,
        "_load_person",
        lambda *, person_id: {
            "id": person_id,
            "full_name": "Heather Gay",
            "external_ids": {},
            "birthday": {},
            "gender": {},
            "biography": {},
            "place_of_birth": {},
            "homepage": {},
            "profile_image_url": {},
            "alternative_names": {},
        },
    )
    monkeypatch.setattr(mod, "_load_related_shows_for_person", lambda _person_id: [{"show_id": "show-1"}])
    monkeypatch.setattr(mod, "_discover_and_persist_person_links", lambda *args, **kwargs: 2)
    monkeypatch.setattr(mod, "_load_approved_person_links", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        mod,
        "_refresh_tmdb_profile",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("tmdb down")),
    )
    monkeypatch.setattr(mod, "_refresh_imdb_profile", lambda *args, **kwargs: (1, 2))
    monkeypatch.setattr(mod, "_refresh_fandom_profile", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(mod, "_refresh_wikipedia_profile", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(mod, "_refresh_bravo_profile", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(mod, "_refresh_credits_for_related_shows", lambda related_shows: (len(related_shows), []))

    result = mod._run_person_profile_refresh(
        person_id=person_id,
        payload=mod.RefreshProfileRequest(),
        db=cast("Any", None),
        actor="admin@example.com",
    )

    assert result["status"] == "partial"
    assert result["links_refreshed"] == 2
    assert result["aliases_added"] == 2
    assert result["profile_fields_changed"] == 1
    assert result["credits_updated"] == 1
    assert any(str(item).startswith("profile_tmdb:") for item in result["failures"])


def test_run_person_profile_refresh_counts_only_successful_credit_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person_id = str(uuid4())
    monkeypatch.setattr(
        mod,
        "_load_person",
        lambda *, person_id: {
            "id": person_id,
            "full_name": "Heather Gay",
            "external_ids": {},
            "birthday": {},
            "gender": {},
            "biography": {},
            "place_of_birth": {},
            "homepage": {},
            "profile_image_url": {},
            "alternative_names": {},
        },
    )
    monkeypatch.setattr(
        mod,
        "_load_related_shows_for_person",
        lambda _person_id: [{"show_id": "show-1"}, {"show_id": "show-2"}],
    )
    monkeypatch.setattr(mod, "_discover_and_persist_person_links", lambda *args, **kwargs: 0)
    monkeypatch.setattr(mod, "_load_approved_person_links", lambda *args, **kwargs: [])
    monkeypatch.setattr(mod, "_refresh_tmdb_profile", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(mod, "_refresh_imdb_profile", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(mod, "_refresh_fandom_profile", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(mod, "_refresh_wikipedia_profile", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(mod, "_refresh_bravo_profile", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(
        mod,
        "_refresh_credits_for_related_shows",
        lambda related_shows: (len(related_shows), ["show-2:episode_appearances"]),
    )

    result = mod._run_person_profile_refresh(
        person_id=person_id,
        payload=mod.RefreshProfileRequest(),
        db=cast("Any", None),
        actor="admin@example.com",
    )

    assert result["shows_processed"] == 2
    assert result["credits_updated"] == 1


def test_run_person_profile_refresh_reports_missing_sources_as_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person_id = str(uuid4())
    monkeypatch.setattr(
        mod,
        "_load_person",
        lambda *, person_id: {
            "id": person_id,
            "full_name": "Andy Cohen",
            "external_ids": {},
            "birthday": {},
            "gender": {},
            "biography": {},
            "place_of_birth": {},
            "homepage": {},
            "profile_image_url": {},
            "alternative_names": {},
        },
    )
    monkeypatch.setattr(mod, "_load_related_shows_for_person", lambda _person_id: [{"show_id": "show-1"}])
    monkeypatch.setattr(mod, "_discover_and_persist_person_links", lambda *args, **kwargs: 0)
    monkeypatch.setattr(mod, "_load_approved_person_links", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        mod,
        "_refresh_tmdb_profile",
        lambda *args, **kwargs: (_ for _ in ()).throw(mod.ProfileSourceSkippedError("No TMDb person id available.")),
    )
    monkeypatch.setattr(
        mod,
        "_refresh_imdb_profile",
        lambda *args, **kwargs: (_ for _ in ()).throw(mod.ProfileSourceSkippedError("No IMDb profile link available.")),
    )
    monkeypatch.setattr(
        mod,
        "_refresh_fandom_profile",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            mod.ProfileSourceSkippedError("No Fandom profile link available.")
        ),
    )
    monkeypatch.setattr(
        mod,
        "_refresh_wikipedia_profile",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            mod.ProfileSourceSkippedError("No Wikipedia profile link available.")
        ),
    )
    monkeypatch.setattr(
        mod,
        "_refresh_bravo_profile",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            mod.ProfileSourceSkippedError("No BravoTV profile link available.")
        ),
    )
    monkeypatch.setattr(mod, "_refresh_credits_for_related_shows", lambda related_shows: (len(related_shows), []))

    result = mod._run_person_profile_refresh(
        person_id=person_id,
        payload=mod.RefreshProfileRequest(refresh_links=False, refresh_credits=False),
        db=cast("Any", None),
        actor="admin@example.com",
    )

    assert result["status"] == "ok"
    assert result["failures"] == []
    assert len(result["skips"]) == 5
    assert any(str(item).startswith("profile_tmdb:") for item in result["skips"])
    assert any(str(item).startswith("profile_imdb:") for item in result["skips"])


def test_build_refresh_profile_event_stream_emits_progress_and_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    person_id = str(uuid4())

    def _fake_run(*, person_id: str, payload, db, actor: str, stage_callback=None):
        assert payload.refresh_links is True
        assert actor == "admin@example.com"
        if stage_callback is not None:
            stage_callback("load_context", {"stage": "load_context", "message": "Loaded context."})
            stage_callback("credits_refresh", {"stage": "credits_refresh", "message": "Refreshing credits..."})
        return {
            "person_id": person_id,
            "status": "ok",
            "links_refreshed": 4,
            "aliases_added": 2,
            "profile_fields_changed": 3,
            "shows_processed": 2,
            "credits_updated": 2,
            "failures": [],
            "skips": [],
        }

    monkeypatch.setattr(mod, "_run_person_profile_refresh", _fake_run)

    events = list(
        mod._build_refresh_profile_event_stream(
            person_id=person_id,
            payload=mod.RefreshProfileRequest(),
            db=cast("Any", None),
            actor="admin@example.com",
        )
    )
    parsed = _parse_sse_events(events)

    assert parsed[0][0] == "progress"
    assert parsed[0][1]["stage"] == "starting"
    assert any(event == "progress" and payload.get("stage") == "load_context" for event, payload in parsed)
    assert any(event == "progress" and payload.get("stage") == "credits_refresh" for event, payload in parsed)
    assert parsed[-1][0] == "complete"
    assert parsed[-1][1]["summary"]["links_refreshed"] == 4
