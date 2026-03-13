from __future__ import annotations

from types import SimpleNamespace

import scripts.backfill.backfill_media_assets as mod


def _base_args(**overrides):
    values = {
        "entity_type": "all",
        "table": None,
        "limit": None,
        "with_variants": False,
        "with_crops": False,
        "verbose": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_resolve_entity_type_supports_new_legacy_tables() -> None:
    assert mod._resolve_entity_type(_base_args(table="season_images")) == "season"
    assert mod._resolve_entity_type(_base_args(table="episode_images")) == "episode"
    assert mod._resolve_entity_type(_base_args(table="cast_photos")) == "cast"


def test_main_backfills_season_episode_and_cast_with_variants(monkeypatch) -> None:
    db = object()
    monkeypatch.setattr(mod, "_parse_args", lambda _argv: _base_args(with_variants=True, with_crops=True))
    monkeypatch.setattr(mod, "load_env_and_db", lambda: db)
    monkeypatch.setattr(mod, "_fetch_show_images", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        mod,
        "_fetch_season_images",
        lambda *_args, **_kwargs: [{"id": "season-row-1", "season_id": "season-1", "source": "tmdb", "kind": "poster"}],
    )
    monkeypatch.setattr(
        mod,
        "_fetch_episode_images",
        lambda *_args, **_kwargs: [
            {"id": "episode-row-1", "episode_id": "episode-1", "source": "tmdb", "kind": "still"}
        ],
    )
    monkeypatch.setattr(mod, "_fetch_person_images", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        mod,
        "_fetch_cast_photos",
        lambda *_args, **_kwargs: [{"id": "cast-row-1", "person_id": "person-1", "source": "imdb"}],
    )

    upsert_calls: list[tuple[str, list[dict[str, str]]]] = []

    def _fake_upsert_media_with_links(_db, rows, *, entity_type: str):
        upsert_calls.append((entity_type, list(rows)))
        return ([{"id": f"{entity_type}-asset-1", "metadata": {"thumbnail_crop": {"x": 10, "y": 20, "zoom": 1.5}}}], [])

    monkeypatch.setattr(mod, "upsert_media_with_links", _fake_upsert_media_with_links)

    variant_calls: list[tuple[str, object | None]] = []

    def _fake_generate_media_asset_variants(_db, *, asset_id: str, force: bool, crop=None):
        variant_calls.append((asset_id, crop))
        return []

    monkeypatch.setattr(mod, "generate_media_asset_variants", _fake_generate_media_asset_variants)

    assert mod.main([]) == 0

    assert [entity_type for entity_type, _rows in upsert_calls] == ["season", "episode", "cast"]
    assert variant_calls == [
        ("season-asset-1", None),
        ("season-asset-1", {"x": 10, "y": 20, "zoom": 1.5}),
        ("episode-asset-1", None),
        ("episode-asset-1", {"x": 10, "y": 20, "zoom": 1.5}),
        ("cast-asset-1", None),
        ("cast-asset-1", {"x": 10, "y": 20, "zoom": 1.5}),
    ]
