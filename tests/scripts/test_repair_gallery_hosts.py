from __future__ import annotations

import importlib

import pytest

mod = importlib.import_module("scripts.media.repair_gallery_hosts")


def _candidate(
    *,
    kind: mod.CandidateKind,
    row_id: str,
    hosted_url: str | None,
    source_url: str | None,
) -> mod.RepairCandidate:
    if kind == "media_link_asset":
        row = {
            "id": row_id,
            "source": "imdb",
            "source_url": source_url,
            "hosted_url": hosted_url,
            "metadata": {},
        }
        return mod.RepairCandidate(
            kind=kind,
            person_id="person-1",
            source="imdb",
            source_url=source_url,
            hosted_url=hosted_url,
            row_id=row_id,
            row=row,
            metadata={},
            link_id=f"link-{row_id}",
            link_context={},
            source_page_url=None,
        )
    row = {
        "id": row_id,
        "person_id": "person-1",
        "source": "imdb",
        "url": source_url,
        "hosted_url": hosted_url,
        "metadata": {},
    }
    return mod.RepairCandidate(
        kind=kind,
        person_id="person-1",
        source="imdb",
        source_url=source_url,
        hosted_url=hosted_url,
        row_id=row_id,
        row=row,
        metadata={},
        link_id=None,
        link_context=None,
        source_page_url=None,
    )


def test_repair_gallery_hosts_dry_run_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ok_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-ok",
        hosted_url="https://cdn.example.com/ok.jpg",
        source_url="https://source.example.com/ok.jpg",
    )
    repaired_candidate = _candidate(
        kind="cast_photo",
        row_id="cast-repair",
        hosted_url="https://cdn.example.com/fail.jpg",
        source_url="https://source.example.com/repair.jpg",
    )
    broken_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-broken",
        hosted_url="https://cdn.example.com/broken.jpg",
        source_url="https://source.example.com/broken.jpg",
    )
    monkeypatch.setattr(
        mod,
        "_collect_candidates",
        lambda *_args, **_kwargs: [ok_candidate, repaired_candidate, broken_candidate],
    )

    def fake_reachability(
        *, url: str | None, source: str, timeout: float, source_page_url: str | None
    ) -> tuple[bool, str]:
        del source, timeout, source_page_url
        if url and url.endswith("/ok.jpg"):
            return True, "http_200"
        if url and url.endswith("/repair.jpg"):
            return True, "http_200"
        return False, "http_403"

    monkeypatch.setattr(mod, "_check_url_reachability", fake_reachability)
    monkeypatch.setattr(mod, "_repair_candidate", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(mod, "_mark_candidate_broken", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=False,
        timeout=1.0,
        verbose=False,
    )

    assert report["summary"]["scanned"] == 3
    assert report["summary"]["ok"] == 1
    assert report["summary"]["repaired"] == 1
    assert report["summary"]["broken_unreachable"] == 1
    assert report["summary"]["error"] == 0
    assert report["repaired_ids"] == ["cast-repair"]
    assert report["broken_ids"] == ["asset-broken"]


def test_repair_gallery_hosts_apply_mutates_expected_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repaired_candidate = _candidate(
        kind="cast_photo",
        row_id="cast-repair",
        hosted_url="https://cdn.example.com/fail.jpg",
        source_url="https://source.example.com/repair.jpg",
    )
    broken_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-broken",
        hosted_url="https://cdn.example.com/broken.jpg",
        source_url="https://source.example.com/broken.jpg",
    )
    monkeypatch.setattr(
        mod,
        "_collect_candidates",
        lambda *_args, **_kwargs: [repaired_candidate, broken_candidate],
    )

    def fake_reachability(
        *, url: str | None, source: str, timeout: float, source_page_url: str | None
    ) -> tuple[bool, str]:
        del source, timeout, source_page_url
        if url and url.endswith("/repair.jpg"):
            return True, "http_200"
        return False, "http_403"

    repaired_calls: list[str] = []
    broken_calls: list[str] = []

    monkeypatch.setattr(mod, "_check_url_reachability", fake_reachability)
    monkeypatch.setattr(
        mod,
        "_repair_candidate",
        lambda _db, candidate, verbose: repaired_calls.append(candidate.row_id),
    )
    monkeypatch.setattr(
        mod,
        "_mark_candidate_broken",
        lambda _db, candidate, reason: broken_calls.append(candidate.row_id),
    )

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=True,
        timeout=1.0,
        verbose=False,
    )

    assert repaired_calls == ["cast-repair"]
    assert broken_calls == ["asset-broken"]
    assert report["summary"]["repaired"] == 1
    assert report["summary"]["broken_unreachable"] == 1


def test_mark_candidate_broken_updates_media_link_context_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-1",
        hosted_url="https://cdn.example.com/a.jpg",
        source_url="https://source.example.com/a.jpg",
    )
    context_updates: list[tuple[str, dict[str, object]]] = []
    cast_updates: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        mod,
        "_update_media_link_context",
        lambda _db, link_id, context: context_updates.append((link_id, context)),
    )
    monkeypatch.setattr(
        mod,
        "_update_cast_photo",
        lambda _db, photo_id, patch: cast_updates.append((photo_id, patch)),
    )

    mod._mark_candidate_broken(object(), candidate, reason="hosted=http_403;source=http_404")

    assert len(context_updates) == 1
    assert context_updates[0][0] == "link-asset-1"
    assert context_updates[0][1]["gallery_status"] == "broken_unreachable"
    assert cast_updates == []


def test_repair_candidate_regenerates_base_and_crop_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    media_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-1",
        hosted_url="https://cdn.example.com/asset.jpg",
        source_url="https://source.example.com/asset.jpg",
    )
    media_candidate = mod.RepairCandidate(
        **{
            **media_candidate.__dict__,
            "metadata": {},
            "link_context": {"thumbnail_crop": {"x": 50, "y": 30, "zoom": 1.4, "mode": "auto"}},
        }
    )
    cast_candidate = _candidate(
        kind="cast_photo",
        row_id="cast-1",
        hosted_url="https://cdn.example.com/cast.jpg",
        source_url="https://source.example.com/cast.jpg",
    )
    cast_candidate = mod.RepairCandidate(
        **{
            **cast_candidate.__dict__,
            "metadata": {"thumbnail_crop": {"x": 52, "y": 40, "zoom": 1.2, "mode": "manual"}},
        }
    )

    media_updates: list[str] = []
    cast_updates: list[str] = []
    media_variant_calls: list[tuple[str, bool]] = []
    cast_variant_calls: list[tuple[str, bool]] = []

    monkeypatch.setattr(mod, "mirror_media_asset_row", lambda *_args, **_kwargs: {"hosted_url": "x"})
    monkeypatch.setattr(mod, "mirror_cast_photo_row", lambda *_args, **_kwargs: {"hosted_url": "x"})
    monkeypatch.setattr(mod, "_update_media_asset", lambda _db, asset_id, patch: media_updates.append(asset_id))
    monkeypatch.setattr(mod, "_update_cast_photo", lambda _db, photo_id, patch: cast_updates.append(photo_id))
    monkeypatch.setattr(
        mod,
        "generate_media_asset_variants",
        lambda _db, *, asset_id, crop, force: media_variant_calls.append((asset_id, crop is not None)),
    )
    monkeypatch.setattr(
        mod,
        "generate_cast_photo_variants",
        lambda _db, *, photo_id, crop, force: cast_variant_calls.append((photo_id, crop is not None)),
    )

    mod._repair_candidate(object(), media_candidate, verbose=False)
    mod._repair_candidate(object(), cast_candidate, verbose=False)

    assert media_updates == ["asset-1"]
    assert cast_updates == ["cast-1"]
    assert media_variant_calls == [("asset-1", False), ("asset-1", True)]
    assert cast_variant_calls == [("cast-1", False), ("cast-1", True)]
