from __future__ import annotations

import hashlib
import importlib

import pytest

mod = importlib.import_module("scripts.media.restore_changed_originals")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _candidate(
    *,
    table: mod.TableName,
    row_id: str,
    source_url: str,
    hosted_sha256: str,
) -> mod.Candidate:
    source = "imdb"
    if table == "cast_photos":
        row = {
            "id": row_id,
            "source": source,
            "url": source_url,
            "hosted_sha256": hosted_sha256,
        }
    else:
        row = {
            "id": row_id,
            "source": source,
            "source_url": source_url,
            "hosted_sha256": hosted_sha256,
        }
    return mod.Candidate(
        table=table,
        row_id=row_id,
        source=source,
        source_url=source_url,
        hosted_sha256=hosted_sha256,
        row=row,
    )


def test_restore_changed_originals_dry_run_classifies_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cast_match = _candidate(
        table="cast_photos",
        row_id="cast-match",
        source_url="https://example.com/match.jpg",
        hosted_sha256=_sha("match"),
    )
    cast_mismatch = _candidate(
        table="cast_photos",
        row_id="cast-mismatch",
        source_url="https://example.com/mismatch.jpg",
        hosted_sha256=_sha("stored"),
    )
    media_unreachable = _candidate(
        table="media_assets",
        row_id="media-unreachable",
        source_url="https://example.com/unreachable.jpg",
        hosted_sha256=_sha("any"),
    )
    media_error = _candidate(
        table="media_assets",
        row_id="media-error",
        source_url="https://example.com/error.jpg",
        hosted_sha256=_sha("any"),
    )

    monkeypatch.setattr(mod, "_fetch_cast_candidates", lambda *_args, **_kwargs: [cast_match, cast_mismatch])
    monkeypatch.setattr(
        mod,
        "_fetch_media_asset_candidates",
        lambda *_args, **_kwargs: [media_unreachable, media_error],
    )

    def fake_download(*, source_url: str, source: str, timeout: float) -> bytes:
        del source, timeout
        if source_url.endswith("/match.jpg"):
            return b"match"
        if source_url.endswith("/mismatch.jpg"):
            return b"remote"
        if source_url.endswith("/unreachable.jpg"):
            raise mod.SourceUnreachableError("http_status_404")
        raise RuntimeError("decoder_failed")

    monkeypatch.setattr(mod, "_download_source_bytes", fake_download)
    monkeypatch.setattr(mod, "_repair_candidate", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))

    report = mod.restore_changed_originals(
        object(),
        source="imdb",
        tables="both",
        limit=None,
        apply_updates=False,
        timeout=1.0,
        batch_size=50,
        verbose=False,
    )

    assert report["summary"]["scanned"] == 4
    assert report["summary"]["match"] == 1
    assert report["summary"]["mismatch"] == 1
    assert report["summary"]["unreachable"] == 1
    assert report["summary"]["error"] == 1
    assert report["summary"]["repaired"] == 0
    assert report["mismatch_ids"]["cast_photos"] == ["cast-mismatch"]
    assert report["unreachable_ids"]["media_assets"] == ["media-unreachable"]
    assert report["error_ids"]["media_assets"] == ["media-error"]


def test_restore_changed_originals_apply_repairs_only_mismatches_and_skips_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cast_match = _candidate(
        table="cast_photos",
        row_id="cast-match",
        source_url="https://example.com/match.jpg",
        hosted_sha256=_sha("match"),
    )
    cast_mismatch = _candidate(
        table="cast_photos",
        row_id="cast-mismatch",
        source_url="https://example.com/mismatch.jpg",
        hosted_sha256=_sha("stored"),
    )
    media_unreachable = _candidate(
        table="media_assets",
        row_id="media-unreachable",
        source_url="https://example.com/unreachable.jpg",
        hosted_sha256=_sha("any"),
    )

    monkeypatch.setattr(mod, "_fetch_cast_candidates", lambda *_args, **_kwargs: [cast_match, cast_mismatch])
    monkeypatch.setattr(mod, "_fetch_media_asset_candidates", lambda *_args, **_kwargs: [media_unreachable])

    def fake_download(*, source_url: str, source: str, timeout: float) -> bytes:
        del source, timeout
        if source_url.endswith("/match.jpg"):
            return b"match"
        if source_url.endswith("/mismatch.jpg"):
            return b"remote"
        raise mod.SourceUnreachableError("http_status_404")

    repaired: list[str] = []

    def fake_repair(_db, candidate: mod.Candidate) -> bool:
        repaired.append(candidate.row_id)
        return True

    monkeypatch.setattr(mod, "_download_source_bytes", fake_download)
    monkeypatch.setattr(mod, "_repair_candidate", fake_repair)

    report = mod.restore_changed_originals(
        object(),
        source="imdb",
        tables="both",
        limit=None,
        apply_updates=True,
        timeout=1.0,
        batch_size=50,
        verbose=False,
    )

    assert repaired == ["cast-mismatch"]
    assert report["summary"]["repaired"] == 1
    assert report["summary"]["unreachable"] == 1
    assert report["unreachable_ids"]["media_assets"] == ["media-unreachable"]


def test_repair_candidate_regenerates_base_variants_for_repaired_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cast_mismatch = _candidate(
        table="cast_photos",
        row_id="cast-mismatch",
        source_url="https://example.com/cast.jpg",
        hosted_sha256=_sha("stored"),
    )
    media_mismatch = _candidate(
        table="media_assets",
        row_id="media-mismatch",
        source_url="https://example.com/media.jpg",
        hosted_sha256=_sha("stored"),
    )
    media_match = _candidate(
        table="media_assets",
        row_id="media-match",
        source_url="https://example.com/media-match.jpg",
        hosted_sha256=_sha("same"),
    )

    monkeypatch.setattr(mod, "_fetch_cast_candidates", lambda *_args, **_kwargs: [cast_mismatch])
    monkeypatch.setattr(mod, "_fetch_media_asset_candidates", lambda *_args, **_kwargs: [media_mismatch, media_match])

    def fake_download(*, source_url: str, source: str, timeout: float) -> bytes:
        del source, timeout
        if source_url.endswith("/media-match.jpg"):
            return b"same"
        return b"remote"

    updated_rows: list[tuple[str, str]] = []
    cast_variant_calls: list[str] = []
    media_variant_calls: list[str] = []

    monkeypatch.setattr(mod, "_download_source_bytes", fake_download)
    monkeypatch.setattr(mod, "_update_row", lambda _db, *, table, row_id, patch: updated_rows.append((table, row_id)))
    monkeypatch.setattr(mod, "mirror_cast_photo_row", lambda *_args, **_kwargs: {"hosted_url": "https://cdn.example.com/cast.jpg"})
    monkeypatch.setattr(mod, "mirror_media_asset_row", lambda *_args, **_kwargs: {"hosted_url": "https://cdn.example.com/media.jpg"})
    monkeypatch.setattr(
        mod,
        "generate_cast_photo_variants",
        lambda _db, *, photo_id, crop, force: cast_variant_calls.append(photo_id),
    )
    monkeypatch.setattr(
        mod,
        "generate_media_asset_variants",
        lambda _db, *, asset_id, crop, force: media_variant_calls.append(asset_id),
    )

    report = mod.restore_changed_originals(
        object(),
        source="imdb",
        tables="both",
        limit=None,
        apply_updates=True,
        timeout=1.0,
        batch_size=50,
        verbose=False,
    )

    assert report["summary"]["mismatch"] == 2
    assert report["summary"]["match"] == 1
    assert sorted(updated_rows) == [
        ("cast_photos", "cast-mismatch"),
        ("media_assets", "media-mismatch"),
    ]
    assert cast_variant_calls == ["cast-mismatch"]
    assert media_variant_calls == ["media-mismatch"]
