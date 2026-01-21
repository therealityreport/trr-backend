from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from trr_backend.repositories import show_images as mod


@pytest.mark.skip(reason="Dual-write not yet implemented in show_images.py - Phase 2 future work")
def test_show_images_dual_write_failure_does_not_break(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_MEDIA_DUAL_WRITE", "1")

    sentinel = [{"id": "row-1"}]

    def fake_upsert_table(db, rows):  # noqa: ANN001
        return sentinel

    def fail_dual_write(*args, **kwargs):  # noqa: ANN001
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_upsert_show_images_table", fake_upsert_table)
    monkeypatch.setattr(mod, "upsert_media_with_links", fail_dual_write)

    result = mod.upsert_show_images(MagicMock(), [{"source": "tmdb", "tmdb_id": 1}])

    assert result == sentinel


@pytest.mark.skip(reason="Dual-write not yet implemented in show_images.py - Phase 2 future work")
def test_show_images_dual_write_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("ENABLE_MEDIA_DUAL_WRITE", raising=False)

    sentinel = [{"id": "row-2"}]
    dual_write_called = False

    def fake_upsert_table(db, rows):  # noqa: ANN001
        return sentinel

    def spy_dual_write(*args, **kwargs):  # noqa: ANN001
        nonlocal dual_write_called
        dual_write_called = True

    monkeypatch.setattr(mod, "_upsert_show_images_table", fake_upsert_table)
    monkeypatch.setattr(mod, "upsert_media_with_links", spy_dual_write)

    result = mod.upsert_show_images(MagicMock(), [{"source": "tmdb", "tmdb_id": 1}])

    assert result == sentinel
    assert dual_write_called is False
