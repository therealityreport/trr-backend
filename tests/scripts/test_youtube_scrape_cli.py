from __future__ import annotations

from pathlib import Path

from scripts.socials.youtube import scrape


def test_resolve_download_dir_defaults_outside_repo(monkeypatch) -> None:
    monkeypatch.delenv("TRR_WORKSPACE_CACHE_ROOT", raising=False)
    expected = Path.home() / "Library" / "Caches" / "TRR" / "youtube-downloads" / "bravo"
    assert scrape.resolve_download_dir(None, "bravo") == expected


def test_resolve_download_dir_honors_override(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TRR_WORKSPACE_CACHE_ROOT", str(tmp_path / "workspace-cache"))
    expected = (tmp_path / "workspace-cache" / "youtube-downloads" / "bravo").resolve()
    assert scrape.resolve_download_dir(None, "bravo") == expected


def test_resolve_download_dir_honors_explicit_path(tmp_path: Path) -> None:
    explicit = tmp_path / "custom-downloads"
    assert scrape.resolve_download_dir(str(explicit), "bravo") == explicit.resolve()
