from __future__ import annotations

from pathlib import Path


def test_instagram_auth_runtime_default_cookie_path_points_to_backend_scripts(monkeypatch) -> None:
    from trr_backend.socials.instagram import auth_runtime

    monkeypatch.delenv("SOCIAL_INSTAGRAM_COOKIES_FILE", raising=False)
    monkeypatch.delenv("INSTAGRAM_COOKIES_FILE", raising=False)

    backend_root = Path(__file__).resolve().parents[2]
    expected = backend_root / "scripts" / "socials" / "instagram" / "instagram_cookies.json"

    assert auth_runtime._default_instagram_cookie_file_path() == expected
    assert expected in auth_runtime._instagram_cookie_file_candidates()
