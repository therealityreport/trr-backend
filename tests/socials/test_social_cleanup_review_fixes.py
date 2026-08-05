from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

from trr_backend.socials import cookie_sources
from trr_backend.socials.instagram import media_mirror
from trr_backend.socials.socialblade import parser


def test_cookie_candidate_prefers_an_authenticated_session() -> None:
    selected = cookie_sources._select_preferred_cookie_candidate(
        [
            {"csrftoken": "first"},
            {"csrftoken": "second", "sessionid": "authenticated"},
        ],
        required_cookie_names_any=("csrftoken",),
    )

    assert selected["sessionid"] == "authenticated"


def test_media_source_update_treats_missing_field_unset_as_not_provided(monkeypatch) -> None:
    calls: list[tuple[object, ...]] = []
    provider = {
        "_instagram_posts_has_column": lambda *_args, **_kwargs: True,
        "pg": SimpleNamespace(
            db_cursor=lambda **_kwargs: nullcontext(object()),
            fetch_one_with_cursor=lambda *args: calls.append(args),
        ),
    }
    monkeypatch.setattr(media_mirror, "_LEGACY_NAMESPACE", None)
    monkeypatch.setattr(media_mirror, "_LEGACY_ORIGINALS", {})
    media_mirror._configure_legacy_provider(provider, {})

    media_mirror._update_instagram_post_source_media_fields(post_id="post-1")

    assert calls == []


def test_socialblade_chart_parsing_ignores_untrusted_rows() -> None:
    payload = parser._history_rows_to_metrics(
        [
            {"date": "2026-08-04", "followers": 10, "following": 2, "media_count": 3},
            "not-a-row",
        ],
        limit=14,
    )

    assert payload["row_count"] == 1
