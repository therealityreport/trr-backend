from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

import scripts.socials.backfill_tiktok_saves as mod


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self.calls: list[str] = []

    def get(self, url: str, *, headers, cookies, timeout):  # noqa: ANN001
        del headers, cookies, timeout
        self.calls.append(url)
        return self._responses.pop(0)


def _html_with_item(video_id: str, collect_count: str) -> str:
    return (
        '<script id="SIGI_STATE">'
        f'{{"ItemModule": {{"{video_id}": {{"id": "{video_id}", "statsV2": {{"collectCount": "{collect_count}"}}}}}}}}'
        "</script>"
    )


def test_parse_args_requires_season_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["backfill_tiktok_saves.py"])
    with pytest.raises(SystemExit):
        mod._parse_args()


def test_extract_saves_from_item_supports_shorthand_metrics() -> None:
    item = {"statsV2": {"collectCount": "4.2K"}}
    assert mod._extract_saves_from_item(item) == 4200


def test_candidate_video_urls_prefers_canonical_first() -> None:
    assert mod._candidate_video_urls(account="creator", video_id="123") == [
        "https://www.tiktok.com/@_/video/123",
        "https://www.tiktok.com/@creator/video/123",
    ]


def test_fetch_saves_retries_handle_url_after_canonical_miss() -> None:
    session = _FakeSession(
        [
            _FakeResponse("<html><body>no payload</body></html>"),
            _FakeResponse(_html_with_item("123", "4.2K")),
        ]
    )
    saves, error = mod._fetch_saves(
        session=cast(Any, session),
        cookies={},
        video_urls=mod._candidate_video_urls(account="creator", video_id="123"),
        video_id="123",
    )

    assert error is None
    assert saves == 4200
    assert session.calls == [
        "https://www.tiktok.com/@_/video/123",
        "https://www.tiktok.com/@creator/video/123",
    ]


def test_load_candidate_rows_filters_to_missing_or_zero_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(mod.pg, "fetch_all", _fake_fetch_all)

    rows = mod._load_candidate_rows(season_id="season-1", limit=50, offset=10, has_saves_column=True)

    assert rows == []
    assert "where existing_saves <= 0" in str(captured["query"])
    assert captured["params"] == ["season-1", 50, 10]


def test_main_dry_run_skips_write_when_value_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: SimpleNamespace(
            season_id="season-1",
            limit=0,
            offset=0,
            delay_seconds=0.0,
            dry_run=True,
        ),
    )
    monkeypatch.setattr(mod.social_repo, "_platform_posts_has_column", lambda platform, column: True)
    monkeypatch.setattr(mod, "_resolve_season_id", lambda **_: "season-1")
    monkeypatch.setattr(
        mod,
        "_load_candidate_rows",
        lambda **_: [
            {
                "id": "row-1",
                "video_id": "123",
                "account": "creator",
                "existing_saves": 4200,
            }
        ],
    )
    monkeypatch.setattr(mod.social_repo, "_load_tiktok_cookies", lambda: {})
    monkeypatch.setattr(mod, "_fetch_saves", lambda **_: (4200, None))

    updated: list[tuple[str, int, bool]] = []

    def _record_update(*, row_id: str, saves: int, has_saves_column: bool) -> None:
        updated.append((row_id, saves, has_saves_column))

    monkeypatch.setattr(mod, "_update_row", _record_update)
    monkeypatch.setattr(mod.time, "sleep", lambda _: None)

    assert mod.main() == 0
    assert updated == []
