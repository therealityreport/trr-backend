from __future__ import annotations

from scripts.socials import media_queue_guard as cli


def test_guard_blocks_when_media_stale_claims_exist(monkeypatch) -> None:
    monkeypatch.setattr(
        cli.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {"platform": "instagram", "stage": "media_mirror", "total": 1},
            {"platform": "instagram", "stage": "comment_media_mirror", "total": 1},
        ],
    )

    payload = cli.build_guard_payload(allow_stale=False)

    assert payload["ok"] is False
    assert payload["blocked"] is True
    assert payload["stale_media_claims"]["total"] == 2


def test_guard_allows_explicit_stale_override(monkeypatch) -> None:
    monkeypatch.setattr(
        cli.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [{"platform": "instagram", "stage": "media_mirror", "total": 1}],
    )

    payload = cli.build_guard_payload(allow_stale=True)

    assert payload["ok"] is True
    assert payload["blocked"] is False
    assert payload["allow_stale"] is True
