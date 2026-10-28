from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from trr_backend.socials.tiktok.transports import public_profile as module


def _item(video_id: str = "123", owner: str = "bravotv") -> dict:
    return {
        "id": video_id,
        "createTime": 1_780_000_000,
        "desc": "A public post #Bravo",
        "author": {"uniqueId": owner, "nickname": "Bravo"},
        "stats": {"playCount": 100, "diggCount": 12, "commentCount": 3},
        "video": {"duration": 9},
    }


def _install_browser(monkeypatch, payloads, *, fail=False):
    captured = {}
    listeners = {}

    def fetch(url, **kwargs):
        captured.update(url=url, **kwargs)
        page = SimpleNamespace(
            on=lambda name, callback: listeners.update({name: callback}),
            route=lambda pattern, callback: captured.update(route_pattern=pattern, route=callback),
            wait_for_timeout=lambda _ms: None,
            evaluate=lambda _script: None,
        )
        kwargs["page_setup"](page)
        for cursor, payload in payloads:

            def decode(value=payload):
                if isinstance(value, Exception):
                    raise value
                return value

            listeners["response"](
                SimpleNamespace(
                    url=f"https://www.tiktok.com/api/post/item_list/?cursor={cursor}&msToken=private-token",
                    ok=True,
                    json=decode,
                )
            )
        if fail:
            raise RuntimeError("cookie=private-cookie https://proxy-user:password@proxy.invalid")
        # Exercise the collection exit after the page budget, without a real browser.
        if payloads and not isinstance(payloads[-1][1], Exception):
            kwargs["page_action"](page)
        return SimpleNamespace(status=200)

    monkeypatch.setattr(module, "build_stealthy_fetcher", lambda: SimpleNamespace(fetch=fetch))
    return captured, listeners


def test_anonymous_first_page_survives_non_json_and_stops_prefetch(monkeypatch):
    captured, _ = _install_browser(
        monkeypatch,
        [
            ("0", ValueError("not JSON")),
            ("0", {"statusCode": 0, "itemList": [_item()], "hasMore": True}),
            ("100", {"statusCode": 0, "itemList": [_item("456")], "hasMore": True}),
        ],
    )
    posts, metadata = module.scrape_public_profile_posts(account_handle="bravotv", max_pages=1, page_size=30)

    assert [post.video_id for post in posts] == ["123"]
    assert posts[0].username == "bravotv"
    assert metadata["pages_scanned"] == 1
    assert metadata["response_failures"] == 1
    assert metadata["stop_reason"] == "max_pages"
    assert metadata["auth_mode"] == "public_anonymous"
    assert captured["cookies"] == []
    assert "proxy" not in captured and "proxy_rotator" not in captured
    assert "private-token" not in json.dumps(metadata)

    assert captured["route_pattern"] == "**/api/comment/**"
    actions = []
    captured["route"](SimpleNamespace(abort=lambda: actions.append("abort")))
    assert actions == ["abort"]


def test_public_listing_deduplicates_cursors_and_ids_and_enforces_cap(monkeypatch):
    _install_browser(
        monkeypatch,
        [
            ("0", {"statusCode": 0, "itemList": [_item()], "hasMore": True}),
            ("0", {"statusCode": 0, "itemList": [_item("999")], "hasMore": True}),
            ("100", {"statusCode": 0, "itemList": [_item(), _item("456"), _item("789")], "hasMore": True}),
        ],
    )
    posts, metadata = module.scrape_public_profile_posts(account_handle="bravotv", max_pages=2, page_size=1)
    assert [post.video_id for post in posts] == ["123", "456"]
    assert metadata["pages_scanned"] == 2
    assert metadata["post_cap"] == 2


def test_public_listing_rejects_unbound_owner(monkeypatch):
    _install_browser(
        monkeypatch, [("0", {"statusCode": 0, "itemList": [_item(owner="anotheraccount")], "hasMore": False})]
    )
    posts, metadata = module.scrape_public_profile_posts(account_handle="bravotv", max_pages=1, page_size=30)
    assert posts == []
    assert metadata["owner_rejections"] == 1


def test_public_listing_browser_error_does_not_publish_partial_or_secrets(monkeypatch):
    _install_browser(monkeypatch, [("0", {"statusCode": 0, "itemList": [_item()], "hasMore": True})], fail=True)
    posts, metadata = module.scrape_public_profile_posts(account_handle="bravotv", max_pages=2, page_size=30)
    assert posts == []
    assert metadata["error_type"] == "RuntimeError"
    assert "private-cookie" not in json.dumps(metadata)
    assert "password" not in json.dumps(metadata)


@pytest.mark.parametrize(("account", "budget"), [("bad/account", 1), ("bravotv", 0)])
def test_public_listing_requires_explicit_valid_budget(account, budget):
    with pytest.raises(ValueError):
        module.scrape_public_profile_posts(account_handle=account, max_pages=budget, page_size=30)


def test_public_listing_retains_posts_when_cap_precedes_page_budget(monkeypatch):
    _install_browser(
        monkeypatch,
        [("0", {"statusCode": 0, "itemList": [_item(str(n)) for n in range(100, 130)], "hasMore": True})],
    )
    posts, metadata = module.scrape_public_profile_posts(account_handle="bravotv", max_pages=2, page_size=10)
    assert len(posts) == 20
    assert metadata["pages_scanned"] == 1
    assert metadata["post_cap"] == 20
    assert metadata["stop_reason"] == "max_posts_reached"


def test_page_sink_commits_before_next_page_and_survives_browser_failure(monkeypatch):
    _install_browser(monkeypatch, [("0", {"statusCode": 0, "itemList": [_item()], "hasMore": True})], fail=True)
    committed = []
    posts, metadata = module.scrape_public_profile_posts(
        account_handle="bravotv",
        max_pages=2,
        page_size=30,
        on_page=lambda posts, checkpoint: committed.append(([p.video_id for p in posts], checkpoint)),
    )
    assert committed[0][0] == ["123"]
    assert [p.video_id for p in posts] == ["123"]
    assert metadata["partial"] is True


def test_persistence_failure_is_propagated_and_stops_capture(monkeypatch):
    _install_browser(monkeypatch, [("0", {"statusCode": 0, "itemList": [_item()], "hasMore": True})])

    def fail(posts, checkpoint):
        raise RuntimeError("database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        module.scrape_public_profile_posts(account_handle="bravotv", max_pages=1, page_size=30, on_page=fail)


def test_resume_skips_only_committed_ids(monkeypatch):
    _install_browser(
        monkeypatch,
        [
            ("0", {"statusCode": 0, "itemList": [_item()], "hasMore": True}),
            ("100", {"statusCode": 0, "itemList": [_item("456")], "hasMore": False}),
        ],
    )
    saved = []
    posts, metadata = module.scrape_public_profile_posts(
        account_handle="bravotv",
        max_pages=1,
        page_size=30,
        checkpoint_ids=["123"],
        persisted_ids=["123"],
        on_page=lambda posts, checkpoint: saved.extend(p.video_id for p in posts),
    )
    assert saved == ["456"]
    assert metadata["checkpoint_reached"] is True
    assert metadata["partial"] is False


def test_missing_checkpoint_never_claims_complete(monkeypatch):
    _install_browser(monkeypatch, [("0", {"statusCode": 0, "itemList": [_item()], "hasMore": False})])
    saved = []
    _, metadata = module.scrape_public_profile_posts(
        account_handle="bravotv",
        max_pages=1,
        page_size=30,
        checkpoint_ids=["999"],
        on_page=lambda posts, checkpoint: saved.extend(posts),
    )
    assert [post.video_id for post in saved] == ["123"]
    assert metadata["stop_reason"] == "checkpoint_not_reached"
    assert metadata["partial"] is True


def test_shifted_replay_preserves_new_posts_on_both_sides_of_frontier(monkeypatch):
    _install_browser(
        monkeypatch,
        [
            ("0", {"statusCode": 0, "itemList": [_item("999"), _item("123")], "hasMore": True}),
            ("100", {"statusCode": 0, "itemList": [_item("456"), _item("789")], "hasMore": False}),
        ],
    )
    saved = []
    _, metadata = module.scrape_public_profile_posts(
        account_handle="bravotv",
        max_pages=5,
        page_size=30,
        checkpoint_ids=["123", "456"],
        persisted_ids=["123", "456"],
        on_page=lambda posts, checkpoint: saved.extend(p.video_id for p in posts),
    )
    assert saved == ["999", "789"]
    assert metadata["partial"] is False


def test_terminal_parse_failure_is_never_complete(monkeypatch):
    bad = _item()
    bad["createTime"] = "invalid"
    _install_browser(monkeypatch, [("0", {"statusCode": 0, "itemList": [bad], "hasMore": False})])
    _, metadata = module.scrape_public_profile_posts(account_handle="bravotv", max_pages=1, page_size=30)
    assert metadata["partial"] is True
    assert metadata["response_failures"] == 1
