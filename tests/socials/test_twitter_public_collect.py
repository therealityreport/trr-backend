import copy
import json

import pytest

from trr_backend.socials.twitter.public_collect import collect_public, preview
from trr_backend.socials.twitter.public_models import normalize
from trr_backend.socials.twitter.public_transport import _validate
from trr_backend.socials.twitter.public_verification import verify_parent


def post(id="100", **kw):
    return {
        "id": id,
        "text": "#RHOSLC",
        "author": {"screen_name": "BravoTV", "id": "15169907"},
        "created_at": "2026-11-12T10:00:00Z",
        **kw,
    }


def page(rows, cursor=None):
    return {"code": 200, "results": rows, "cursor": {"bottom": cursor}}


def serve(*pages):
    pending = iter(pages)

    def read(url, **kw):
        p = next(pending)
        if isinstance(p, Exception):
            raise p
        return json.dumps(p).encode()

    return read


def test_unknown_and_metric_mapping_and_full_media():
    p = normalize(
        post(
            reposts=12,
            quotes=4,
            likes=0,
            is_note_tweet=True,
            media={
                "photos": [{"url": "https://img"}],
                "videos": [{"url": "https://video", "thumbnail_url": "https://poster"}],
            },
        )
    )
    assert (p.retweets, p.quotes, p.likes) == (12, 4, 0)
    assert p.views is None and p.bookmarks is None and p.shares is None and p.user_verified is None
    assert p.long_text is True and len(p.media) == 3 and len(p.media_urls) == 2
    assert normalize(post(created_at=None)).created_at is None
    assert normalize(post(likes=True)).likes is None


def test_local_author_hashtag_and_exclusive_dates_continue_to_fill():
    r = collect_public(
        "from:BravoTV #RHOSLC since:2026-11-01 until:2026-11-13",
        max_items=2,
        fetcher=serve(
            page([post("100", created_at="2026-11-13T00:00:00Z"), post("101", text="#RHOSLCextra")], "next"),
            page([post("102"), post("103", created_at="2026-11-01T00:00:00Z")]),
        ),
    )
    assert [p["tweet_id"] for p in r["posts"]] == ["102", "103"]
    assert r["metadata"]["rejected"] == 2 and r["metadata"]["pages_scanned"] == 2
    assert r["metadata"]["complete"] is False


@pytest.mark.parametrize("capability", ["search", "list", "profile"])
def test_two_pages(capability):
    r = collect_public(
        "#RHOSLC" if capability == "search" else "78783491" if capability == "list" else "@BravoTV",
        capability=capability,
        fetcher=serve(page([post()], "next"), page([post("101")])),
    )
    assert len(r["posts"]) == 2 and r["metadata"]["pages"][1]["new_ids"] == 1


def test_profile_404_uses_identity_only():
    urls = []
    mock = serve(ValueError("Mirror HTTP 404"), page([post("999")]), page([post("100")]))

    def fetch(url, **kw):
        urls.append(url)
        return mock(url, **kw)

    r = collect_public("BravoTV", capability="profile", fetcher=fetch)
    assert [p["tweet_id"] for p in r["posts"]] == ["100"]
    assert "/profile/id:15169907/statuses" in urls[-1]
    assert r["metadata"]["requests"] == 3


def test_empty_page_with_cursor_and_repeated_page_resume():
    r = collect_public("#RHOSLC", max_pages=2, fetcher=serve(page([], "a"), page([post()], "b")))
    assert len(r["posts"]) == 1
    resumed = collect_public("#RHOSLC", mode="resume", state=r["state"], fetcher=serve(page([post()], "c")))
    assert resumed["metadata"]["stop_reason"] == "repeated-page"


def test_refresh_resume_updates_metrics_preserves_state():
    base = collect_public("#RHOSLC", max_items=1, fetcher=serve(page([post(likes=1)])))
    refreshed = collect_public(
        "#RHOSLC",
        max_items=1,
        mode="refresh",
        state=base["state"],
        max_pages=1,
        fetcher=serve(page([post("101", created_at="2026-11-13T00:00:00Z")], "next")),
    )
    r = collect_public(
        "#RHOSLC", max_items=1, mode="resume", state=refreshed["state"], fetcher=serve(page([post(likes=3)]))
    )
    assert r["metadata"]["metrics_updated"] == 1 and r["posts"][0]["tweet_id"] == "101"
    assert len(r["state"]["posts"]) == 2 and base["state"]["posts"][0]["likes"] == 1


def test_cancellation_zero_and_blocked_preserve_partial():
    assert collect_public("#RHOSLC", max_items=0, fetcher=serve())["metadata"]["requests"] == 0
    assert collect_public("#RHOSLC", cancelled=lambda: True, fetcher=serve())["metadata"]["stop_reason"] == "cancelled"
    r = collect_public("#RHOSLC", fetcher=serve(page([post()], "a"), ValueError("Mirror HTTP 429")))
    assert len(r["posts"]) == 1 and r["metadata"]["stop_reason"] == "blocked"


def test_malformed_and_incompatible_state_no_mutation():
    r = collect_public("#RHOSLC", fetcher=serve(page([post()], "a")), max_pages=1)
    state = copy.deepcopy(r["state"])
    with pytest.raises(ValueError, match="Incompatible"):
        collect_public("different", state=state, mode="resume")
    assert state == r["state"]
    bad = collect_public("#RHOSLC", fetcher=serve(page([{"id": "bad"}])))
    assert bad["metadata"]["status"] == "partial" and bad["posts"] == []


def test_conversation_root_and_explicit_edges_only():
    data = {"code": 200, "status": post(), "thread": [], "replies": [post("101", replying_to={"status": "999"})]}
    r = collect_public("100", capability="conversation", fetcher=serve(data))
    assert r["metadata"]["missing_parents"] == ["999"]
    bad = collect_public("102", capability="conversation", fetcher=serve(data))
    assert bad["posts"] == []


def test_provider_semantics_and_url_allowlist():
    assert preview("#RHOSLC OR #RHOBH -from:BravoTV")["filters"]["author"] is None
    for url in ["https://api.fxtwitter.com/2/search?q=x", "https://api.fxtwitter.com/2/profile/BravoTV/statuses"]:
        _validate(url)
    for url in [
        "https://api.fxtwitter.com/2/status/123?token=secret",
        "https://user:pass@api.fxtwitter.com/2/status/123",
        "https://api.fxtwitter.com/evil",
    ]:
        with pytest.raises(ValueError):
            _validate(url)


def test_independent_parent_requires_same_child():
    p = {"tweet_id": "100", "reply_to_tweet_id": "99"}
    r = verify_parent(p, deadline=10**12, fetcher=serve({"id_str": "100", "in_reply_to_status_id_str": "99"}))
    assert r["status"] == "verified"
    r = verify_parent(p, deadline=10**12, fetcher=serve({"id_str": "101", "in_reply_to_status_id_str": "99"}))
    assert r["status"] == "unavailable"


def test_quote_reply_long_text_and_metric_nulls():
    text = "#RHOSLC " + "long body " * 60
    p = normalize(
        post(text=text, is_note_tweet=True, quote={"id": "99", "text": "quoted"}, replying_to={"status": "98"})
    )
    assert p.text == text and p.long_text is True
    assert p.quoted_tweet_id == "99" and p.reply_to_tweet_id == "98"
    assert p.quotes is None and p.is_quote is True


def test_checkpoint_item_cap_bound_and_timeout_retains_partial():
    r = collect_public("#RHOSLC", max_items=1, fetcher=serve(page([post(), post("101")], "next")))
    with pytest.raises(ValueError, match="Incompatible"):
        collect_public("#RHOSLC", max_items=2, state=r["state"], mode="resume")
    r = collect_public("#RHOSLC", fetcher=serve(page([post()], "a"), ValueError("Public provider deadline")))
    assert len(r["posts"]) == 1 and r["metadata"]["stop_reason"] == "limit-reached"


def test_terminal_cursor_issue_survives_resume_and_typed_state():
    r = collect_public("#RHOSLC", fetcher=serve(page([post()], "same"), page([post("101")], "same")))
    assert r["metadata"]["stop_reason"] == "repeated-cursor"
    resumed = collect_public("#RHOSLC", state=r["state"], mode="resume", fetcher=serve())
    assert resumed["metadata"]["status"] == "partial"
    assert resumed["metadata"]["stop_reason"] == "repeated-cursor"
    bad = copy.deepcopy(r["state"])
    bad["posts"][0]["likes"] = "12"
    with pytest.raises(ValueError, match="numeric"):
        collect_public("#RHOSLC", state=bad, mode="resume", fetcher=serve())


def test_context_shared_and_optional_field_validation():
    data = {
        "code": 200,
        "status": post(),
        "thread": [post("101")],
        "replies": [post("102", replying_to={"status": "101"})],
    }
    r = collect_public("100", capability="conversation", fetcher=serve(data))
    by_id = {p["tweet_id"]: p for p in r["posts"]}
    assert [by_id[k]["twitter_context_role"] for k in ("100", "101", "102")] == ["root", "thread", "reply"]
    assert all(p["thread_root_tweet_id"] == "100" for p in r["posts"])
    assert normalize(post(author={"screen_name": "BravoTV", "name": {}})).display_name is None
    with pytest.raises(ValueError, match="ID"):
        normalize(post(quote={"id": "unrelated"}))


def test_refresh_page_checkpoint_is_terminal_before_interruption():
    initial = collect_public("#RHOSLC", fetcher=serve(page([post("100"), post("101")])))
    checkpoints = []

    def save(state):
        checkpoints.append(state)
        if state["known_pages"] == 2:
            raise RuntimeError("worker interrupted after durable checkpoint")

    with pytest.raises(RuntimeError):
        collect_public(
            "#RHOSLC",
            mode="refresh",
            state=initial["state"],
            fetcher=serve(page([post("100")], "a"), page([post("101")], "b")),
            on_checkpoint=save,
        )
    resumed = collect_public("#RHOSLC", mode="resume", state=checkpoints[-1], fetcher=serve())
    assert resumed["metadata"]["requests"] == 0
    assert resumed["metadata"]["stop_reason"] == "known-records-reached"


def test_conversation_context_keeps_thread_role_with_duplicate_and_later_reply():
    first = {
        "code": 200,
        "status": post("100"),
        "thread": [post("101")],
        "replies": [post("101"), post("102")],
        "cursor": {"bottom": "a"},
    }
    second = {
        "code": 200,
        "status": post("100"),
        "thread": [],
        "replies": [post("101", likes=5), post("103")],
        "cursor": {"bottom": None},
    }
    result = collect_public("100", capability="conversation", fetcher=serve(first, second))
    found = {p["tweet_id"]: p for p in result["posts"]}
    assert len(found) == 4
    assert found["101"]["twitter_context_role"] == "thread"
    assert found["101"]["is_thread_part"] is True
    assert found["101"]["likes"] == 5
    assert found["100"]["twitter_context_role"] == "root"
