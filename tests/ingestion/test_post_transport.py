from types import SimpleNamespace

import pytest

from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.post_transport import DetailSource, ScraplingListingSource, SourceFailure


def test_listing_callback_failure_escapes_legacy_recovery_and_closes():
    events = []

    class Fetcher:
        def __init__(self, **kwargs):
            self.args = kwargs
            assert kwargs["consume_credentials_only"] and not kwargs["allow_requests_recovery"]
            assert kwargs["identity_provider"] is None

        async def warmup(self, account):
            try:
                self.args["before_source_request"]("browser_warmup")
            except Exception:
                events.append("swallowed")
            events.append("sent")

        async def aclose(self):
            events.append("closed")

    def fence():
        raise LostOwnership("cancelled")

    source = ScraplingListingSource(
        account="bravotv", cookies=[], raw_cookies={}, browser_account_id=None, fetcher_factory=Fetcher
    )
    with pytest.raises(LostOwnership):
        source(cursor=None, pre_source_request=fence, source_observer=lambda payload: None)
    assert events == ["closed"]


def test_listing_returns_preparser_raw_evidence_and_rejects_failed_summary():
    events = []
    raw = {"data": {"items": []}}

    class Fetcher:
        failed = False

        def __init__(self, **kwargs):
            self.args = kwargs

        async def warmup(self, account):
            pass

        async def fetch_posts_page(self, account, cursor):
            self.args["before_source_request"]("httpx")
            self.args["source_payload_observer"]("listing", raw)
            events.append("parsed")
            return SimpleNamespace(fetch_failed=self.failed, auth_failed=False)

        async def aclose(self):
            events.append("closed")

    source = ScraplingListingSource(
        account="bravotv", cookies=[], raw_cookies={}, browser_account_id=None, fetcher_factory=Fetcher
    )
    callbacks = {
        "cursor": None,
        "pre_source_request": lambda: events.append("reserved"),
        "source_observer": lambda payload: events.append("retained"),
    }
    assert source(**callbacks) == raw
    assert events == ["reserved", "retained", "parsed", "closed"]
    Fetcher.failed = True
    with pytest.raises(SourceFailure):
        source(**callbacks)


def test_detail_consumes_explicit_credentials_and_raw_observer():
    events = []
    raw = {"items": [{"code": "ABC"}]}

    def scraper_factory(**kwargs):
        assert kwargs["attach_auth_session"] is False
        assert kwargs["cookies"] == {"sessionid": "fixture"}
        return SimpleNamespace(session=SimpleNamespace(close=lambda: events.append("closed")))

    class Transport:
        def __init__(self, **kwargs):
            self.args = kwargs

        def fetch(self, shortcode, allow_public_fallback):
            assert shortcode == "ABC" and not allow_public_fallback
            assert self.args["check_cancelled_in_transaction"] is transaction_fence
            self.args["on_request"](True)
            self.args["source_payload_observer"]("details", raw)
            return raw

    def transaction_fence(conn):
        return None

    source = DetailSource(
        shortcode="ABC",
        cookies={"sessionid": "fixture"},
        browser_account_id=None,
        scraper_factory=scraper_factory,
        transport_factory=Transport,
        check_cancelled_in_transaction=transaction_fence,
    )
    assert (
        source(
            check_cancelled=lambda: events.append("fence"),
            on_request=lambda: events.append("request"),
            source_observer=lambda kind, payload: events.append("retained"),
        )
        == raw
    )
    assert events == ["fence", "request", "retained", "closed"]


@pytest.mark.parametrize("status,code,retry_after", [(429, "rate_limited", 37.0), (401, "source_auth_failed", None)])
def test_native_listing_rejection_aborts_before_internal_retry(status, code, retry_after):
    """Use maintained HTTP/JSON methods; inject only client and browser lifecycle."""
    from trr_backend.ingestion.domain import Failure
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    events = []
    response = SimpleNamespace(status_code=status, text="", headers={"retry-after": "37"})

    class Fetcher(InstagramPostsScraplingFetcher):
        def __init__(self, **kwargs):
            self._before_source_request = kwargs["before_source_request"]
            self._source_failure_observer = kwargs["source_failure_observer"]
            self._request_count = 0
            self._http_client = self  # pyright: ignore[reportAttributeAccessIssue]

        async def warmup(self, account):  # pyright: ignore[reportIncompatibleMethodOverride]
            pass

        async def _pace_api_requests(self):
            pass

        async def post(self, *args, **kwargs):
            events.append("sent")
            return response

        async def fetch_posts_page(self, *args, **kwargs):
            await self._fetch_json_response("https://fixture.invalid", referer="fixture", data={}, headers={})
            pytest.fail("rejection returned instead of aborting")

        async def aclose(self):
            events.append("closed")

    def rejected(actual, seconds):
        events.append((actual, seconds))
        raise SourceFailure(Failure(actual))

    source = ScraplingListingSource(
        account="bravotv", cookies=[], raw_cookies={}, browser_account_id=None, fetcher_factory=Fetcher
    )
    with pytest.raises(SourceFailure) as caught:
        source(
            cursor=None,
            pre_source_request=lambda: events.append("permit"),
            source_observer=lambda *args: None,
            source_failure_observer=rejected,
        )
    assert caught.value.failure.code == code
    assert events == ["permit", "sent", (code, retry_after), "closed"]


@pytest.mark.parametrize("lane", ["posts", "comments"])
def test_failure_hook_uses_only_explicit_rejection_and_is_optional(lane):
    import importlib

    module = importlib.import_module(f"trr_backend.socials.instagram.{lane}_scrapling.fetcher")
    cls = getattr(module, "InstagramPostsScraplingFetcher" if lane == "posts" else "InstagramCommentsScraplingFetcher")
    fetcher = cls.__new__(cls)
    # Default legacy callers do not even inspect the response here.
    fetcher._observe_source_failure(object())
    seen = []
    fetcher._source_failure_observer = lambda *args: seen.append(args)
    for text in ['{"data":{"caption":"login challenge"}}', '{"status":"ok","message":"login"}']:
        fetcher._observe_source_failure(SimpleNamespace(status_code=200, text=text, headers={}))
    assert seen == []
    fetcher._observe_source_failure(
        SimpleNamespace(status_code=200, text='{"status":"fail","message":"login_required"}', headers={})
    )
    assert seen == [("source_auth_failed", None)]
    fetcher._observe_source_failure(SimpleNamespace(status_code=429, text="", headers={"retry-after": "inf"}))
    assert seen[-1] == ("rate_limited", None)
