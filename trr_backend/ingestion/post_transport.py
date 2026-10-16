"""Consume supplied credentials and expose pre-parser raw source boundaries.

Request counters count explicit HTTP sends and browser operations. Browser
subresources are not claimed as individually counted network requests.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from typing import Any

from trr_backend.ingestion.domain import Failure


class SourceFailure(RuntimeError):  # noqa: N818 - domain failure wrapper
    def __init__(self, failure: Failure):
        self.failure = failure
        super().__init__(failure.code)


class _CallbackAbort(BaseException):
    def __init__(self, error: Exception):
        self.error = error


def _guard(callback, *args):
    try:
        return callback(*args)
    except Exception as exc:
        # Legacy recovery catches Exception. A durable callback failure must
        # escape it without being mistaken for a retryable provider error.
        raise _CallbackAbort(exc) from exc


class ScraplingListingSource:
    def __init__(
        self,
        *,
        account: str,
        cookies: list,
        raw_cookies: dict,
        browser_account_id: str | None,
        fetcher_factory: Callable | None = None,
    ):
        self.account = account
        self.cookies = cookies
        self.raw_cookies = raw_cookies
        self.browser_account_id = browser_account_id
        self.fetcher_factory = fetcher_factory

    def __call__(self, *, cursor, pre_source_request, source_observer, source_failure_observer=None) -> dict[str, Any]:
        async def run():
            factory = self.fetcher_factory
            if factory is None:
                from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

                factory = InstagramPostsScraplingFetcher
            captured: list[dict[str, Any]] = []

            def observe(kind: str, payload: Mapping[str, Any]):
                _guard(source_observer, dict(payload))
                captured.append(dict(payload))

            fetcher = factory(
                cookies=self.cookies,
                raw_cookies=self.raw_cookies,
                browser_account_id=self.browser_account_id,
                consume_credentials_only=True,
                allow_requests_recovery=False,
                identity_provider=None,
                before_source_request=lambda kind: _guard(pre_source_request),
                source_payload_observer=observe,
                source_failure_observer=(
                    (lambda code, retry_after: _guard(source_failure_observer, code, retry_after))
                    if source_failure_observer is not None
                    else None
                ),
            )
            try:
                await fetcher.warmup(self.account)
                result = await fetcher.fetch_posts_page(self.account, cursor=cursor)
                if result.fetch_failed or result.auth_failed:
                    raise SourceFailure(
                        Failure(
                            code="source_auth_failed" if result.auth_failed else "listing_fetch_failed",
                            identity_blocked=bool(result.auth_failed),
                        )
                    )
                # Successful fetcher return must correspond to the last raw
                # response. Activity independently validates pagination and IDs.
                if not captured:
                    raise SourceFailure(Failure("source_evidence_missing"))
                return captured[-1]
            finally:
                await fetcher.aclose()

        try:
            return asyncio.run(run())
        except _CallbackAbort as exc:
            raise exc.error from exc


class DetailSource:
    def __init__(
        self,
        *,
        shortcode: str,
        cookies: dict,
        browser_account_id: str | None,
        deadline_seconds: float = 90,
        scraper_factory: Callable | None = None,
        transport_factory: Callable | None = None,
        transaction: Callable | None = None,
        check_cancelled_in_transaction: Callable | None = None,
    ):
        self.shortcode = shortcode
        self.cookies = cookies
        self.browser_account_id = browser_account_id
        self.deadline_seconds = deadline_seconds
        self.scraper_factory = scraper_factory
        self.transport_factory = transport_factory
        self.transaction = transaction
        self.check_cancelled_in_transaction = check_cancelled_in_transaction

    def __call__(self, *, check_cancelled, on_request, source_observer):
        if self.scraper_factory is None:
            from trr_backend.socials.instagram.scraper import InstagramScraper

            scraper_factory = InstagramScraper
        else:
            scraper_factory = self.scraper_factory
        if self.transport_factory is None:
            from trr_backend.socials.instagram.detail_transport import DetailTransport

            transport_factory = DetailTransport
        else:
            transport_factory = self.transport_factory
        check_cancelled()
        scraper = scraper_factory(
            cookies=dict(self.cookies), browser_account_id=self.browser_account_id, attach_auth_session=False
        )
        try:
            transport = transport_factory(
                scraper=scraper,
                check_cancelled=check_cancelled,
                on_request=lambda first: on_request(),
                deadline=time.monotonic() + self.deadline_seconds,
                source_payload_observer=source_observer,
                transaction=self.transaction,
                **(
                    {"check_cancelled_in_transaction": self.check_cancelled_in_transaction}
                    if self.check_cancelled_in_transaction is not None
                    else {}
                ),
            )
            return transport.fetch(self.shortcode, allow_public_fallback=False)
        finally:
            scraper.session.close()
