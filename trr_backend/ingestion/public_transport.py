"""Observed public Instagram adapters; no credentials, proxies or auth fallback.

The maintained GraphQL/permalink parsers remain the source of truth. Every
explicit HTTP send, including profile bootstrap, crosses the same public gate
and durable request counter. Anonymous runtime cookies live only in this attempt.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from trr_backend.ingestion.domain import Failure, LostOwnership
from trr_backend.ingestion.identity_policy import IdentityDeferred
from trr_backend.ingestion.post_transport import SourceFailure, _CallbackAbort, _guard
from trr_backend.ingestion.providers import InvalidSourceError
from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch, PublicCookieIdentity
from trr_backend.socials.instagram.public_response import PublicRequestClient, parse_public_retry_after
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

LOGGER = logging.getLogger(__name__)
_LOGGED_ERRORS = frozenset(
    {
        "network_error",
        "unauthorized",
        "forbidden",
        "rate_limited",
        "redirect_login",
        "login_required",
        "challenge_required",
        "checkpoint_required",
        "feedback_required",
        "consent_required",
        "sentry_block",
        "endpoint_redirect",
        "request_failed",
        "parse_error",
        "public_access_backoff",
    }
)


def _log_public_failure(*, error_code, status, method, path):
    # Paths are inspected only to select a fixed category. Never attach the
    # path, account, query, response or exception object to a LogRecord.
    segments = path.strip("/").split("/")
    if path.rstrip("/") in {"/graphql/query", "/api/graphql"}:
        endpoint = "graphql"
    elif len(segments) == 2 and segments[0] in {"p", "reel", "reels", "tv"}:
        endpoint = "permalink"
    elif path == "/" or len(segments) == 1:
        endpoint = "profile_bootstrap"
    else:
        endpoint = "other"
    event = {
        "event": "instagram_public_request_failed",
        "error_code": error_code if isinstance(error_code, str) and error_code in _LOGGED_ERRORS else "request_failed",
        "status": status if type(status) is int and 100 <= status <= 599 else 0,
        "method": method.upper() if isinstance(method, str) and method.upper() in {"GET", "POST"} else "OTHER",
        "endpoint_category": endpoint,
    }
    try:
        LOGGER.warning(json.dumps(event, sort_keys=True))
    except Exception:
        # Observability cannot replace the existing counted failure/cooldown.
        pass


class ObservedPublicSession:
    def __init__(
        self,
        *,
        session,
        gate,
        deadline: float,
        check_cancelled: Callable,
        on_request: Callable,
        on_payload: Callable,
        on_failure: Callable,
        clock: Callable = time.monotonic,
    ):
        self.inner, self.gate, self.deadline, self.clock = session, gate, deadline, clock
        self.check_cancelled, self.on_request, self.on_payload, self.on_failure = (
            check_cancelled,
            on_request,
            on_payload,
            on_failure,
        )
        self.guard = PublicCookieIdentity({})
        self.raw_cookies = {}
        self.cookies, self.headers, self.adapters = session.cookies, session.headers, session.adapters
        self.cookies.clear()
        session.trust_env = False
        session.auth = None
        session.proxies.clear()
        for key in tuple(self.headers):
            if key.lower() in {"cookie", "authorization", "proxy-authorization"}:
                self.headers.pop(key, None)
        for scheme in ("http://", "https://"):
            session.mount(scheme, HTTPAdapter(max_retries=0))
        self.classifier = PublicRequestClient(session=self)

    @property
    def trust_env(self):
        return self.inner.trust_env

    @trust_env.setter
    def trust_env(self, value):
        self.inner.trust_env = value

    @property
    def auth(self):
        return self.inner.auth

    @auth.setter
    def auth(self, value):
        self.inner.auth = value

    def check_deadline(self):
        self.check_cancelled()
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise SourceFailure(Failure("target_deadline", retry_at=datetime.now(UTC)))
        return remaining

    def request(self, method, url, **kwargs):
        def execute():
            remaining = self.check_deadline()
            target = urlsplit(url)
            if target.scheme != "https" or target.hostname != "www.instagram.com" or target.username or target.password:
                raise SourceFailure(Failure("public_origin_rejected"))
            if kwargs.get("proxies") or kwargs.get("auth"):
                raise SourceFailure(Failure("public_credentials_rejected"))
            self.raw_cookies = self.guard.prepare_jar(self.cookies, self.raw_cookies)
            self.raw_cookies = self.guard.merge(self.raw_cookies, kwargs.get("cookies") or {})
            kwargs["cookies"] = dict(self.raw_cookies)
            kwargs["headers"] = {
                key: value
                for key, value in kwargs.get("headers", {}).items()
                if key.lower() not in {"cookie", "authorization", "proxy-authorization"}
            }
            kwargs.update(
                allow_redirects=False, proxies={}, auth=None, timeout=(min(10, remaining / 3), min(45, remaining / 2))
            )
            permit = self.gate.before_request()
            self.on_request()
            try:
                response = self.inner.request(method, url, **kwargs)
            except requests.RequestException:
                _log_public_failure(error_code="network_error", status=0, method=method, path=target.path)
                failure = self.gate.record_failure("network_error", permit=permit)
                self.on_failure(failure)
                raise SourceFailure(failure) from None
            # Decode only native JSON. Bootstrap HTML/tokens never enter evidence.
            payload = None
            looks_json = "json" in response.headers.get(
                "content-type", ""
            ).lower() or response.text.lstrip().startswith(("{", "["))
            if looks_json:
                try:
                    payload = response.json()
                except ValueError:
                    payload = None
                if isinstance(payload, dict):
                    self.on_payload(payload)
            try:
                self.classifier._classify_response(response)
                if str(method).upper() == "POST" and not isinstance(payload, dict):
                    raise InstagramRequestFailure("parse_error", status_code=response.status_code, retryable=True)
                # Requests/httpx response jars cannot introduce a logged-in identity.
                self.raw_cookies = self.guard.prepare_jar(self.cookies, self.raw_cookies)
            except (InstagramRequestFailure, CredentialIdentityMismatch) as error:
                _log_public_failure(
                    error_code=getattr(error, "error_code", "public_access_backoff"),
                    status=response.status_code,
                    method=method,
                    path=target.path,
                )
                failure = self.gate.record_failure(
                    getattr(error, "error_code", "public_access_backoff"),
                    retry_after_seconds=parse_public_retry_after(response.headers.get("retry-after")),
                    permit=permit,
                )
                self.on_failure(failure)
                raise SourceFailure(failure) from None
            if not isinstance(payload, dict) or not payload.get("errors") and payload.get("status") in (None, "ok"):
                self.gate.record_success(permit)
            self.check_deadline()
            return response

        return _guard(execute)

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST", url, **kwargs)

    def close(self):
        self.inner.close()


class _PublicSource:
    def __init__(self, *, gate, deadline: float, check_cancelled: Callable, session_factory: Callable | None = None):
        self.gate, self.deadline, self.check_cancelled = gate, deadline, check_cancelled
        self.session_factory = session_factory
        self._failure, self._finished = None, False

    def _session(self, *, on_request, on_payload):
        _guard(self.check_cancelled)
        if self.deadline <= time.monotonic():
            raise SourceFailure(Failure("target_deadline"))
        return ObservedPublicSession(
            session=(self.session_factory or requests.Session)(),
            gate=self.gate,
            deadline=self.deadline,
            check_cancelled=self.check_cancelled,
            on_request=on_request,
            on_payload=on_payload,
            on_failure=lambda failure: setattr(self, "_failure", failure),
        )

    def finish_success(self):
        if self._finished:
            return None
        self._finished = True
        return self.gate.finish_success() if self.gate.request_count else self.gate.finish_failure()

    def finish_failure(self, error=None):
        if self._finished:
            return None
        self._finished = True
        if self._failure or not self.gate.request_count or isinstance(error, (LostOwnership, IdentityDeferred)):
            return self.gate.finish_failure()
        return self.gate.finish_failure("parse_error" if isinstance(error, InvalidSourceError) else "request_failed")


class PublicListingSource(_PublicSource):
    def __init__(self, *, account, scraper_factory=None, **kwargs):
        super().__init__(**kwargs)
        self.account, self.scraper_factory = account, scraper_factory

    def __call__(self, *, cursor, pre_source_request, source_observer):
        from trr_backend.socials.instagram.scraper import InstagramScraper

        session = None
        try:
            session = self._session(on_request=pre_source_request, on_payload=source_observer)
            scraper = (self.scraper_factory or InstagramScraper)(
                cookies={},
                session=session,
                browser_account_id=None,
                attach_auth_session=False,
                strict_public_credentials=True,
            )
            payload = scraper.fetch_posts_graphql(
                self.account,
                cursor=cursor,
                allow_browser_fallback=False,
                allow_recovery=False,
                request_timeout=(10, 45),
            )
            if not isinstance(payload, dict):
                raise SourceFailure(Failure("public_listing_unresolved"))
            return payload
        except _CallbackAbort as error:
            raise error.error from None
        finally:
            if session is not None:
                session.close()


class PublicDetailSource(_PublicSource):
    def __init__(self, *, shortcode, permalink_fetcher=None, **kwargs):
        super().__init__(**kwargs)
        self.shortcode, self.permalink_fetcher = shortcode, permalink_fetcher

    def __call__(self, *, check_cancelled, on_request, source_observer):
        from trr_backend.socials.instagram.permalink_metadata import _DEFAULT_HEADERS, fetch_permalink_media_item

        session = None
        try:
            session = self._session(on_request=on_request, on_payload=lambda payload: None)

            class Transport:
                def check_deadline(self):
                    return _guard(session.check_deadline)

                def request(self, url, *, text=False):
                    response = session.get(url, headers=_DEFAULT_HEADERS, cookies={})
                    return response.text if text else response.json()

            check_cancelled()
            payload = (self.permalink_fetcher or fetch_permalink_media_item)(
                self.shortcode,
                session=session,
                cookies={},
                transport=Transport(),
            )
            if not isinstance(payload, dict):
                raise SourceFailure(Failure("public_detail_unresolved"))
            source_observer("details", payload)
            check_cancelled()
            return payload
        except _CallbackAbort as error:
            raise error.error from None
        finally:
            if session is not None:
                session.close()
