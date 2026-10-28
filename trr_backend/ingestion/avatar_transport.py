"""One explicit native profile request; no warming, retries or auth repair."""

from __future__ import annotations

import time
from collections.abc import Callable

import requests
from requests.adapters import HTTPAdapter

from trr_backend.ingestion.avatar_resolution import normalize_handle
from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.ingestion.profile_failure_diagnostics import ResponseFacts
from trr_backend.socials.instagram.constants import PROFILE_INFO_URL
from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch, PublicCookieIdentity
from trr_backend.socials.instagram.public_response import PublicRequestClient, parse_public_retry_after
from trr_backend.socials.instagram.request_client import InstagramRequestClient, InstagramRequestFailure


class AvatarProfileSource:
    def __init__(
        self,
        *,
        cookies: dict,
        deadline: float,
        session=None,
        clock: Callable = time.monotonic,
        public_only: bool = False,
    ):
        self.cookies, self.deadline, self.clock = dict(cookies), deadline, clock
        self.response_facts = ResponseFacts()
        self._public_identity = PublicCookieIdentity(cookies) if public_only else None
        self.session = session or requests.Session()
        # A source activity owns this private session. Ambient proxy/netrc
        # settings cannot silently add another credential or retry owner.
        self.session.trust_env = False
        self.session.mount("https://", HTTPAdapter(max_retries=0))
        if self._public_identity is not None:
            self.session.auth = None
            self.session.proxies.clear()
            self._public_cookies()
        self.client = (PublicRequestClient if public_only else InstagramRequestClient)(session=self.session)

    def _public_cookies(self, response=None):
        identity = self._public_identity
        if identity is None:
            raise RuntimeError("public_cookie_identity_missing")
        try:
            if response is not None:
                self.cookies = identity.prepare_jar(response.cookies, self.cookies)
            self.cookies = identity.prepare_jar(self.session.cookies, self.cookies)
        except CredentialIdentityMismatch:
            raise InstagramRequestFailure(
                "public_access_backoff",
                status_code=getattr(response, "status_code", None),
                retryable=False,
                retry_after_seconds=parse_public_retry_after(response.headers.get("retry-after"))
                if response is not None
                else None,
            ) from None
        self.session.auth = None
        for key in tuple(self.session.headers):
            if key.lower() in {"cookie", "authorization", "proxy-authorization"}:
                self.session.headers.pop(key, None)

    def __call__(self, *, username: str, pre_source_request: Callable):
        username = normalize_handle(username)
        remaining = self.deadline - self.clock()
        if remaining <= 1:
            raise SourceFailure(Failure("avatar_request_deadline"))
        headers = {
            "accept": "application/json",
            "referer": f"https://www.instagram.com/{username}/",
            "origin": "https://www.instagram.com",
            "x-ig-app-id": "936619743392459",
            "x-requested-with": "XMLHttpRequest",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }
        if self.cookies.get("csrftoken"):
            headers["x-csrftoken"] = self.cookies["csrftoken"]

        def send(url, **kwargs):
            if self._public_identity is not None:
                self._public_cookies()
                kwargs["cookies"] = dict(self.cookies)
                kwargs.update(allow_redirects=False, proxies={}, auth=None)
            permit = pre_source_request()
            self.response_facts.begin("legacy_profile", len(self.response_facts.facts) + 1, permit)
            response = self.session.get(url, **kwargs)
            self.response_facts.preloaded(response)
            if self._public_identity is not None:
                self._public_cookies(response)
            return response

        payload = self.client.get_json(
            PROFILE_INFO_URL,
            query_type="avatar_profile",
            headers=headers,
            cookies=self.cookies,
            params={"username": username},
            timeout=(min(5, remaining / 3), min(20, remaining / 2)),
            sender=send,
        )
        self.response_facts.enrich(payload)
        return payload

    def close(self):
        self.session.close()
