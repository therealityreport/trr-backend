"""One explicit native profile request; no warming, retries or auth repair."""

from __future__ import annotations

import time
from collections.abc import Callable

import requests
from requests.adapters import HTTPAdapter

from trr_backend.ingestion.avatar_resolution import normalize_handle
from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.constants import PROFILE_INFO_URL
from trr_backend.socials.instagram.request_client import InstagramRequestClient


class AvatarProfileSource:
    def __init__(self, *, cookies: dict, deadline: float, session=None, clock: Callable = time.monotonic):
        self.cookies, self.deadline, self.clock = dict(cookies), deadline, clock
        self.session = session or requests.Session()
        # A source activity owns this private session. Ambient proxy/netrc
        # settings cannot silently add another credential or retry owner.
        self.session.trust_env = False
        self.session.mount("https://", HTTPAdapter(max_retries=0))
        self.client = InstagramRequestClient(session=self.session)

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
            pre_source_request()
            return self.session.get(url, **kwargs)

        return self.client.get_json(
            PROFILE_INFO_URL,
            query_type="avatar_profile",
            headers=headers,
            cookies=self.cookies,
            params={"username": username},
            timeout=(min(5, remaining / 3), min(20, remaining / 2)),
            sender=send,
        )

    def close(self):
        self.session.close()
