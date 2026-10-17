"""Native public profile query: one bootstrap and one identity-bound POST."""

from __future__ import annotations

import json
import re
import time

import requests

from trr_backend.ingestion.avatar_transport import AvatarProfileSource
from trr_backend.ingestion.profiles import ProfileError, handle, native_id
from trr_backend.socials.instagram.public_response import parse_public_retry_after
from trr_backend.socials.instagram.request_client import InstagramRequestFailure
from trr_backend.socials.instagram.scraper import InstagramScraper

PROFILE_DOC_ID = "28649589871350527"
PROFILE_QUERY = "PolarisProfilePageContentQuery"
PROFILE_GRAPHQL_PARSER = "instagram-public-commenter-profile-graphql-v1"
PROVIDERS = {
    "PolarisCASB976ProfileEnabled": ("gkxData", "17028"),
    "PolarisCannesGuardianExperienceEnabled": ("gkxData", "6793"),
    "PolarisShortDramaEnabled": ("justknobxData", "14"),
    "PolarisProfileRingsGradientEnabled": ("gkxData", "25273"),
    "PolarisRepostsConsumptionEnabled": ("gkxData", "9448"),
    "PolarisWebSchoolsEnabled": ("gkxData", "17363"),
}


def provider_flags(html):
    maps = {}
    for group in ("gkxData", "justknobxData"):
        for match in re.finditer(r'"' + group + r'"\s*:\s*', html):
            try:
                value, _ = json.JSONDecoder().raw_decode(html[match.end() :])
            except ValueError:
                continue
            if isinstance(value, dict):
                for key, item in value.items():
                    previous = maps.setdefault(group, {}).get(key, item)
                    if previous != item:
                        raise ProfileError("profile_provider_flags_ambiguous")
                    maps[group][key] = item
    flags = {}
    for name, (group, key) in PROVIDERS.items():
        value = maps.get(group, {}).get(key)
        if isinstance(value, dict):
            value = value.get("result", value.get("r"))
        if type(value) is not bool:
            raise ProfileError("profile_provider_flags_missing")
        flags[name] = value
    return flags


class PublicGraphQLProfileSource(AvatarProfileSource):
    request_budget = 2
    parser_version = PROFILE_GRAPHQL_PARSER

    def __init__(self, *, user_id, deadline, session=None, clock=time.monotonic):
        super().__init__(cookies={}, deadline=deadline, session=session, clock=clock, public_only=True)
        self.user_id = native_id(user_id)
        self.provenance = None
        self.retry_after_seconds = None
        self._sends = self._bytes = 0

    def _send(self, method, url, *, before, **kwargs):
        remaining = self.deadline - self.clock()
        if remaining <= 1 or self._sends >= self.request_budget:
            raise ProfileError("profile_request_boundary_missing")
        self._public_cookies()
        before()
        self._sends += 1
        try:
            response = self.session.request(
                method,
                url,
                **kwargs,
                cookies=dict(self.cookies),
                allow_redirects=False,
                proxies={},
                auth=None,
                stream=True,
                timeout=(min(5, remaining / 3), min(10, remaining / 2)),
            )
        except requests.RequestException:
            raise InstagramRequestFailure("network_error", status_code=None, retryable=True) from None
        try:
            body = bytearray()
            for chunk in response.iter_content(16384):
                if self.clock() >= self.deadline or self._bytes + len(body) + len(chunk) > 2 * 1024 * 1024:
                    raise ProfileError("profile_response_limit")
                body.extend(chunk)
            self._bytes += len(body)
            bounded = requests.Response()
            bounded.status_code, bounded.headers = response.status_code, response.headers
            bounded._content, bounded._content_consumed = bytes(body), True
            bounded.encoding, bounded.cookies = response.encoding, response.cookies
            self._public_cookies(bounded)
            self.client._classify_response(bounded)
            self.retry_after_seconds = parse_public_retry_after(bounded.headers.get("retry-after"))
            return bounded
        except requests.RequestException:
            raise InstagramRequestFailure("network_error", status_code=None, retryable=True) from None
        finally:
            response.close()

    def __call__(self, *, username, pre_source_request):
        username = handle(username)
        url = f"https://www.instagram.com/{username}/"
        # Use maintained bootstrap context parsing, without invoking its retry,
        # document fallback, authentication or profile normalization paths.
        scraper = InstagramScraper(
            cookies={},
            session=self.session,
            browser_account_id=None,
            attach_auth_session=False,
            strict_public_credentials=True,
        )
        headers = scraper._get_headers(url)
        html = self._send("GET", url, before=pre_source_request, headers=headers).text
        flags = provider_flags(html)
        context = scraper._extract_profile_page_context(html)
        variables = {"id": self.user_id, "enable_integrity_filters": True}
        variables.update({"__relay_internal__pv__" + name + "relayprovider": value for name, value in flags.items()})
        data = {
            "av": "0",
            "__d": "www",
            "__user": "0",
            "__a": "1",
            "__req": "1",
            "__comet_req": "7",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": PROFILE_QUERY,
            "variables": json.dumps(variables),
            "server_timestamps": "true",
            "doc_id": PROFILE_DOC_ID,
        }
        data.update(scraper._graphql_form_runtime_fields(page_context=context, request_cookies=self.cookies))
        headers.update(
            {
                "content-type": "application/x-www-form-urlencoded",
                "x-fb-friendly-name": PROFILE_QUERY,
                "x-root-field-name": scraper.PROFILE_PAGE_CONTENT_ROOT_FIELD_NAME,
                "x-asbd-id": scraper.WEB_X_ASBD_ID,
                "x-ig-max-touch-points": "0",
            }
        )
        if context.get("lsd"):
            headers["x-fb-lsd"] = context["lsd"]
        if context.get("bloks_version"):
            headers["x-bloks-version-id"] = context["bloks_version"]
        if self.cookies.get("csrftoken"):
            headers["x-csrftoken"] = self.cookies["csrftoken"]
        response = self._send(
            "POST", "https://www.instagram.com/graphql/query", before=pre_source_request, headers=headers, data=data
        )
        try:
            payload = response.json()
        except ValueError:
            raise ProfileError("profile_source_capability_missing") from None
        self.provenance = {
            "profile_query_id": PROFILE_DOC_ID,
            "profile_query_name": PROFILE_QUERY,
            "profile_provider_flags": flags,
        }
        return payload
