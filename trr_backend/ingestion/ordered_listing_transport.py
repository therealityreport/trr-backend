"""Qualified anonymous ordered-profile HTTP transport; no browser or fallback.

The source owns one public request gate through a listing activity, including
serial permalink date hydration. Every explicit send crosses the same counter,
deadline, credential guard and identity lane.
"""

from __future__ import annotations

import json
import re

from trr_backend.ingestion.ordered_listing import (
    ADAPTER_VERSION,
    CONTRACT_VERSION,
    DOC_ID,
    ENDPOINT,
    FAMILY,
    OPERATION,
    PAGE_SIZE,
    PROVIDER,
    extract_document,
    identity_fields,
    identity_matches,
    parse_page,
)
from trr_backend.ingestion.post_transport import _CallbackAbort
from trr_backend.ingestion.providers import InvalidSourceError
from trr_backend.ingestion.public_http import PUBLIC_DOCUMENT_HEADERS, PublicDocumentSession
from trr_backend.ingestion.public_transport import PublicDetailSource, _PublicSource
from trr_backend.socials.instagram.public_post_extractor import iter_application_json_payloads
from trr_backend.socials.instagram.public_response import decode_public_json


class OrderedListingSource(_PublicSource):
    def __init__(self, *, account, profile=None, **kwargs):
        if not isinstance(account, str) or re.fullmatch(r"[a-z0-9._]{1,30}", account) is None:
            raise InvalidSourceError("Ordered listing account is not a canonical public handle")
        if kwargs.get("session_factory") is None:
            kwargs["session_factory"] = PublicDocumentSession
        super().__init__(**kwargs)
        self.account = account
        self.profile = profile

    def __call__(self, *, cursor, pre_source_request, source_observer):
        from trr_backend.socials.instagram.scraper import InstagramScraper

        if cursor is not None and (not isinstance(cursor, str) or not 0 < len(cursor) <= 8192):
            raise InvalidSourceError("Ordered request has an invalid native cursor")
        session = None
        try:
            session = self._session(on_request=pre_source_request, on_payload=lambda payload: None)
            profile_url = f"https://www.instagram.com/{self.account}/"
            response = session.get(profile_url, headers=PUBLIC_DOCUMENT_HEADERS)
            if len(response.content) > 8 * 1024 * 1024:
                raise InvalidSourceError("Ordered profile document exceeds its finite byte bound")
            html = response.text
            payloads = iter_application_json_payloads(html)
            if len(payloads) > 500:
                raise InvalidSourceError("Ordered profile document has too many source payloads")
            metadata, root = extract_document(payloads, account=self.account)
            fields = identity_fields(root)
            if self.profile is not None and not identity_matches(fields, self.profile):
                raise InvalidSourceError("Ordered continuation profile differs from its frozen checkpoint")
            if cursor is not None and (not isinstance(self.profile, dict) or "id" not in self.profile):
                raise InvalidSourceError("Ordered continuation has no prior native profile checkpoint")

            def envelope(data, request_cursor, *, status=None):
                value = {
                    "adapter_family": FAMILY,
                    "adapter_version": ADAPTER_VERSION,
                    "version": CONTRACT_VERSION,
                    "listing_profile_username": self.account,
                    "listing_profile_id": fields.get("id"),
                    "user": metadata,
                    "request_cursor": request_cursor,
                    "relay_doc_id": DOC_ID if request_cursor is not None else None,
                    "relay_friendly_name": OPERATION if request_cursor is not None else None,
                    "source_url": ENDPOINT if request_cursor is not None else profile_url,
                    "page_size": PAGE_SIZE,
                    "data": data,
                }
                if status is not None:
                    value["status"] = status
                return value

            bootstrap = envelope({"xig_user_by_username": root}, None)
            source_observer(bootstrap)
            parse_page(bootstrap, account=self.account, cursor=None)
            if cursor is None:
                return bootstrap
            scraper = object.__new__(InstagramScraper)
            context = scraper._extract_profile_page_context(html)
            runtime = scraper._graphql_form_runtime_fields(page_context=context, request_cookies=session.raw_cookies)
            if not context.get("lsd") or runtime.get("lsd") != context["lsd"]:
                raise InvalidSourceError("Fresh ordered document has no maintained public bootstrap token")
            variables = {"after": cursor, "first": PAGE_SIZE, "id": fields["id"], PROVIDER: False}
            form = {
                "av": "0",
                "__user": "0",
                "__d": "www",
                "__a": "1",
                "__req": "1",
                "__comet_req": "7",
                "fb_api_caller_class": "RelayModern",
                "fb_api_req_friendly_name": OPERATION,
                "doc_id": DOC_ID,
                "variables": json.dumps(variables, separators=(",", ":")),
                "server_timestamps": "true",
                **runtime,
            }
            headers = {
                key: value
                for key, value in PUBLIC_DOCUMENT_HEADERS.items()
                if key.lower() == "user-agent" or key.lower().startswith("sec-ch-")
            }
            headers.update(
                {
                    "accept": "*/*",
                    "content-type": "application/x-www-form-urlencoded",
                    "origin": "https://www.instagram.com",
                    "referer": profile_url,
                    "x-fb-friendly-name": OPERATION,
                    "x-fb-lsd": context["lsd"],
                    "x-asbd-id": str(scraper.WEB_X_ASBD_ID),
                    "x-ig-app-id": "936619743392459",
                }
            )
            if form.get("av") != "0" or form.get("__user") != "0":
                raise InvalidSourceError("Ordered request lost its anonymous viewer fence")
            response = session.post(ENDPOINT, data=form, headers=headers)
            if len(response.content) > 4 * 1024 * 1024:
                raise InvalidSourceError("Ordered continuation exceeds its finite byte bound")
            payload = decode_public_json(response)
            if not isinstance(payload, dict):
                raise InvalidSourceError("Ordered continuation is not a native object")
            failed = bool(payload.get("errors") or payload.get("error") or payload.get("status") not in (None, "ok"))
            result = envelope(payload.get("data"), cursor, status="unresolved" if failed else payload.get("status"))
            source_observer(result)
            parse_page(result, account=self.account, cursor=cursor)
            return result
        except _CallbackAbort as error:
            raise error.error from None
        finally:
            if session is not None:
                session.close()

    def fetch_detail(self, *, shortcode, check_cancelled, on_request, source_observer):
        """Serial date hydration shares this source's existing public gate."""
        detail = PublicDetailSource(
            shortcode=shortcode,
            gate=self.gate,
            deadline=self.deadline,
            check_cancelled=self.check_cancelled,
            session_factory=self.session_factory,
        )
        try:
            return detail(check_cancelled=check_cancelled, on_request=on_request, source_observer=source_observer)
        finally:
            if detail._failure is not None:
                self._failure = detail._failure
