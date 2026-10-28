"""Bounded anonymous owner-timeline candidate; no authenticated fallback.

Kept opt-in until native continuation qualification succeeds. Contract follows
Instaloader's anonymous Profile.get_posts path, not its authenticated posts tab.
"""

import json
import re
from typing import Any
from urllib.parse import urlencode

from trr_backend.ingestion.post_transport import _CallbackAbort
from trr_backend.ingestion.providers import InvalidSourceError
from trr_backend.ingestion.public_transport import _PublicSource
from trr_backend.socials.instagram.public_response import decode_public_json

ANONYMOUS_FAMILY = "instagram-public-owner-timeline"
ANONYMOUS_VERSION = "v1"
ANONYMOUS_DOC_ID = "7950326061742207"


def validate_anonymous_listing(payload, *, account=None, target_key=None):
    from trr_backend.ingestion.listing import page_target

    bound_account = payload.get("listing_profile_username")
    bound_id = payload.get("listing_profile_id")
    if (
        not isinstance(bound_account, str)
        or not bound_account
        or (account and bound_account.casefold() != account.casefold())
    ):
        raise InvalidSourceError("Anonymous evidence account binding missing or mismatched")
    if not isinstance(bound_id, str) or not bound_id.isdigit() or int(bound_id) <= 0:
        raise InvalidSourceError("Anonymous evidence native ID binding missing")
    cursor, doc = payload.get("request_cursor"), payload.get("relay_doc_id")
    if cursor is None:
        if doc is not None or _profile(payload, bound_account) != bound_id:
            raise InvalidSourceError("Anonymous bootstrap binding mismatch")
        expected_target = "page:root"
    else:
        if not isinstance(cursor, str) or not cursor.strip() or doc != ANONYMOUS_DOC_ID:
            raise InvalidSourceError("Anonymous continuation binding invalid")
        _profile(payload, bound_account, expected_id=bound_id)
        expected_target = page_target(cursor)
    if target_key is not None and target_key != expected_target:
        raise InvalidSourceError("Anonymous evidence request target mismatch")


def _profile(payload, account, *, expected_id=None):
    data = payload.get("data")
    user = data.get("user") if isinstance(data, dict) else None
    if not isinstance(user, dict):
        raise InvalidSourceError("Anonymous profile identity missing")
    username = user.get("username")
    if (expected_id is None or username is not None) and (
        not isinstance(username, str) or username.casefold() != account.casefold()
    ):
        raise InvalidSourceError("Anonymous profile identity mismatch")
    ids = [user[key] for key in ("id", "pk") if key in user]
    if any(type(value) not in (str, int) or not str(value).isdigit() or int(value) <= 0 for value in ids):
        raise InvalidSourceError("Anonymous profile ID invalid")
    if len({str(value) for value in ids}) > 1 or (expected_id is None and not ids):
        raise InvalidSourceError("Anonymous profile ID missing or conflicting")
    native_id = str(ids[0]) if ids else expected_id
    if expected_id is not None and native_id != expected_id:
        raise InvalidSourceError("Anonymous continuation identity mismatch")
    if user.get("is_private") is not False and (expected_id is None or "is_private" in user):
        raise InvalidSourceError("Anonymous profile is not explicitly public")
    return native_id


class AnonymousListingSource(_PublicSource):
    def __init__(self, *, account, **kwargs):
        from trr_backend.ingestion.public_http import PublicDocumentSession

        if not re.fullmatch(r"[A-Za-z0-9_.]+", account):
            raise InvalidSourceError("Invalid profile name")
        if kwargs.get("session_factory") is None:
            kwargs["session_factory"] = PublicDocumentSession
        super().__init__(**kwargs)
        self.account = account

    def __call__(self, *, cursor, pre_source_request, source_observer):
        from trr_backend.ingestion.listing import parse_listing_page

        session = None
        observed_pages: list[dict[str, Any]] = []
        request_cursor = None
        native_id = None

        def retain(payload):
            observed_id = native_id
            if request_cursor is None:
                try:
                    observed_id = _profile(payload, self.account)
                except InvalidSourceError:
                    observed_id = None
            observed = dict(
                payload,
                adapter_family=ANONYMOUS_FAMILY,
                adapter_version=ANONYMOUS_VERSION,
                request_cursor=request_cursor,
                relay_doc_id=ANONYMOUS_DOC_ID if request_cursor else None,
                page_size=12,
                listing_profile_id=observed_id,
                listing_profile_username=self.account,
            )
            observed_pages[:] = [observed]
            source_observer(observed)

        def observed_page() -> dict[str, Any]:
            if not observed_pages:
                raise InvalidSourceError("Anonymous listing source response missing")
            return observed_pages[0]

        try:
            if cursor is not None and (not isinstance(cursor, str) or not cursor.strip()):
                raise InvalidSourceError("Invalid native continuation cursor")
            session = self._session(on_request=pre_source_request, on_payload=retain)
            referer = f"https://www.instagram.com/{self.account}/"
            headers = {"accept": "*/*", "referer": referer, "x-ig-app-id": "936619743392459"}
            response = session.get(
                "https://www.instagram.com/api/v1/users/web_profile_info/?" + urlencode({"username": self.account}),
                headers=headers,
            )
            payload = decode_public_json(response)
            native_id = _profile(payload, self.account)
            observed = observed_page()
            parse_listing_page(observed)
            if cursor is None:
                return observed
            if native_id is None:
                raise InvalidSourceError("Anonymous profile ID missing or conflicting")
            csrf = session.raw_cookies.get("csrftoken")
            if not csrf:
                raise InvalidSourceError("Anonymous bootstrap did not supply CSRF token")
            headers["x-csrftoken"] = csrf
            variables = {
                "id": int(native_id),
                "data": {
                    "count": 12,
                    "include_relationship_info": True,
                    "latest_besties_reel_media": True,
                    "latest_reel_media": True,
                },
                "__relay_internal__pv__PolarisFeedShareMenurelayprovider": False,
                "after": cursor,
                "before": None,
                "first": 12,
                "last": None,
            }
            request_cursor = cursor
            response = session.post(
                "https://www.instagram.com/graphql/query",
                headers=headers,
                data={
                    "doc_id": ANONYMOUS_DOC_ID,
                    "variables": json.dumps(variables, separators=(",", ":")),
                    "server_timestamps": "true",
                },
            )
            payload = decode_public_json(response)
            _profile(payload, self.account, expected_id=native_id)
            observed = observed_page()
            parse_listing_page(observed)
            return observed
        except _CallbackAbort as error:
            raise error.error from None
        finally:
            if session is not None:
                session.close()
