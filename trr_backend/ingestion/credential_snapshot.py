"""One explicitly pinned ingestion identity; no discovery, repair or source I/O."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.post_transport import SourceFailure

AUTHORIZED_USERNAME = "livestnation"


class _Cookies(dict):
    def __repr__(self):
        return "<redacted Instagram cookies>"

    __str__ = __repr__


class _BrowserCookies(list):
    def __repr__(self):
        return "<redacted Instagram browser cookies>"

    __str__ = __repr__


def _unavailable():
    return SourceFailure(Failure("credential_snapshot_unavailable", identity_blocked=True))


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate cookie name")
        result[key] = value
    return result


@dataclass(frozen=True)
class CredentialSnapshot:
    username: str
    user_id: str = field(repr=False)
    _cookies: Mapping[str, str] = field(repr=False)

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None):
        env = os.environ if environ is None else environ
        # No aliases, files, browser sessions, runtime overrides or resolver.
        username = env.get("TRR_INSTAGRAM_INGESTION_AUTH_USERNAME", "")
        user_id = env.get("TRR_INSTAGRAM_INGESTION_AUTH_USER_ID", "")
        raw = env.get("SOCIAL_INSTAGRAM_COOKIES_JSON", "")
        if username != AUTHORIZED_USERNAME or not re.fullmatch(r"[1-9][0-9]{0,30}", user_id):
            raise _unavailable()
        if not raw or len(raw) > 1024 * 1024:
            raise _unavailable()
        try:
            cookies = json.loads(raw, object_pairs_hook=_unique_object)
        except (TypeError, ValueError):
            raise _unavailable() from None
        if not isinstance(cookies, dict) or not cookies:
            raise _unavailable()
        if any(
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", name)
            or not isinstance(value, str)
            or not value
            or any(char in value for char in "\r\n\x00")
            for name, value in cookies.items()
        ):
            raise _unavailable()
        if any(not cookies.get(key, "").strip() for key in ("sessionid", "csrftoken", "ds_user_id")):
            raise _unavailable()
        if cookies["ds_user_id"] != user_id:
            raise _unavailable()
        return cls(username, user_id, MappingProxyType(cookies))

    def source_kwargs(self):
        cookies = _Cookies(self._cookies)
        return {
            "cookies": _BrowserCookies(
                {"name": name, "value": value, "domain": ".instagram.com", "path": "/"}
                for name, value in cookies.items()
            ),
            "raw_cookies": cookies,
            "browser_account_id": self.username,
        }
