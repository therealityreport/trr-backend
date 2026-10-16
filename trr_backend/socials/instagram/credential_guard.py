"""Pure identity fence for explicitly supplied Instagram cookie snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

_IDENTITY_FIELDS = ("sessionid", "ds_user_id")


class CredentialIdentityMismatch(RuntimeError):  # noqa: N818 - shared identity boundary vocabulary
    def __init__(self):
        super().__init__("instagram_credential_identity_mismatch")


@dataclass(frozen=True, init=False)
class PinnedCookieIdentity:
    _sessionid: str = field(repr=False)
    _user_id: str = field(repr=False)

    def __init__(self, cookies: Mapping[str, str]):
        if not isinstance(cookies, Mapping) or any(
            not isinstance(cookies.get(key), str) or not cookies[key].strip() for key in _IDENTITY_FIELDS
        ):
            raise CredentialIdentityMismatch()
        object.__setattr__(self, "_sessionid", cookies["sessionid"])
        object.__setattr__(self, "_user_id", cookies["ds_user_id"])
        self.validate_snapshot(cookies)

    def _mapping(self, value):
        if not isinstance(value, Mapping) or any(
            not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
        ):
            raise CredentialIdentityMismatch()
        return dict(value)

    def _validate_delta(self, cookies):
        if ("sessionid" in cookies and cookies["sessionid"] != self._sessionid) or (
            "ds_user_id" in cookies and cookies["ds_user_id"] != self._user_id
        ):
            raise CredentialIdentityMismatch()

    def validate_snapshot(self, cookies: Mapping[str, str]) -> dict[str, str]:
        result = self._mapping(cookies)
        if any(key not in result for key in _IDENTITY_FIELDS):
            raise CredentialIdentityMismatch()
        self._validate_delta(result)
        return result

    def merge(self, cookies: Mapping[str, str], updates: Mapping[str, str]) -> dict[str, str]:
        result, delta = self.validate_snapshot(cookies), self._mapping(updates)
        self._validate_delta(delta)
        return result | delta

    def prepare_jar(self, jar, cookies: Mapping[str, str]) -> dict[str, str]:
        snapshot = self.validate_snapshot(cookies)
        container = getattr(jar, "jar", jar)
        try:
            if hasattr(container, "set_cookie"):
                updates = {}
                for cookie in container:
                    if (
                        cookie.name in _IDENTITY_FIELDS
                        and cookie.name in updates
                        and updates[cookie.name] != cookie.value
                    ):
                        raise CredentialIdentityMismatch()
                    updates[cookie.name] = cookie.value
            else:
                updates = dict(jar)
            # Inspect every cookie, including duplicate names under other domains.
            snapshot = self.merge(snapshot, updates)
        except CredentialIdentityMismatch:
            raise
        except (TypeError, ValueError, AttributeError, KeyError):
            raise CredentialIdentityMismatch() from None
        jar.clear()
        jar.update(snapshot)
        return snapshot


class PublicCookieIdentity:
    """Empty supplied state; permit anonymous cookies learned in this attempt."""

    _mapping = PinnedCookieIdentity._mapping
    merge = PinnedCookieIdentity.merge
    prepare_jar = PinnedCookieIdentity.prepare_jar

    def __init__(self, cookies: Mapping[str, str]):
        if self._mapping(cookies):
            raise CredentialIdentityMismatch()

    def __repr__(self):
        return "PublicCookieIdentity()"

    def _validate_delta(self, cookies):
        if any(key.strip().lower() in _IDENTITY_FIELDS for key in cookies):
            raise CredentialIdentityMismatch()

    def validate_snapshot(self, cookies: Mapping[str, str]) -> dict[str, str]:
        result = self._mapping(cookies)
        self._validate_delta(result)
        return result
