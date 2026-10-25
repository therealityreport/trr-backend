"""Fenced, auxiliary native detail refresh for an existing media target.

Refresh never updates canonical observations, storage identities or target
completion. Signed URLs stay ephemeral; retained evidence is sanitized. The
existing DetailTransport owns shared identity pressure and counted retries.
"""

from __future__ import annotations

import logging
import re
import sys
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime

import requests
from requests.adapters import HTTPAdapter

from trr_backend.ingestion.application import validate_detail
from trr_backend.ingestion.avatar_resolution import (
    AVATAR_PARSER_VERSION,
    AVATAR_SOURCE_PREFIX,
    AvatarResolutionActivity,
    AvatarResolutionError,
    _AvatarControlAbort,
    _profile,
    normalize_handle,
    parse_avatar_evidence,
)
from trr_backend.ingestion.avatar_transport import AvatarProfileSource
from trr_backend.ingestion.canonical import normalize_post_source
from trr_backend.ingestion.credential_snapshot import AUTHORIZED_USERNAME
from trr_backend.ingestion.domain import Failure, LostOwnership, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.identity_policy import IdentityDeferred, SourceRequestGate, is_content_absence
from trr_backend.ingestion.media import MediaIncomplete, MediaSnapshot, avatar_delivery_equivalent
from trr_backend.ingestion.media import source_reference as public_reference
from trr_backend.ingestion.post_transport import DetailSource, SourceFailure
from trr_backend.ingestion.providers import PARSER_VERSION, InvalidSourceError, parse_evidence
from trr_backend.socials.instagram.constants import POST_INFO_URL
from trr_backend.socials.media_url_safety import (
    MediaUrlSafetyPolicy,
    allowed_hosts_for_platform,
    validate_media_url,
)

_POLICY = MediaUrlSafetyPolicy(allowed_hosts_for_platform("instagram"), resolve_dns=False)
LOGGER = logging.getLogger(__name__)
_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


def _retained_payload(value):
    # Delivery signatures are needed only for this attempt. The retained native
    # shape still contains public asset paths and all parser identity fields.
    if isinstance(value, dict):
        return {key: _retained_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_retained_payload(item) for item in value]
    if isinstance(value, str) and value.startswith("https://"):
        return public_reference(value)
    return value


class ExplicitDetailClient:
    """Minimal DetailTransport scraper protocol; no ambient InstagramScraper."""

    POST_INFO_URL = POST_INFO_URL

    def __init__(self, *, cookies, browser_account_id, attach_auth_session=False, session=None):
        if attach_auth_session or browser_account_id != AUTHORIZED_USERNAME:
            raise MediaIncomplete("credential_snapshot_unavailable")
        self.cookies = dict(cookies)
        self.session = requests.Session() if session is None else session
        self.session.trust_env = False
        self.session.auth = None
        self.session.cookies.clear()
        for name in ("Cookie", "Authorization", "Proxy-Authorization"):
            self.session.headers.pop(name, None)
        self.session.mount("https://", HTTPAdapter(max_retries=0))

    @staticmethod
    def _shortcode_to_media_id(shortcode):
        if not isinstance(shortcode, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", shortcode):
            raise MediaIncomplete("media_refresh_identity_invalid")
        value = 0
        for char in shortcode:
            value = value * 64 + _ALPHABET.index(char)
        return str(value)

    def _request_cookies(self):
        # Response cookies cannot rotate or supplement the authorized snapshot.
        self.session.cookies.clear()
        return dict(self.cookies)

    def _get_headers(self, referer=None):
        self.session.cookies.clear()
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-requested-with": "XMLHttpRequest",
            "origin": "https://www.instagram.com",
            "referer": referer or "https://www.instagram.com/",
            "x-ig-app-id": "936619743392459",
            "x-csrftoken": self.cookies["csrftoken"],
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }


class MediaRefresh:
    def __init__(
        self,
        *,
        store,
        owned,
        owner_expires_at: datetime,
        credential_provider: Callable,
        public_only: bool = False,
        public_detail_source_factory: Callable | None = None,
        detail_source_factory: Callable = DetailSource,
        avatar_source_factory: Callable = AvatarProfileSource,
        public_avatar_source_factory: Callable | None = None,
        avatar_gate_factory: Callable = SourceRequestGate,
        now: Callable = lambda: datetime.now(UTC),
        network_lane: str | None = None,
    ):
        if Stage(owned["stage"]) != Stage.MEDIA:
            raise ValueError("Media refresh requires a media owner")
        if network_lane is not None and not public_only:
            raise ValueError("Network lanes apply only to public media refresh")
        # Logged-out limits belong to the egress network: public fallback
        # requests share the run placement's lane with listing and comments.
        self.network_lane = network_lane
        self.store = store
        self.owned = {key: owned[key] for key in ("run_id", "account", "generation", "target_key", "owner_token")}
        self.owned["stage"] = Stage.MEDIA
        self.owner_expires_at = owner_expires_at
        self.public_only, self.public_detail_source_factory = public_only, public_detail_source_factory
        self.credential_provider, self.detail_source_factory, self.now = credential_provider, detail_source_factory, now
        self.avatar_source_factory, self.avatar_gate_factory = avatar_source_factory, avatar_gate_factory
        self.public_avatar_source_factory = public_avatar_source_factory
        self._credentials_snapshot = None
        self._avatars = {}
        self._avatar_native_ids = {}
        self._avatar_attempted = set()
        self._snapshot = None
        self._fresh = None
        self._attempted = False

    def bind_snapshot(self, snapshot: MediaSnapshot):
        if self._snapshot is not None:
            raise MediaIncomplete("media_refresh_snapshot_already_bound")
        snapshot.validate()
        if snapshot.shortcode != self.owned["target_key"]:
            raise MediaIncomplete("media_refresh_identity_mismatch")
        self._snapshot = snapshot

    def __call__(self, *, source_reference: str, check_cancelled: Callable, on_request: Callable) -> str:
        snapshot = self._snapshot
        if snapshot is None:
            raise MediaIncomplete("media_refresh_snapshot_required")
        snapshot.validate()
        self.store.check_owner(**self.owned)
        check_cancelled()
        reference = source_reference
        supported = {public_reference(url) for url in (*snapshot.media_urls, snapshot.thumbnail_url)}
        comments = {public_reference(url) for comment in snapshot.comments for url in comment.media_urls}
        avatars = defaultdict(set)
        aliases = defaultdict(set)
        for avatar in snapshot.avatars:
            canonical = public_reference(avatar.source_url)
            username = normalize_handle(avatar.username)
            avatars[canonical].add(username)
            if avatar.download_url and public_reference(avatar.download_url) != canonical:
                proof = avatar.download_provenance
                if (
                    isinstance(proof, tuple) and len(proof) == 2 and proof[0] == avatar.username
                    and avatar_delivery_equivalent(avatar.source_url, avatar.download_url, native_id=proof[1])
                ):
                    aliases[public_reference(avatar.download_url)].add((canonical, username, proof[1]))
        requested = reference
        required_native_id = None
        if requested in aliases:
            matches = aliases[requested]
            if len(matches) != 1 or requested in comments or requested in supported or requested in avatars:
                raise MediaIncomplete("media_refresh_role_ambiguous")
            reference, _, required_native_id = next(iter(matches))
        usernames = avatars.get(reference, set())
        if reference in comments or (reference in supported and usernames) or len(usernames) > 1:
            raise MediaIncomplete("media_refresh_role_ambiguous")
        if usernames:
            username = next(iter(usernames))
            if username not in self._avatars:
                if username in self._avatar_attempted:
                    raise MediaIncomplete("avatar_refresh_exhausted")
                self._avatar_attempted.add(username)
                self._avatars[username] = self._fetch_avatar(
                    username, check_cancelled=check_cancelled, on_request=on_request
                )
            fresh = self._avatars[username]
            pin = self._avatar_native_ids.get(username)
            if (
                (required_native_id is not None and pin != required_native_id)
                or not fresh
                or not avatar_delivery_equivalent(reference, fresh, native_id=pin)
            ):
                raise MediaIncomplete("avatar_refresh_asset_unresolved")
            self.store.check_owner(**self.owned)
            check_cancelled()
            return fresh
        if reference not in supported:
            raise MediaIncomplete("media_refresh_role_unsupported")
        if self._fresh is None:
            if self._attempted:
                raise MediaIncomplete("media_refresh_exhausted")
            self._attempted = True
            self._fresh = self._fetch(check_cancelled=check_cancelled, on_request=on_request)
        fresh = self._fresh.get(reference)
        if fresh is None:
            raise MediaIncomplete("media_refresh_asset_unresolved")
        self.store.check_owner(**self.owned)
        check_cancelled()
        return fresh

    def _credentials(self):
        if self.public_only:
            return {"raw_cookies": {}, "browser_account_id": None}
        if self._credentials_snapshot is None:
            self._credentials_snapshot = self.credential_provider()
        return self._credentials_snapshot

    def _fetch_public_avatar(self, username, *, check_cancelled, on_request):
        from trr_backend.ingestion.reply_context import source_deadline

        remaining = (self.owner_expires_at - self.now()).total_seconds() - 2
        deadline = source_deadline(time.monotonic() + max(0, min(90, remaining)))
        if deadline - time.monotonic() <= 1:
            raise IdentityDeferred("target_deadline", None)

        def check():
            self.store.check_owner(**self.owned)
            check_cancelled()

        gate = self.avatar_gate_factory(
            cookies={},
            transaction=self.store.transaction,
            check_cancelled=check,
            check_cancelled_in_transaction=lambda conn: self.store.check_owner(**self.owned, connection=conn),
            deadline=deadline,
            public_only=True,
            **({"network_lane": self.network_lane} if self.network_lane is not None else {}),
        )
        source = None
        success = False
        try:
            check()
            source = (
                self.public_avatar_source_factory(username=username, deadline=deadline)
                if self.public_avatar_source_factory is not None
                else self.avatar_source_factory(cookies={}, deadline=deadline, public_only=True)
            )
            capture = AvatarResolutionActivity(self.store, gate=gate).observe(
                owned=self.owned,
                username=username,
                source=source,
                on_request=on_request,
                check_cancelled=check_cancelled,
                clock=self.now,
                canonical_urls=True,
            )
            success = True
            self._avatar_native_ids[username] = capture.expected_id
            # The caller still requires the exact old image reference. This
            # observation never updates the registry, roles, clocks or receipt.
            return capture.observation.source_url
        except AvatarResolutionError as error:
            raise SourceFailure(error.failure) from None
        finally:
            active_error = sys.exc_info()[0] is not None
            try:
                if source is not None:
                    try:
                        source.close()
                    except Exception:
                        if not active_error:
                            raise
                        LOGGER.warning("ingestion_avatar_refresh_source_cleanup_failed")
            finally:
                try:
                    gate.finish_success() if success else gate.finish_failure()
                except Exception:
                    LOGGER.warning("ingestion_avatar_refresh_gate_cleanup_failed")

    def _fetch_avatar(self, username, *, check_cancelled, on_request):
        if self.public_only:
            return self._fetch_public_avatar(username, check_cancelled=check_cancelled, on_request=on_request)
        remaining = (self.owner_expires_at - self.now()).total_seconds() - 2
        if remaining <= 1:
            raise MediaIncomplete("avatar_refresh_deadline")
        credentials = self._credentials()
        deadline = time.monotonic() + min(90, remaining)

        def check():
            self.store.check_owner(**self.owned)
            check_cancelled()

        gate = self.avatar_gate_factory(
            cookies=credentials["raw_cookies"],
            transaction=self.store.transaction,
            check_cancelled=check,
            check_cancelled_in_transaction=lambda conn: self.store.check_owner(**self.owned, connection=conn),
            deadline=deadline,
            public_only=self.public_only,
            **({"network_lane": self.network_lane} if self.network_lane is not None else {}),
        )
        permit, source, requests_count, success = None, None, 0, False
        payload = None

        def before_request():
            nonlocal permit, requests_count
            try:
                if requests_count:
                    raise MediaIncomplete("avatar_hidden_retry_forbidden")
                check()
                permit = gate.before_request()
                on_request()
                requests_count += 1
            except Exception as error:
                raise _AvatarControlAbort(error) from None

        try:
            check()
            source = self.avatar_source_factory(
                cookies=credentials["raw_cookies"],
                deadline=deadline,
                **({"public_only": True} if self.public_only else {}),
            )
            payload = source(username=username, pre_source_request=before_request)
            if requests_count != 1 or not isinstance(payload, dict):
                raise MediaIncomplete("avatar_refresh_request_boundary_missing")
            data = payload.get("data")
            user = data.get("user") if isinstance(data, dict) else None
            explicit_missing = (
                isinstance(user, dict)
                and any(key in user for key in ("profile_pic_url_hd", "profile_pic_url"))
                and all(user.get(key) in (None, "") for key in ("profile_pic_url_hd", "profile_pic_url"))
            )
            evidence = Evidence.capture(
                source_identity=AVATAR_SOURCE_PREFIX + username,
                captured_at=self.now(),
                parser_version=AVATAR_PARSER_VERSION,
                payload=_retained_payload({**payload, "avatar_url_explicitly_missing": explicit_missing}),
            )
            self.store.retain_evidence(**self.owned, evidence=evidence)
            parse_avatar_evidence(evidence, username=username, pinned=True)
            fresh = _profile(payload, username)
            gate.record_success(permit)
            success = True
            return fresh
        except _AvatarControlAbort as abort:
            raise abort.error from None
        except (LostOwnership, IdentityDeferred):
            raise
        except Exception as error:
            if permit is None:
                raise MediaIncomplete("avatar_refresh_request_failed") from None
            code = (
                "parse_error"
                if isinstance(error, (AvatarResolutionError, MediaIncomplete))
                else getattr(error, "error_code", "request_failed")
            )
            health = (
                gate.record_content_absence(code, permit=permit)
                if is_content_absence(error=error, payload=payload)
                else gate.record_failure(
                    code, retry_after_seconds=getattr(error, "retry_after_seconds", None), permit=permit
                )
            )
            public_code = error.code if isinstance(error, AvatarResolutionError) else health.code
            raise SourceFailure(
                Failure(public_code, retry_at=health.retry_at, identity_blocked=health.identity_blocked)
            ) from None
        finally:
            try:
                if source is not None:
                    source.close()
            finally:
                try:
                    if success:
                        gate.finish_success(permit)
                    else:
                        gate.finish_failure()
                except Exception:
                    LOGGER.warning("ingestion_avatar_refresh_gate_cleanup_failed")

    def _fetch(self, *, check_cancelled, on_request):
        remaining = (self.owner_expires_at - self.now()).total_seconds() - 2
        if remaining <= 0:
            raise MediaIncomplete("media_refresh_deadline")
        credentials = self._credentials()
        self.store.check_owner(**self.owned)
        retained = []

        def observe(kind, payload):
            evidence = Evidence.capture(
                source_identity=f"instagram:post:{self.owned['target_key']}",
                captured_at=self.now(),
                parser_version=PARSER_VERSION,
                payload=_retained_payload(payload),
            )
            self.store.retain_evidence(**self.owned, evidence=evidence)
            retained.append(evidence)

        if self.public_only:
            from trr_backend.ingestion.public_transport import PublicDetailSource

            deadline = time.monotonic() + min(90, remaining)
            gate = SourceRequestGate(
                cookies={},
                transaction=self.store.transaction,
                check_cancelled=check_cancelled,
                check_cancelled_in_transaction=lambda conn: self.store.check_owner(**self.owned, connection=conn),
                deadline=deadline,
                public_only=True,
                network_lane=self.network_lane,
            )
            source = (self.public_detail_source_factory or PublicDetailSource)(
                shortcode=self.owned["target_key"],
                gate=gate,
                deadline=deadline,
                check_cancelled=check_cancelled,
            )
        else:
            source = self.detail_source_factory(
                shortcode=self.owned["target_key"],
                cookies=credentials["raw_cookies"],
                browser_account_id=credentials["browser_account_id"],
                deadline_seconds=min(90, remaining),
                scraper_factory=ExplicitDetailClient,
                transaction=self.store.transaction,
                check_cancelled_in_transaction=lambda conn: self.store.check_owner(**self.owned, connection=conn),
            )
        try:
            payload = source(check_cancelled=check_cancelled, on_request=on_request, source_observer=observe)
            candidate = Evidence.capture(
                source_identity=f"instagram:post:{self.owned['target_key']}",
                captured_at=self.now(),
                parser_version=PARSER_VERSION,
                payload=_retained_payload(payload),
            )
            matched = [item for item in retained if item.content_hash == candidate.content_hash]
            if not matched:
                observe("details", payload)
                matched = retained[-1:]
            try:
                parsed = parse_evidence(
                    matched[-1],
                    account=self.owned["account"],
                    stage=Stage.DETAILS,
                    target_key=self.owned["target_key"],
                    pinned=True,
                )
                validate_detail(parsed)
                if parsed.post.source_id.split("_", 1)[0] != ExplicitDetailClient._shortcode_to_media_id(
                    self.owned["target_key"]
                ):
                    raise InvalidSourceError("Native post identity mismatch")
                live = normalize_post_source(payload, account=self.owned["account"])
                snapshot = self._snapshot
                if len(live.media_urls) != snapshot.expected_media_count:
                    raise InvalidSourceError("Native media coverage changed")
                if tuple(map(public_reference, live.media_urls)) != tuple(map(public_reference, snapshot.media_urls)):
                    raise InvalidSourceError("Native media positions changed")
                matches = defaultdict(set)
                for old, fresh in [
                    *zip(snapshot.media_urls, live.media_urls, strict=True),
                    (snapshot.thumbnail_url, live.thumbnail_url),
                ]:
                    if fresh and public_reference(old) == public_reference(fresh):
                        validate_media_url(fresh, policy=_POLICY)
                        matches[public_reference(old)].add(fresh)
                result = {key: next(iter(urls)) for key, urls in matches.items() if len(urls) == 1}
            except (ValueError, TypeError, AttributeError, KeyError):
                raise MediaIncomplete("media_refresh_source_invalid") from None
        except Exception as error:
            if self.public_only:
                source.finish_failure(error)
            raise
        else:
            if self.public_only:
                source.finish_success()
            return result
