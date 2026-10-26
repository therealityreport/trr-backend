"""Independent Instagram media activity and its fenced canonical write.

Downloads and immutable object writes precede the database transaction. The
snapshot loader must seal the persisted comment and avatar inventory; this
module never guesses pagination completion or turns a CDN failure into deletion.
Existing fixed keys remain immutable. Explicit post roles may retain a changed
rendition under a content-versioned sibling key, preserving every prior object.
"""

from __future__ import annotations

import hashlib
import io
import logging
import math
import mimetypes
import re
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import requests
from botocore.exceptions import BotoCoreError, ClientError
from PIL import Image
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor

from trr_backend.ingestion.canonical import CanonicalWriteError, OlderObservation
from trr_backend.ingestion.domain import Failure, LostOwnership, Outcome, Stage, stable_json, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.persistence import Store
from trr_backend.media.s3_mirror import (
    _media_payload_error,
    _sniff_image_content_type,
    _sniff_video_content_type,
    build_instagram_profile_pic_s3_key,
    guess_ext_from_content_type,
    upload_bytes_to_s3,
)
from trr_backend.media.social_image_variants import generate_social_cover_display_variants
from trr_backend.socials.media_url_safety import (
    MediaUrlSafetyPolicy,
    UnsafeMediaUrlError,
    allowed_hosts_for_platform,
    safe_requests_get,
    validate_media_redirect,
    validate_media_url,
)

MEDIA_VERSION = "instagram-media-v1"
MEDIA_INVENTORY_VERSION = "instagram-media-inventory-v1"
PRIOR_MANIFEST_VERSION = "instagram-media-prior-manifest-v1"
LOGGER = logging.getLogger(__name__)
_POLICY = MediaUrlSafetyPolicy(allowed_hosts_for_platform("instagram"), allow_test_hosts=False, resolve_dns=False)
_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".m4v", ".webm"}
# Instagram's static xg_5YoVlvjp.gif renders "Content unavailable". A valid
# image response is not proof that the requested attachment was delivered.
# Match observed bytes, never size/dimensions (which legitimate GIFs can share).
_SOURCE_PLACEHOLDER_SHA256 = {"05e81bfed1fda5743fea1da04586e635c4f82cd2258c2716265b538508efd5a9"}


class MediaIncomplete(ValueError):  # noqa: N818 - completion domain vocabulary
    """Required media coverage is unresolved; never settle a success receipt."""

    def __init__(self, code: str, *, retry_at: datetime | None = None):
        self.code = code
        self.failure = Failure(code=code, retry_at=retry_at)
        super().__init__(code)


class MediaOwnershipCheck:
    """Force fences at side effects; bound body cancellation checks to one second."""

    def __init__(self, durable: Callable, *, deadline: float, clock: Callable = time.monotonic):
        self.durable, self.deadline, self.clock = durable, deadline, clock
        self.last_checked = float("-inf")

    def check_deadline(self) -> float:
        now = self.clock()
        if now >= self.deadline:
            raise MediaIncomplete("target_deadline")
        return now

    def remaining(self) -> float:
        return self.deadline - self.check_deadline()

    def require_budget(self, seconds: float) -> None:
        if self.remaining() <= seconds:
            raise MediaIncomplete("target_deadline")

    def __call__(self) -> None:
        self.check_deadline()
        self.durable()
        self.last_checked = self.check_deadline()

    def body(self) -> None:
        if self.check_deadline() - self.last_checked >= 1.0:
            self()


def _retry_at(value: str | None) -> datetime | None:
    now = datetime.now(UTC)
    if not isinstance(value, str) or not value.strip():
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():
        # An explicit source delay may exceed datetime's range. Keep it closed
        # rather than replacing that actual delay with a shorter local wait.
        digits = value.lstrip("0") or "0"
        if len(digits) > 12:
            return datetime.max.replace(tzinfo=UTC)
        try:
            return now + timedelta(seconds=int(digits))
        except OverflowError:
            return datetime.max.replace(tzinfo=UTC)
    try:
        stamp = parsedate_to_datetime(value)
        if stamp.tzinfo is not None and stamp > now:
            return stamp.astimezone(UTC)
    except (TypeError, ValueError, OverflowError):
        pass
    return None


def source_reference(value: str) -> str:
    """Public path only; signed queries and credentials never enter durable data."""
    parts = urlsplit(value)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        raise MediaIncomplete("invalid_media_reference")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))



def avatar_delivery_equivalent(required: str, fresh: str, *, native_id: str | None = None) -> bool:
    """Delivery-only CDN relocation; canonical references remain unchanged.

    The caller supplies a strict same-handle/native-ID profile observation.
    Matching a filename alone cannot establish the required image version.
    """
    try:
        if source_reference(required) == source_reference(fresh):
            return True
        if not isinstance(native_id, str) or not re.fullmatch(r"[0-9]{1,64}", native_id):
            return False
        old, new = urlsplit(required), urlsplit(fresh)
        for parts in (old, new):
            if (
                parts.scheme != "https"
                or parts.fragment
                or parts.username is not None
                or parts.password is not None
                or parts.port not in (None, 443)
                or not re.fullmatch(r"scontent-[a-z0-9-]+\.cdninstagram\.com", parts.hostname or "")
            ):
                return False
        return bool(old.path and old.path != "/" and old.path.rsplit("/", 1)[-1] and old.path == new.path)
    except (MediaIncomplete, ValueError, TypeError):
        return False


def legacy_comment_source_key(row_id: str, media_urls: tuple[str, ...]) -> str:
    """Compute once from the original inputs and retain the key in the snapshot.

    Legacy comment keys include the original URL strings in their hash. Re-signing
    those URLs cannot reconstruct that identity; loaders must preserve this key.
    """
    seed = "|".join([row_id, *sorted(url.strip() for url in media_urls if url.strip())]) or "instagram-media"
    return "unknown-" + hashlib.sha256(seed.encode("utf-8", "ignore")).hexdigest()[:24]


def inventory_fingerprint(post: Mapping[str, Any], comments: list[dict]) -> dict[str, str]:
    """Bind source inventory, including text-only rows, independently of hosted outputs.

    CDN signatures are transport credentials, not a new source identity. Source
    observations and attachment topology still invalidate a changed inventory.
    """

    def clean(value):
        if isinstance(value, datetime):
            return utc_timestamp(value)
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, str) and value.startswith("https://"):
            return source_reference(value)
        if isinstance(value, Mapping):
            return {key: clean(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [clean(item) for item in value]
        return value

    raw = {**(post.get("raw_data") or {}), **((post.get("payload_sidecar") or {}).get("raw_data") or {})}
    post_columns = (
        "id",
        "shortcode",
        "scraped_at",
        "media_urls",
        "thumbnail_url",
        "source_account",
        "username",
        "owner_username",
        "owner_profile_pic_url",
        "owner_profile_pic_url_hd",
        "profile_tags",
        "collaborators",
        "mentions",
        "tagged_users_detail",
        "collaborators_detail",
        "child_posts_data",
    )
    raw_columns = (
        "owner_detail",
        "owner",
        "user",
        "profile_tags",
        "collaborators",
        "mentions",
        "tagged_users_detail",
        "collaborators_detail",
        "carousel_media_count",
        "detail_snapshot",
    )
    comment_columns = (
        "id",
        "comment_id",
        "parent_comment_id",
        "scraped_at",
        "source_account",
        "source_snapshot_type",
        "media_urls",
        "username",
        "author_profile_pic_url",
        "author_profile_pic_url_hd",
    )
    material = clean(
        {
            "version": MEDIA_INVENTORY_VERSION,
            "post": {key: post.get(key) for key in post_columns},
            "source": {key: raw.get(key) for key in raw_columns},
            "children": (post.get("payload_sidecar") or {}).get("child_posts_data")
            or post.get("child_posts_data")
            or [],
            "comments": [
                {key: row.get(key) for key in comment_columns}
                for row in sorted(comments, key=lambda row: (str(row["comment_id"]), str(row["id"])))
            ],
        }
    )
    return {"version": MEDIA_INVENTORY_VERSION, "sha256": hashlib.sha256(stable_json(material).encode()).hexdigest()}


def projected_inventory_fingerprint(post, comments, attachment_refreshes) -> dict[str, str]:
    """Project only verified attachment source changes onto the complete inventory."""
    from trr_backend.ingestion.comment_attachment_refresh import attachment_baseline, validate_attachment_order

    projected = {str(row["id"]): dict(row) for row in comments}
    seen = set()
    for proof in attachment_refreshes:
        proof.validate()
        row = projected.get(proof.row_id)
        if (
            proof.row_id in seen
            or row is None
            or proof.post_id != str(post["id"])
            or proof.shortcode != post["shortcode"]
            or proof.comment_id != str(row["comment_id"])
        ):
            raise CanonicalWriteError("Comment attachment inventory projection identity differs")
        seen.add(proof.row_id)
        retained = (row.get("raw_data") or {}).get("ingestion_attachment")
        if retained == proof.to_payload():
            if tuple(map(source_reference, row.get("media_urls") or [])) != (source_reference(proof.source_url),):
                raise CanonicalWriteError("Applied comment attachment inventory projection differs")
        else:
            try:
                validate_attachment_order(row, proof.captured_at)
            except ValueError as error:
                raise CanonicalWriteError("Comment attachment inventory projection version changed") from error
            if attachment_baseline(row) != proof.baseline_sha256:
                raise CanonicalWriteError("Comment attachment inventory projection baseline changed")
        row["media_urls"] = [source_reference(proof.source_url)]
    return inventory_fingerprint(post, list(projected.values()))


def read_inventory_fingerprint(conn, post_id: str, *, attachment_refreshes: tuple = ()) -> dict[str, str]:
    """Read after the caller's post identity lock; no source or hosted writes."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select p.*,to_jsonb(b) as payload_sidecar from social.instagram_posts p
            left join social.instagram_post_payloads b on b.post_id=p.id where p.id=%s""",
            (post_id,),
        )
        post = cur.fetchone()
        if not post:
            raise CanonicalWriteError("Media source post is missing")
        cur.execute(
            """select * from social.instagram_comments where post_id=%s and not is_missing
            and deleted_at is null order by comment_id,id""",
            (post_id,),
        )
        comments = [dict(row) for row in cur.fetchall()]
        if attachment_refreshes:
            return projected_inventory_fingerprint(post, comments, attachment_refreshes)
        return inventory_fingerprint(post, comments)


def invalidate_media_inventory(cur, run_id: str, target_key: str) -> None:
    """Fence media before taking the post lock; retain receipts and checkpoints.

    Call in the source writer's transaction before its post advisory/row lock,
    matching commit_target's media-target -> post ordering.
    Source backoff and safety blocks survive a new inventory; only their owning
    recovery path can clear them.
    """
    cur.execute(
        """update social.instagram_ingestion_targets set
        checkpoint=checkpoint || jsonb_build_object('media_inventory_invalidations',
            coalesce(checkpoint->'media_inventory_invalidations','[]'::jsonb) || jsonb_build_array(
                jsonb_build_object('at',clock_timestamp(),'outcome',outcome,'owner_token',owner_token,
                    'next_attempt_at',next_attempt_at,'blocked_reason',blocked_reason))),
        outcome='unresolved',observed_at=null,committed_at=null,owner_token=null,owner_started_at=null,
        owner_expires_at=null,
        max_attempts=greatest(max_attempts,attempts+12)
        where run_id=%s and stage='media' and target_key=%s
        and (outcome<>'unresolved' or owner_token is not null or next_attempt_at is not null
            or blocked_reason is not null)""",
        (run_id, target_key),
    )


@dataclass(frozen=True)
class AvatarSource:
    username: str
    source_url: str = field(repr=False)
    role: str = "tagged"
    comment_id: str | None = None
    observed_at: datetime | None = None
    cached_hosted_url: str | None = field(default=None, repr=False)
    cached_sha256: str | None = None
    download_url: str | None = field(default=None, repr=False, compare=False)
    download_provenance: tuple[str, str] | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class CommentMedia:
    comment_id: str
    row_id: str
    media_urls: tuple[str, ...] = field(repr=False)
    storage_source_key: str = ""
    observed_at: datetime | None = None
    attachment_refresh: Any | None = field(default=None, repr=False)
    attachment_version: str | None = None


@dataclass(frozen=True)
class MediaSnapshot:
    observed_at: datetime
    post_id: str  # legacy social.instagram_posts.id, never social_posts.id
    canonical_post_id: str  # social.social_posts.id
    shortcode: str
    show_id: str
    season_number: int
    week_index: int | None
    media_urls: tuple[str, ...] = field(repr=False)
    thumbnail_url: str = field(repr=False)
    expected_media_count: int
    comments: tuple[CommentMedia, ...]
    avatars: tuple[AvatarSource, ...]
    comments_complete: bool
    avatars_complete: bool
    unavailable: Evidence | None = None
    preserved_storage_keys: Mapping[str, str] = field(default_factory=dict, repr=False)
    existing_manifest: Mapping[str, Any] = field(default_factory=dict, repr=False)
    include_comment_avatars: bool = False
    unavailable_avatars: tuple[str, ...] = ()
    inventory_fingerprint: Mapping[str, str] | None = None
    result_inventory_fingerprint: Mapping[str, str] | None = None
    comment_settlement: Mapping[str, Any] = field(default_factory=dict)

    def storage_keys(self) -> dict[str, str]:
        keys = dict(self.preserved_storage_keys)

        def retain(role: str, entry: Any) -> None:
            if not isinstance(entry, Mapping):
                return
            key = entry.get("storage_key")
            if not key:
                if entry.get("url"):
                    raise MediaIncomplete("preserved_storage_identity_missing")
                return
            if role in keys and keys[role] != key:
                raise MediaIncomplete("preserved_storage_identity_conflict")
            keys[role] = key

        originals = self.existing_manifest.get("originals") or {}
        retain("cover_original", originals.get("cover"))
        for position, entry in enumerate(originals.get("media") or [], 1):
            retain(f"media_original_{position:02d}", entry)
        for role, entry in (self.existing_manifest.get("display_variants") or {}).items():
            retain(role, entry)
        return keys

    def validate(self) -> None:
        for fingerprint in (self.inventory_fingerprint, self.result_inventory_fingerprint):
            if fingerprint is not None and (
                set(fingerprint) != {"version", "sha256"}
                or fingerprint.get("version") != MEDIA_INVENTORY_VERSION
                or not re.fullmatch(r"[a-f0-9]{64}", fingerprint.get("sha256", ""))
            ):
                raise MediaIncomplete("invalid_media_inventory_fingerprint")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise MediaIncomplete("original_observation_required")
        if not self.post_id or not self.canonical_post_id or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.shortcode):
            raise MediaIncomplete("media_identity_required")
        try:
            UUID(self.post_id)
            UUID(self.canonical_post_id)
        except (ValueError, TypeError, AttributeError):
            raise MediaIncomplete("distinct_media_model_identities_required") from None
        if not re.fullmatch(r"[A-Za-z0-9_-]+", self.show_id) or type(self.season_number) is not int:
            raise MediaIncomplete("media_storage_context_required")
        if self.week_index is not None and type(self.week_index) is not int:
            raise MediaIncomplete("invalid_media_week")
        if not self.comments_complete or not self.avatars_complete:
            raise MediaIncomplete("media_inventory_not_sealed")
        if type(self.include_comment_avatars) is not bool:
            raise MediaIncomplete("explicit_comment_avatar_policy_required")
        for role, key in self.storage_keys().items():
            if not re.fullmatch(
                r"cover_original|media_original_\d{2,}|(?:thumb|card|detail|poster_card|poster_detail)_(?:webp|jpeg)",
                role,
            ):
                raise MediaIncomplete("unsupported_preserved_media_role")
            if not re.fullmatch(r"[A-Za-z0-9_./-]{1,1024}", key) or ".." in key.split("/"):
                raise MediaIncomplete("invalid_preserved_storage_key")
        if self.unavailable:
            proof = self.unavailable.read(pinned=True)
            if (
                self.unavailable.source_identity != f"instagram:media:{self.shortcode}"
                or utc_timestamp(self.unavailable.captured_at) != utc_timestamp(self.observed_at)
                or proof.get("status") not in {"deleted", "restricted", "media_unavailable"}
            ):
                raise MediaIncomplete("explicit_media_unavailability_required")
            return
        if type(self.expected_media_count) is not int or self.expected_media_count < 1:
            raise MediaIncomplete("media_coverage_required")
        if len(self.media_urls) != self.expected_media_count or not self.thumbnail_url:
            raise MediaIncomplete("media_coverage_incomplete")
        if len({c.comment_id for c in self.comments}) != len(self.comments):
            raise MediaIncomplete("duplicate_comment_identity")
        for comment in self.comments:
            if not comment.comment_id or not comment.row_id:
                raise MediaIncomplete("comment_identity_required")
            try:
                UUID(comment.row_id)
            except (ValueError, TypeError, AttributeError):
                raise MediaIncomplete("comment_row_identity_required") from None
            if comment.media_urls and not re.fullmatch(r"unknown-[a-f0-9]{24}", comment.storage_source_key):
                raise MediaIncomplete("preserved_comment_storage_key_required")
            if comment.observed_at is not None and comment.observed_at.utcoffset() is None:
                raise MediaIncomplete("original_comment_observation_required")
            if comment.attachment_refresh is not None:
                proof = comment.attachment_refresh
                proof.validate()
                if (proof.row_id, proof.comment_id, proof.post_id, proof.shortcode) != (
                    comment.row_id,
                    comment.comment_id,
                    self.post_id,
                    self.shortcode,
                ) or tuple(map(source_reference, comment.media_urls)) != (source_reference(proof.source_url),):
                    raise MediaIncomplete("attachment_refresh_identity_mismatch")
            if comment.attachment_version is not None and not re.fullmatch(r"[a-f0-9]{64}", comment.attachment_version):
                raise MediaIncomplete("attachment_version_invalid")
        comments = {comment.comment_id for comment in self.comments}
        if any(not re.fullmatch(r"[A-Za-z0-9._]{1,30}", handle) for handle in self.unavailable_avatars):
            raise MediaIncomplete("invalid_unavailable_avatar_identity")
        for avatar in self.avatars:
            proof = avatar.download_provenance
            pin = (
                proof[1] if isinstance(proof, tuple) and len(proof) == 2 and proof[0] == avatar.username else None
            )
            if avatar.download_url is not None and not avatar_delivery_equivalent(
                avatar.source_url, avatar.download_url, native_id=pin
            ):
                raise MediaIncomplete("avatar_download_identity_mismatch")
            if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", avatar.username):
                raise MediaIncomplete("invalid_avatar_identity")
            if avatar.role not in {"owner", "tagged", "collaborator", "comment", "mention"}:
                raise MediaIncomplete("unsupported_avatar_role")
            if avatar.role == "comment" and avatar.comment_id not in comments:
                raise MediaIncomplete("avatar_comment_identity_missing")
            if avatar.role == "comment" and not self.include_comment_avatars:
                raise MediaIncomplete("comment_avatar_policy_mismatch")
            if avatar.observed_at is not None and avatar.observed_at.utcoffset() is None:
                raise MediaIncomplete("original_avatar_observation_required")
            if avatar.cached_hosted_url or avatar.cached_sha256:
                if not avatar.cached_hosted_url or not re.fullmatch(r"[a-f0-9]{64}", avatar.cached_sha256 or ""):
                    raise MediaIncomplete("cached_avatar_proof_incomplete")
                source_reference(avatar.cached_hosted_url)


@dataclass(frozen=True)
class MediaResult:
    snapshot: MediaSnapshot = field(repr=False)
    post_fields: dict[str, Any]
    comment_fields: dict[str, dict[str, Any]]
    avatars: tuple[dict[str, Any], ...]
    outcome: Outcome = Outcome.COMMITTED


class MediaStorage:
    """Explicit S3/R2 dependencies with idempotent, conditional immutable writes."""

    def __init__(
        self,
        *,
        client: Any,
        bucket: str,
        build_hosted_url: Callable[[str], str],
        prefix: str = "",
        max_bytes: int = 50 * 1024 * 1024,
        retries: int = 3,
        opener: Callable[..., Any] = safe_requests_get,
        refresh: Callable[..., str | None] | None = None,
        variant_generator: Callable[..., Any] = generate_social_cover_display_variants,
    ):
        if not bucket or max_bytes <= 0 or not 1 <= retries <= 5:
            raise ValueError("Explicit storage bucket and bounded download limits are required")
        config = getattr(getattr(client, "meta", None), "config", None)
        self._storage_call_seconds = self._storage_read_seconds = 0.0
        if config is not None:
            sdk_retries = config.retries or {}
            total = sdk_retries.get("total_max_attempts", sdk_retries.get("max_attempts", 3) + 1)
            if total != 1:
                raise MediaIncomplete("storage_sdk_retries_must_be_disabled")
            connect, read = float(config.connect_timeout), float(config.read_timeout)
            if not all(math.isfinite(value) and value > 0 for value in (connect, read)):
                raise MediaIncomplete("storage_sdk_timeouts_must_be_bounded")
            self._storage_call_seconds, self._storage_read_seconds = connect + read, read
        self.client, self.bucket, self.build_hosted_url = client, bucket, build_hosted_url
        self.prefix, self.max_bytes, self.retries = prefix.strip("/"), max_bytes, retries
        self.opener, self.refresh, self.variant_generator = opener, refresh, variant_generator

    def _url(self, key: str) -> str:
        value = self.build_hosted_url(key)
        if source_reference(value) != value:
            raise MediaIncomplete("hosted_url_must_be_public")
        return value

    def _storage_budget(self, check: Callable | None, *, body: bool = False) -> None:
        guard = getattr(check, "require_budget", None)
        if callable(guard):
            guard(self._storage_read_seconds if body else self._storage_call_seconds)

    def _storage_call(self, operation: str, *, check: Callable, **parameters):
        from trr_backend.ingestion.media_storage_io import (
            BoundedS3Client,
            StorageDeadlineExceededError,
            StorageProcessError,
        )

        if not isinstance(self.client, BoundedS3Client):
            return getattr(self.client, operation)(**parameters)
        try:
            return self.client.call(operation, check=check, **parameters)
        except StorageDeadlineExceededError:
            raise MediaIncomplete("target_deadline") from None
        except StorageProcessError:
            raise MediaIncomplete("media_storage_failed") from None

    def _download(self, url: str, *, check: Callable, request: Callable) -> tuple[bytes, str]:
        current = url
        for attempt in range(self.retries):
            try:
                for redirects in range(6):
                    check()
                    current = validate_media_url(current, policy=_POLICY)
                    request()
                    remaining = getattr(check, "remaining", lambda: None)()
                    total_timeout = min(120, remaining) if remaining is not None else 120
                    with self.opener(
                        requests,
                        current,
                        policy=_POLICY,
                        allow_redirects=False,
                        timeout=(min(10, total_timeout), min(60, total_timeout)),
                        stream=True,
                        max_bytes=self.max_bytes,
                        total_timeout=total_timeout,
                        body_timeout=min(90, total_timeout),
                    ) as response:
                        if response.status_code in {301, 302, 303, 307, 308}:
                            if redirects == 5:
                                raise MediaIncomplete("media_redirect_limit")
                            current = validate_media_redirect(
                                current, response.headers.get("location", ""), policy=_POLICY
                            )
                            continue
                        if response.status_code in {401, 403} and self.refresh and attempt + 1 < self.retries:
                            check()
                            fresh = self.refresh(
                                source_reference=source_reference(url), check_cancelled=check, on_request=request
                            )
                            if fresh:
                                current = fresh
                                break
                        if response.status_code == 429 or (
                            response.status_code == 503 and response.headers.get("Retry-After")
                        ):
                            raise MediaIncomplete(
                                "media_source_throttled", retry_at=_retry_at(response.headers.get("Retry-After"))
                            )
                        response.raise_for_status()
                        length = response.headers.get("Content-Length")
                        if length and int(length) > self.max_bytes:
                            raise MediaIncomplete("asset_too_large")
                        chunks, size = [], 0
                        for chunk in response.iter_content(chunk_size=64 * 1024):
                            getattr(check, "body", check)()
                            size += len(chunk)
                            if size > self.max_bytes:
                                raise MediaIncomplete("asset_too_large")
                            chunks.append(chunk)
                        data = b"".join(chunks)
                        if hashlib.sha256(data).hexdigest() in _SOURCE_PLACEHOLDER_SHA256:
                            raise MediaIncomplete("media_source_placeholder")
                        content_type = str(response.headers.get("Content-Type") or "").split(";", 1)[0].lower()
                        error = _media_payload_error(source_url=current, content_type=content_type, data=data)
                        if not data or error:
                            raise MediaIncomplete(error or "empty_media_response")
                        content_type = (
                            _sniff_image_content_type(data[:4096])
                            or _sniff_video_content_type(data[:4096])
                            or content_type
                        )
                        return data, content_type
            except LostOwnership:
                raise
            except UnsafeMediaUrlError:
                raise MediaIncomplete("unsafe_media_url") from None
            except (requests.RequestException, ValueError) as exc:
                if isinstance(exc, MediaIncomplete):
                    raise
                getattr(check, "check_deadline", lambda: None)()
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status and status not in {401, 403, 408, 425, 429} and status < 500:
                    raise MediaIncomplete(f"media_http_{status}") from None
                if attempt + 1 == self.retries:
                    raise MediaIncomplete("media_download_failed") from None
        raise MediaIncomplete("media_download_failed")

    def _head(self, key: str, *, request: Callable, check: Callable | None = None) -> dict[str, Any] | None:
        for attempt in range(self.retries):
            self._storage_budget(check)
            request()
            self._storage_budget(check)
            try:
                return self._storage_call("head_object", check=check, Bucket=self.bucket, Key=key)
            except ClientError as exc:
                if str(exc.response.get("Error", {}).get("Code")) in {"404", "NoSuchKey", "NotFound"}:
                    return None
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
                if status != 429 and status < 500:
                    raise MediaIncomplete("media_storage_failed") from None
            except BotoCoreError:
                pass
            if attempt + 1 == self.retries:
                raise MediaIncomplete("media_storage_failed") from None
        raise MediaIncomplete("media_storage_failed")

    def _same(self, key: str, data: bytes, head: Mapping[str, Any], *, check: Callable, request: Callable) -> bool:
        if int(head.get("ContentLength", -1)) != len(data):
            return False
        digest = hashlib.sha256(data).hexdigest()
        stored = (head.get("Metadata") or {}).get("sha256")
        if stored:
            return stored == digest
        # Older objects have no digest metadata. Compare their bytes instead of
        # trusting multipart ETags or silently overwriting their maintained key.
        self._storage_budget(check)
        request()
        self._storage_budget(check)
        from trr_backend.ingestion.media_storage_io import BoundedS3Client

        if isinstance(self.client, BoundedS3Client):
            result = self._storage_call("digest_object", check=check, Bucket=self.bucket, Key=key)
            return result["bytes"] == len(data) and result["sha256"] == digest
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"]
        try:
            found, size = hashlib.sha256(), 0
            while True:
                self._storage_budget(check, body=True)
                chunk = body.read(64 * 1024)
                if not chunk:
                    break
                getattr(check, "body", check)()
                size += len(chunk)
                if size > self.max_bytes:
                    return False
                found.update(chunk)
            return size == len(data) and found.hexdigest() == digest
        finally:
            body.close()

    def put(self, key: str, data: bytes, content_type: str, *, check: Callable, request: Callable) -> None:
        check()
        head = self._head(key, request=request, check=check)
        if head is not None:
            if not self._same(key, data, head, check=check, request=request):
                raise MediaIncomplete("existing_object_content_conflict")
            return

        owner = self

        class ConditionalClient:
            def put_object(self, **kwargs):
                owner._storage_budget(check)
                request()
                owner._storage_budget(check)
                return owner._storage_call(
                    "put_object",
                    check=check,
                    **kwargs,
                    IfNoneMatch="*",
                    Metadata={"sha256": hashlib.sha256(data).hexdigest()},
                )

        for attempt in range(self.retries):
            check()
            try:
                upload_bytes_to_s3(
                    ConditionalClient(), bucket=self.bucket, key=key, data=data, content_type=content_type
                )
                return
            except (BotoCoreError, ClientError) as exc:
                status = (
                    exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
                    if isinstance(exc, ClientError)
                    else 503
                )
                if status not in {409, 412, 429} and status < 500:
                    raise MediaIncomplete("media_storage_failed") from None
                # A lost response may have left the object committed remotely.
                head = self._head(key, request=request, check=check)
                if head is not None:
                    if self._same(key, data, head, check=check, request=request):
                        return
                    raise MediaIncomplete("existing_object_content_conflict") from None
                if attempt + 1 == self.retries:
                    raise MediaIncomplete("media_storage_failed") from None

    def host(
        self,
        url: str,
        *,
        root: str,
        name: str,
        check: Callable,
        request: Callable,
        avatar: str | None = None,
        variants: bool = False,
        post_id: str = "",
        preserved_key: str | None = None,
        preserved_variants: Mapping[str, str] | None = None,
        version_post_role: str | None = None,
    ) -> tuple[dict, dict]:
        if version_post_role is not None and not re.fullmatch(
            r"cover_original|media_original_\d{2,}", version_post_role
        ):
            raise MediaIncomplete("invalid_content_version_role")
        if avatar and version_post_role is not None:
            raise MediaIncomplete("avatar_content_version_forbidden")
        data, content_type = self._download(url, check=check, request=request)
        digest = hashlib.sha256(data).hexdigest()
        suffix = Path(urlsplit(url).path).suffix.lower()
        ext = suffix if suffix in _EXTENSIONS else mimetypes.guess_extension(content_type) or ".bin"
        key = preserved_key or f"{root}/{name}{ext}"
        if avatar:
            if not content_type.startswith("image/"):
                raise MediaIncomplete("avatar_image_required")
            key = build_instagram_profile_pic_s3_key(avatar, digest, guess_ext_from_content_type(content_type))
        width = height = None
        if content_type.startswith("image/"):
            try:
                with Image.open(io.BytesIO(data)) as image:
                    width, height = image.size
                    image.verify()
            except (OSError, ValueError):
                raise MediaIncomplete("invalid_media_image") from None

        def put_role(destination, body, mime, role):
            try:
                self.put(destination, body, mime, check=check, request=request)
                return destination
            except MediaIncomplete as error:
                if version_post_role is None or error.code != "existing_object_content_conflict":
                    raise
            # A preserved key can itself be a previous content version. Replace
            # its suffix so retries and A/B/A renditions reuse one immutable key.
            path = Path(destination)
            stem = re.sub(r"\.sha256-[a-f0-9]{64}$", "", path.stem)
            content_hash = hashlib.sha256(body).hexdigest()
            resolved = str(path.with_name(f"{stem}.sha256-{content_hash}{path.suffix}"))
            if len(resolved) > 1024:
                raise MediaIncomplete("versioned_storage_key_too_long")
            self.put(resolved, body, mime, check=check, request=request)
            LOGGER.info(
                "instagram_media_content_version_resolved",
                extra={"media_role": role, "content_sha256": content_hash, "content_bytes": len(body)},
            )
            return resolved

        key = put_role(key, data, content_type, version_post_role)
        entry = {
            "url": self._url(key),
            "format": ext.lstrip("."),
            "content_type": content_type,
            "width": width,
            "height": height,
            "bytes": len(data),
            "storage_key": key,
            "sha256": digest,
        }
        display = {}
        if variants:
            if not content_type.startswith("image/") or content_type == "image/svg+xml":
                raise MediaIncomplete("cover_display_image_required")
            owner = self
            resolved_variants = {}
            variant_hashes = {}

            def variant_key(key: str) -> str:
                name = Path(key).stem
                return resolved_variants.get(key, (preserved_variants or {}).get(name, key))

            class VariantClient:
                def put_object(self, **kwargs):
                    if kwargs["Bucket"] != owner.bucket:
                        raise MediaIncomplete("variant_bucket_mismatch")
                    resolved_variants[kwargs["Key"]] = put_role(
                        variant_key(kwargs["Key"]), kwargs["Body"], kwargs["ContentType"], Path(kwargs["Key"]).stem
                    )
                    variant_hashes[kwargs["Key"]] = hashlib.sha256(kwargs["Body"]).hexdigest()

                def delete_object(self, **kwargs):
                    # The maintained generator cleans partial uploads on error.
                    # These immutable objects may belong to a prior attempt;
                    # retain them for retry instead of deleting shared storage.
                    return {}

            prefix = f"social-variants/instagram/{post_id}"
            if self.prefix:
                prefix = self.prefix + "/" + prefix
            with tempfile.NamedTemporaryFile(suffix=ext) as source:
                source.write(data)
                source.flush()
                generated = self.variant_generator(
                    image_path=source.name,
                    s3_client=VariantClient(),
                    bucket=self.bucket,
                    key_prefix=prefix,
                    build_hosted_url=lambda key: self._url(variant_key(key)),
                )
            display = {
                item.variant_key: {
                    "url": owner._url(variant_key(item.storage_key)),
                    "format": item.format,
                    "content_type": item.content_type,
                    "width": item.width,
                    "height": item.height,
                    "bytes": item.bytes,
                    "source_role": "cover",
                    "variant_key": item.variant_key,
                    "storage_key": variant_key(item.storage_key),
                    "sha256": variant_hashes[item.storage_key],
                }
                for item in generated
            }
            expected = {
                f"{size}_{fmt}"
                for size in ("thumb", "card", "detail", "poster_card", "poster_detail")
                for fmt in ("webp", "jpeg")
            }
            if set(display) != expected:
                raise MediaIncomplete("display_variants_incomplete")
        return entry, display


def mirror_snapshot(
    snapshot: MediaSnapshot, storage: MediaStorage, *, check: Callable, request: Callable
) -> MediaResult:
    snapshot.validate()
    check()
    if snapshot.unavailable:
        return MediaResult(snapshot, {}, {}, (), Outcome.UNAVAILABLE)
    week = f"week-{snapshot.week_index}" if snapshot.week_index is not None else "week-unknown"
    root = f"social/instagram/{snapshot.show_id}/{snapshot.season_number}/{week}"
    if storage.prefix:
        root = storage.prefix + "/" + root
    post_root = root + "/" + snapshot.shortcode
    stamp = utc_timestamp(snapshot.observed_at)
    preserved = snapshot.storage_keys()
    cover, variants = storage.host(
        snapshot.thumbnail_url,
        root=post_root,
        name="cover-original",
        check=check,
        request=request,
        variants=True,
        post_id=snapshot.post_id,
        preserved_key=preserved.get("cover_original"),
        preserved_variants=preserved,
        version_post_role="cover_original",
    )
    cover.update(source_role="cover", variant_key="cover_original", generated_at=stamp)
    # Match maintained within-post reuse for the exact source URL. A signed
    # query or a different path can identify different bytes; neither is
    # normalized here. Existing role keys always take precedence over reuse.
    hosted_by_source_url = {snapshot.thumbnail_url.strip(): cover}
    originals = []
    for position, url in enumerate(snapshot.media_urls, 1):
        preserved_key = preserved.get(f"media_original_{position:02d}")
        shared = hosted_by_source_url.get(url.strip())
        if shared is not None and (preserved_key is None or preserved_key == shared["storage_key"]):
            check()
            entry = dict(shared)  # Cover and media retain independent role metadata.
        else:
            entry, _ = storage.host(
                url,
                root=post_root,
                name=f"media-{position:02d}",
                check=check,
                request=request,
                preserved_key=preserved_key,
                version_post_role=f"media_original_{position:02d}",
            )
            hosted_by_source_url.setdefault(url.strip(), entry)
        entry.update(source_role="media", variant_key=f"media_original_{position:02d}", generated_at=stamp)
        originals.append(entry)
    for variant in variants.values():
        variant["generated_at"] = stamp
    physical_originals = {item["storage_key"]: item for item in [cover, *originals]}
    physical_variants = {item["storage_key"]: item for item in variants.values()}
    physical_objects = {**physical_originals, **physical_variants}
    manifest = {
        "selection_policy": "best_per_role_with_cover_display_variants",
        "originals": {"cover": cover, "media": originals},
        "display_variants": variants,
        "display_variants_status": "ready",
        "display_variants_reason": None,
        "updated_at": stamp,
        "ingestion_media": {"version": MEDIA_VERSION, "observed_at": stamp},
        "storage_summary": {
            "original_bytes": sum(i["bytes"] for i in physical_originals.values()),
            "variant_bytes": sum(v["bytes"] for v in physical_variants.values()),
            "object_count": len(physical_objects),
            "variant_count": len(physical_variants),
            "cdn_url_count": len({item["url"] for item in physical_objects.values()}),
        },
    }
    post = {
        "hosted_thumbnail_url": cover["url"],
        "hosted_media_urls": [item["url"] for item in originals],
        "asset_manifest": manifest,
        "media_mirror_status": "mirrored",
        "media_mirror_error": None,
        "profile_pic_mirror_status": "unsupported"
        if snapshot.unavailable_avatars and not snapshot.avatars
        else "mirrored",
        "profile_pic_mirror_error": None,
    }
    comments = {}
    for comment in snapshot.comments:
        hosted = []
        for position, url in enumerate(comment.media_urls, 1):
            entry, _ = storage.host(
                url,
                root=root + "/" + comment.storage_source_key,
                name=f"media-{position:02d}",
                check=check,
                request=request,
            )
            hosted.append(entry["url"])
        comments[comment.comment_id] = {
            "hosted_media_urls": hosted,
            "media_mirror_status": "mirrored",
            "media_mirror_error": None,
        }
    avatars = []
    tagged = {}
    hosted_avatars = {}
    for avatar in snapshot.avatars:
        # Attempt-local reuse only. Signed delivery URLs remain byte-for-byte
        # keys; role, lineage and original observation stay on every slot.
        hosting_key = (
            avatar.username.lower(),
            avatar.download_url or avatar.source_url,
            avatar.cached_hosted_url,
            avatar.cached_sha256,
            avatar.download_provenance,
        )
        if hosting_key in hosted_avatars:
            check()
            entry = hosted_avatars[hosting_key]
        elif avatar.cached_hosted_url:
            # Trusted persisted mirrored proof, not a new remote-object check.
            check()
            entry = {"url": avatar.cached_hosted_url, "sha256": avatar.cached_sha256}
        else:
            entry, _ = storage.host(
                avatar.download_url or avatar.source_url,
                root="",
                name="",
                avatar=avatar.username,
                check=check,
                request=request,
            )
        hosted_avatars[hosting_key] = entry
        avatars.append(
            {
                "username": avatar.username.lower(),
                "source_url": source_reference(avatar.source_url),
                "hosted_url": entry["url"],
                "sha256": entry["sha256"],
                "observed_at": utc_timestamp(avatar.observed_at or snapshot.observed_at),
            }
        )
        if avatar.role == "owner":
            post["hosted_owner_profile_pic_url"] = entry["url"]
        elif avatar.role == "comment":
            comments[avatar.comment_id]["hosted_author_profile_pic_url"] = entry["url"]
        else:
            tagged[avatar.username] = entry["url"]
    if tagged:
        post["hosted_tagged_profile_pics"] = tagged
    resolved_keys = {
        "cover_original": cover["storage_key"],
        **{f"media_original_{position:02d}": item["storage_key"] for position, item in enumerate(originals, 1)},
        **{role: item["storage_key"] for role, item in variants.items()},
    }
    if any(resolved_keys.get(role) != key for role, key in preserved.items()):
        # Result-v1 binds its inventory role keys to the actual immutable output.
        # The pre-host inventory and auxiliary prior-manifest evidence retain
        # the superseded identities; native source fields/clocks do not change.
        snapshot = replace(snapshot, preserved_storage_keys=resolved_keys, existing_manifest={})
    return MediaResult(snapshot, post, comments, tuple(avatars))


def _locked(conn: Any, table: str, keys: Mapping[str, Any]) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            sql.SQL("select * from social.{} where {} for update").format(
                sql.Identifier(table), sql.SQL(" and ").join(sql.SQL("{}=%s").format(sql.Identifier(k)) for k in keys)
            ),
            list(keys.values()),
        )
        return dict(cur.fetchone() or {})


def _update(conn: Any, table: str, identity: str, fields: Mapping[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("update social.{} set {} where id=%s").format(
                sql.Identifier(table), sql.SQL(",").join(sql.SQL("{}=%s").format(sql.Identifier(k)) for k in fields)
            ),
            [Json(v) if isinstance(v, (dict, list)) else v for v in fields.values()] + [identity],
        )
        if cur.rowcount != 1:
            raise CanonicalWriteError("Media identity disappeared")


def _newer(observed_at: datetime, *rows: Mapping[str, Any]) -> None:
    for row in rows:
        for key in (
            "scraped_at",
            "last_seen_at",
            "last_scraped_at",
            "metadata_scraped_at",
            "last_checked_at",
        ):
            stamp = row.get(key)
            if stamp and observed_at < stamp:
                raise OlderObservation("Media snapshot predates stored observation")


def _media_observation_guard(observed_at: datetime, payload: Mapping[str, Any]) -> None:
    """Source ordering is separate from legacy attempt/payload wall clocks."""
    marker = payload.get("ingestion_media")
    if marker is None:
        return  # Legacy freshness is proved by the current source row + object identity.
    if not isinstance(marker, Mapping) or marker.get("version") != MEDIA_VERSION:
        raise CanonicalWriteError("Unsupported media source observation marker")
    try:
        source_time = datetime.fromisoformat(marker["observed_at"])
        if source_time.utcoffset() is None:
            raise ValueError("Timezone required")
    except (KeyError, TypeError, ValueError):
        raise CanonicalWriteError("Invalid media source observation marker") from None
    if observed_at < source_time:
        raise OlderObservation("Media snapshot predates stored media source observation")


def write_avatar_version(
    conn: Any,
    *,
    username: str,
    source_url: str,
    hosted_url: str,
    content_hash: str,
    observed_at: datetime,
) -> dict[str, Any]:
    """Persist hosted proof for one handle and source image version atomically.

    The caller owns its business fence, native identity binding and transaction.
    A different current profile image never invalidates a retained older version.
    This helper does not update profiles, source slots, or their observation time.
    """
    if conn is None or conn.autocommit:
        raise CanonicalWriteError("Avatar write requires an explicit transaction")
    if not isinstance(username, str) or not re.fullmatch(r"[A-Za-z0-9._]{1,30}", username):
        raise CanonicalWriteError("Avatar handle identity is invalid")
    username = username.lower()
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise CanonicalWriteError("Original avatar observation is required")
    if (
        not isinstance(source_url, str)
        or not source_url
        or source_reference(source_url) != source_url
        or not isinstance(hosted_url, str)
        or not hosted_url
        or source_reference(hosted_url) != hosted_url
        or not isinstance(content_hash, str)
        or not re.fullmatch(r"[a-f0-9]{64}", content_hash)
    ):
        raise CanonicalWriteError("Complete avatar version proof is required")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:avatar:" + username,))
        # Old registry rows may retain signed URL variants of this same public
        # path. Compare all matching versions before writing its canonical key.
        cur.execute(
            """select * from social.avatar_registry where platform='instagram' and account_handle=%s
            and source_url is not null order by last_checked_at desc nulls last,
            coalesce(status='mirrored' and nullif(hosted_url,'') is not null
              and content_hash ~ '^[a-f0-9]{64}$',false) desc,id desc for update""",
            (username,),
        )
        old = {}
        for row in cur.fetchall():
            try:
                matches = source_reference(row["source_url"]) == source_url
            except (TypeError, ValueError, MediaIncomplete):
                continue
            if matches:
                old = dict(row)
                break
        if (
            old.get("status") == "mirrored"
            and old.get("content_hash") == content_hash
            and old.get("hosted_url") == hosted_url
            and old.get("last_checked_at") is not None
        ):
            # Reuse the actual retained proof; do not relabel it with a caller's
            # earlier slot time or manufacture a later source observation.
            return {
                "username": username,
                "source_url": source_url,
                "hosted_url": hosted_url,
                "content_hash": content_hash,
                "observed_at": old["last_checked_at"],
            }
        _newer(observed_at, old)
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,hosted_url,
            status,failure_reason,last_checked_at,updated_at) values('instagram',%s,%s,%s,%s,'mirrored',null,%s,%s)
            on conflict(platform,account_handle,source_url) do update set content_hash=excluded.content_hash,
            hosted_url=excluded.hosted_url,status='mirrored',failure_reason=null,
            last_checked_at=excluded.last_checked_at,updated_at=excluded.updated_at""",
            (username, source_url, content_hash, hosted_url, observed_at, observed_at),
        )
    return {
        "username": username,
        "source_url": source_url,
        "hosted_url": hosted_url,
        "content_hash": content_hash,
        "observed_at": observed_at,
    }


def write_comment_media(
    conn: Any, *, run_id: str, post_id: str, observed_at: datetime, comments: tuple, comment_fields: Mapping
) -> None:
    """Apply comment media under the caller's transaction and ownership fence."""
    if conn is None or conn.autocommit:
        raise CanonicalWriteError("Comment media write requires an explicit transaction")
    for comment in comments:
        row = _locked(
            conn,
            "instagram_comments",
            {"id": comment.row_id, "post_id": post_id, "comment_id": comment.comment_id},
        )
        if not row:
            raise CanonicalWriteError("Media comment identity differs from snapshot")
        comment_observed_at = comment.observed_at or observed_at
        _newer(comment_observed_at, row)
        _media_observation_guard(comment_observed_at, row.get("raw_data") or {})
        raw = dict(row.get("raw_data") or {})
        retained = raw.get("ingestion_attachment") or {}
        proof = comment.attachment_refresh
        same_proof = proof is not None and retained == proof.to_payload()
        if retained.get("evidence_sha256") != comment.attachment_version and not same_proof:
            raise CanonicalWriteError("Comment attachment version changed")
        if proof is not None:
            from trr_backend.ingestion.comment_attachment_refresh import (
                attachment_baseline,
                validate_attachment_order,
            )

            proof.validate()
            if same_proof:
                if raw.get("giphy_media_info") != proof.giphy_media_info or [
                    source_reference(url) for url in row.get("media_urls") or []
                ] != [source_reference(proof.source_url)]:
                    raise CanonicalWriteError("Applied comment attachment differs from proof")
            else:
                if attachment_baseline(row) != proof.baseline_sha256:
                    raise CanonicalWriteError("Comment attachment baseline changed")
                validate_attachment_order(row, proof.captured_at)
            raw["ingestion_attachment"] = proof.to_payload()
            raw["giphy_media_info"] = dict(proof.giphy_media_info)
        elif [source_reference(url) for url in row.get("media_urls") or []] != [
            source_reference(url) for url in comment.media_urls
        ]:
            raise CanonicalWriteError("Comment media inventory changed")
        changes = dict(comment_fields[comment.comment_id])
        if proof is not None:
            changes["media_urls"] = [source_reference(proof.source_url)]
        changes.update(
            media_mirror_last_attempt_at=datetime.now(UTC),
            media_mirror_last_job_id=run_id,
            media_mirror_attempt_count=int(row.get("media_mirror_attempt_count") or 0) + 1,
            raw_data={
                **raw,
                "ingestion_media": {"version": MEDIA_VERSION, "observed_at": utc_timestamp(comment_observed_at)},
            },
        )
        _update(conn, "instagram_comments", comment.row_id, changes)


def _avatar_versions(avatars: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    """Select each registry version once; reject contradictory proof up front.

    Stable ordering retains the first slot's clock, matching the existing writer:
    later identical mirrored proof reuses its original registry observation.
    Every role slot and its own clock remain intact in MediaResult/evidence.
    """
    versions = {}
    for avatar in sorted(avatars, key=lambda item: (item["username"].lower(), item["source_url"])):
        key = (avatar["username"].lower(), avatar["source_url"])
        previous = versions.get(key)
        if previous is not None:
            if (previous["hosted_url"], previous["sha256"]) != (avatar["hosted_url"], avatar["sha256"]):
                raise CanonicalWriteError("Contradictory duplicate avatar version proof")
        else:
            versions[key] = avatar
    return tuple(versions.values())


def _persist_avatar_versions(conn, avatar_versions):
    for avatar in avatar_versions:
        write_avatar_version(
            conn,
            username=avatar["username"],
            source_url=avatar["source_url"],
            hosted_url=avatar["hosted_url"],
            content_hash=avatar["sha256"],
            observed_at=datetime.fromisoformat(avatar["observed_at"]),
        )


def write_media(
    conn: Any, *, run_id: str, account: str, target_key: str, observed_at: datetime, result: MediaResult
) -> dict[str, Any]:
    """Update media-only columns in the same transaction as the business receipt."""
    source = result.snapshot
    source.validate()
    avatar_versions = _avatar_versions(result.avatars)
    if conn is None or conn.autocommit or target_key != source.shortcode or observed_at != source.observed_at:
        raise CanonicalWriteError("Media write requires its original fenced snapshot")
    with conn.cursor() as cur:
        # Match the canonical post writer's identity lock, including missing rows.
        cur.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:post:" + source.shortcode,))
    post = _locked(conn, "instagram_posts", {"id": source.post_id, "shortcode": source.shortcode})
    canonical = _locked(
        conn, "social_posts", {"id": source.canonical_post_id, "platform": "instagram", "source_id": source.shortcode}
    )
    if (
        not post
        or not canonical
        or (post.get("source_account") or post.get("username") or "").lower() != account.lower()
    ):
        raise CanonicalWriteError("Media post identity or account differs from snapshot")
    sidecar = _locked(conn, "instagram_post_payloads", {"post_id": source.post_id})
    _newer(observed_at, post, canonical)
    _media_observation_guard(observed_at, post.get("asset_manifest") or {})
    _media_observation_guard(observed_at, sidecar.get("asset_manifest") or {})
    if source.inventory_fingerprint is None:
        raise CanonicalWriteError("Media inventory fingerprint is required for canonical apply")
    refreshes = tuple(comment.attachment_refresh for comment in source.comments if comment.attachment_refresh)
    current_fingerprint = read_inventory_fingerprint(conn, source.post_id)
    if refreshes:
        projected = read_inventory_fingerprint(conn, source.post_id, attachment_refreshes=refreshes)
        if projected != source.result_inventory_fingerprint:
            raise CanonicalWriteError("Attachment result inventory fingerprint changed")
    elif source.result_inventory_fingerprint is not None:
        raise CanonicalWriteError("Alternate inventory fingerprint requires exact attachment proofs")
    if current_fingerprint != source.inventory_fingerprint and (
        not refreshes or current_fingerprint != source.result_inventory_fingerprint
    ):
        raise CanonicalWriteError("Media inventory fingerprint changed")
    identities = {
        "post_id": source.post_id,
        "canonical_post_id": source.canonical_post_id,
        "shortcode": source.shortcode,
        "inventory_fingerprint": dict(source.inventory_fingerprint),
        "comment_settlement": dict(source.comment_settlement),
    }
    if result.outcome == Outcome.UNAVAILABLE:
        if source.unavailable is None:
            raise CanonicalWriteError("Unavailability lacks retained source proof")
        return identities
    if result.outcome != Outcome.COMMITTED:
        raise CanonicalWriteError("Unsupported media outcome")
    from trr_backend.ingestion.saved_owner_asset import commit_guard as saved_owner_asset_commit_guard

    saved_owner_asset_commit_guard(conn, run_id=run_id, account=account, target_key=target_key, result=result)
    if [source_reference(url) for url in post.get("media_urls") or []] != [
        source_reference(url) for url in source.media_urls
    ]:
        raise CanonicalWriteError("Media source inventory changed")
    fields = dict(result.post_fields)
    if (
        fields.get("media_mirror_status") != "mirrored"
        or len(fields.get("hosted_media_urls", [])) != source.expected_media_count
    ):
        raise CanonicalWriteError("Media result is incomplete")
    manifest = dict(sidecar.get("asset_manifest") or post.get("asset_manifest") or {})
    manifest.update(fields["asset_manifest"])
    fields.update(
        asset_manifest=manifest,
        media_mirror_last_attempt_at=datetime.now(UTC),
        media_mirror_last_job_id=run_id,
        media_mirror_attempt_count=int(post.get("media_mirror_attempt_count") or 0) + 1,
    )
    if fields.get("hosted_tagged_profile_pics"):
        fields["hosted_tagged_profile_pics"] = {
            **(post.get("hosted_tagged_profile_pics") or {}),
            **fields["hosted_tagged_profile_pics"],
        }
    _update(conn, "instagram_posts", source.post_id, fields)
    with conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_post_payloads
            (post_id,raw_data,child_posts_data,asset_manifest,payload_updated_at)
            values(%s,%s,%s,%s,%s) on conflict(post_id) do update
            set asset_manifest=excluded.asset_manifest,payload_updated_at=excluded.payload_updated_at""",
            (
                source.post_id,
                Json(sidecar.get("raw_data") or post.get("raw_data") or {}),
                Json(sidecar.get("child_posts_data") or post.get("child_posts_data") or []),
                Json(manifest),
                observed_at,
            ),
        )
    for position, url in enumerate(fields["hosted_media_urls"]):
        asset = _locked(
            conn,
            "social_post_media_assets",
            {"platform": "instagram", "post_id": source.canonical_post_id, "position": position},
        )
        if not asset:
            raise CanonicalWriteError("Canonical media asset is absent")
        _newer(observed_at, asset)
        _media_observation_guard(observed_at, asset.get("media_payload") or {})
        changed = {
            "hosted_url": url,
            "mirror_status": "mirrored",
            "mirror_error": None,
            "mirror_last_attempt_at": datetime.now(UTC),
            "media_payload": {
                **(asset.get("media_payload") or {}),
                "ingestion_media": {"version": MEDIA_VERSION, "observed_at": utc_timestamp(observed_at)},
            },
        }
        if position == 0:
            changed["hosted_thumbnail_url"] = fields["hosted_thumbnail_url"]
        _update(conn, "social_post_media_assets", str(asset["id"]), changed)
    write_comment_media(
        conn,
        run_id=run_id,
        post_id=source.post_id,
        observed_at=observed_at,
        comments=source.comments,
        comment_fields=result.comment_fields,
    )
    _persist_avatar_versions(conn, avatar_versions)
    identities["inventory_fingerprint"] = read_inventory_fingerprint(conn, source.post_id)
    if refreshes and identities["inventory_fingerprint"] != source.result_inventory_fingerprint:
        raise CanonicalWriteError("Applied attachment inventory fingerprint differs from exact projection")
    return identities


def prior_manifest_evidence(snapshot: MediaSnapshot, *, captured_at: datetime | None = None) -> Evidence | None:
    """Preserve flat prior object provenance before replacing current role URLs.

    This auxiliary evidence is not native inventory or a completion receipt.
    No prior history is copied, and delivery signatures never enter the payload.
    Its envelope uses the archival clock for retention; payload clocks preserve
    the original manifest and object observations without relabeling them.
    """
    manifest = snapshot.existing_manifest
    if not manifest:
        return None

    def stamp(value):
        try:
            parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
            return utc_timestamp(parsed) if isinstance(parsed, datetime) and parsed.utcoffset() is not None else None
        except (TypeError, ValueError):
            return None

    originals = manifest.get("originals") or {}
    roles = [
        ("cover_original", "cover", originals.get("cover")),
        *[
            (f"media_original_{index:02d}", "media", entry)
            for index, entry in enumerate(originals.get("media") or [], 1)
        ],
        *[(role, "cover", entry) for role, entry in sorted((manifest.get("display_variants") or {}).items())],
    ]
    entries = []
    for role, source_role, entry in roles:
        if not isinstance(entry, Mapping):
            continue
        key = entry.get("storage_key")
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9_./-]{1,1024}", key) or ".." in key.split("/"):
            continue
        item = {"variant_key": role, "source_role": source_role, "storage_key": key}
        for attribute in ("bytes", "width", "height"):
            if type(entry.get(attribute)) is int and entry[attribute] > 0:
                item[attribute] = entry[attribute]
        if re.fullmatch(r"[a-f0-9]{64}", str(entry.get("sha256") or "")):
            item["sha256"] = entry["sha256"]
        for attribute in ("format", "content_type"):
            value = entry.get(attribute)
            if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_+./-]{1,100}", value):
                item[attribute] = value
        if entry.get("url"):
            item["url"] = source_reference(entry["url"])
        if generated_at := stamp(entry.get("generated_at")):
            item["generated_at"] = generated_at
        entries.append(item)
    if not entries:
        return None
    observed = stamp((manifest.get("ingestion_media") or {}).get("observed_at")) or stamp(manifest.get("updated_at"))
    return Evidence.capture(
        source_identity=f"instagram:media-prior-manifest:{snapshot.shortcode}",
        captured_at=captured_at or datetime.now(UTC),
        parser_version=PRIOR_MANIFEST_VERSION,
        payload={
            "shortcode": snapshot.shortcode,
            "post_id": snapshot.post_id,
            "canonical_post_id": snapshot.canonical_post_id,
            "media": entries,
            **({"updated_at": stamp(manifest.get("updated_at"))} if stamp(manifest.get("updated_at")) else {}),
            **({"captured_at": observed} if observed else {}),
        },
    )


def snapshot_evidence(snapshot: MediaSnapshot) -> Evidence:
    """Construct the ordinary source seal with its original clocks and payload."""
    target_key = snapshot.shortcode
    return snapshot.unavailable or Evidence.capture(
        source_identity=f"instagram:media:{target_key}",
        captured_at=snapshot.observed_at,
        parser_version=MEDIA_VERSION,
        payload={
            "shortcode": target_key,
            "inventory_fingerprint": snapshot.inventory_fingerprint,
            "result_inventory_fingerprint": snapshot.result_inventory_fingerprint,
            "comment_settlement": dict(snapshot.comment_settlement),
            "ingestion_media": {"version": MEDIA_VERSION, "observed_at": utc_timestamp(snapshot.observed_at)},
            "media_urls": list(snapshot.media_urls),
            "thumbnail_url": snapshot.thumbnail_url,
            "post_id": snapshot.post_id,
            "canonical_post_id": snapshot.canonical_post_id,
            "show_id": snapshot.show_id,
            "season_number": snapshot.season_number,
            "week_index": snapshot.week_index,
            "expected_media_count": snapshot.expected_media_count,
            "comments_complete": snapshot.comments_complete,
            "avatars_complete": snapshot.avatars_complete,
            "unavailable_avatars": list(snapshot.unavailable_avatars),
            "include_comment_avatars": snapshot.include_comment_avatars,
            "media": [{"id": role, "storage_source_key": key} for role, key in snapshot.storage_keys().items()],
            "comments": [
                {
                    "comment_id": c.comment_id,
                    "row_id": c.row_id,
                    "media_urls": list(c.media_urls),
                    "storage_source_key": c.storage_source_key,
                    "observed_at": utc_timestamp(c.observed_at or snapshot.observed_at),
                    **({"attachment_refresh": c.attachment_refresh.to_payload()} if c.attachment_refresh else {}),
                    **({"attachment_version": c.attachment_version} if c.attachment_version else {}),
                }
                for c in snapshot.comments
            ],
            "avatars": [
                {
                    "username": a.username,
                    "profile_pic_url": a.source_url,
                    "role": a.role,
                    "comment_id": a.comment_id,
                    "observed_at": utc_timestamp(a.observed_at or snapshot.observed_at),
                    "hosted_url": a.cached_hosted_url,
                    "content_hash": a.cached_sha256,
                }
                for a in snapshot.avatars
            ],
        },
    )


class MediaActivity:
    def __init__(
        self, store: Store, *, storage: MediaStorage, writer: Callable = write_media, clock: Callable = time.monotonic
    ):
        self.store, self.storage, self.writer = store, storage, writer
        self.clock = clock

    def execute(
        self,
        *,
        run_id: str,
        account: str,
        generation: int,
        target_key: str,
        owner_token: str,
        load: Callable[..., MediaSnapshot],
        stage: Stage = Stage.MEDIA,
    ) -> dict[str, Any]:
        if stage != Stage.MEDIA:
            raise ValueError("Media activity only accepts the media stage")
        owned = {
            "run_id": run_id,
            "account": account,
            "generation": generation,
            "target_key": target_key,
            "owner_token": owner_token,
            "stage": stage,
        }
        acquired = self.store.acquire(**owned)
        from trr_backend.ingestion.reply_context import source_deadline

        remaining = (acquired["owner_expires_at"] - datetime.now(UTC)).total_seconds() - 2
        check = MediaOwnershipCheck(
            lambda: self.store.check_owner(**owned),
            deadline=source_deadline(self.clock() + min(590, remaining)),
            clock=self.clock,
        )

        def request():
            check.check_deadline()
            self.store.check_owner(**owned, record_request=True)
            check.check_deadline()

        check()
        snapshot = load(check_cancelled=check, on_request=request)
        check()
        snapshot.validate()
        if snapshot.shortcode != target_key:
            raise MediaIncomplete("media_target_identity_mismatch")
        prior = prior_manifest_evidence(snapshot)
        if prior is not None:
            with self.store.transaction() as conn, conn.cursor() as cur:
                cur.execute(
                    """select evidence_id from social.instagram_ingestion_evidence
                    where run_id=%s and stage='media' and target_key=%s and parser_version=%s
                    and content_hash=%s and payload is not null limit 1""",
                    (run_id, target_key, PRIOR_MANIFEST_VERSION, prior.content_hash),
                )
                retained = cur.fetchone()
            if not retained:
                self.store.retain_evidence(**owned, evidence=prior)
            check()
        evidence = snapshot_evidence(snapshot)
        evidence_id = self.store.retain_evidence(**owned, evidence=evidence)
        bind_snapshot = getattr(getattr(self.storage, "refresh", None), "bind_snapshot", None)
        if callable(bind_snapshot):
            bind_snapshot(snapshot)
        result = mirror_snapshot(snapshot, self.storage, check=check, request=request)
        check()
        if result.outcome == Outcome.COMMITTED:
            from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, serialize_media_result

            complete = Evidence.capture(
                source_identity=f"instagram:media:{target_key}",
                captured_at=snapshot.observed_at,
                parser_version=MEDIA_RESULT_REPLAY_VERSION,
                payload=serialize_media_result(result),
            )
            evidence_id = self.store.retain_evidence(**owned, evidence=complete)
        return self.store.commit_target(
            **owned,
            observed_at=snapshot.observed_at,
            evidence_id=evidence_id,
            outcome=result.outcome,
            write_canonical=lambda conn: self.writer(
                conn,
                run_id=run_id,
                account=account,
                target_key=target_key,
                observed_at=snapshot.observed_at,
                result=result,
            ),
        )
