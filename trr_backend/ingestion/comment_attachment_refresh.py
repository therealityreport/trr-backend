"""Pure, source-bound attachment proofs; never a complete comment observation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit
from uuid import UUID

from trr_backend.ingestion.domain import fingerprint, utc_timestamp
from trr_backend.ingestion.evidence import Evidence, sanitize_payload
from trr_backend.socials.media_url_safety import (
    MediaUrlSafetyPolicy,
    allowed_hosts_for_platform,
    validate_media_url,
)

ATTACHMENT_PARSER_VERSION = "instagram-comment-attachment-v1"
_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
_HASH = re.compile(r"[a-f0-9]{64}")
_POLICY = MediaUrlSafetyPolicy(allowed_hosts_for_platform("instagram"), allow_test_hosts=False, resolve_dns=False)
_PAYLOAD_KEYS = frozenset(
    "version row_id comment_id post_id shortcode parent_comment_id native_gif_id captured_at baseline_sha256 "
    "source_url giphy_media_info source_identity evidence_sha256".split()
)


class AttachmentRefreshError(ValueError):
    """A native row or retained proof does not establish the exact attachment."""

    def __init__(self):
        super().__init__("invalid_comment_attachment_refresh")


def _invalid():
    return AttachmentRefreshError()


def _identity(value: Any) -> str:
    if type(value) is int and value >= 0:
        value = str(value)
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,64}", value):
        raise _invalid()
    return value


def _uuid(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if not isinstance(value, str):
        raise _invalid()
    try:
        if str(UUID(value)) != value:
            raise _invalid()
    except ValueError:
        raise _invalid() from None
    return value


def _captured(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise _invalid()
    return datetime.fromisoformat(utc_timestamp(value))


def _public_reference(value: str) -> str:
    try:
        parts = urlsplit(value)
        if parts.scheme not in {"https", "http"} or not parts.hostname or parts.username or parts.password:
            raise _invalid()
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    except ValueError:
        raise _invalid() from None


def _strip_urls(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _strip_urls(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_strip_urls(item) for item in value]
    if isinstance(value, str) and value.startswith(("https://", "http://")):
        return _public_reference(value)
    if isinstance(value, datetime):
        return utc_timestamp(_captured(value))
    if isinstance(value, UUID):
        return str(value)
    if value is None or type(value) in {str, bool, int, float}:
        return value
    raise _invalid()


def _gif_url(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 8192
        or "\\" in value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise _invalid()
    try:
        parts = urlsplit(value)
        path = unquote(parts.path).lower()
        if (
            parts.scheme != "https"
            or parts.fragment
            or parts.port not in {None, 443}
            or not path.endswith(".gif")
            or path.endswith("/xg_5yovlvjp.gif")
        ):
            raise _invalid()
        if validate_media_url(value, policy=_POLICY) != value:
            raise _invalid()
    except ValueError:
        raise _invalid() from None
    return value


def _selected_url(info: Mapping[str, Any]) -> str:
    images = info.get("first_party_cdn_proxied_images")
    if images is None:
        images = info.get("images")
    fixed = images.get("fixed_height") if isinstance(images, Mapping) else None
    return _gif_url(fixed.get("url") if isinstance(fixed, Mapping) else None)


def _retained_giphy(info: Mapping[str, Any]) -> dict[str, Any]:
    # The shared allowlist removes noncontent fields; strip every delivery query
    # first, including parameters the ordinary CDN evidence policy may retain.
    return sanitize_payload({"giphy_media_info": _strip_urls(info)})["giphy_media_info"]


def _row_parent(row: Mapping[str, Any]) -> str | None:
    raw = row.get("raw_data") or {}
    if not isinstance(raw, Mapping):
        raise _invalid()
    parents = [
        _identity(value)
        for value in (row.get("parent_comment_external_id"), raw.get("parent_comment_id"))
        if value is not None
    ]
    if len(set(parents)) > 1:
        raise _invalid()
    parent = parents[0] if parents else None
    if row.get("is_reply") is True or row.get("parent_comment_id") is not None:
        if parent is None:
            raise _invalid()
    elif parent is not None:
        raise _invalid()
    return parent


def attachment_baseline(row: Mapping[str, Any]) -> str:
    """Fingerprint the row's attachment and protected identity/observation state."""
    if not isinstance(row, Mapping):
        raise _invalid()
    _uuid(row.get("id"))
    _uuid(row.get("post_id"))
    _identity(row.get("comment_id"))
    raw = row.get("raw_data") or {}
    if not isinstance(raw, Mapping):
        raise _invalid()
    keys = (
        "id post_id comment_id user_id username text source_account parent_comment_id parent_comment_external_id "
        "is_reply is_missing deleted_at scraped_at media_urls hosted_media_urls media_mirror_status media_mirror_error"
    ).split()
    value = {key: row.get(key) for key in keys}
    value["native_attachment"] = {
        key: raw.get(key)
        for key in (
            "giphy_media_info",
            "giphyMediaInfo",
            "parent_comment_id",
            "ingestion_comment_attachment",
            "ingestion_attachment",
        )
    }
    try:
        return fingerprint(_strip_urls(value))
    except (TypeError, ValueError, RecursionError):
        raise _invalid() from None


@dataclass(frozen=True)
class AttachmentRefresh:
    row_id: str
    comment_id: str
    post_id: str
    shortcode: str
    parent_comment_id: str | None
    native_gif_id: str
    captured_at: datetime
    baseline_sha256: str
    source_url: str = field(repr=False)
    giphy_media_info: dict[str, Any]
    source_identity: str
    evidence_sha256: str

    def _payload(self) -> dict[str, Any]:
        return {
            "version": ATTACHMENT_PARSER_VERSION,
            "row_id": self.row_id,
            "comment_id": self.comment_id,
            "post_id": self.post_id,
            "shortcode": self.shortcode,
            "parent_comment_id": self.parent_comment_id,
            "native_gif_id": self.native_gif_id,
            "captured_at": utc_timestamp(self.captured_at),
            "baseline_sha256": self.baseline_sha256,
            "source_url": _public_reference(self.source_url),
            "giphy_media_info": _strip_urls(self.giphy_media_info),
            "source_identity": self.source_identity,
        }

    def _evidence(self) -> Evidence:
        payload = self._payload()
        result = Evidence.capture(
            source_identity=self.source_identity,
            captured_at=self.captured_at,
            parser_version=ATTACHMENT_PARSER_VERSION,
            payload=payload,
        )
        if result.read(pinned=True) != payload:
            raise _invalid()
        return result

    def validate(self) -> None:
        """Validate bindings and the reproducible retained hash, without I/O."""
        if not isinstance(self.row_id, str) or not isinstance(self.post_id, str):
            raise _invalid()
        _uuid(self.row_id)
        _uuid(self.post_id)
        if not isinstance(self.comment_id, str) or not isinstance(self.native_gif_id, str):
            raise _invalid()
        _identity(self.comment_id)
        _identity(self.native_gif_id)
        if not isinstance(self.shortcode, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", self.shortcode):
            raise _invalid()
        if self.parent_comment_id is not None:
            if not isinstance(self.parent_comment_id, str):
                raise _invalid()
            _identity(self.parent_comment_id)
            if self.parent_comment_id == self.comment_id:
                raise _invalid()
        _captured(self.captured_at)
        if (
            not isinstance(self.baseline_sha256, str)
            or not _HASH.fullmatch(self.baseline_sha256)
            or not isinstance(self.evidence_sha256, str)
            or not _HASH.fullmatch(self.evidence_sha256)
            or self.source_identity != f"instagram:comment-attachment:{self.shortcode}:{self.comment_id}"
            or not isinstance(self.giphy_media_info, dict)
            or self.giphy_media_info != _retained_giphy(self.giphy_media_info)
            or self.giphy_media_info.get("id") != self.native_gif_id
            or _public_reference(_gif_url(self.source_url)) != _selected_url(self.giphy_media_info)
            or self._evidence().content_hash != self.evidence_sha256
        ):
            raise _invalid()

    def evidence(self) -> Evidence:
        self.validate()
        return self._evidence()

    def to_payload(self) -> dict[str, Any]:
        self.validate()
        return {**self._payload(), "evidence_sha256": self.evidence_sha256}

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> AttachmentRefresh:
        if not isinstance(payload, Mapping) or set(payload) != _PAYLOAD_KEYS:
            raise _invalid()
        if payload["version"] != ATTACHMENT_PARSER_VERSION:
            raise _invalid()
        try:
            stamp = payload["captured_at"]
            if not isinstance(stamp, str):
                raise _invalid()
            captured = _captured(datetime.fromisoformat(stamp))
            if utc_timestamp(captured) != stamp:
                raise _invalid()
            data = {key: value for key, value in payload.items() if key not in {"version", "captured_at"}}
            if not isinstance(data["source_url"], str) or _public_reference(data["source_url"]) != data["source_url"]:
                raise _invalid()
            result = cls(**dict(deepcopy(data), captured_at=captured))
            result.validate()
            return result
        except (TypeError, ValueError, RecursionError):
            raise _invalid() from None


def validate_attachment_order(row: Mapping[str, Any], captured_at: datetime) -> None:
    """Reject stale observations at parse, snapshot and locked-write boundaries.

    The caller handles an exact previously committed proof idempotently before
    calling this guard; equal attachment capture times cannot grant a rewrite.
    """
    try:
        if not isinstance(row, Mapping):
            raise _invalid()
        captured = _captured(captured_at)
        observed = row.get("scraped_at")
        if isinstance(observed, str):
            observed = datetime.fromisoformat(observed)
        if captured < _captured(observed):
            raise _invalid()
        raw = row.get("raw_data") or {}
        if not isinstance(raw, Mapping):
            raise _invalid()
        marker = raw.get("ingestion_attachment")
        if marker is not None:
            previous = AttachmentRefresh.from_payload(marker)
            if (
                previous.row_id != _uuid(row.get("id"))
                or previous.post_id != _uuid(row.get("post_id"))
                or previous.comment_id != _identity(row.get("comment_id"))
                or captured <= previous.captured_at
            ):
                raise _invalid()
    except (TypeError, ValueError, RecursionError):
        raise _invalid() from None


def parse_targeted_rest_attachment(
    payload: Mapping[str, Any],
    *,
    row: Mapping[str, Any],
    shortcode: str,
    expected_gif_id: str,
    captured_at: datetime,
) -> AttachmentRefresh:
    """Bind one native REST attachment without inferring inventory completion."""
    from trr_backend.ingestion.comments import comment_page_failure
    from trr_backend.socials.instagram.graphql_response import _primitive_failure

    try:
        captured_at = _captured(captured_at)
        validate_attachment_order(row, captured_at)
        baseline = attachment_baseline(row)
        comment_id = _identity(row["comment_id"])
        if row.get("is_missing") is not False or row.get("deleted_at") is not None:
            raise _invalid()
        if not isinstance(shortcode, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", shortcode):
            raise _invalid()
        if not isinstance(expected_gif_id, str):
            raise _invalid()
        expected_gif_id = _identity(expected_gif_id)
        media_id = 0
        for char in shortcode:
            media_id = media_id * 64 + _ALPHABET.index(char)
        if (
            not isinstance(payload, Mapping)
            or payload.get("status") != "ok"
            or _primitive_failure(payload)
            or comment_page_failure(payload)
            or not isinstance(payload.get("comments"), list)
            or any(not isinstance(node, Mapping) for node in payload["comments"])
        ):
            raise _invalid()
        matches = []
        for node in payload["comments"]:
            ids = [_identity(node[key]) for key in ("pk", "id", "comment_id") if node.get(key) is not None]
            if comment_id in ids:
                if set(ids) != {comment_id}:
                    raise _invalid()
                matches.append(node)
        if len(matches) != 1:
            raise _invalid()
        node = matches[0]
        # Native comment lifecycle status is not the response-envelope status.
        # "Active" is the observed healthy REST row state; it must not hide any
        # explicit request failure markers carried alongside the row.
        if node.get("status") not in {None, "Active"}:
            raise _invalid()
        node_failures = {key: value for key, value in node.items() if key != "status"}
        if _primitive_failure(node_failures) or node.get("errors"):
            raise _invalid()
        if _identity(node.get("pk")) != comment_id or _identity(node.get("media_id")) != str(media_id):
            raise _invalid()
        if payload.get("media_id") is not None and _identity(payload["media_id"]) != str(media_id):
            raise _invalid()
        parent = _row_parent(row)
        native_parent = node.get("parent_comment_id")
        if (None if native_parent is None else _identity(native_parent)) != parent:
            raise _invalid()
        user = node.get("user") or {}
        if not isinstance(user, Mapping):
            raise _invalid()
        if row.get("user_id") is not None:
            author_ids = [
                _identity(value) for value in (node.get("user_id"), user.get("pk"), user.get("id")) if value is not None
            ]
            if not author_ids or set(author_ids) != {_identity(row["user_id"])}:
                raise _invalid()
        if user.get("username") is not None and row.get("username") is not None:
            if not isinstance(user["username"], str) or user["username"].lower() != str(row["username"]).lower():
                raise _invalid()
        info = node.get("giphy_media_info")
        if not isinstance(info, Mapping) or _identity(info.get("id")) != expected_gif_id:
            raise _invalid()
        url = _selected_url(info)
        retained = _retained_giphy(info)
        retained["id"] = expected_gif_id
        result = AttachmentRefresh(
            row_id=_uuid(row["id"]),
            comment_id=comment_id,
            post_id=_uuid(row["post_id"]),
            shortcode=shortcode,
            parent_comment_id=parent,
            native_gif_id=expected_gif_id,
            captured_at=captured_at,
            baseline_sha256=baseline,
            source_url=url,
            giphy_media_info=retained,
            source_identity=f"instagram:comment-attachment:{shortcode}:{comment_id}",
            evidence_sha256="",
        )
        result = replace(result, evidence_sha256=result._evidence().content_hash)
        result.validate()
        return result
    except (KeyError, TypeError, ValueError, RecursionError):
        raise _invalid() from None
